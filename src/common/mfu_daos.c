#define _GNU_SOURCE

#include "mfu_daos.h"

#include "mpi.h"
#include "mfu_errors.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <daos_fs.h>
#include <daos_uns.h>
#include <gurt/common.h>
#include <gurt/hash.h>

/*
 * Private definitions.
 */

static bool daos_uuid_valid(const uuid_t uuid)
{
    return uuid && !uuid_is_null(uuid);
}

/* Verify DAOS arguments are valid */
static int daos_check_args(
    int rank,
    char** argpaths,
    daos_args_t* da,
    int* flag_daos_args)
{
    char* src_path = argpaths[0];
    char* dst_path = argpaths[1];

    bool have_src_path  = src_path != NULL;
    bool have_dst_path  = dst_path != NULL;
    bool have_src_pool  = daos_uuid_valid(da->src_pool_uuid);
    bool have_src_cont  = daos_uuid_valid(da->src_cont_uuid);
    bool have_dst_pool  = daos_uuid_valid(da->dst_pool_uuid);
    bool have_dst_cont  = daos_uuid_valid(da->dst_cont_uuid);
    bool have_prefix    = da->dfs_prefix != NULL;

    /* Determine whether any DAOS arguments are supplied. 
     * If not, then there is nothing to check. */
    *flag_daos_args = 0;
    if (have_src_pool || have_src_cont || have_dst_pool || have_dst_cont
            || have_prefix) {
        *flag_daos_args = 1;
    }
    if ((have_src_path && (strncmp(src_path, "daos:", 5) == 0)) ||
            (have_dst_path && (strncmp(dst_path, "daos:", 5) == 0))) {
        *flag_daos_args = 1;
    }
    if (*flag_daos_args == 0) {
        return 0;
    }
    
    /* Determine whether the source and destination
     * use the same pool and container */
    bool same_pool = false;
    bool same_cont = false;
    if (uuid_compare(da->src_pool_uuid, da->dst_pool_uuid) == 0) {
        same_pool = true;
        if (uuid_compare(da->src_cont_uuid, da->dst_cont_uuid) == 0) {
            same_cont = true;
        }
    }

    /* Determine whether the source and destination paths are the same.
     * Assume NULL == NULL. */
    bool same_path = false;
    if (have_src_path && have_dst_path && strcmp(src_path, dst_path) == 0) {
        same_path = true;
    } else if (!have_src_path && !have_dst_path) {
        same_path = true;
    }

    int rc = 0;

    /* Make sure the source and destination are different */
    if (same_cont && same_path) {
        if (rank == 0) {
             MFU_LOG(MFU_LOG_ERR, "DAOS source is DAOS destination");
        }
        rc = 1;
    }

    return rc;
}

/* Checks if the prefix is valid.
 * If valid, returns matching string into suffix */
static bool daos_check_prefix(
    char* path,
    const char* dfs_prefix,
    char** suffix)
{
    bool is_prefix = false;
    int prefix_len = strlen(dfs_prefix);
    int path_len = strlen(path);

    /* ignore trailing '/' on the prefix */
    if (dfs_prefix[prefix_len-1] == '/') {
        prefix_len--;
    }

    /* figure out if dfs_prefix is a prefix of the file path */
    if (strncmp(path, dfs_prefix, prefix_len) == 0) {
        /* if equal, assume root */
        if (path_len == prefix_len) {
            *suffix = strdup("/");
            is_prefix = true;
        }
        /* if path is longer, it must start with '/' */
        else if (path_len > prefix_len && path[prefix_len] == '/') {
            *suffix = strdup(path + prefix_len);
            is_prefix = true;
        }
    }
    return is_prefix;
}

/*
 * Parse a path of the format:
 * daos://<pool>/<cont>/<path> | <UNS path> | <POSIX path>
 * Modifies "path" to be the relative container path, defaulting to "/".
 * Returns 0 IFF a daos path was successfully parsed.
 * Returns 1 if a daos path was not parsed.
 * Returns -1 for actual errors.
 */
static int daos_parse_path(
    char* path,
    size_t path_len,
    uuid_t* p_uuid,
    uuid_t* c_uuid)
{
    struct duns_attr_t  dattr = {0};
    int                 rc;

    rc = duns_resolve_path(path, &dattr);
    if (rc == 0) {
        /* daos:// or UNS path */
        uuid_copy(*p_uuid, dattr.da_puuid);
        uuid_copy(*c_uuid, dattr.da_cuuid);
        if (dattr.da_rel_path == NULL) {
            strncpy(path, "/", path_len);
        } else {
            strncpy(path, dattr.da_rel_path, path_len);
        }
    } else if (strncmp(path, "daos:", 5) == 0) {
        /* Actual error, since we expect a daos path */
        rc = -1;
    } else {
        /* We didn't parse a daos path,
         * but we weren't necessarily looking for one */
        rc = 1;
    }

    mfu_free(&dattr.da_rel_path);

    return rc;
}

/* Checks for UNS paths and sets
 * paths and DAOS args accordingly */
static int daos_set_paths(
  int rank,
  char** argpaths,
  daos_args_t* da)
{
    int     rc = 0;
    bool    prefix_on_src = false;
    bool    prefix_on_dst = false;

    /* find out if a dfs_prefix is being used,
     * if so, then that means that the container
     * is not being copied from the root of the
     * UNS path  */
    if (da->dfs_prefix != NULL) {
        uuid_t  prefix_p_uuid;
        uuid_t  prefix_c_uuid;
        int     prefix_rc;

        size_t prefix_len = strlen(da->dfs_prefix);
        char* prefix_path = strndup(da->dfs_prefix, prefix_len);
        if (prefix_path == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Unable to allocate space for DAOS prefix.");
            rc = 1;
            goto out;
        }

        uuid_clear(prefix_p_uuid);
        uuid_clear(prefix_c_uuid);

        /* Get the pool/container uuids from the prefix */
        prefix_rc = daos_parse_path(prefix_path, prefix_len, &prefix_p_uuid, &prefix_c_uuid);
        if (prefix_rc != 0 || prefix_p_uuid == NULL || prefix_c_uuid == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Failed to resolve DAOS Prefix UNS path");
            mfu_free(&prefix_path);
            rc = 1;
            goto out;
        }

        /* In case the user tries to give a sub path in the UNS path */
        if (strcmp(prefix_path, "/") != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS prefix must be a UNS path");
            mfu_free(&prefix_path);
            rc = 1;
            goto out;
        }

        /* Check if the prefix matches the source */
        prefix_on_src = daos_check_prefix(argpaths[0], da->dfs_prefix, &da->src_path);
        if (prefix_on_src) {
            uuid_copy(da->src_pool_uuid, prefix_p_uuid);
            uuid_copy(da->src_cont_uuid, prefix_c_uuid);
            argpaths[0] = da->src_path;
        }

        /* Check if the prefix matches the destination */
        prefix_on_dst = daos_check_prefix(argpaths[1], da->dfs_prefix, &da->dst_path);
        if (prefix_on_dst) {
            uuid_copy(da->dst_pool_uuid, prefix_p_uuid);
            uuid_copy(da->dst_cont_uuid, prefix_c_uuid);
            argpaths[1] = da->dst_path;
        }

        if (!prefix_on_src && !prefix_on_dst) {
            MFU_LOG(MFU_LOG_ERR, "DAOS prefix does not match source or destination");
            mfu_free(&prefix_path);
            rc = 1;
            goto out;
        }
    }

    /*
     * For the source and destination paths,
     * if they are not using a prefix,
     * then just directly try to parse a DAOS path.
     */
    if (!prefix_on_src) {
        size_t src_len = strlen(argpaths[0]);
        char* src_path = strndup(argpaths[0], src_len);
        if (src_path == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Unable to allocate space for source path.");
            rc = 1;
            goto out;
        }
        int src_rc = daos_parse_path(src_path, src_len, &da->src_pool_uuid, &da->src_cont_uuid);
        if (src_rc == 0) {
            argpaths[0] = da->src_path = strdup(src_path);
            mfu_free(&src_path);
        } else if (src_rc == -1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to parse DAOS source path: daos://<pool>/<cont>[/<path>]");
            mfu_free(&src_path);
            rc = 1;
            goto out;
        }
    }

    if (!prefix_on_dst) {
        size_t dst_len = strlen(argpaths[1]);
        char* dst_path = strndup(argpaths[1], dst_len);
        int dst_rc = daos_parse_path(dst_path, dst_len, &da->dst_pool_uuid, &da->dst_cont_uuid);
        if (dst_rc == 0) {
            argpaths[1] = da->dst_path = strdup(dst_path);
            mfu_free(&dst_path);
        } else if (dst_rc == -1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to parse DAOS destination path: daos://<pool>/<cont>[/<path>]");
            mfu_free(&dst_path);
            rc = 1;
            goto out;
        }
    }

out:
    return rc;
}

static int daos_get_cont_type(
    daos_handle_t coh,
    enum daos_cont_props* type)
{
    daos_prop_t*            prop = daos_prop_alloc(1);
    struct daos_prop_entry  entry;
    int                     rc;

    if (prop == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to allocate prop (%d)", rc);
        return 1;
    }

    prop->dpp_entries[0].dpe_type = DAOS_PROP_CO_LAYOUT_TYPE;

    rc = daos_cont_query(coh, NULL, prop, NULL);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "daos_cont_query() failed (%d)", rc);
        daos_prop_free(prop);
        return 1;
    }

    *type = prop->dpp_entries[0].dpe_val;
    daos_prop_free(prop);
    return 0;
}

/*
 * Try to set the file type based on the container type,
 * using api as a guide.
 */
static int daos_set_api_cont_type(
    mfu_file_t* mfu_file,
    daos_handle_t coh,
    daos_api_t api)
{
    /* If explicitly using DAOS, just set the type to DAOS */
    if (api == DAOS_API_DAOS) {
        mfu_file->type = DAOS;
        return 0;
    }

    /* Otherwise, query the container type, and use DFS for POSIX containers. */
    enum daos_cont_props cont_type;

    int rc = daos_get_cont_type(coh, &cont_type);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "Failed to get DAOS container type.");
        return rc;
    }

    if (cont_type == DAOS_PROP_CO_LAYOUT_POSIX) {
        mfu_file->type = DFS;
    } else {
        mfu_file->type = DAOS;
    }

    /* If explicitly using DFS, the container types must be POSIX */
    if (api == DAOS_API_DFS && mfu_file->type != DFS) {
        MFU_LOG(MFU_LOG_ERR, "Cannot use non-POSIX container with DFS API.");
        return 1;
    }

    return 0;
}

/*
 * Set the mfu_file types to either DAOS or DFS,
 * and make sure they are compatible.
 */
static int daos_set_api(
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file,
    daos_args_t* da,
    char** argpaths)
{
    /* Check whether we have pool/cont uuids */
    bool have_src_pool  = daos_uuid_valid(da->src_pool_uuid);
    bool have_src_cont  = daos_uuid_valid(da->src_cont_uuid);
    bool have_dst_pool  = daos_uuid_valid(da->dst_pool_uuid);
    bool have_dst_cont  = daos_uuid_valid(da->dst_cont_uuid);

    int rc;

    /* If the user explicitly wants to use the DAOS API,
     * then set both types to DAOS.
     * Otherwise, query the container type and set to DFS
     * for POSIX containers. */
    if (have_src_pool && have_src_cont) {
        rc = daos_set_api_cont_type(mfu_src_file, da->src_coh, da->api);
        if (rc) {
            return rc;
        }
    }
    if (have_dst_pool && have_dst_cont) {
        rc = daos_set_api_cont_type(mfu_dst_file, da->dst_coh, da->api);
        if (rc) {
            return rc;
        }
    }

    /* Check whether we have source and destination paths */
    char* src_path = argpaths[0];
    char* dst_path = argpaths[1];
    bool have_src_path = src_path != NULL;
    bool have_dst_path = dst_path != NULL;
    bool src_path_is_root = have_src_path && (strcmp(src_path, "/") == 0);
    bool dst_path_is_root = have_dst_path && (strcmp(dst_path, "/") == 0);

    /* If either type is DAOS:
     * Both paths must be root.
     * The other must be DAOS or DFS.
     * The other will be set to DAOS, for obj-level copy. */
    if (mfu_src_file->type == DAOS) {
        if (!src_path_is_root || !dst_path_is_root) {
            MFU_LOG(MFU_LOG_ERR, "Cannot use path with non-POSIX container.");
            return 1;
        }
        if (mfu_dst_file->type != DAOS && mfu_dst_file->type != DFS) {
            MFU_LOG(MFU_LOG_ERR, "Cannot copy non-POSIX container outside DAOS.");
            return 1;
        }
        mfu_dst_file->type = DAOS;
    }
    if (mfu_dst_file->type == DAOS) {
        if (!dst_path_is_root || !src_path_is_root) {
            MFU_LOG(MFU_LOG_ERR, "Cannot use path with non-POSIX container.");
            return 1;
        }
        if (mfu_src_file->type != DAOS && mfu_src_file->type != DFS) {
            MFU_LOG(MFU_LOG_ERR, "Cannot copy non-POSIX container outside DAOS.");
            return 1;
        }
        mfu_src_file->type = DAOS;
    }

    return 0;
}

/* return 1 if any process has a local error
 * return 0 otherwise
 * ignore if no daos args supplied */
static int daos_any_error(int rank, bool local_daos_error, int flag_daos_args)
{
    bool no_error = !local_daos_error;
    if (flag_daos_args == 0) {
        no_error = true;
    }

    if (! mfu_alltrue(no_error, MPI_COMM_WORLD)) {
        if (rank == 0) {
           MFU_LOG(MFU_LOG_ERR, "Detected one or more DAOS errors: "
                   MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
        }
        return 1;
    }

    return 0;
}

/* Distribute process 0's pool or container handle to others. */
static void daos_bcast_handle(
  int rank,              /* root rank for broadcast */
  daos_handle_t* handle, /* handle value to be broadcasted */
  daos_handle_t* poh,    /* daos pool for global2local conversion of container handle */
  enum handleType type)  /* handle type: POOL_HANDLE or CONT_HANDLE */
{
    int rc;

    d_iov_t global;
    global.iov_buf     = NULL;
    global.iov_buf_len = 0;
    global.iov_len     = 0;

    /* Get the global handle size. */
    if (rank == 0) {
        if (type == POOL_HANDLE) {
            rc = daos_pool_local2global(*handle, &global);
        } else {
            rc = daos_cont_local2global(*handle, &global);
        }
        if (rc != 0) {
            MFU_ABORT(-1, "Failed to get global handle size");
        }
    }

    /* broadcast size of global handle */
    MPI_Bcast(&global.iov_buf_len, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* allocate memory to hold global handle value */
    global.iov_len = global.iov_buf_len;
    global.iov_buf = MFU_MALLOC(global.iov_buf_len);

    /* convert from local handle to global handle */
    if (rank == 0) {
       if (type == POOL_HANDLE) {
           rc = daos_pool_local2global(*handle, &global);
       } else {
           rc = daos_cont_local2global(*handle, &global);
       }
       if (rc != 0) {
           MFU_ABORT(-1, "Failed to create global handle");
       }
    }

    /* broadcast global handle value */
    MPI_Bcast(global.iov_buf, global.iov_buf_len, MPI_BYTE, 0, MPI_COMM_WORLD);

    /* convert global handle to local value */
    if (rank != 0) {
        if (type == POOL_HANDLE) {
            rc = daos_pool_global2local(global, handle);
        } else {
            rc = daos_cont_global2local(*poh, global, handle);
        }
        if (rc != 0) {
            MFU_ABORT(-1, "Failed to get local handle");
        }
    }

    /* free temporary buffer used to hold global handle value */
    mfu_free(&global.iov_buf);
}

/* connect to DAOS pool, and then open container */
static int daos_connect(
  int rank,
  uuid_t pool_uuid,
  uuid_t cont_uuid,
  daos_handle_t* poh,
  daos_handle_t* coh,
  bool connect_pool,
  bool create_cont)
{
    /* assume failure until otherwise */
    int valid = 0;
    int rc;

    /* have rank 0 connect to the pool and container,
     * we'll then broadcast the handle ids from rank 0 to everyone else */
    if (rank == 0) {
        /* Connect to DAOS pool */
        if (connect_pool) {
            daos_pool_info_t pool_info = {0};
#if DAOS_API_VERSION_MAJOR < 1
            rc = daos_pool_connect(pool_uuid, NULL, NULL, DAOS_PC_RW,
                    poh, &pool_info, NULL);
#else
            rc = daos_pool_connect(pool_uuid, NULL, DAOS_PC_RW,
                    poh, &pool_info, NULL);
#endif
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to connect to pool");
                goto bcast;
            }
        }

        /* Try to open the container
         * If NOEXIST we create it */
        daos_cont_info_t co_info = {0};
        rc = daos_cont_open(*poh, cont_uuid, DAOS_COO_RW, coh, &co_info, NULL);
        if (rc != 0) {
            if (!create_cont) {
                MFU_LOG(MFU_LOG_ERR, "Failed to open DFS container");
                goto bcast;
            }

            rc = dfs_cont_create(*poh, cont_uuid, NULL, NULL, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to create DFS container");
                goto bcast;
            }

            /* try to open it again */
            rc = daos_cont_open(*poh, cont_uuid, DAOS_COO_RW, coh, &co_info, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to open DFS container");
                goto bcast;
            }
        }

        /* everything looks good so far */
        valid = 1;
    }

bcast:
    /* broadcast valid from rank 0 */
    MPI_Bcast(&valid, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* return if invalid */
    if (valid == 0) {
        return -1;
    }

    /* broadcast pool handle from rank 0
     * If connect_pool is false, then the handle was unchanged */
    if (connect_pool) {
        daos_bcast_handle(rank, poh, poh, POOL_HANDLE);
    }

    /* broadcast container handle from rank 0 */
    daos_bcast_handle(rank, coh, poh, CONT_HANDLE);

    return 0;
}

/* Mount DAOS dfs */
static int daos_mount(
  mfu_file_t* mfu_file,
  daos_handle_t* poh,
  daos_handle_t* coh)
{
    /* Mount dfs */
    int rc = dfs_mount(*poh, *coh, O_RDWR, &mfu_file->dfs);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to mount DAOS filesystem (DFS): "
                MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
        rc = -1;
    }

    return rc;
}

/* Unmount DAOS dfs.
 * Cleanup up hash */
static int daos_umount(
  mfu_file_t* mfu_file)
{
    /* Unmount dfs */
    int rc = dfs_umount(mfu_file->dfs);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to unmount DFS namespace");
        rc = -1;
    }

    /* Clean up the hash */
    if (mfu_file->dfs_hash != NULL) {
        d_hash_table_destroy(mfu_file->dfs_hash, true);
    }

    return rc;
}

/* 
 * Public definitions.
 */

daos_args_t* daos_args_new(void)
{
    daos_args_t* da = (daos_args_t*) MFU_MALLOC(sizeof(daos_args_t));

    da->src_poh    = DAOS_HDL_INVAL;
    da->dst_poh    = DAOS_HDL_INVAL;
    da->src_coh    = DAOS_HDL_INVAL;
    da->dst_coh    = DAOS_HDL_INVAL;
    da->dfs_prefix = NULL;
    da->src_path   = NULL;
    da->dst_path   = NULL;

    /* initalize value of DAOS UUID's to NULL with uuid_clear */
    uuid_clear(da->src_pool_uuid);
    uuid_clear(da->dst_pool_uuid);
    uuid_clear(da->src_cont_uuid);
    uuid_clear(da->dst_cont_uuid);

    /* By default, try to automatically determine the API */
    da->api = DAOS_API_AUTO;

    da->epc = 0;

    return da;
}

void daos_args_delete(daos_args_t** pda)
{
    if (pda != NULL) {
        daos_args_t* da = *pda;
        mfu_free(&da->dfs_prefix);
        mfu_free(&da->src_path);
        mfu_free(&da->dst_path);
        mfu_free(pda);
    }
}

int daos_parse_api_str(
    const char* api_str,
    daos_api_t* api)
{
    int rc = 0;

    if (strcasecmp(api_str, "AUTO") == 0) {
        *api = DAOS_API_AUTO;
    } else if (strcasecmp(api_str, "DFS") == 0) {
        *api = DAOS_API_DFS;
    } else if (strcasecmp(api_str, "DAOS") == 0) {
        *api = DAOS_API_DAOS;
    } else {
        rc = 1;
    }

    return rc;
}

int daos_parse_epc_str(
    const char* epc_str,
    daos_epoch_t* epc)
{
    *epc = strtoull(epc_str, NULL, 10);
    if (*epc == 0 || (*epc == ULLONG_MAX && errno != 0)) {
        return 1;
    }

    return 0;
}

int daos_setup(
    int rank,
    char** argpaths,
    daos_args_t* da,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    int tmp_rc;

    /* Each process keeps track of whether it had any DAOS errors.
     * If there weren't any daos args, then ignore daos_init errors.
     * Then, perform a reduction and exit if any process errored. */
    bool local_daos_error = false;
    int flag_daos_args = 0;

    /* For now, track the error.
     * Later, ignore if no daos args supplied */
    tmp_rc = daos_init();
    if (tmp_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialize daos");
        local_daos_error = true;
    }

    /* Do a preliminary check on the DAOS args */
    if (!local_daos_error) {
        tmp_rc = daos_check_args(rank, argpaths, da, &flag_daos_args);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid DAOS args: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS_INVAL_ARG));
            local_daos_error = true;
        }
    }

    /* Figure out if daos path is the src or dst,
     * using UNS path, then chop off UNS path
     * prefix since the path is mapped to the root
     * of the container in the DAOS DFS mount */
    if (!local_daos_error) {
        tmp_rc = daos_set_paths(rank, argpaths, da);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid DAOS args: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS_INVAL_ARG));
            local_daos_error = true;
        }
    }

    /* Re-check the required DAOS arguments (if any) */
    if (!local_daos_error) {
        tmp_rc = daos_check_args(rank, argpaths, da, &flag_daos_args);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid DAOS args: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS_INVAL_ARG));
            local_daos_error = true;
        }
    }

    /* Make sure there weren't any errors before continuing.
     * Since daos_connect has a collective broadcast.
     * we have to make sure same_pool below is valid. */
    if (daos_any_error(rank, local_daos_error, flag_daos_args)) {
        tmp_rc = daos_fini();
        return 1;
    }

    /* Check whether we have pool/cont uuids */
    bool have_src_pool  = daos_uuid_valid(da->src_pool_uuid);
    bool have_src_cont  = daos_uuid_valid(da->src_cont_uuid);
    bool have_dst_pool  = daos_uuid_valid(da->dst_pool_uuid);
    bool have_dst_cont  = daos_uuid_valid(da->dst_cont_uuid);

    /* Check if containers are in the same pool */
    bool same_pool = (uuid_compare(da->src_pool_uuid, da->dst_pool_uuid) == 0);

    /* connect to DAOS source pool if uuid is valid */
    if (!local_daos_error && have_src_pool && have_src_cont) {
        /* Open pool connection, but do not create container if non-existent */
        tmp_rc = daos_connect(rank, da->src_pool_uuid,
                da->src_cont_uuid, &da->src_poh, &da->src_coh, true, false);
        if (tmp_rc != 0) {
            /* tmp_rc from daos_connect is collective */
            local_daos_error = true;
        }
    }

    /* If the source and destination are in the same pool,
     * then open the container in that pool.
     * Otherwise, connect to the second pool and open the container */
    if (!local_daos_error && have_dst_pool && have_dst_cont) {
        if (same_pool) {
            /* Don't reconnect to pool, but do create container if non-existent */
            tmp_rc = daos_connect(rank, da->dst_pool_uuid,
                    da->dst_cont_uuid, &da->src_poh, &da->dst_coh, false, true);
        } else {
            /* Open pool connection, and create container if non-existent */
            tmp_rc = daos_connect(rank, da->dst_pool_uuid,
                    da->dst_cont_uuid, &da->dst_poh, &da->dst_coh, true, true);
        }
        if (tmp_rc != 0) {
            /* tmp_rc from daos_connect is collective */
            local_daos_error = true;
        }
    }

    /* Figure out if we should use the DFS or DAOS API */
    if (!local_daos_error) {
        tmp_rc = daos_set_api(mfu_src_file, mfu_dst_file, da, argpaths);
        if (tmp_rc != 0) {
            local_daos_error = true;
        }
    }

    /* Mount source DFS container */
    if (!local_daos_error && mfu_src_file->type == DFS) {
        tmp_rc = daos_mount(mfu_src_file, &da->src_poh, &da->src_coh);
        if (tmp_rc != 0) {
            local_daos_error = true;
        }
    }

    /* Mount destination DFS container */
    if (!local_daos_error && mfu_dst_file->type == DFS) {
        if (same_pool) {
            tmp_rc = daos_mount(mfu_dst_file, &da->src_poh, &da->dst_coh);
        } else {
            tmp_rc = daos_mount(mfu_dst_file, &da->dst_poh, &da->dst_coh);
        }
        if (tmp_rc != 0) {
            local_daos_error = true;
        }
    }

    /* Return if any process had a daos error */
    if (daos_any_error(rank, local_daos_error, flag_daos_args)) {
        tmp_rc = daos_fini();
        return 1;
    }

    /* Everything looks good so far */
    return 0;
}

int daos_cleanup(
    daos_args_t* da,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume success until failure */
    int rc = 0;
    int tmp_rc;

    bool same_pool = false;
    if (daos_uuid_valid(da->src_pool_uuid) && daos_uuid_valid(da->dst_pool_uuid)
            && uuid_compare(da->src_pool_uuid, da->dst_pool_uuid) == 0) {
        same_pool = true;
    }

    /* Unmount source DFS container */
    if (mfu_src_file->type == DFS) {
        tmp_rc = daos_umount(mfu_src_file);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            rc = 1;
        }
    }

    /* Close source container */
    if (mfu_src_file->type == DFS || mfu_src_file->type == DAOS) {
        tmp_rc = daos_cont_close(da->src_coh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
            rc = 1;
        }
    }

    /* Unmount destination DFS container */
    if (mfu_dst_file->type == DFS) {
        tmp_rc = daos_umount(mfu_dst_file);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            rc = 1;
        }
    }

    /* Close destination container */
    if (mfu_dst_file->type == DFS || mfu_dst_file->type == DAOS) {
        tmp_rc = daos_cont_close(da->dst_coh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
            rc = 1;
        }
    }

    /* Close source pool */
    if (mfu_src_file->type == DFS || mfu_src_file->type == DAOS) {
        tmp_rc = daos_pool_disconnect(da->src_poh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to disconnect from source pool");
            rc = 1;
        }
    }

    /* Close destination pool */
    if ((mfu_dst_file->type == DFS || mfu_dst_file->type == DAOS) && !same_pool) {
        tmp_rc = daos_pool_disconnect(da->dst_poh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to disconnect from destination pool");
            rc = 1;
        }
    }

    /* Finalize DAOS */
    tmp_rc = daos_fini();
    if (tmp_rc != 0) {
        rc = 1;
    }

    /* Free DAOS args */
    daos_args_delete(&da);

    return rc;
}

static int daos_copy_recx_single(daos_key_t *dkey,
                            daos_handle_t *src_oh,
                            daos_handle_t *dst_oh,
                            daos_iod_t *iod)
{
    /* if iod_type is single value just fetch iod size from source
     * and update in destination object */
    int buf_len = (int)(*iod).iod_size;
    char buf[buf_len];
    d_sg_list_t sgl;
    d_iov_t iov;
    int rc;

    /* set sgl values */
    sgl.sg_nr     = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs   = &iov;
    d_iov_set(&iov, buf, buf_len);
    rc = daos_obj_fetch(*src_oh, DAOS_TX_NONE, 0, dkey, 1, iod, &sgl, NULL, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS object fetch returned with errors: ", MFU_ERRF,
	        MFU_ERRP(-MFU_ERR_DAOS));
        return 1;
    }
    rc = daos_obj_update(*dst_oh, DAOS_TX_NONE, 0, dkey, 1, iod, &sgl, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS object update returned with errors: ", MFU_ERRF,
	        MFU_ERRP(-MFU_ERR_DAOS));
        return 1;
    }
    return rc;
}

static int daos_copy_recx_array(daos_key_t *dkey,
                           daos_key_t *akey,
                           daos_handle_t *src_oh,
                           daos_handle_t *dst_oh,
                           daos_iod_t *iod)
{
    daos_anchor_t recx_anchor = {0}; 
    int rc;
    int i;
    while (!daos_anchor_is_eof(&recx_anchor)) {
        daos_epoch_range_t	eprs[5];
        daos_recx_t		recxs[5];
        daos_size_t		size;

        /* list all recx for this dkey/akey */
        uint32_t number = 5;
        rc = daos_obj_list_recx(*src_oh, DAOS_TX_NONE, dkey, akey,
	                        &size, &number, recxs, eprs,
				&recx_anchor, true, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS daos_obj_list_recx returned with errors: ", MFU_ERRF,
	            MFU_ERRP(-MFU_ERR_DAOS));
            return 1;
        }

        /* if no recx is returned for this dkey/akey move on */
        if (number == 0) 
            continue;

        for (i = 0; i < number; i++) {
            uint64_t    buf_len = recxs[i].rx_nr;
            char        buf[buf_len];
            d_sg_list_t sgl;
            d_iov_t     iov;

            /* set iod values */
            (*iod).iod_type  = DAOS_IOD_ARRAY;
            (*iod).iod_size  = 1;
            (*iod).iod_nr    = 1;
            (*iod).iod_recxs = &recxs[i];

            /* set sgl values */
            sgl.sg_nr     = 1;
            sgl.sg_nr_out = 0;
            sgl.sg_iovs   = &iov;

            /* fetch recx values from source */
            d_iov_set(&iov, buf, buf_len);	
            rc = daos_obj_fetch(*src_oh, DAOS_TX_NONE, 0, dkey, 1, iod,
                                &sgl, NULL, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "DAOS object fetch returned with errors: ", MFU_ERRF,
	                MFU_ERRP(-MFU_ERR_DAOS));
                return 1;
            }

            /* update fetched recx values and place in destination object */
            rc = daos_obj_update(*dst_oh, DAOS_TX_NONE, 0, dkey, 1, iod,
                                 &sgl, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "DAOS object update returned with errors: ", MFU_ERRF,
	                MFU_ERRP(-MFU_ERR_DAOS));
                return 1;
            }
	
        }
    }
    return rc;
}

static int daos_copy_list_keys(daos_handle_t *src_oh,
                          daos_handle_t *dst_oh)
{
    /* loop to enumerate dkeys */
    daos_anchor_t dkey_anchor = {0}; 
    int rc;
    while (!daos_anchor_is_eof(&dkey_anchor)) {
        d_sg_list_t     dkey_sgl;
        d_iov_t         dkey_iov;
        daos_key_desc_t dkey_kds[ENUM_DESC_NR]       = {0};
        uint32_t        dkey_number                  = ENUM_DESC_NR;
        char            dkey_enum_buf[ENUM_DESC_BUF] = {0};
        char            dkey[ENUM_KEY_BUF]           = {0};

        dkey_sgl.sg_nr     = 1;
        dkey_sgl.sg_nr_out = 0;
        dkey_sgl.sg_iovs   = &dkey_iov;

        d_iov_set(&dkey_iov, dkey_enum_buf, ENUM_DESC_BUF);

        /* get dkeys */
        rc = daos_obj_list_dkey(*src_oh, DAOS_TX_NONE, &dkey_number, dkey_kds,
                                &dkey_sgl, &dkey_anchor, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS daos_obj_list_dkey returned with errors: ", MFU_ERRF,
	            MFU_ERRP(-MFU_ERR_DAOS));
            return 1;
        }

        /* if no dkeys were returned move on */
        if (dkey_number == 0)
            continue;

        char* dkey_ptr;
        int   i;

        /* parse out individual dkeys based on key length and numver of dkeys returned */
        for (dkey_ptr = dkey_enum_buf, i = 0; i < dkey_number; i++) {

            /* Print enumerated dkeys */
            daos_key_t diov;
            snprintf(dkey, dkey_kds[i].kd_key_len + 1, "%s", dkey_ptr);
            d_iov_set(&diov, (void*)dkey, dkey_kds[i].kd_key_len);
            dkey_ptr += dkey_kds[i].kd_key_len;

            /* loop to enumerate akeys */
            daos_anchor_t akey_anchor = {0}; 
            while (!daos_anchor_is_eof(&akey_anchor)) {
                d_sg_list_t     akey_sgl;
                d_iov_t         akey_iov;
                daos_key_desc_t akey_kds[ENUM_DESC_NR]       = {0};
                uint32_t        akey_number                  = ENUM_DESC_NR;
                char            akey_enum_buf[ENUM_DESC_BUF] = {0};
                char            akey[ENUM_KEY_BUF]           = {0};

                akey_sgl.sg_nr     = 1;
                akey_sgl.sg_nr_out = 0;
                akey_sgl.sg_iovs   = &akey_iov;

                d_iov_set(&akey_iov, akey_enum_buf, ENUM_DESC_BUF);

                /* get akeys */
                rc = daos_obj_list_akey(*src_oh, DAOS_TX_NONE, &diov, &akey_number, akey_kds,
                                        &akey_sgl, &akey_anchor, NULL);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "DAOS daos_obj_list_akey returned with errors: ", MFU_ERRF,
	                    MFU_ERRP(-MFU_ERR_DAOS));
                    return 1;
                }

                /* if no akeys returned move on */
                if (akey_number == 0)
                    continue;

                int j;
                char* akey_ptr;

                /* parse out individual akeys based on key length and numver of dkeys returned */
                for (akey_ptr = akey_enum_buf, j = 0; j < akey_number; j++) {
                    daos_key_t aiov;
                    daos_iod_t iod;
                    daos_recx_t recx;
                    snprintf(akey, akey_kds[j].kd_key_len + 1, "%s", akey_ptr);
                    d_iov_set(&aiov, (void*)akey, akey_kds[j].kd_key_len);

                    /* set iod values */
                    iod.iod_nr    = 1;
                    iod.iod_type  = DAOS_IOD_SINGLE;
                    iod.iod_size  = DAOS_REC_ANY;
                    iod.iod_recxs = NULL;

                    d_iov_set(&iod.iod_name, (void*)akey, strlen(akey));

                    /* Do a fetch (with NULL sgl) of single value type, and if that
                     * returns iod_size == 0, then a single value does not exist. */
                    rc = daos_obj_fetch(*src_oh, DAOS_TX_NONE, 0, &diov, 1, &iod, NULL, NULL, NULL);
                    if (rc != 0) {
                        MFU_LOG(MFU_LOG_ERR, "DAOS daos_obj_fetch returned with errors: ", MFU_ERRF,
	                        MFU_ERRP(-MFU_ERR_DAOS));
                        return 1;
                    }

                    /* if iod_size == 0 then this is a DAOS_IOD_ARRAY type */
                    if ((int)iod.iod_size == 0) {
                        rc = daos_copy_recx_array(&diov, &aiov, src_oh, dst_oh, &iod);
                        if (rc != 0) {
                            MFU_LOG(MFU_LOG_ERR, "DAOS daos_copy_recx_array returned with errors: ", MFU_ERRF,
	                            MFU_ERRP(-MFU_ERR_DAOS));
                            return 1;
                        }
                    } else {
                        rc = daos_copy_recx_single(&diov, src_oh, dst_oh, &iod);
                        if (rc != 0) {
                            MFU_LOG(MFU_LOG_ERR, "DAOS daos_copy_recx_single returned with errors: ", MFU_ERRF,
	                            MFU_ERRP(-MFU_ERR_DAOS));
                            return 1;
                        }
                    }

                    /* advance to next akey returned */	
                    akey_ptr += akey_kds[j].kd_key_len;
                }
            }
        }
    }
    return rc;
}

static int daos_obj_copy(
    daos_args_t* da,
    mfu_flist bflist)
{
    int rc = 0;

    flist_t* flist = (flist_t*) bflist;

    uint64_t i;
    const elem_t* p = flist->list_head;
    for (i = 0; i < flist->list_count; i++) {
        /* open DAOS object based on oid[i] to get obj handle */
        daos_handle_t oh;
        daos_obj_id_t oid;
        oid.lo = p->obj_id_lo;	
        oid.hi = p->obj_id_hi;	
        rc = daos_obj_open(da->src_coh, oid, 0, &oh, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS object open returned with errors: ", MFU_ERRF,
	            MFU_ERRP(-MFU_ERR_DAOS));
	    return 1;
        }

        /* open handle of object in dst container */
        daos_handle_t dst_oh;
        rc = daos_obj_open(da->dst_coh, oid, 0, &dst_oh, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS object open returned with errors: ", MFU_ERRF,
	            MFU_ERRP(-MFU_ERR_DAOS));
            /* make sure to close the source if opening dst fails */
            daos_obj_close(oh, NULL);
	    return 1;
        }
        rc = daos_copy_list_keys(&oh, &dst_oh);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS copy list keys returned with errors: ", MFU_ERRF,
                    MFU_ERRP(-MFU_ERR_DAOS));
            /* cleanup object handles on failure */
            daos_obj_close(oh, NULL);
            daos_obj_close(dst_oh, NULL);
	    return 1;
        }

        /* close source and destination object */
        daos_obj_close(oh, NULL);
        daos_obj_close(dst_oh, NULL);
        p = p->next;
    }

    return rc;
}

static int daos_obj_list_oids(daos_args_t* da, mfu_flist bflist) {
    /* List objects in src container to be copied to 
     * destination container */
    static const int     OID_ARR_SIZE = 50;
    daos_obj_id_t        oids[OID_ARR_SIZE];
    daos_anchor_t        anchor;
    uint32_t             oids_nr;
    daos_handle_t        toh;
    uint32_t             oids_total = 0;
    int                  rc = 0;

    /* create snapshot to pass to object iterator table */
    rc = daos_cont_create_snap_opt(da->src_coh, &da->epc, NULL,
    				   DAOS_SNAP_OPT_CR | DAOS_SNAP_OPT_OIT,
				   NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS failed to create snapshot: ", MFU_ERRF,
                MFU_ERRP(-MFU_ERR_DAOS));
        return 1;
    }

    /* open object iterator table */
    rc = daos_oit_open(da->src_coh, da->epc, &toh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS failed to open oit: ", MFU_ERRF,
                MFU_ERRP(-MFU_ERR_DAOS));
        return 1;
    }
    memset(&anchor, 0, sizeof(anchor));
    flist_t* flist = (flist_t*) bflist;

    /* list and store all object ids in flist for this epoch */
    while (1) {
        oids_nr = OID_ARR_SIZE;
        rc = daos_oit_list(toh, oids, &oids_nr, &anchor, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS daos_oit_list returned with errors: ", MFU_ERRF,
	            MFU_ERRP(-MFU_ERR_DAOS));
            daos_oit_close(toh, NULL);
            return 1;
        }
	int i;
	/* create element in flist for each obj id retrived */
 	for (i = 0; i < oids_nr; i++) {
            uint64_t idx = mfu_flist_file_create(bflist);
            mfu_flist_file_set_oid(bflist, idx, oids[i]);
	}
	oids_total = oids_nr + oids_total;
 	if (daos_anchor_is_eof(&anchor)) {
 		break;
 	}
    }

    /* close object iterator */
    rc = daos_oit_close(toh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS failed to close oit: ", MFU_ERRF,
                MFU_ERRP(-MFU_ERR_DAOS));
    }
    return rc;
}

int mfu_flist_walk_daos(
    daos_args_t* da,
    mfu_flist flist)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* have rank 0 do the work of listing the objects */
    if (rank == 0) {
        rc = daos_obj_list_oids(da, flist);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS failed to list oids: ",
                MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
        }
    }

    /* summarize list since we added items to it */
    mfu_flist_summarize(flist);

    /* broadcast return code from rank 0 so everyone knows whether walk succeeded */
    MPI_Bcast(&rc, 1, MPI_INT, 0, MPI_COMM_WORLD);

    return rc;
}

int mfu_flist_copy_daos(
    daos_args_t* da,
    mfu_flist flist)
{
    /* copy object ids listed in flist to destination in daos args */
    int rc = daos_obj_copy(da, flist);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "DAOS object copy failed: ",
            MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
    }

    /* wait until all procs are done copying,
     * and determine whether everyone succeeded */
    if (! mfu_alltrue(rc == 0, MPI_COMM_WORLD)) {
        /* someone failed, so return failure on all ranks */
        rc = 1;
    }

    return rc;
}
