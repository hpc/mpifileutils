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

#ifdef HDF5_SUPPORT
#include <hdf5.h>
#if H5_VERS_MAJOR == 1 && H5_VERS_MINOR < 12
#define H5Sencode1 H5Sencode
#endif
#endif

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
int daos_parse_path(
    char* path,
    size_t path_len,
    uuid_t* p_uuid,
    uuid_t* c_uuid,
    bool daos_no_prefix)
{
    struct duns_attr_t  dattr = {0};
    int                 rc;

    dattr.da_no_prefix = daos_no_prefix;
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
    /* da_no_prefix is only used when only daos paths are expected,
     * so if above parsing fails, then it is an error */
    } else if (strncmp(path, "daos:", 5) == 0 || daos_no_prefix) {
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
    bool    daos_no_prefix = false;

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
        prefix_rc = daos_parse_path(prefix_path, prefix_len,
                                    &prefix_p_uuid, &prefix_c_uuid,
                                    daos_no_prefix);
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
        int src_rc = daos_parse_path(src_path, src_len,
                                     &da->src_pool_uuid, &da->src_cont_uuid,
                                     daos_no_prefix);
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
        int dst_rc = daos_parse_path(dst_path, dst_len,
                                     &da->dst_pool_uuid, &da->dst_cont_uuid,
                                     daos_no_prefix);
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
int daos_connect(
  int rank,
  uuid_t pool_uuid,
  uuid_t cont_uuid,
  daos_handle_t* poh,
  daos_handle_t* coh,
  bool connect_all_ranks,
  bool connect_pool,
  bool create_cont)
{
    /* assume failure until otherwise */
    int valid = 0;
    int rc;

    /* have rank 0 connect to the pool and container,
     * we'll then broadcast the handle ids from rank 0 to everyone else */
    if (rank == 0 || connect_all_ranks) {
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
                MFU_LOG(MFU_LOG_ERR, "Failed to open container");
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
    if (connect_all_ranks)
        goto skip_bcast;

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
skip_bcast:
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
                da->src_cont_uuid, &da->src_poh, &da->src_coh, false, true, false);
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
                    da->dst_cont_uuid, &da->src_poh, &da->dst_coh, false, false, true);
        } else {
            /* Open pool connection, and create container if non-existent */
            tmp_rc = daos_connect(rank, da->dst_pool_uuid,
                    da->dst_cont_uuid, &da->dst_poh, &da->dst_coh, false, true, true);
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
        daos_epoch_range_t  eprs[5];
        daos_recx_t     recxs[5];
        daos_size_t     size;

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

#ifdef HDF5_SUPPORT
static inline void init_hdf5_args(struct hdf5_args *hdf5)
{
    hdf5->status = 0;
    hdf5->file = -1;
    /* User attribute data */
    hdf5->usr_attr_memtype = 0;
    hdf5->usr_attr_name_vtype = 0;
    hdf5->usr_attr_val_vtype = 0;
    /* OID Data */
    hdf5->oid_memtype = 0;
    hdf5->oid_dspace = 0;
    hdf5->oid_dset = 0;
    /* DKEY Data */
    hdf5->dkey_memtype = 0;
    hdf5->dkey_vtype = 0;
    hdf5->dkey_dspace = 0;
    hdf5->dkey_dset = 0;
    /* AKEY Data */
    hdf5->akey_memtype = 0;
    hdf5->akey_vtype = 0;
    hdf5->akey_dspace = 0;
    hdf5->akey_dset = 0;
    /* dims for dsets */
    hdf5->oid_dims[0] = 0;
    hdf5->dkey_dims[0] = 0;     
    hdf5->akey_dims[0] = 0;     
    /* data for keys */
    hdf5->oid_data = NULL;
    hdf5->dkey_data = NULL;
    hdf5->akey_data = NULL;
    hdf5->oid = NULL;
    hdf5->dk = NULL;
    hdf5->ak = NULL;
}

static int serialize_recx_single(struct hdf5_args *hdf5, 
                                 daos_key_t *dkey,
                                 daos_handle_t *oh,
                                 daos_iod_t *iod)
{
    /* if iod_type is single value just fetch iod size from source
     * and update in destination object */
    int         buf_len = (int)(*iod).iod_size;
    char        buf[buf_len];
    d_sg_list_t sgl;
    d_iov_t     iov;
    int         rc;

    /* set sgl values */
    sgl.sg_nr     = 1;
    sgl.sg_nr_out = 0;
    sgl.sg_iovs   = &iov;
    d_iov_set(&iov, buf, buf_len);
    rc = daos_obj_fetch(*oh, DAOS_TX_NONE, 0, dkey, 1, iod, &sgl,
                        NULL, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to fetch object");
        goto out;
    }
    /* write single val record to dataset */
    H5Dwrite(hdf5->single_dset, hdf5->single_dtype, H5S_ALL,
             hdf5->single_dspace, H5P_DEFAULT, sgl.sg_iovs[0].iov_buf);
    printf("\tSINGLE DSET ID WRITTEN: %d\n", (int)hdf5->single_dset);
out:
    return rc;
}

static int serialize_recx_array(struct hdf5_args *hdf5,
                                daos_key_t *dkey,
                                daos_key_t *akey,
                                uint64_t *ak_index,
                                daos_handle_t *oh,
                                daos_iod_t *iod)
{
    int                 rc = 0;
    int                 i = 0;
    int                 attr_num = 0;
    int                 buf_len = 0;
    int                 path_len = 0;
    int                 encode_buf_len;
    uint32_t            number = 5;
    size_t              nalloc;
    daos_anchor_t       recx_anchor = {0}; 
    daos_epoch_range_t  eprs[5];
    daos_recx_t         recxs[5];
    daos_size_t         size = 0;
    char                attr_name[64];
    char                number_str[16];
    char                attr_num_str[16];
    unsigned char       *encode_buf = NULL;
    d_sg_list_t         sgl;
    d_iov_t             iov;
    hid_t               status = 0;

    hdf5->rx_dims[0] = 0;   
    while (!daos_anchor_is_eof(&recx_anchor)) {
        memset(recxs, 0, sizeof(recxs));
        memset(eprs, 0, sizeof(eprs));

        /* list all recx for this dkey/akey */
            number = 5;
        rc = daos_obj_list_recx(*oh, DAOS_TX_NONE, dkey,
                                akey, &size, &number, recxs, eprs, &recx_anchor,
                                true, NULL);
        printf("RECX RETURNED: %d\n", (int)number);
        printf("RECX SIZE: %d\n", (int)size);

        /* if no recx is returned for this dkey/akey move on */
        if (number == 0) 
            continue;
        printf("\n\nNUM RECX RET: %d\n\n", (int)number);
        for (i = 0; i < number; i++) {
            buf_len = recxs[i].rx_nr * size;
            char buf[buf_len];

            memset(&sgl, 0, sizeof(sgl));
            memset(&iov, 0, sizeof(iov));

            /* set iod values */
            (*iod).iod_type  = DAOS_IOD_ARRAY;
            (*iod).iod_size  = size;
            (*iod).iod_nr    = 1;
            (*iod).iod_recxs = &recxs[i];

            /* set sgl values */
            sgl.sg_nr     = 1;
            sgl.sg_nr_out = 0;
            sgl.sg_iovs   = &iov;

            d_iov_set(&iov, buf, buf_len);  
            printf("\ti: %d iod_size: %d rx_nr:%d, rx_idx:%d\n",
                   i, (int)size, (int)recxs[i].rx_nr,
                   (int)recxs[i].rx_idx);
            /* fetch recx values from source */
            rc = daos_obj_fetch(*oh, DAOS_TX_NONE, 0, dkey, 1, iod,
                                &sgl, NULL, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to fetch object: %d", rc);
            }
            /* write data to record dset */
            printf("\n\nTOTAL DIMS SO FAR: %d\n\n",
                   (int)hdf5->rx_dims[0]);
            hdf5->mem_dims[0] = recxs[i].rx_nr;
            hdf5->rx_memspace = H5Screate_simple(1, hdf5->mem_dims,
                                                 hdf5->mem_dims);
            if (hdf5->rx_memspace < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to create rx_memspace");
                rc = 1;
                goto out;
            }
            /* extend dataset */
            hdf5->rx_dims[0] += recxs[i].rx_nr;
            status = H5Dset_extent(hdf5->rx_dset, hdf5->rx_dims);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to extend rx_dset");
                rc = 1;
                goto out;
            }
            printf("RX DIMS: %d\n", (int)hdf5->rx_dims[0]);
            /* retrieve extended dataspace */
            hdf5->rx_dspace = H5Dget_space(hdf5->rx_dset);
            if (hdf5->rx_dspace < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to get rx_dspace");
                rc = 1;
                goto out;
            }
            /* TODO: remove debugging printf's and calls */
            hsize_t dset_size = H5Sget_simple_extent_npoints(hdf5->rx_dspace);
            printf("DSET_SIZE: %d\n", (int)dset_size);
            hsize_t start = (hsize_t)recxs[i].rx_idx;
            hsize_t count = (hsize_t)recxs[i].rx_nr;

            status = H5Sselect_hyperslab(hdf5->rx_dspace,
                                         H5S_SELECT_AND, &start,
                                         NULL, &count, NULL);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to select hyperslab");
                rc = 1;
                goto out;
            }

            /* TODO: remove random checking/printing to make sure
             * right number of blocks is selected
             */
            hsize_t sel = H5Sget_select_npoints(hdf5->rx_dspace);
            printf("SEL: %d\n", (int)sel);
            hsize_t mem_sel = H5Sget_select_npoints(hdf5->rx_memspace);
            printf("MEM SEL: %d\n", (int)mem_sel);
            hssize_t nblocks = H5Sget_select_hyper_nblocks(hdf5->rx_dspace);
            printf("NUM BLOCKS SELECTED: %d\n", (int)nblocks);
            htri_t valid = H5Sselect_valid(hdf5->rx_dspace);
            printf("VALID: %d\n", (int)valid);

            hdf5->rx_dtype = H5Tcreate(H5T_OPAQUE, (*iod).iod_size);
            if (hdf5->rx_dtype < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to create rx_dtype");
                rc = 1;
                goto out;
            }
            /* HDF5 should not try to interpret the datatype */
            status = H5Tset_tag(hdf5->rx_dtype, "Opaque dtype");
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to set dtype tag");
                rc = 1;
                goto out;
            }
            status = H5Dwrite(hdf5->rx_dset, hdf5->rx_dtype,
                              hdf5->rx_memspace, hdf5->rx_dspace,
                              H5P_DEFAULT, sgl.sg_iovs[0].iov_buf);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write rx_dset");
                rc = 1;
                goto out;
            }
            printf("\tRECX DSET ID WRITTEN: %d\n",
                (int)hdf5->rx_dset);
            printf("\tRECX DSPACE ID WRITTEN: %d\n",
                (int)hdf5->rx_dspace);
            /* get size of buffer needed
             * from nalloc
             */
            status = H5Sencode1(hdf5->rx_dspace, NULL, &nalloc);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to get size of buffer needed");
                rc = 1;
                goto out;
            }
            /* encode dataspace description
             * in buffer then store in
             * attribute on dataset
             */
            encode_buf = malloc(nalloc * sizeof(unsigned char));
            status = H5Sencode1(hdf5->rx_dspace, encode_buf,
                                &nalloc);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to encode dataspace");
                rc = 1;
                goto out;
            }
            /* created attribute in HDF5 file with encoded
             * dataspace for this record extent */
            memset(attr_name, 64, sizeof(attr_name));
            memset(number_str, 16, sizeof(number_str));
            memset(attr_num_str, 16, sizeof(attr_num_str));
            path_len = snprintf(number_str, 10, "%d",
                        (int)(*ak_index));
            if (path_len >= 16) {
                MFU_LOG(MFU_LOG_ERR, "number_str is too long");
                rc = 1;
                goto out;
            }
            path_len = snprintf(attr_num_str, 10, "-%d", attr_num);
            if (path_len >= 16) {
                MFU_LOG(MFU_LOG_ERR, "attr number str is too long");
                rc = 1;
                goto out;
            }
            path_len = snprintf(attr_name, 64, "%s", "A-");
            if (path_len >= 64) {
                MFU_LOG(MFU_LOG_ERR, "attr name is too long");
                rc = 1;
                goto out;
            }
            strcat(attr_name, number_str);
            strcat(attr_name, attr_num_str);
            printf("\n\nATTR NAME: %s\n\n", attr_name);
            encode_buf_len = nalloc * sizeof(unsigned char);
            hdf5->attr_dims[0] = encode_buf_len;
            hdf5->attr_dspace = H5Screate_simple(1, hdf5->attr_dims,
                                     NULL);
            if (hdf5->attr_dspace < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to create attr");
                rc = 1;
                goto out;
            }
            hdf5->selection_attr = H5Acreate2(hdf5->rx_dset,
                                              attr_name,
                                              hdf5->rx_dtype,
                                              hdf5->attr_dspace,
                                              H5P_DEFAULT,
                                              H5P_DEFAULT);
            if (hdf5->selection_attr < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to create selection attr");
                rc = 1;
                goto out;
            }   
            status = H5Awrite(hdf5->selection_attr, hdf5->rx_dtype,
                              encode_buf);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to write attr");
                rc = 1;
                goto out;
            }
            status = H5Aclose(hdf5->selection_attr);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to close attr");
                rc = 1;
                goto out;
            }
            status = H5Sclose(hdf5->rx_memspace);
            if (status < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to close rx_memspace");
                rc = 1;
                goto out;
            }
            if (encode_buf != NULL) 
                free(encode_buf);
            attr_num++;
        }
    }
out:
    return rc;
}

static int init_recx_data(struct hdf5_args *hdf5)
{
    int     rc = 0;
    herr_t  err = 0;

    hdf5->single_dims[0] = 1;
    hdf5->rx_dims[0] = 0;
    hdf5->rx_max_dims[0] = H5S_UNLIMITED;
    hdf5->rx_chunk_dims[0] = 1;

    hdf5->plist = H5Pcreate(H5P_DATASET_CREATE);
    if (hdf5->plist < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create prop list");
        rc = 1;
        goto out;
    }
    hdf5->rx_dspace = H5Screate_simple(1, hdf5->rx_dims, hdf5->rx_max_dims);
    if (hdf5->rx_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create rx_dspace");
        rc = 1;
        goto out;
    }
    hdf5->single_dspace = H5Screate_simple(1, hdf5->single_dims, NULL);
    if (hdf5->single_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create single_dspace");
        rc = 1;
        goto out;
    }
    hdf5->rx_dtype = H5Tcreate(H5T_OPAQUE, 1);
    if (hdf5->rx_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create rx_dtype");
        rc = 1;
        goto out;
    }
    err = H5Pset_layout(hdf5->plist, H5D_CHUNKED);
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set property layout");
        rc = 1;
        goto out;
    }
    err = H5Pset_chunk(hdf5->plist, 1, hdf5->rx_chunk_dims);
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set chunk size");
        rc = 1;
        goto out;
    }
    err = H5Tset_tag(hdf5->rx_dtype, "Opaque dtype");
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set recx tag");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

static int serialize_akeys(struct hdf5_args *hdf5,
                           daos_key_t diov,
                           uint64_t *dk_index,
                           uint64_t *ak_index,
                           uint64_t *total_akeys,
                           daos_handle_t *oh)
{
    int             rc = 0;
    herr_t          err = 0;
    int             j = 0;
    daos_anchor_t   akey_anchor = {0}; 
    d_sg_list_t     akey_sgl;
    d_iov_t         akey_iov;
    daos_key_desc_t akey_kds[ENUM_DESC_NR] = {0};
    uint32_t        akey_number = ENUM_DESC_NR;
    char            akey_enum_buf[ENUM_DESC_BUF] = {0};
    char            akey[ENUM_KEY_BUF] = {0};
    char            *akey_ptr = NULL;
    daos_key_t      aiov;
    daos_iod_t      iod;
    char            rec_name[32];
    int             path_len = 0;
    int             size = 0;
    hvl_t           *akey_val;

    while (!daos_anchor_is_eof(&akey_anchor)) {
        memset(akey_kds, 0, sizeof(akey_kds));
        memset(akey, 0, sizeof(akey));
        memset(akey_enum_buf, 0, sizeof(akey_enum_buf));
        akey_number = ENUM_DESC_NR;

        akey_sgl.sg_nr     = 1;
        akey_sgl.sg_nr_out = 0;
        akey_sgl.sg_iovs   = &akey_iov;

        d_iov_set(&akey_iov, akey_enum_buf, ENUM_DESC_BUF);

        /* get akeys */
        rc = daos_obj_list_akey(*oh, DAOS_TX_NONE, &diov,
                                &akey_number, akey_kds,
                                &akey_sgl, &akey_anchor, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to list akeys: %d", rc);
            goto out;
        }

        /* if no akeys returned move on */
        if (akey_number == 0)
            continue;

        size = (akey_number + *total_akeys) * sizeof(akey_t);
        *hdf5->ak = realloc(*hdf5->ak, size);

        /* parse out individual akeys based on key length and
         * numver of dkeys returned
         */
        (*hdf5->dk)[*dk_index].akey_offset = *ak_index;
        printf("\n\nWRITE AKEY OFF: %lu\n\n",
            (*hdf5->dk)[*dk_index].akey_offset);
        for (akey_ptr = akey_enum_buf, j = 0; j < akey_number; j++) {
            path_len = snprintf(akey, akey_kds[j].kd_key_len + 1,
                                "%s", akey_ptr);
            if (path_len >= ENUM_KEY_BUF) {
                MFU_LOG(MFU_LOG_ERR, "akey is too big");
                rc = 1;
                goto out;
            }
            memset(&aiov, 0, sizeof(diov));
            d_iov_set(&aiov, (void*)akey,
                      akey_kds[j].kd_key_len);
            printf("\tj:%d akey:%s len:%d\n", j,
                (char*)aiov.iov_buf,
                (int)akey_kds[j].kd_key_len);
            akey_val = &(*hdf5->ak)[*ak_index].akey_val;
            size = (uint64_t)akey_kds[j].kd_key_len * sizeof(char);
            akey_val->p = malloc(size);
            memcpy(akey_val->p, (void*)akey_ptr,
                   (uint64_t)akey_kds[j].kd_key_len);
            akey_val->len = (uint64_t)akey_kds[j].kd_key_len; 
            (*hdf5->ak)[*ak_index].rec_dset_id = *ak_index;

            /* set iod values */
            iod.iod_nr   = 1;
            iod.iod_type = DAOS_IOD_SINGLE;
            iod.iod_size = DAOS_REC_ANY;
            iod.iod_recxs = NULL;

            d_iov_set(&iod.iod_name, (void*)akey, strlen(akey));
            /* do a fetch (with NULL sgl) of single value type,
            * and if that returns iod_size == 0, then a single
            * value does not exist.
            */
            rc = daos_obj_fetch(*oh, DAOS_TX_NONE, 0, &diov,
                                1, &iod, NULL, NULL, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to fetch object");
                rc = 1;
                goto out;
            }

            /* if iod_size == 0 then this is a
             * DAOS_IOD_ARRAY type
             */
            /* TODO: create a record dset for each
             * akey
             */
            memset(&rec_name, 32, sizeof(rec_name));
            path_len = snprintf(rec_name, 32, "%lu", *ak_index);
            printf("REC NAME: %s\n", rec_name);
            printf("*ak_index: %d\n", *ak_index);
            if (path_len > 32) {
                MFU_LOG(MFU_LOG_ERR, "rec name too long");
                rc = 1;
                goto out;
            }
            if ((int)iod.iod_size == 0) {
                hdf5->rx_dset = H5Dcreate(hdf5->file,
                                          rec_name,
                                          hdf5->rx_dtype,
                                          hdf5->rx_dspace,
                                          H5P_DEFAULT,
                                          hdf5->plist,
                                          H5P_DEFAULT);
                if (hdf5->rx_dset < 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to create rx_dset");
                    rc = 1;
                    goto out;
                }
                printf("rx dset created: %lu\n",
                    (uint64_t)hdf5->rx_dset);
                printf("rec dset id: %lu\n",
                    (*hdf5->ak)[*ak_index].rec_dset_id);
                printf("dset name serialize: %s\n", rec_name);
                printf("ak index: %d\n", (int)*ak_index);
                rc = serialize_recx_array(hdf5, &diov, &aiov,
                                          ak_index, oh, &iod);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to serialize recx array: %d",
                            rc);
                    goto out;
                }
                err = H5Dclose(hdf5->rx_dset);
                if (err < 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to serialize recx array");
                    rc = 1;
                    goto out;
                }
            } else {
                hdf5->single_dtype = H5Tcreate(H5T_OPAQUE,
                                               iod.iod_size);
                H5Tset_tag(hdf5->single_dtype, "Opaque dtype");
                hdf5->single_dset = H5Dcreate(hdf5->file,
                                              rec_name,
                                              hdf5->single_dtype,
                                              hdf5->single_dspace,
                                              H5P_DEFAULT,
                                              H5P_DEFAULT,
                                              H5P_DEFAULT);
                printf("single dset created: %lu\n",
                    (uint64_t)hdf5->single_dset);
                printf("single dset id: %lu\n",
                    (*hdf5->ak)[*ak_index].rec_dset_id);
                printf("dset name serialize: %s\n",
                    rec_name);
                printf("ak index: %d\n",
                    (int)*ak_index);
                rc = serialize_recx_single(hdf5, &diov, oh,
                                           &iod);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to serialize recx single: %d",
                            rc);
                    goto out;
                }
                err = H5Dclose(hdf5->single_dset);
                if (err < 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to serialize recx single");
                    rc = 1;
                    goto out;
                }
                err = H5Tclose(hdf5->single_dtype);
                if (err < 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to close single_dtype");
                    rc = 1;
                    goto out;
                }
            }
            /* advance to next akey returned */ 
            akey_ptr += akey_kds[j].kd_key_len;
            (*ak_index)++;
        }
        *total_akeys = (*total_akeys) + akey_number;
    }
out:
    return rc;
}

static int serialize_dkeys(struct hdf5_args *hdf5,
                           uint64_t *dk_index,
                           uint64_t *ak_index,
                           uint64_t *total_dkeys,
                           uint64_t *total_akeys,
                           daos_handle_t *oh,
                           uint64_t *oid_index)
{
    int             rc = 0;
    herr_t          err = 0;
    int             i = 0;
    daos_anchor_t   dkey_anchor = {0}; 
    d_sg_list_t     dkey_sgl;
    d_iov_t         dkey_iov;
    daos_key_desc_t dkey_kds[ENUM_DESC_NR] = {0};
    uint32_t        dkey_number = ENUM_DESC_NR;
    char            dkey_enum_buf[ENUM_DESC_BUF] = {0};
    char            dkey[ENUM_KEY_BUF] = {0};
    char            *dkey_ptr;
    daos_key_t      diov;
    int             path_len = 0;
    hvl_t           *dkey_val;
    int             size = 0;

    rc = init_recx_data(hdf5);
    (*hdf5->oid)[*oid_index].dkey_offset = *dk_index;
    printf("\n\nWRITE DKEY OFF: %d\n\n",
        (int)(*hdf5->oid)[*oid_index].dkey_offset);
    while (!daos_anchor_is_eof(&dkey_anchor)) {
        memset(dkey_kds, 0, sizeof(dkey_kds));
        memset(dkey, 0, sizeof(dkey));
        memset(dkey_enum_buf, 0, sizeof(dkey_enum_buf));
        dkey_number = ENUM_DESC_NR;

        dkey_sgl.sg_nr     = 1;
        dkey_sgl.sg_nr_out = 0;
        dkey_sgl.sg_iovs   = &dkey_iov;

        d_iov_set(&dkey_iov, dkey_enum_buf, ENUM_DESC_BUF);

        /* get dkeys */
        rc = daos_obj_list_dkey(*oh, DAOS_TX_NONE, &dkey_number,
                                dkey_kds, &dkey_sgl, &dkey_anchor,
                                NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to list dkeys: %d", rc);
            goto out;
        }
        /* if no dkeys were returned move on */
        if (dkey_number == 0)
            continue;
        *hdf5->dk = realloc(*hdf5->dk,
                            (dkey_number + *total_dkeys) * sizeof(dkey_t));
        /* parse out individual dkeys based on key length and
         * number of dkeys returned
         */
        for (dkey_ptr = dkey_enum_buf, i = 0; i < dkey_number; i++) {
            /* Print enumerated dkeys */
            path_len = snprintf(dkey, dkey_kds[i].kd_key_len + 1,
                                "%s", dkey_ptr);
            if (path_len >= ENUM_KEY_BUF) {
                MFU_LOG(MFU_LOG_ERR, "key is too long");
                rc = 1;
                goto out;
            }
            memset(&diov, 0, sizeof(diov));
            d_iov_set(&diov, (void*)dkey, dkey_kds[i].kd_key_len);
            dkey_val = &(*hdf5->dk)[*dk_index].dkey_val;
            size = (uint64_t)dkey_kds[i].kd_key_len * sizeof(char);
            printf("i:%d dkey iov buf:%s len:%lu\n", i,
                (char*)diov.iov_buf,
                (uint64_t)dkey_kds[i].kd_key_len);
            dkey_val->p = malloc(size);
            memcpy(dkey_val->p, (void*)dkey_ptr,
                   (uint64_t)dkey_kds[i].kd_key_len);
            dkey_val->len = (uint64_t)dkey_kds[i].kd_key_len; 
            rc = serialize_akeys(hdf5, diov, dk_index, ak_index,
                                 total_akeys, oh); 
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to list akeys: %d", rc);
                rc = 1;
                goto out;
            }
            dkey_ptr += dkey_kds[i].kd_key_len;
            (*dk_index)++;
        }
        *total_dkeys = (*total_dkeys) + dkey_number;
    }
    err = H5Sclose(hdf5->rx_dspace);
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close rx_dspace");
        rc = 1;
        goto out;
    }
    err = H5Sclose(hdf5->single_dspace);
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close sincle_dspace");
        rc = 1;
        goto out;
    }
    err = H5Tclose(hdf5->rx_dtype);
    if (err < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close rx_dtype");
        goto out;
    }
out:
    return rc;
}

static int init_hdf5_file(struct hdf5_args *hdf5, char *filename) {
    int rc = 0;
    hdf5->file = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT,
                           H5P_DEFAULT);
    if (hdf5->file < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create hdf5 file");
        rc = 1;
        goto out;
    }

    /* Set user attribute dataset types */
    hdf5->usr_attr_memtype = H5Tcreate(H5T_COMPOUND, sizeof(usr_attr_t));
    if (hdf5->usr_attr_memtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr memtype");
        rc = 1;
        goto out;
    }
    hdf5->usr_attr_name_vtype = H5Tcopy(H5T_C_S1);
    if (hdf5->usr_attr_name_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr name vtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tset_size(hdf5->usr_attr_name_vtype, H5T_VARIABLE);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr name vtype");
        rc = 1;
        goto out;
    }
    hdf5->usr_attr_val_vtype = H5Tvlen_create(H5T_NATIVE_OPAQUE);
    if (hdf5->usr_attr_val_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr val vtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->usr_attr_memtype, "Attribute Name",
                             HOFFSET(usr_attr_t, attr_name), hdf5->usr_attr_name_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert user attr name");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->usr_attr_memtype, "Attribute Value",
                             HOFFSET(usr_attr_t, attr_val), hdf5->usr_attr_val_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert user attr val");
        rc = 1;
        goto out;
    }

    /* Set oid dataset types */
    hdf5->oid_memtype = H5Tcreate(H5T_COMPOUND, sizeof(oid_t));
    if (hdf5->oid_memtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create oid memtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->oid_memtype, "OID Hi",
                             HOFFSET(oid_t, oid_hi), H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert oid hi");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->oid_memtype, "OID Low",
                             HOFFSET(oid_t, oid_low), H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert oid low");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->oid_memtype, "Dkey Offset",
                             HOFFSET(oid_t, dkey_offset), H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert dkey offset");
        rc = 1;
        goto out;
    }

    /* Set dkey dataset types */
    hdf5->dkey_memtype = H5Tcreate(H5T_COMPOUND, sizeof(dkey_t));
    if (hdf5->dkey_memtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dkey memtype");
        rc = 1;
        goto out;
    }
    hdf5->dkey_vtype = H5Tvlen_create(H5T_NATIVE_CHAR);
    if (hdf5->dkey_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dkey vtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->dkey_memtype, "Akey Offset",
                             HOFFSET(dkey_t, akey_offset),
                             H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert akey offset");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->dkey_memtype, "Dkey Value",
                             HOFFSET(dkey_t, dkey_val), hdf5->dkey_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert dkey value");
        rc = 1;
        goto out;
    }

    /* Set akey dataset types */
    hdf5->akey_memtype = H5Tcreate(H5T_COMPOUND, sizeof(akey_t));
    if (hdf5->akey_memtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create akey memtype");
        rc = 1;
        goto out;
    }
    hdf5->akey_vtype = H5Tvlen_create(H5T_NATIVE_CHAR);
    if (hdf5->akey_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create akey vtype");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->akey_memtype, "Dataset ID",
                             HOFFSET(akey_t, rec_dset_id),
                             H5T_NATIVE_UINT64);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert record dset id");
        rc = 1;
        goto out;
    }
    hdf5->status = H5Tinsert(hdf5->akey_memtype, "Akey Value",
                             HOFFSET(akey_t, akey_val), hdf5->akey_vtype);
    if (hdf5->status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to insert akey value");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

static int cont_serialize_version(struct hdf5_args *hdf5, float version)
{
    int     rc = 0;
    hid_t   status = 0;
    char    *version_name = "Version";

    hdf5->version_attr_dims[0] = 1;
    hdf5->version_attr_type = H5Tcopy(H5T_NATIVE_FLOAT);
    status = H5Tset_size(hdf5->version_attr_type, 4);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version dtype");
        rc = 1;
        goto out;
    }
    if (hdf5->version_attr_type < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr type");
        rc = 1;
        goto out;
    }
    hdf5->version_attr_dspace = H5Screate_simple(1, hdf5->version_attr_dims,
                                                 NULL);
    if (hdf5->version_attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr dspace");
        rc = 1;
        goto out;
    }
    hdf5->version_attr = H5Acreate2(hdf5->file,
                                    version_name,
                                    hdf5->version_attr_type,
                                    hdf5->version_attr_dspace,
                                    H5P_DEFAULT,
                                    H5P_DEFAULT);
    if (hdf5->version_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(hdf5->version_attr, hdf5->version_attr_type,
                      &version);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attribute");
        rc = 1;
        goto out;
    }
    status = H5Aclose(hdf5->version_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

/*
 * Free the user attribute buffers created by cont_get_usr_attrs.
 */
static void cont_free_usr_attrs(int n, char*** _names, void*** _buffers,
                               size_t** _sizes)
{
    char**  names = *_names;
    void**  buffers = *_buffers;

    if (names != NULL) {
        for (size_t i = 0; i < n; i++) {
            mfu_free(&names[i]);
        }
        mfu_free(_names);
    }
    if (buffers != NULL) {
        for (size_t i = 0; i < n; i++) {
            mfu_free(&buffers[i]);
        }
        mfu_free(_buffers);
    }
    mfu_free(_sizes);
}

/*
 * Get the user attributes for a container in a format similar
 * to what daos_cont_set_attr expects.
 * cont_free_usr_attrs should be called to free the allocations.
 * Returns 1 on error, 0 on success.
 */
static int cont_get_usr_attrs(daos_handle_t coh,
                              int* _n, char*** _names, void*** _buffers,
                              size_t** _sizes)
{
    int         rc = 0;
    uint64_t    total_size = 0;
    uint64_t    cur_size = 0;
    uint64_t    num_attrs = 0;
    uint64_t    name_len = 0;
    char*       name_buf = NULL;
    char**      names = NULL;
    void**      buffers = NULL;
    size_t*     sizes = NULL;

    /* Get the total size needed to store all names */
    rc = daos_cont_list_attr(coh, NULL, &total_size, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to list user attributes "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* no attributes found */
    if (total_size == 0) {
        *_n = 0;
        goto out;
    }

    /* Allocate a buffer to hold all attribute names */
    name_buf = MFU_MALLOC(total_size);
    if (name_buf == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
        rc = 1;
        goto out;
    }

    /* Get the attribute names */
    rc = daos_cont_list_attr(coh, name_buf, &total_size, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to list user attributes "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* Figure out the number of attributes */
    while (cur_size < total_size) {
        name_len = strnlen(name_buf + cur_size, total_size - cur_size);
        if (name_len == total_size - cur_size) {
            /* end of buf reached but no end of string, ignoring */
            break;
        }
        num_attrs++;
        cur_size += name_len + 1;
    }

    /* Sanity check */
    if (num_attrs == 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to parse user attributes");
        rc = 1;
        goto out;
    }

    /* Allocate arrays for attribute names, buffers, and sizes */
    names = MFU_CALLOC(num_attrs, sizeof(char*));
    if (names == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
        rc = 1;
        goto out;
    }
    sizes = MFU_CALLOC(num_attrs, sizeof(size_t));
    if (sizes == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
        rc = 1;
        goto out;
    }
    buffers = MFU_CALLOC(num_attrs, sizeof(void*));
    if (buffers == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
        rc = 1;
        goto out;
    }

    /* Create the array of names */
    cur_size = 0;
    for (uint64_t i = 0; i < num_attrs; i++) {
        name_len = strnlen(name_buf + cur_size, total_size - cur_size);
        if (name_len == total_size - cur_size) {
            /* end of buf reached but no end of string, ignoring */
            break;
        }
        names[i] = strndup(name_buf + cur_size, name_len + 1);
        cur_size += name_len + 1;
    }

    /* Get the buffer sizes */
    rc = daos_cont_get_attr(coh, num_attrs,
                            (const char* const*)names,
                            NULL, sizes, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attribute sizes "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* Allocate space for each value */
    for (uint64_t i = 0; i < num_attrs; i++) {
        buffers[i] = MFU_MALLOC(sizes[i]);
        if (buffers[i] == NULL) {
            MFU_LOG(MFU_LOG_ERR, "failed to allocate user attribute buffer");
            rc = 1;
            goto out;
        }
    }

    /* Get the attribute values */
    rc = daos_cont_get_attr(coh, num_attrs,
                            (const char* const*)names,
                            (void * const*)buffers, sizes,
                            NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attribute values "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* Return values to the caller */
    *_n = num_attrs;
    *_names = names;
    *_buffers = buffers;
    *_sizes = sizes;

out:
    if (rc != 0) {
        cont_free_usr_attrs(num_attrs, &names, &buffers, &sizes);
    }

    mfu_free(&name_buf);
    return rc;
}

static int cont_serialize_usr_attrs(struct hdf5_args *hdf5, daos_handle_t cont)
{
    int         rc = 0;
    hid_t       status = 0;
    hid_t       dset = 0;
    hid_t       dspace = 0;
    hid_t       vtype = 0;
    hsize_t     dims[1];
    usr_attr_t* attr_data = NULL;
    int         num_attrs = 0;
    char**      names = NULL;
    void**      buffers = NULL;
    size_t*     sizes = NULL;

    /* Get all user attributes */
    rc = cont_get_usr_attrs(cont, &num_attrs, &names, &buffers, &sizes);
    if (rc != 0) {
        rc = 1;
        goto out;
    }

    /* Create the user attribute data space */
    dims[0] = num_attrs;
    dspace = H5Screate_simple(1, dims, NULL);
    if (dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attr dspace");
        rc = 1;
        goto out;
    }

    /* Create the user attribute dataset */
    dset = H5Dcreate(hdf5->file, "User Attributes",
                     hdf5->usr_attr_memtype, dspace,
                     H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create user attribute dset");
        rc = 1;
        goto out;
    }

    /* Allocate space for all attributes */
    attr_data = MFU_MALLOC(num_attrs * sizeof(usr_attr_t));
    if (attr_data == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }

    /* Set the data for all attributes */
    for (int i = 0; i < num_attrs; i++) {
        attr_data[i].attr_name = names[i];
        attr_data[i].attr_val.p = buffers[i];
        attr_data[i].attr_val.len = sizes[i];
    }

    status = H5Dwrite(dset, hdf5->usr_attr_memtype, H5S_ALL,
                      H5S_ALL, H5P_DEFAULT, attr_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write user attr dset");
        rc = 1;
        goto out;
    }

out:
    cont_free_usr_attrs(num_attrs, &names, &buffers, &sizes);
    mfu_free(&attr_data);
    H5Dclose(dset);
    H5Sclose(dspace);

    return rc;
}

static int cont_serialize_prop_acl(struct hdf5_args* hdf5,
                                   struct daos_prop_entry* entry,
                                   const char* prop_str)
{
    int                 rc = 0;
    hid_t               status = 0;
	struct daos_acl     *acl = NULL;
    char                **acl_strs = NULL;
    size_t              len_acl = 0;

    if (entry == NULL || entry->dpe_val_ptr == NULL) {
        goto out;
    }

    /* convert acl to list of strings */
    acl = (struct daos_acl *)entry->dpe_val_ptr;                         
    rc = daos_acl_to_strs(acl, &acl_strs, &len_acl);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to convert acl to strs");
        goto out;
    }
    hdf5->attr_dims[0] = len_acl;
    hdf5->attr_dtype = H5Tcopy(H5T_C_S1);
    if (hdf5->attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create acl type");
        rc = 1;
        goto out;
    }
    status = H5Tset_size(hdf5->attr_dtype, H5T_VARIABLE);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set acl dtype size");
        rc = 1;
        goto out;
    }
    hdf5->attr_dspace = H5Screate_simple(1, hdf5->attr_dims,
                                         NULL);
    if (hdf5->attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attribute");
        rc = 1;
        goto out;
    }
    hdf5->usr_attr = H5Acreate2(hdf5->file,
                                prop_str,
                                hdf5->attr_dtype,
                                hdf5->attr_dspace,
                                H5P_DEFAULT,
                                H5P_DEFAULT);
    if (hdf5->usr_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create attribute");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(hdf5->usr_attr, hdf5->attr_dtype,
                      acl_strs);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attribute");
        rc = 1;
        goto out;
    }
    status = H5Aclose(hdf5->usr_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

static int cont_serialize_prop_str(struct hdf5_args* hdf5,
                                   struct daos_prop_entry* entry,
                                   const char* prop_str)
{
    int rc = 0;
    hid_t status = 0;

    if (entry == NULL || entry->dpe_str == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Property %s not found", prop_str);
        rc = 1;
        goto out;
    }

    hdf5->attr_dims[0] = 1;
    hdf5->attr_dtype = H5Tcopy(H5T_C_S1);
    if (hdf5->attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create usr attr type");
        rc = 1;
        goto out;
    }
    status = H5Tset_size(hdf5->attr_dtype, strlen(entry->dpe_str) + 1);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set dtype size");
        rc = 1;
        goto out;
    }
    status = H5Tset_strpad(hdf5->attr_dtype, H5T_STR_NULLTERM);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set null terminator");
        rc = 1;
        goto out;
    }
    hdf5->attr_dspace = H5Screate_simple(1, hdf5->attr_dims,
                                         NULL);
    if (hdf5->attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attribute dataspace");
        rc = 1;
        goto out;
    }
    hdf5->usr_attr = H5Acreate2(hdf5->file,
                                prop_str,
                                hdf5->attr_dtype,
                                hdf5->attr_dspace,
                                H5P_DEFAULT,
                                H5P_DEFAULT);
    if (hdf5->usr_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create attribute");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(hdf5->usr_attr, hdf5->attr_dtype,
                      entry->dpe_str);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attribute");
        rc = 1;
        goto out;
    }
    status = H5Aclose(hdf5->usr_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attribute");
        rc = 1;
        goto out;
    }

out:
    return rc;
}

static int cont_serialize_prop_uint(struct hdf5_args *hdf5,
                                    struct daos_prop_entry* entry,
                                    const char *prop_str)
{
    int     rc = 0;
    hid_t   status = 0;

    if (entry == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Property %s not found", prop_str);
        rc = 1;
        goto out;
    }

    hdf5->attr_dims[0] = 1;
    hdf5->attr_dtype = H5Tcopy(H5T_NATIVE_UINT64);
    status = H5Tset_size(hdf5->attr_dtype, 8);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version dtype");
        rc = 1;
        goto out;
    }
    if (hdf5->attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create usr attr type");
        rc = 1;
        goto out;
    }
    hdf5->attr_dspace = H5Screate_simple(1, hdf5->attr_dims,
                                         NULL);
    if (hdf5->attr_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create version attr dspace");
        rc = 1;
        goto out;
    }
    hdf5->usr_attr = H5Acreate2(hdf5->file,
                                prop_str,
                                hdf5->attr_dtype,
                                hdf5->attr_dspace,
                                H5P_DEFAULT,
                                H5P_DEFAULT);
    if (hdf5->usr_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create attr");
        rc = 1;
        goto out;
    }   
    status = H5Awrite(hdf5->usr_attr, hdf5->attr_dtype,
                      &entry->dpe_val);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write attr");
        rc = 1;
        goto out;
    }
    status = H5Aclose(hdf5->usr_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close attr");
        rc = 1;
        goto out;
    }

out:
    return rc;
}


static int cont_serialize_props(struct hdf5_args *hdf5,
                                    daos_handle_t cont)
{
    int                     rc = 0;
    daos_prop_t*            prop_query;
    daos_prop_t*            prop_acl = NULL;
    struct daos_prop_entry* entry;
    char                    cont_str[DAOS_PROP_LABEL_MAX_LEN];

    /*
     * Get all props except the ACL first.
     */
    prop_query = daos_prop_alloc(16);
    if (prop_query == NULL) {
        return ENOMEM;
    }

    prop_query->dpp_entries[0].dpe_type = DAOS_PROP_CO_LABEL;
    prop_query->dpp_entries[1].dpe_type = DAOS_PROP_CO_LAYOUT_TYPE;
    prop_query->dpp_entries[2].dpe_type = DAOS_PROP_CO_LAYOUT_VER;
    prop_query->dpp_entries[3].dpe_type = DAOS_PROP_CO_CSUM;
    prop_query->dpp_entries[4].dpe_type = DAOS_PROP_CO_CSUM_CHUNK_SIZE;
    prop_query->dpp_entries[5].dpe_type = DAOS_PROP_CO_CSUM_SERVER_VERIFY;
    prop_query->dpp_entries[6].dpe_type = DAOS_PROP_CO_REDUN_FAC;
    prop_query->dpp_entries[7].dpe_type = DAOS_PROP_CO_REDUN_LVL;
    prop_query->dpp_entries[8].dpe_type = DAOS_PROP_CO_SNAPSHOT_MAX;
    prop_query->dpp_entries[9].dpe_type = DAOS_PROP_CO_COMPRESS;
    prop_query->dpp_entries[10].dpe_type = DAOS_PROP_CO_ENCRYPT;
    prop_query->dpp_entries[11].dpe_type = DAOS_PROP_CO_OWNER;
    prop_query->dpp_entries[12].dpe_type = DAOS_PROP_CO_OWNER_GROUP;
    prop_query->dpp_entries[13].dpe_type = DAOS_PROP_CO_DEDUP;
    prop_query->dpp_entries[14].dpe_type = DAOS_PROP_CO_DEDUP_THRESHOLD;
    prop_query->dpp_entries[15].dpe_type = DAOS_PROP_CO_ALLOCED_OID;

    rc = daos_cont_query(cont, NULL, prop_query, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to query container "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    /* Fetch the ACL separately in case user doesn't have access */
    rc = daos_cont_get_acl(cont, &prop_acl, NULL);
    if (rc && rc != -DER_NO_PERM) {
        MFU_LOG(MFU_LOG_ERR, "failed to query container ACL "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

    entry = &prop_query->dpp_entries[0];
    rc = cont_serialize_prop_str(hdf5, entry, "DAOS_PROP_CO_LABEL");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[1];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_LAYOUT_TYPE");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[2];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_LAYOUT_VER");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[3];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[4];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM_CHUNK_SIZE");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[5];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_CSUM_SERVER_VERIFY");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[6];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_REDUN_FAC");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[7];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_REDUN_LVL");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[8];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_SNAPSHOT_MAX");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[9];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_COMPRESS");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[10];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_ENCRYPT");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[11];
    rc = cont_serialize_prop_str(hdf5, entry, "DAOS_PROP_CO_OWNER");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[12];
    rc = cont_serialize_prop_str(hdf5, entry, "DAOS_PROP_CO_OWNER_GROUP");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[13];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_DEDUP");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[14];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_DEDUP_THRESHOLD");
    if (rc != 0) {
        goto out;
    }

    entry = &prop_query->dpp_entries[15];
    rc = cont_serialize_prop_uint(hdf5, entry, "DAOS_PROP_CO_ALLOCED_OID");
    if (rc != 0) {
        goto out;
    }

    /* serialize ACL */
    if (prop_acl != NULL) {
        entry = &prop_acl->dpp_entries[0];
        rc = cont_serialize_prop_acl(hdf5, entry, "DAOS_PROP_CO_ACL");
        if (rc != 0) {
            goto out;
        }
    }

out:
    daos_prop_free(prop_query);
    daos_prop_free(prop_acl);
    return rc;
}

int cont_serialize_hdlr(uuid_t pool_uuid, uuid_t cont_uuid,
                        daos_handle_t poh, daos_handle_t coh)
{
    int             rc = 0;
    int             i = 0;
    /* TODO: update this  to PATH_MAX, currently using too much
     * static memory to use it */
    int             PMAX = 64;
    int             path_len = 0;
    struct          hdf5_args hdf5;
    uint64_t        total_dkeys = 0;
    uint64_t        total_akeys = 0;
    uint64_t        dk_index = 0;
    uint64_t        ak_index = 0;
    uint64_t        oid_index = 0;
    daos_obj_id_t   oids[OID_ARR_SIZE];
    daos_anchor_t   anchor;
    uint32_t        oids_nr;
    int             num_oids;
    daos_handle_t   toh;
    daos_epoch_t    epoch;
    uint32_t        total = 0;
    daos_handle_t   oh;
    float           version = 0.0;
    herr_t          err = 0;
    char            cont_str[130];

    /* init HDF5 args */
    init_hdf5_args(&hdf5);

    uuid_unparse(cont_uuid, cont_str);

    printf("Serializing Container to %s\n", cont_str);

    /* init HDF5 datatypes in HDF5 file */
    rc = init_hdf5_file(&hdf5, cont_str);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to init hdf5 file");
        rc = 1;
        goto out;
    }
    rc = daos_cont_create_snap_opt(coh, &epoch, NULL,
                                   DAOS_SNAP_OPT_CR | DAOS_SNAP_OPT_OIT,
                                   NULL);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "failed to create snapshot: %d", rc);
        goto out;
    }

    rc = daos_oit_open(coh, epoch, &toh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open oit: %d", rc);
        goto out;
    }
    memset(&anchor, 0, sizeof(anchor));
    hdf5.oid_data = malloc(num_oids * sizeof(oid_t));
    if (hdf5.oid_data == NULL)
        return ENOMEM;
    hdf5.dkey_data = malloc(sizeof(dkey_t));
    if (hdf5.dkey_data == NULL)
        return ENOMEM;
    hdf5.akey_data = malloc(sizeof(akey_t));
    if (hdf5.akey_data == NULL)
        return ENOMEM;
    hdf5.dk = &(hdf5.dkey_data);
    hdf5.ak = &(hdf5.akey_data);
    hdf5.oid = &(hdf5.oid_data);

    while (!daos_anchor_is_eof(&anchor)) {
        memset(oids, 0, sizeof(oids));
        oids_nr = OID_ARR_SIZE;
        rc = daos_oit_list(toh, oids, &oids_nr, &anchor, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to open oit: %d", rc);
            goto out;
        }
        num_oids = (int)oids_nr;
        total += num_oids;
        D_PRINT("returned %d oids\n", num_oids);

        *hdf5.oid = realloc(*hdf5.oid, (total) * sizeof(oid_t));

        /* list object ID's */
        for (i = 0; i < num_oids; i++) {
            /* open DAOS object based on oid[i] to get obj
             * handle
             */
            (*hdf5.oid)[oid_index].oid_hi = oids[i].hi;
            (*hdf5.oid)[oid_index].oid_low = oids[i].lo;
            rc = daos_obj_open(coh, oids[i], 0, &oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to open object: %d", rc);
                goto out;
            }
            rc = serialize_dkeys(&hdf5, &dk_index, &ak_index,
                                 &total_dkeys, &total_akeys,
                                 &oh, &oid_index);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to serialize keys: %d", rc);
                goto out;
            }
            /* close source and destination object */
            rc = daos_obj_close(oh, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to close object: %d", rc);
                goto out;
            }
            oid_index++;
        }
    }

    /* write container version as attribute */
    rc = cont_serialize_version(&hdf5, version);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to serialize version");
        goto out;
    }

    rc = cont_serialize_usr_attrs(&hdf5, coh);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to serialize user attributes");
        goto out;
    }

    rc = cont_serialize_props(&hdf5, coh);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to serialize cont layout");
        goto out;
    }

    printf("total dkeys: %lu\n", total_dkeys);
    printf("total akeys: %lu\n", total_akeys);
    printf("total oids: %lu\n", total);

    hdf5.oid_dims[0] = total;
    hdf5.oid_dspace = H5Screate_simple(1, hdf5.oid_dims, NULL);
    if (hdf5.oid_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create oid dspace");
        rc = 1;
        goto out;
    }
    hdf5.oid_dset = H5Dcreate(hdf5.file, "Oid Data",
                              hdf5.oid_memtype, hdf5.oid_dspace,
                              H5P_DEFAULT, H5P_DEFAULT,
                              H5P_DEFAULT);
    if (hdf5.oid_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create oid dset");
        rc = 1;
        goto out;
    }
    hdf5.dkey_dims[0] = total_dkeys;     
    hdf5.dkey_dspace = H5Screate_simple(1, hdf5.dkey_dims, NULL);
    if (hdf5.dkey_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dkey dspace");
        rc = 1;
        goto out;
    }
    hdf5.dkey_dset = H5Dcreate(hdf5.file, "Dkey Data",
                               hdf5.dkey_memtype, hdf5.dkey_dspace,
                               H5P_DEFAULT, H5P_DEFAULT,
                               H5P_DEFAULT);
    if (hdf5.dkey_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dkey dset");
        rc = 1;
        goto out;
    }
    hdf5.akey_dims[0] = total_akeys;     
    hdf5.akey_dspace = H5Screate_simple(1, hdf5.akey_dims, NULL);
    if (hdf5.akey_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create akey dspace");
        rc = 1;
        goto out;
    }
    hdf5.akey_dset = H5Dcreate(hdf5.file, "Akey Data",
                               hdf5.akey_memtype, hdf5.akey_dspace,
                               H5P_DEFAULT, H5P_DEFAULT,
                               H5P_DEFAULT);
    if (hdf5.akey_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create akey dset");
        rc = 1;
        goto out;
    }
    hdf5.status = H5Dwrite(hdf5.oid_dset, hdf5.oid_memtype, H5S_ALL,
                           H5S_ALL, H5P_DEFAULT, *(hdf5.oid));
    if (hdf5.status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write oid dset");
        rc = 1;
        goto out;
    }
    hdf5.status = H5Dwrite(hdf5.dkey_dset, hdf5.dkey_memtype,
                           H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           *(hdf5.dk));
    if (hdf5.status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write dkey dset");
        rc = 1;
        goto out;
    }
    hdf5.status = H5Dwrite(hdf5.akey_dset, hdf5.akey_memtype,
                           H5S_ALL, H5S_ALL, H5P_DEFAULT,
                           *(hdf5.ak));
    if (hdf5.status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to write akey dset");
        rc = 1;
        goto out;
    }

    /* close object iterator */
    rc = daos_oit_close(toh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close oit: %d", rc);
        goto out;
    }
    daos_epoch_range_t epr;
    epr.epr_lo = epoch;
    epr.epr_hi = epoch;
    rc = daos_cont_destroy_snap(coh, epr, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to destroy snap: %d", rc);
        goto out;
    }
    
    err = H5Fflush(hdf5.file, H5F_SCOPE_GLOBAL);
    if (err < 0 ) {
        MFU_LOG(MFU_LOG_ERR, "failed to flush hdf5 file: %d", rc);
        rc = 1;
        goto out;
    }
out:
    if (hdf5.oid_dset > 0)
        H5Dclose(hdf5.oid_dset);
    if (hdf5.dkey_dset > 0)
        H5Dclose(hdf5.dkey_dset);
    if (hdf5.akey_dset > 0)
        H5Dclose(hdf5.akey_dset);
    if (hdf5.oid_dspace > 0)
        H5Sclose(hdf5.oid_dspace);
    if (hdf5.dkey_dspace > 0)
        H5Sclose(hdf5.dkey_dspace);
    if (hdf5.akey_dspace > 0)
        H5Sclose(hdf5.akey_dspace);
    if (hdf5.oid_memtype > 0)
        H5Tclose(hdf5.oid_memtype);
    if (hdf5.dkey_memtype > 0)
        H5Tclose(hdf5.dkey_memtype);
    if (hdf5.akey_memtype > 0)
        H5Tclose(hdf5.akey_memtype);
    if (hdf5.oid_data != NULL)
        free(hdf5.oid_data);
    if (hdf5.dkey_data != NULL)
        free(hdf5.dkey_data);
    if (hdf5.akey_data != NULL)
        free(hdf5.akey_data);
    if (hdf5.file < 0)
        H5Fclose(hdf5.file);
    return rc;
}

static int hdf5_read_key_data(struct hdf5_args *hdf5)
{
    int     rc = 0;
    hid_t   status = 0;
    int     oid_ndims = 0;
    int     dkey_ndims = 0;
    int     akey_ndims = 0;

    /* read oid data */
    hdf5->oid_dset = H5Dopen(hdf5->file, "Oid Data", H5P_DEFAULT);
    if (hdf5->oid_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open OID dset");
        rc = 1;
        goto out;
    }
    hdf5->oid_dspace = H5Dget_space(hdf5->oid_dset);
    if (hdf5->oid_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get oid dspace");
        rc = 1;
        goto out;
    }
    hdf5->oid_dtype = H5Dget_type(hdf5->oid_dset);
    if (hdf5->oid_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get oid dtype");
        rc = 1;
        goto out;
    }
    oid_ndims = H5Sget_simple_extent_dims(hdf5->oid_dspace, hdf5->oid_dims,
                                          NULL);
    if (oid_ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get oid dimensions");
        rc = 1;
        goto out;
    }
    printf("oid_dims: %d\n", (int)oid_ndims);
    hdf5->oid_data = malloc(hdf5->oid_dims[0] * sizeof(oid_t));
    if (hdf5->oid_data == NULL) {
        D_GOTO(out, rc = -DER_NOMEM);
    }
    status = H5Dread(hdf5->oid_dset, hdf5->oid_dtype, H5S_ALL, H5S_ALL,
                     H5P_DEFAULT, hdf5->oid_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read oid data");
        rc = 1;
        goto out;
    }

    /* read dkey data */
    hdf5->dkey_dset = H5Dopen(hdf5->file, "Dkey Data", H5P_DEFAULT);
    if (hdf5->dkey_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open dkey dset");
        rc = 1;
        goto out;
    }
    hdf5->dkey_dspace = H5Dget_space(hdf5->dkey_dset);
    if (hdf5->dkey_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dkey dspace");
        rc = 1;
        goto out;
    }
    hdf5->dkey_vtype = H5Dget_type(hdf5->dkey_dset);
    if (hdf5->dkey_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dkey vtype");
        rc = 1;
        goto out;
    }
    dkey_ndims = H5Sget_simple_extent_dims(hdf5->dkey_dspace,
                                           hdf5->dkey_dims, NULL);
    if (dkey_ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dkey dimensions");
        rc = 1;
        goto out;
    }
    printf("dkey_dims: %d\n", (int)dkey_ndims);
    hdf5->dkey_data = malloc(hdf5->dkey_dims[0] * sizeof(dkey_t));
    if (hdf5->dkey_data == NULL) {
        rc = ENOMEM;
        goto out;
    }
    status = H5Dread(hdf5->dkey_dset, hdf5->dkey_vtype, H5S_ALL, H5S_ALL,
                     H5P_DEFAULT, hdf5->dkey_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read dkey data");
        rc = 1;
        goto out;
    }

    /* read akey data */
    hdf5->akey_dset = H5Dopen(hdf5->file, "Akey Data", H5P_DEFAULT);
    if (hdf5->akey_dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open akey dset");
        rc = 1;
        goto out;
    }
    hdf5->akey_dspace = H5Dget_space(hdf5->akey_dset);
    if (hdf5->akey_dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get akey dset");
        rc = 1;
        goto out;
    }
    hdf5->akey_vtype = H5Dget_type(hdf5->akey_dset);
    if (hdf5->akey_vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get akey vtype");
        rc = 1;
        goto out;
    }
    akey_ndims = H5Sget_simple_extent_dims(hdf5->akey_dspace,
                                           hdf5->akey_dims, NULL);
    if (akey_ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get akey dimensions");
        rc = 1;
        goto out;
    }
    printf("akey_dims: %d\n", (int)akey_ndims);
    hdf5->akey_data = malloc(hdf5->akey_dims[0] * sizeof(akey_t));
    if (hdf5->akey_data == NULL) {
        rc = ENOMEM;
        goto out;
    }
    status = H5Dread(hdf5->akey_dset, hdf5->akey_vtype, H5S_ALL, H5S_ALL,
                     H5P_DEFAULT, hdf5->akey_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read akey data");
        rc = 1;
        goto out;
    }
out:
    return rc;
}

static int cont_deserialize_recx(struct hdf5_args *hdf5,
                                 daos_handle_t *oh,
                                 daos_key_t diov,
                                 int num_attrs,
                                 uint64_t ak_off,
                                 int k)
{
    int             rc = 0;
    hid_t           status = 0;
    int             i = 0;
    ssize_t         attr_len = 0;
    char            attr_name_buf[124]; 
    hsize_t         attr_space;
    hid_t           attr_type;
    size_t          type_size;
    size_t          rx_dtype_size;
    unsigned char   *decode_buf;
    hid_t           rx_range_id;
    hsize_t         *rx_range = NULL;
    uint64_t        recx_len = 0;
    void            *recx_data = NULL;
    hssize_t        nblocks_sel;
    hssize_t        nblocks = 0;
    d_sg_list_t     sgl;
    d_iov_t         iov;
    daos_iod_t      iod;
    daos_recx_t     recxs;
    hid_t           aid;

    for (i = 0; i < num_attrs; i++) {
        memset(attr_name_buf, 0, sizeof(attr_name_buf));
        aid = H5Aopen_idx(hdf5->rx_dset, (unsigned int)i);
        if (aid < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get open attr");
            rc = 1;
            goto out;
        }
        attr_len = H5Aget_name(aid, 124, attr_name_buf);
        if (attr_len < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get attr name");
            rc = 1;
            goto out;
        }
        printf("\t\t\t    Attribute Name : %s\n", attr_name_buf);
        printf("\t\t\t    Attribute Len : %d\n",(int)attr_len);
        attr_space = H5Aget_storage_size(aid);
        if (attr_len < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get attr space");
            rc = 1;
            goto out;
        }
        attr_type = H5Aget_type(aid);
        if (attr_type < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get attr type");
            rc = 1;
            goto out;
        }
        type_size = H5Tget_size(attr_type);
        if (type_size < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get type size");
            rc = 1;
            goto out;
        }
        rx_dtype_size = H5Tget_size(hdf5->rx_dtype);
        if (rx_dtype_size < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get rx type size");
            rc = 1;
            goto out;
        }
        printf("\t\t\ttype size: %d\n", (int)type_size);
        printf("\t\t\trx type size: %d\n", (int)rx_dtype_size);
        printf("\t\t\tattr id: %lu\n", (uint64_t)aid);
        printf("\t\t\tattr space: %d\n", (int)attr_space);
        
        decode_buf = malloc(type_size * attr_space);
        if (decode_buf == NULL) {
            rc = ENOMEM;
            goto out;
        }
        rx_range = malloc(type_size * attr_space);
        if (rx_range == NULL) {
            rc = ENOMEM;
            goto out;
        }
        status = H5Aread(aid, attr_type, decode_buf);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read attribute");
            rc = 1;
            goto out;
        }
        rx_range_id = H5Sdecode(decode_buf);
        if (rx_range_id < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to decode attribute buffer");
            rc = 1;
            goto out;
        }
        nblocks = H5Sget_select_hyper_nblocks(rx_range_id);
        if (nblocks < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get hyperslab blocks");
            rc = 1;
            goto out;
        }
        status = H5Sget_select_hyper_blocklist(rx_range_id, 0,
                                               nblocks, rx_range);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get blocklist");
            rc = 1;
            goto out;
        }
        printf("\t\t\tRX IDX: %d\n", (int)rx_range[0]);
        printf("\t\t\tRX NR: %d\n", (int)rx_range[1]);

        /* read recx data then update */
        hdf5->rx_dspace = H5Dget_space(hdf5->rx_dset);
        if (hdf5->rx_dspace < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get rx_dspace");
            rc = 1;
            goto out;
        }

        /* TODO: remove these debugging calls */
        hsize_t dset_size = H5Sget_simple_extent_npoints(hdf5->rx_dspace);
        printf("DSET_SIZE RX DSPACE: %d\n", (int)dset_size);
        hsize_t dset_size2 = H5Sget_simple_extent_npoints(rx_range_id);
        printf("DSET_SIZE RX ID: %d\n", (int)dset_size2);

        hsize_t start = rx_range[0];
        hsize_t count = (rx_range[1] - rx_range[0]) + 1;
        status = H5Sselect_hyperslab(hdf5->rx_dspace,
                                     H5S_SELECT_AND,
                                     &start, NULL,
                                     &count, NULL);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to select hyperslab");
            rc = 1;
            goto out;
        }
        recx_len = count * 1;
        recx_data = malloc(recx_len);
        if (recx_data == NULL) {
            rc = ENOMEM;
            goto out;
        }
        printf("\t\t\tRECX LEN: %d\n", (int)recx_len);
        nblocks_sel = H5Sget_select_hyper_nblocks(hdf5->rx_dspace);
        printf("NUM BLOCKS SELECTED: %d\n", (int)nblocks_sel);
        hdf5->mem_dims[0] = count;
        hdf5->rx_memspace = H5Screate_simple(1, hdf5->mem_dims,
                                             hdf5->mem_dims);
        status = H5Dread(hdf5->rx_dset,
                         hdf5->rx_dtype,
                         hdf5->rx_memspace,
                         hdf5->rx_dspace,
                         H5P_DEFAULT,
                         recx_data);
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read record extent");
            rc = 1;
            goto out;
        }
        memset(&sgl, 0, sizeof(sgl));
        memset(&iov, 0, sizeof(iov));
        memset(&iod, 0, sizeof(iod));
        memset(&recxs, 0, sizeof(recxs));
        d_iov_set(&iod.iod_name,
                  (void*)hdf5->akey_data[ak_off + k].akey_val.p,
                  hdf5->akey_data[ak_off + k].akey_val.len);
        /* set iod values */
        iod.iod_type  = DAOS_IOD_ARRAY;
        iod.iod_size  = rx_dtype_size;
        iod.iod_nr    = 1;

        printf("START TO WRITE: %d\n", (int)start);
        printf("COUNT TO WRITE: %d\n", (int)count);
        recxs.rx_nr = recx_len;
        recxs.rx_idx = start;
        iod.iod_recxs = &recxs;

        /* set sgl values */
        sgl.sg_nr     = 1;
        sgl.sg_iovs   = &iov;

        d_iov_set(&iov, recx_data, recx_len);   

        /* update fetched recx values and place in destination object */
        rc = daos_obj_update(*oh, DAOS_TX_NONE, 0, &diov, 1, &iod,
                             &sgl, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to update object: %d", rc);
            goto out;
        }
        H5Aclose(aid);
        mfu_free(&rx_range);
        mfu_free(&recx_data);
        mfu_free(&decode_buf);
    }
out:
    return rc;
}

static int cont_deserialize_keys(struct hdf5_args *hdf5,
                                 uint64_t *total_dkeys_this_oid,
                                 uint64_t *dk_off,
                                 daos_handle_t *oh)
{
    int             rc = 0;
    hid_t           status = 0;
    int             j = 0;
    daos_key_t      diov;
    char            dkey[ENUM_KEY_BUF] = {0};
    uint64_t        ak_off = 0;
    uint64_t        ak_next = 0;
    uint64_t        total_akeys_this_dkey = 0;
    int             k = 0;
    daos_key_t      aiov;
    char            akey[ENUM_KEY_BUF] = {0};
    int             rx_ndims;
    uint64_t        index = 0;
    int             len = 0;
    int             num_attrs;
    size_t          single_tsize;
    char            *single_data = NULL;
    d_sg_list_t     sgl;
    d_iov_t         iov;
    daos_iod_t      iod;
    
    for(j = 0; j < *total_dkeys_this_oid; j++) {
        memset(&diov, 0, sizeof(diov));
        memset(dkey, 0, sizeof(dkey));
        snprintf(dkey, hdf5->dkey_data[*dk_off + j].dkey_val.len + 1,
                 "%s", (char*)(hdf5->dkey_data[*dk_off + j].dkey_val.p));
        d_iov_set(&diov, (void*)hdf5->dkey_data[*dk_off + j].dkey_val.p,
                  hdf5->dkey_data[*dk_off + j].dkey_val.len);
        printf("\tDKEY VAL: %s\n", (char*)dkey);
        printf("\tDKEY VAL LEN: %lu\n",
        hdf5->dkey_data[*dk_off + j].dkey_val.len);
        ak_off = hdf5->dkey_data[*dk_off + j].akey_offset;
        ak_next = 0;
        total_akeys_this_dkey = 0;
        if (*dk_off + j + 1 < (int)hdf5->dkey_dims[0]) {
            ak_next = hdf5->dkey_data[(*dk_off + j) + 1].akey_offset;
            total_akeys_this_dkey = ak_next - ak_off;
        } else if (*dk_off + j == ((int)hdf5->dkey_dims[0] - 1)) {
            total_akeys_this_dkey = ((int)hdf5->akey_dims[0]) - ak_off;
        }
        printf("\nTOTAL AK THIS DK: %lu\n",
            total_akeys_this_dkey);
        for(k = 0; k < total_akeys_this_dkey; k++) {
            memset(&aiov, 0, sizeof(aiov));
            memset(akey, 0, sizeof(akey));
            snprintf(akey, hdf5->akey_data[ak_off + k].akey_val.len + 1,
                 "%s", (char*)hdf5->akey_data[ak_off + k].akey_val.p);
            d_iov_set(&aiov, (void*)hdf5->akey_data[ak_off + k].akey_val.p,
                  hdf5->akey_data[ak_off + k].akey_val.len);
            printf("\t\tAKEY VAL: %s\n", (char*)akey);
            printf("\t\tAKEY VAL LEN: %lu\n",
            hdf5->akey_data[ak_off + k].akey_val.len);

            /* read record data for each akey */
            index = ak_off + k;
            len = snprintf(NULL, 0, "%lu", index);
            char dset_name[len + 1];    
            snprintf(dset_name, len + 1, "%lu", index);
            printf("\t\t\tdset name: %s\n", dset_name);
            hdf5->rx_dset = H5Dopen(hdf5->file, dset_name,
                                    H5P_DEFAULT);
            if (hdf5->rx_dset < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to read rx_dset");
                rc = 1;
                goto out;
            }
            printf("\t\t\trx_dset: %lu\n", (uint64_t)hdf5->rx_dset);
            hdf5->rx_dspace = H5Dget_space(hdf5->rx_dset);
            if (hdf5->rx_dspace < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to read rx_dspace");
                rc = 1;
                goto out;
            }
            printf("\t\t\trx_dspace id: %d\n", (int)hdf5->rx_dspace);
            hdf5->rx_dtype = H5Dget_type(hdf5->rx_dset);
            if (hdf5->rx_dtype < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to read rx_dtype");
                rc = 1;
                goto out;
            }
            hdf5->plist = H5Dget_create_plist(hdf5->rx_dset);
            if (hdf5->plist < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to get plist");
                rc = 1;
                goto out;
            }
            rx_ndims = H5Sget_simple_extent_dims(hdf5->rx_dspace,
                                                 hdf5->rx_dims,
                                                 NULL);
            if (rx_ndims < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to get rx_ndims");
                rc = 1;
                goto out;
            }
            printf("\t\t\trx_dims: %d\n", (int)hdf5->rx_dims[0]);
            num_attrs = H5Aget_num_attrs(hdf5->rx_dset);
            if (num_attrs < 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to get num attrs");
                rc = 1;
                goto out;
            }
            printf("\t\t\tnum attrs: %d\n", num_attrs);
            if (num_attrs > 0) {
                rc = cont_deserialize_recx(hdf5, oh, diov,
                                           num_attrs, ak_off, k);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to deserialize recx");
                    rc = 1;
                    goto out;
                }
            } else {
                memset(&sgl, 0, sizeof(sgl));
                memset(&iov, 0, sizeof(iov));
                memset(&iod, 0, sizeof(iod));
                single_tsize = H5Tget_size(hdf5->rx_dtype);
                single_data = malloc(single_tsize);
                printf("\t\t\tSINGLE LEN: %d\n", (int)single_tsize);
                status = H5Dread(hdf5->rx_dset, hdf5->rx_dtype,
                                 H5S_ALL, hdf5->rx_dspace,
                                 H5P_DEFAULT, single_data);
                if (status < 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to read record");
                    rc = 1;
                    goto out;
                }
                d_iov_set(&iod.iod_name,
                          (void*)hdf5->akey_data[ak_off + k].akey_val.p,
                          hdf5->akey_data[ak_off + k].akey_val.len);

                /* set iod values */
                iod.iod_type  = DAOS_IOD_SINGLE;
                iod.iod_size  = single_tsize;
                iod.iod_nr    = 1;
                iod.iod_recxs = NULL;

                /* set sgl values */
                sgl.sg_nr     = 1;
                sgl.sg_iovs   = &iov;

                d_iov_set(&iov, single_data, single_tsize); 

                /* update fetched recx values and place in destination object */
                rc = daos_obj_update(*oh, DAOS_TX_NONE, 0,
                                     &diov, 1, &iod, &sgl, NULL);
                if (rc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "failed to update object: %d", rc);
                    goto out;
                }

            }
            H5Pclose(hdf5->plist);  
            H5Tclose(hdf5->rx_dtype);   
            H5Sclose(hdf5->rx_dspace);  
            H5Dclose(hdf5->rx_dset);    
        }
    }
out:
    mfu_free(&single_data);
    return rc;
}

static int cont_deserialize_prop_str(struct hdf5_args* hdf5,
                                     struct daos_prop_entry* entry,
                                     const char* prop_str)
{
    hid_t   status = 0;
    int     rc = 0;

    hdf5->cont_attr = H5Aopen(hdf5->file, prop_str, H5P_DEFAULT);
    if (hdf5->cont_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    hdf5->attr_dtype = H5Aget_type(hdf5->cont_attr);
    if (hdf5->attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    }
    size_t buf_size = H5Tget_size(hdf5->attr_dtype);
    if (buf_size <= 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get size for property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    entry->dpe_str = MFU_MALLOC(buf_size);
    if (entry->dpe_str == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Aread(hdf5->cont_attr, hdf5->attr_dtype, entry->dpe_str);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Aclose(hdf5->cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close property attribute %s", prop_str);
        rc = 1;
        goto out;
    }

out:
    return rc;
}

static int cont_deserialize_prop_uint(struct hdf5_args* hdf5,
                                      struct daos_prop_entry* entry,
                                      const char* prop_str)
{
    hid_t   status = 0;
    int     rc = 0;

    hdf5->cont_attr = H5Aopen(hdf5->file, prop_str, H5P_DEFAULT);
    if (hdf5->cont_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    hdf5->attr_dtype = H5Aget_type(hdf5->cont_attr);
    if (hdf5->attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Aread(hdf5->cont_attr, hdf5->attr_dtype, &entry->dpe_val);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    status = H5Aclose(hdf5->cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close property attribute %s", prop_str);
        rc = 1;
        goto out;
    }

out:
    return rc;
}

static int cont_deserialize_prop_acl(struct hdf5_args* hdf5,
                                     struct daos_prop_entry* entry,
                                     const char* prop_str)
{
    hid_t           status = 0;
    int             rc = 0;
    int             i = 0;
    int             ndims = 0;
    const char      **rdata = NULL;
    struct daos_acl *acl;

    hdf5->cont_attr = H5Aopen(hdf5->file, prop_str, H5P_DEFAULT);
    if (hdf5->cont_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    hdf5->attr_dtype = H5Aget_type(hdf5->cont_attr);
    if (hdf5->attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open property attribute type %s", prop_str);
        rc = 1;
        goto out;
    }
    hdf5->attr_dspace = H5Aget_space(hdf5->cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read acl dspace");
        rc = 1;
        goto out;
    }
    ndims = H5Sget_simple_extent_dims(hdf5->attr_dspace, hdf5->attr_dims, NULL);
    if (ndims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get dimensions of dspace");
        rc = 1;
        goto out;
    }
    rdata = malloc(hdf5->attr_dims[0] * sizeof(char*));
    if (rdata == NULL) {
        rc = ENOMEM;
        goto out;
    }
    hdf5->attr_dtype = H5Tcopy(H5T_C_S1);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to create dtype");
        rc = 1;
        goto out;
    }
    status = H5Tset_size(hdf5->attr_dtype, H5T_VARIABLE);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set acl dtype size");
        rc = 1;
        goto out;
    }
    status = H5Aread(hdf5->cont_attr, hdf5->attr_dtype, rdata);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
    /* convert acl strings back to struct acl, then store in entry */
    rc = daos_acl_from_strs(rdata, (size_t)hdf5->attr_dims[0], &acl);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to convert acl strs");
        goto out;
    }
    entry->dpe_val_ptr = (void*)acl;
    status = H5Aclose(hdf5->cont_attr);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to close property attribute %s", prop_str);
        rc = 1;
        goto out;
    }
out:
    mfu_free(&rdata);
    return rc;
}

static int cont_deserialize_usr_attrs(struct hdf5_args* hdf5,
                                      daos_handle_t coh)
{
    hid_t       status = 0;
    int         rc = 0;
    int         num_attrs = 0;
    int         num_dims = 0;
    char**      names = NULL;
    void**      buffers = NULL;
    size_t*     sizes = NULL;
    hid_t       dset = 0;
    hid_t       dspace = 0;
    hid_t       vtype = 0;
    hsize_t     dims[1];
    usr_attr_t* attr_data = NULL;

    /* Read the user attributes */
    dset= H5Dopen(hdf5->file, "User Attributes", H5P_DEFAULT);
    if (dset < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open user attributes dset");
        rc = 1;
        goto out;
    }
    dspace = H5Dget_space(dset);
    if (dspace < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attributes dspace");
        rc = 1;
        goto out;
    }
    vtype = H5Dget_type(dset);
    if (vtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attributes vtype");
        rc = 1;
        goto out;
    }
    num_dims = H5Sget_simple_extent_dims(dspace, dims, NULL);
    if (num_dims < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get user attributes dimensions");
        rc = 1;
        goto out;
    }
    num_attrs = dims[0];
    attr_data = MFU_CALLOC(dims[0], sizeof(usr_attr_t));
    if (attr_data == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }
    status = H5Dread(dset, vtype, H5S_ALL, H5S_ALL,
                     H5P_DEFAULT, attr_data);
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read user attributes data");
        rc = 1;
        goto out;
    }

    names = MFU_CALLOC(num_attrs, sizeof(char*));
    if (names == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }
    buffers = MFU_CALLOC(num_attrs, sizeof(void*));
    if (buffers == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }
    sizes = MFU_CALLOC(num_attrs, sizeof(size_t));
    if (sizes == NULL) {
        MFU_LOG(MFU_LOG_ERR, "failed to allocate user attributes");
        rc = 1;
        goto out;
    }

    /* Set the user attribute buffers */
    MFU_LOG(MFU_LOG_INFO, "num_attrs = %llu", num_attrs);
    for (int i = 0; i < num_attrs; i++) {
        MFU_LOG(MFU_LOG_INFO, "attr_data[%llu].attr_name = %s", i, attr_data[i].attr_name);
        MFU_LOG(MFU_LOG_INFO, "attr_data[%llu].attr_val.p = %s", i, attr_data[i].attr_val.p);
        MFU_LOG(MFU_LOG_INFO, "attr_data[%llu].attr_val.len = %llu", i, attr_data[i].attr_val.len);
        names[i] = attr_data[i].attr_name;
        buffers[i] = attr_data[i].attr_val.p;
        sizes[i] = attr_data[i].attr_val.len;
    }

    rc = daos_cont_set_attr(coh, num_attrs,
                            (const char * const*) names,
                            (const void * const*) buffers,
                            sizes, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to set user attributes "DF_RC, DP_RC(rc));
        rc = 1;
        goto out;
    }

out:
    H5Dclose(dset);
    H5Sclose(dspace);
    H5Tclose(vtype);
    cont_free_usr_attrs(num_attrs, &names, &buffers, &sizes);
    mfu_free(&attr_data);
    return rc;
}

int cont_deserialize_hdlr(uuid_t pool_uuid, uuid_t cont_uuid,
                          daos_handle_t *poh, daos_handle_t *coh,
                          char *h5filename)
{
    int                     rc = 0;
    int                     i = 0;
    daos_cont_info_t        cont_info;
    struct                  hdf5_args hdf5;
    daos_obj_id_t           oid;
    daos_handle_t           oh;
    uint64_t                dk_off = 0;
    uint64_t                dk_next = 0;
    uint64_t                total_dkeys_this_oid = 0;
    hid_t                   status = 0;
    float                   version;
    daos_cont_layout_t      cont_type;
    daos_prop_t*            prop;
    struct daos_prop_entry* entry;

    /* init HDF5 args */
    init_hdf5_args(&hdf5);

    printf("\th5filename: %s\n", h5filename);

    /* open passed in HDF5 file */
    hdf5.file = H5Fopen(h5filename, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (hdf5.file < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open hdf5 file");
        rc = 1;
        goto out;
    }

    /* deserialize version -- serialization version/format should
     * be compatible with deserialization version
     */
    hdf5.version_attr = H5Aopen(hdf5.file, "Version", H5P_DEFAULT);
    if (hdf5.version_attr < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open version attr");
        rc = 1;
        goto out;
    }
    hdf5.attr_dtype = H5Aget_type(hdf5.version_attr);
    if (hdf5.attr_dtype < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to get attr type");
        rc = 1;
        goto out;
    }
    status = H5Aread(hdf5.version_attr, hdf5.attr_dtype, &version); 
    if (status < 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read version");
        rc = 1;
        goto out;
    }
    if (version > 0.0) {
        MFU_LOG(MFU_LOG_ERR, "serialization format is not compatible with "
                "deserialization version\n");
        rc = 1;
        goto out;
    }

    prop = daos_prop_alloc(17);
    if (prop == NULL) {
        return ENOMEM;
    }

    prop->dpp_entries[0].dpe_type = DAOS_PROP_CO_LABEL; 
    prop->dpp_entries[1].dpe_type = DAOS_PROP_CO_LAYOUT_TYPE;
    prop->dpp_entries[2].dpe_type = DAOS_PROP_CO_LAYOUT_VER;
    prop->dpp_entries[3].dpe_type = DAOS_PROP_CO_CSUM;
    prop->dpp_entries[4].dpe_type = DAOS_PROP_CO_CSUM_CHUNK_SIZE;
    prop->dpp_entries[5].dpe_type = DAOS_PROP_CO_CSUM_SERVER_VERIFY;
    prop->dpp_entries[6].dpe_type = DAOS_PROP_CO_REDUN_FAC;
    prop->dpp_entries[7].dpe_type = DAOS_PROP_CO_REDUN_LVL;
    prop->dpp_entries[8].dpe_type = DAOS_PROP_CO_SNAPSHOT_MAX;
    prop->dpp_entries[9].dpe_type = DAOS_PROP_CO_COMPRESS;
    prop->dpp_entries[10].dpe_type = DAOS_PROP_CO_ENCRYPT;
    prop->dpp_entries[11].dpe_type = DAOS_PROP_CO_OWNER;
    prop->dpp_entries[12].dpe_type = DAOS_PROP_CO_OWNER_GROUP;
    prop->dpp_entries[13].dpe_type = DAOS_PROP_CO_DEDUP;
    prop->dpp_entries[14].dpe_type = DAOS_PROP_CO_DEDUP_THRESHOLD;
    prop->dpp_entries[15].dpe_type = DAOS_PROP_CO_ALLOCED_OID;
    prop->dpp_entries[16].dpe_type = DAOS_PROP_CO_ACL;

    entry = &prop->dpp_entries[0];
    rc = cont_deserialize_prop_str(&hdf5, entry, "DAOS_PROP_CO_LABEL");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[1];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_LAYOUT_TYPE");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[2];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_LAYOUT_VER");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[3];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_CSUM");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[4];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_CSUM_CHUNK_SIZE");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[5];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_CSUM_SERVER_VERIFY");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[6];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_REDUN_FAC");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[7];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_REDUN_LVL");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[8];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_SNAPSHOT_MAX");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[9];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_COMPRESS");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[10];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_ENCRYPT");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[11];
    rc = cont_deserialize_prop_str(&hdf5, entry, "DAOS_PROP_CO_OWNER");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[12];
    rc = cont_deserialize_prop_str(&hdf5, entry, "DAOS_PROP_CO_OWNER_GROUP");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[13];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_DEDUP");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[14];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_DEDUP_THRESHOLD");
    if (rc != 0) {
        goto out;
    }

    entry = &prop->dpp_entries[15];
    rc = cont_deserialize_prop_uint(&hdf5, entry, "DAOS_PROP_CO_ALLOCED_OID");
    if (rc != 0) {
        goto out;
    }

    /* TODO fetch ACL into a different prop and merge the two props
     * only if the ACL succeeds. Otherwise, if the ACL fails, the entry will
     * be invalid/NULL */
    /* Ignore missing ACL in case user didn't have access when serializing */
    entry = &prop->dpp_entries[16];
    /* read acl as a list of strings in deserialize, then convert
     * back to acl for property entry
     */
    rc = cont_deserialize_prop_acl(&hdf5, entry, "DAOS_PROP_CO_ACL");
    
    cont_type = prop->dpp_entries[1].dpe_val;
    if (cont_type == DAOS_PROP_CO_LAYOUT_POSIX) {
        dfs_attr_t attr;
        attr.da_id = 0;
        attr.da_props = prop;
        attr.da_oclass_id = OC_UNKNOWN; /* TODO serialize this? */
        attr.da_chunk_size = 0; /* TODO serialize this? */
        rc = dfs_cont_create(*poh, cont_uuid, &attr, NULL, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to create posix container: %d", rc);
            goto out;
        }
    } else {
        rc = daos_cont_create(*poh, cont_uuid, prop, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to create container: %d", rc);
            goto out;
        }
    }
    daos_prop_free(prop);

    /* print out created cont uuid */
    char cont_str[130];
    uuid_unparse(cont_uuid, cont_str);
    fprintf(stdout, "Successfully created container %s\n", cont_str);
    rc = daos_cont_open(*poh, cont_uuid, DAOS_COO_RW, coh,
                        &cont_info, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to open container: %d", rc);
        goto out;
    }

    /* deserialize and set the user attributes */
    rc = cont_deserialize_usr_attrs(&hdf5, *coh);
    if (rc != 0) {
        rc = 1;
        goto out;
    }

    rc = hdf5_read_key_data(&hdf5);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to read hdf5 key data");
        rc = 1;
        goto out;
    }
    for(i = 0; i < (int)hdf5.oid_dims[0]; i++) {
        oid.lo = hdf5.oid_data[i].oid_low;
        oid.hi = hdf5.oid_data[i].oid_hi;
        printf("oid_data[i].oid_low: %lu\n", hdf5.oid_data[i].oid_low);
        printf("oid_data[i].oid_hi: %lu\n", hdf5.oid_data[i].oid_hi);
        rc = daos_obj_open(*coh, oid, 0, &oh, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to open object: %d", rc);
            goto out;
        }
        dk_off = hdf5.oid_data[i].dkey_offset;
        dk_next = 0;
        total_dkeys_this_oid = 0;
        if (i + 1 < (int)hdf5.oid_dims[0]) {
            dk_next = hdf5.oid_data[i + 1].dkey_offset;
            total_dkeys_this_oid = dk_next - dk_off;
        } else if (i == ((int)hdf5.oid_dims[0] - 1)){
            printf("LAST OID: i: %d\n", i);
            total_dkeys_this_oid = (int)hdf5.dkey_dims[0] - (dk_off);
        } 
        printf("\nTOTAL DK THIS OID: %lu\n", total_dkeys_this_oid);
        rc = cont_deserialize_keys(&hdf5, &total_dkeys_this_oid, &dk_off, &oh);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to deserialize keys: %d", rc);
            goto out;
        }
        rc = daos_obj_close(oh, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to close object: %d", rc);
            goto out;
        }
    }

out:
    H5Dclose(hdf5.oid_dset);
    H5Dclose(hdf5.dkey_dset);
    H5Dclose(hdf5.akey_dset);
    H5Sclose(hdf5.oid_dspace);
    H5Sclose(hdf5.dkey_dspace);
    H5Sclose(hdf5.akey_dspace);
    H5Tclose(hdf5.oid_dtype);
    H5Tclose(hdf5.dkey_vtype);
    H5Tclose(hdf5.akey_vtype);
    H5Fclose(hdf5.file);
    return rc;
}
#endif
