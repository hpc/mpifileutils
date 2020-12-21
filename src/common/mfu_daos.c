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
    else {
        return 0;
    } 
    
    bool same_pool = false;
    bool same_cont = false;
    if (have_src_pool && have_dst_pool 
            && uuid_compare(da->src_pool_uuid, da->dst_pool_uuid) == 0) {
        same_pool = true;
        if (have_src_cont && have_dst_cont
                && uuid_compare(da->src_cont_uuid, da->dst_cont_uuid) == 0) {
            same_cont = true;
        }
    }

    int rc = 0;

    if (have_src_cont && !have_src_pool) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Source container requires source pool");
        }
        rc = 1;
    }
    if (have_src_pool && !have_src_cont) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Source pool requires source container");
        }
        rc = 1;
    }
    if (have_dst_cont && !have_dst_pool) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Destination container requires destination pool");
        }
        rc = 1;
    }

    /* Make sure the source and destination are different */
    if (same_cont && have_src_path && have_dst_path) {
        if (strcmp(src_path, dst_path) == 0) {
            if (rank == 0) {
                 MFU_LOG(MFU_LOG_ERR, "DAOS source is DAOS destination");
            }
            rc = 1;
        }
    }

    return rc;
}

/* Checks if the prefix is valid.
 * If valid, returns matching string into suffix */
static bool daos_check_prefix(char* path, const char* dfs_prefix, char** suffix)
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
            *suffix = "/";
            is_prefix = true;
        }
        /* if path is longer, it must start with '/' */
        else if (path_len > prefix_len &&
            path[prefix_len] == '/') {
            *suffix = path + prefix_len;
            is_prefix = true;
        }
    }
    return is_prefix;
}

/* Checks for UNS paths and sets
 * paths and DAOS args accordingly */
static int daos_set_paths(
  int rank,
  char** argpaths,
  daos_args_t* da,
  mfu_file_t* mfu_src_file,
  mfu_file_t* mfu_dst_file)
{
    int rc = 0;

    bool have_src_pool  = daos_uuid_valid(da->src_pool_uuid);
    bool have_dst_pool  = daos_uuid_valid(da->dst_pool_uuid);
    
    char* src_path = argpaths[0];
    char* dst_path = argpaths[1];

    bool prefix_on_src = false;
    bool prefix_on_dst = false;

    /* find out if a dfs_prefix is being used,
     * if so, then that means that the container
     * is not being copied from the root of the
     * UNS path  */
    if (da->dfs_prefix != NULL) {
        struct duns_attr_t dattr = {0};
        rc = duns_resolve_path(da->dfs_prefix, &dattr);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to resolve DAOS Prefix UNS path");
            return 1;
        }

        /* figure out if prefix is on dst or src for
         * copying container subsets */
        if (daos_check_prefix(src_path, da->dfs_prefix, &argpaths[0])) {
            if (have_src_pool) {
                MFU_LOG(MFU_LOG_ERR, "DAOS source pool should not be used with DAOS source prefix");
                return 1;
            }
            mfu_src_file->type = DAOS;
            uuid_copy(da->src_pool_uuid, dattr.da_puuid);
            uuid_copy(da->src_cont_uuid, dattr.da_cuuid);
            prefix_on_src = true;
        } else if (daos_check_prefix(dst_path, da->dfs_prefix, &argpaths[1])) {
            if (have_dst_pool) {
                MFU_LOG(MFU_LOG_ERR, "DAOS destination pool should not be used with DAOS destination prefix");
                return 1;
            }
            mfu_dst_file->type = DAOS;
            uuid_copy(da->dst_pool_uuid, dattr.da_puuid);
            uuid_copy(da->dst_cont_uuid, dattr.da_cuuid);
            prefix_on_dst = true;
        } else {
            MFU_LOG(MFU_LOG_ERR, "DAOS prefix does not match source or destination");
            return 1;
        }
    }

    /* Forward slash is "root" of container to walk
     * in daos. Cannot walk from Unified namespace
     * path given /tmp/dsikich/dfs, it is only used
     * to lookup pool/cont uuids, and tells you
     * if that path is mapped to pool/cont uuid in
     * DAOS 
     *
     * For each of the source and destination,
     * if it is not using a prefix then assume
     * it is a daos path for UNS. If resolve path
     * doesn't succeed then it might be a POSIX path */
    if (!prefix_on_src) {
        struct duns_attr_t src_dattr = {0};
        int src_rc = duns_resolve_path(src_path, &src_dattr);
        
        if (src_rc == 0) {
            if (have_src_pool) {
                MFU_LOG(MFU_LOG_ERR, "DAOS source pool should not be used with DAOS source UNS path");
                return 1;
            }
            mfu_src_file->type = DAOS;
            uuid_copy(da->src_pool_uuid, src_dattr.da_puuid);
            uuid_copy(da->src_cont_uuid, src_dattr.da_cuuid);
            argpaths[0] = "/";
        }
    }

    if (!prefix_on_dst) {
        struct duns_attr_t dst_dattr = {0};
        int dst_rc = duns_resolve_path(dst_path, &dst_dattr);

        if (dst_rc == 0) {
            if (have_dst_pool) {
                MFU_LOG(MFU_LOG_ERR, "DAOS destination pool should not be used with DAOS destination UNS path");
                return 1;
            }
            mfu_dst_file->type = DAOS;
            uuid_copy(da->dst_pool_uuid, dst_dattr.da_puuid);
            uuid_copy(da->dst_cont_uuid, dst_dattr.da_cuuid);
            argpaths[1] = "/";
        }
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

    /* initalize value of DAOS UUID's to NULL with uuid_clear */
    uuid_clear(da->src_pool_uuid);
    uuid_clear(da->dst_pool_uuid);
    uuid_clear(da->src_cont_uuid);
    uuid_clear(da->dst_cont_uuid);

    return da;
}

void daos_args_delete(daos_args_t** pda)
{
    if (pda != NULL) {
        daos_args_t* da = *pda;
        mfu_free(&da->dfs_prefix);
        mfu_free(pda);
    }
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
    int flag_daos_args;

    /* Make sure we have the required DAOS arguments (if any).
     * Safe to return here, since all processes have the same values. */
    tmp_rc = daos_check_args(rank, argpaths, da, &flag_daos_args);
    if (tmp_rc != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid DAOS args: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS_INVAL_ARG));
        }
        return 1;
    }

    /* For now, track the error.
     * Later, ignore if no daos args supplied */
    tmp_rc = daos_init();
    if (tmp_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialize daos");
        local_daos_error = true;
    }

    /* Figure out if daos path is the src or dst,
     * using UNS path, then chop off UNS path
     * prefix since the path is mapped to the root
     * of the container in the DAOS DFS mount */
    if (!local_daos_error
            && (!daos_uuid_valid(da->src_pool_uuid) || !daos_uuid_valid(da->dst_pool_uuid))) {
        tmp_rc = daos_set_paths(rank, argpaths, da, mfu_src_file, mfu_dst_file);
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

    /* check if DAOS source and destination containers are in the same pool */
    bool same_pool = false;
    if (mfu_src_file->type == DAOS && mfu_dst_file->type == DAOS) {
        if (uuid_compare(da->src_pool_uuid, da->dst_pool_uuid) == 0) {
            same_pool = true;
        }
    }

    /* connect to DAOS source pool if uuid is valid */
    if (!local_daos_error && mfu_src_file->type == DAOS) {
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
    if (!local_daos_error && mfu_dst_file->type == DAOS) {
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

    if (!local_daos_error && mfu_src_file->type == DAOS) {
        /* DFS is mounted for the source container */
        tmp_rc = daos_mount(mfu_src_file, &da->src_poh, &da->src_coh);
        if (tmp_rc != 0) {
            local_daos_error = true;
        }
    }

    if (!local_daos_error && mfu_dst_file->type == DAOS) {
        /* DFS is mounted for the destination container */
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

    if (mfu_src_file->type == DAOS) {
        tmp_rc = daos_umount(mfu_src_file);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            rc = 1;
        }

        /* Close the container */
        tmp_rc = daos_cont_close(da->src_coh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
            rc = 1;
        }
    }

    if (mfu_dst_file->type == DAOS) {
        tmp_rc = daos_umount(mfu_dst_file);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            rc = 1;
        }

        /* Close the container */
        tmp_rc = daos_cont_close(da->dst_coh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
            rc = 1;
        }
    }

    if (mfu_src_file->type == DAOS) {
        tmp_rc = daos_pool_disconnect(da->src_poh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to disconnect from source pool");
            rc = 1;
        }
    }

    if (mfu_dst_file->type == DAOS && !same_pool) {
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
