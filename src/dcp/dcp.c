#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>

/* for daos */
#ifdef DAOS_SUPPORT
#include <uuid/uuid.h>
#include <gurt/common.h>
#include <gurt/hash.h>
#include <daos.h>
#include <sys/stat.h>
#include <daos_fs.h>
#include <daos_uns.h>
#endif

#include "mpi.h"
#include "libcircle.h"
#include "mfu.h"

#include "mfu_errors.h"

#ifdef DAOS_SUPPORT
/* struct for holding DAOS arguments */
typedef struct {
    daos_handle_t src_poh; /* source pool handle */
    daos_handle_t dst_poh; /* destination pool handle */
    daos_handle_t src_coh; /* source container handle */
    daos_handle_t dst_coh; /* destination container handle */
    uuid_t src_pool_uuid;  /* source pool UUID */
    uuid_t dst_pool_uuid;  /* destination pool UUID */
    uuid_t src_cont_uuid;  /* source container UUID */
    uuid_t dst_cont_uuid;  /* destination container UUID */
    char* src_svc;         /* source service level */
    char* dst_svc;         /* destination service level */
    char* dfs_prefix;      /* prefix for UNS */
} daos_args_t;

/* Return a newly allocated daos_args_t structure.
 * Set default values on its fields. */
static daos_args_t* daos_args_new(void)
{
    daos_args_t* da = (daos_args_t*) MFU_MALLOC(sizeof(daos_args_t));
    
    da->src_poh    = DAOS_HDL_INVAL;
    da->dst_poh    = DAOS_HDL_INVAL;
    da->src_coh    = DAOS_HDL_INVAL;
    da->dst_coh    = DAOS_HDL_INVAL;
    da->src_svc    = NULL;
    da->dst_svc    = NULL;
    da->dfs_prefix = NULL;

    /* initalize value of DAOS UUID's to NULL with uuid_clear */
    uuid_clear(da->src_pool_uuid);
    uuid_clear(da->dst_pool_uuid);
    uuid_clear(da->src_cont_uuid);
    uuid_clear(da->dst_cont_uuid);
    
    return da;
}

/* free a daos_args_t structure */
static void daos_args_delete(daos_args_t** pda)
{
    if (pda != NULL) {
        daos_args_t* da = *pda;
        mfu_free(&da->src_svc);
        mfu_free(&da->dst_svc);
        mfu_free(&da->dfs_prefix);
        mfu_free(pda);
    }
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

    /* If only the source or destination svc is
     * given, default the other */
    if (da->src_svc != NULL && da->dst_svc == NULL) {
        da->dst_svc = MFU_STRDUP(da->src_svc);
    }
    else if (da->src_svc == NULL && da->dst_svc != NULL) {
        da->src_svc = MFU_STRDUP(da->dst_svc);
    }

    bool have_src_path  = src_path != NULL;
    bool have_dst_path  = dst_path != NULL;
    bool have_src_pool  = daos_uuid_valid(da->src_pool_uuid);
    bool have_src_cont  = daos_uuid_valid(da->src_cont_uuid);
    bool have_dst_pool  = daos_uuid_valid(da->dst_pool_uuid);
    bool have_dst_cont  = daos_uuid_valid(da->dst_cont_uuid);
    bool have_src_svc   = da->src_svc != NULL;
    bool have_dst_svc   = da->dst_svc != NULL;
    bool have_prefix    = da->dfs_prefix != NULL;

    /* Determine whether any DAOS arguments are supplied. 
     * If not, then there is nothing to check. */
    *flag_daos_args = 0;
    if (have_src_pool || have_src_cont || have_dst_pool || have_dst_cont
            || have_src_svc || have_dst_svc || have_prefix) {
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
    if (have_src_pool && !have_src_svc) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Source pool requires source svcl");
        }
        rc = 1;
    }
    if (have_dst_cont && !have_dst_pool) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Destination container requires destination pool");
        }
        rc = 1;
    }
    if (have_dst_pool && !have_dst_svc) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Destination pool requires destination svcl");
        }
        rc = 1;
    }
    if (have_prefix && !have_src_svc && !have_dst_svc) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Prefix requires source or destination svcl");
        }
        rc = 1;
    }

    /* Containers are using the same pool uuid.
     * Make sure they are also using the same svc.
     * This is unlikely to ever happen but we can print an error just in case. */
    if (same_pool && have_src_svc && have_dst_svc) {
        if (strcmp(da->dst_svc, da->src_svc) != 0) {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Using same pool uuid with different svcl's");
            }
            rc = 1;
        }
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

/* Setup DAOS arguments.
 * Connect to pools.
 * Open containers.
 * Mount DFS. 
 * Returns 1 on error, 0 on success */
static int daos_setup(
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
        tmp_rc = daos_connect(rank, da->src_svc, da->src_pool_uuid,
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
            tmp_rc = daos_connect(rank, da->dst_svc, da->dst_pool_uuid,
                    da->dst_cont_uuid, &da->src_poh, &da->dst_coh, false, true);
        } else {
            /* Open pool connection, and create container if non-existent */
            tmp_rc = daos_connect(rank, da->dst_svc, da->dst_pool_uuid,
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

/* Unmount DFS.
 * Disconnect from pool/cont.
 * Cleanup DAOS-related vars, handles. 
 * Finalize DAOS. */
static int daos_cleanup(
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
#endif 

static int input_flist_skip(const char* name, void *args)
{
    /* nothing to do if args are NULL */
    if (args == NULL) {
        MFU_LOG(MFU_LOG_INFO, "Skip %s.", name);
        return 1;
    }

    /* get pointer to arguments */
    struct mfu_flist_skip_args *sk_args = (struct mfu_flist_skip_args *)args;

    /* create mfu_path from name */
    mfu_path* path = mfu_path_from_str(name);

    /* iterate over each source path */
    int i;
    for (i = 0; i < sk_args->numpaths; i++) {
        /* create mfu_path of source path */
        const char* src_name = sk_args->paths[i].path;
        mfu_path* src_path = mfu_path_from_str(src_name);

        /* check whether path is contained within or equal to
         * source path and if so, we need to copy this file */
        mfu_path_result result = mfu_path_cmp(path, src_path);
        if (result == MFU_PATH_SRC_CHILD || result == MFU_PATH_EQUAL) {
            MFU_LOG(MFU_LOG_INFO, "Need to copy %s because of %s.",
                    name, src_name);
            mfu_path_delete(&src_path);
            mfu_path_delete(&path);
            return 0;
        }
        mfu_path_delete(&src_path);
    }

    /* the path in name is not a child of any source paths,
     * so skip this file */
    MFU_LOG(MFU_LOG_INFO, "Skip %s.", name);
    mfu_path_delete(&path);
    return 1;
}

/** Print a usage message. */
void print_usage(void)
{
    printf("\n");
    printf("Usage: dcp [options] source target\n");
    printf("       dcp [options] source ... target_dir\n");
    printf("\n");
    printf("Options:\n");
    /* printf("  -d, --debug <level> - specify debug verbosity level (default info)\n"); */
#ifdef LUSTRE_SUPPORT
    /* printf("  -g, --grouplock <id> - use Lustre grouplock when reading/writing file\n"); */
#endif
    printf("  -b, --blocksize <SIZE>   - IO buffer size in bytes (default " MFU_BLOCK_SIZE_STR ")\n");
    printf("  -k, --chunksize <SIZE>   - work size per task in bytes (default " MFU_CHUNK_SIZE_STR ")\n");
#ifdef DAOS_SUPPORT
    printf("      --daos-src-pool      - DAOS source pool \n");
    printf("      --daos-dst-pool      - DAOS destination pool \n");
    printf("      --daos-src-cont      - DAOS source container \n");
    printf("      --daos-dst-cont      - DAOS destination container \n");
    printf("      --daos-src-svcl      - DAOS service level used by source DAOS pool \n");
    printf("      --daos-dst-svcl      - DAOS service level used by destination DAOS pool \n");
    printf("      --daos-prefix        - DAOS prefix for unified namespace path \n");
#endif
    printf("  -i, --input <file>       - read source list from file\n");
    printf("  -L, --dereference        - copy original files instead of links\n");
    printf("  -P, --no-dereference     - don't follow links in source\n");
    printf("  -p, --preserve           - preserve permissions, ownership, timestamps, extended attributes\n");
    printf("  -s, --direct             - open files with O_DIRECT\n");
    printf("  -S, --sparse             - create sparse files when possible\n");
    printf("      --progress <N>       - print progress every N seconds\n");
    printf("  -v, --verbose            - verbose output\n");
    printf("  -q, --quiet              - quiet output\n");
    printf("  -h, --help               - print usage\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
}

int main(int argc, char** argv)
{
    /* assume we'll exit with success */
    int rc = 0;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* pointer to mfu_file src and dest objects */
    mfu_file_t* mfu_src_file = mfu_file_new();
    mfu_file_t* mfu_dst_file = mfu_file_new();

    /* pointer to mfu_copy opts */
    mfu_copy_opts_t* mfu_copy_opts = mfu_copy_opts_new();

    /* pointer to mfu_walk opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    /* By default, don't have iput file. */
    char* inputname = NULL;

#ifdef DAOS_SUPPORT
    /* DAOS vars */ 
    daos_args_t* daos_args = daos_args_new();    
#endif

    int option_index = 0;
    static struct option long_options[] = {
        {"blocksize"            , required_argument, 0, 'b'},
        {"debug"                , required_argument, 0, 'd'}, // undocumented
        {"grouplock"            , required_argument, 0, 'g'}, // untested
        {"daos-src-pool"        , required_argument, 0, 'x'},
        {"daos-dst-pool"        , required_argument, 0, 'D'},
        {"daos-src-cont"        , required_argument, 0, 'y'},
        {"daos-dst-cont"        , required_argument, 0, 'Y'},
        {"daos-src-svcl"        , required_argument, 0, 'z'},
        {"daos-dst-svcl"        , required_argument, 0, 'Z'},
        {"daos-prefix"          , required_argument, 0, 'X'},
        {"input"                , required_argument, 0, 'i'},
        {"chunksize"            , required_argument, 0, 'k'},
        {"dereference"          , no_argument      , 0, 'L'},
        {"no-dereference"       , no_argument      , 0, 'P'},
        {"preserve"             , no_argument      , 0, 'p'},
        {"synchronous"          , no_argument      , 0, 's'},
        {"direct"               , no_argument      , 0, 's'},
        {"sparse"               , no_argument      , 0, 'S'},
        {"progress"             , required_argument, 0, 'R'},
        {"verbose"              , no_argument      , 0, 'v'},
        {"quiet"                , no_argument      , 0, 'q'},
        {"help"                 , no_argument      , 0, 'h'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    unsigned long long bytes = 0;
    int usage = 0;
    while(1) {
        int c = getopt_long(
                    argc, argv, "b:d:g:i:k:LPpsSvqh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
            case 'b':
                if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR,
                                "Failed to parse block size: '%s'", optarg);
                    }
                    usage = 1;
                } else {
                    mfu_copy_opts->block_size = (size_t)bytes;
                }
                break;
            case 'd':
                if(strncmp(optarg, "fatal", 5) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_FATAL;
                    mfu_debug_level = MFU_LOG_FATAL;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: fatal");
                    }
                }
                else if(strncmp(optarg, "err", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_ERR;
                    mfu_debug_level = MFU_LOG_ERR;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: errors");
                    }
                }
                else if(strncmp(optarg, "warn", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN;
                    mfu_debug_level = MFU_LOG_WARN;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: warnings");
                    }
                }
                else if(strncmp(optarg, "info", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN; /* we back off a level on CIRCLE verbosity */
                    mfu_debug_level = MFU_LOG_INFO;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: info");
                    }
                }
                else if(strncmp(optarg, "dbg", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_DBG;
                    mfu_debug_level = MFU_LOG_DBG;
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: debug");
                    }
                }
                else {
                    if(rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level `%s' not recognized. " \
                            "Defaulting to `info'.", optarg);
                    }
                }
                break;
#ifdef LUSTRE_SUPPORT
            case 'g':
                mfu_copy_opts->grouplock_id = atoi(optarg);
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "grouplock ID: %d.",
                        mfu_copy_opts->grouplock_id);
                }
                break;
#endif
#ifdef DAOS_SUPPORT
            case 'x':
                rc = uuid_parse(optarg, daos_args->src_pool_uuid);
                if (rc != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse source pool uuid: '%s'", optarg);
                    }
                    usage = 1;
                }
                break;
            case 'D':
                rc = uuid_parse(optarg, daos_args->dst_pool_uuid);
                if (rc != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse dst pool uuid: '%s'", optarg);
                    }
                    usage = 1;
                }
                break;
            case 'y':
                rc = uuid_parse(optarg, daos_args->src_cont_uuid);
                if (rc != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse source cont uuid: '%s'", optarg);
                    }
                    usage = 1;
                }
                mfu_src_file->type = DAOS;
                break;
            case 'Y':
                rc = uuid_parse(optarg, daos_args->dst_cont_uuid);
                if (rc != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse dst cont uuid: '%s'", optarg);
                    }
                    usage = 1;
                }
                mfu_dst_file->type = DAOS;
                break;
            case 'z':
                daos_args->src_svc = MFU_STRDUP(optarg);
                break;
            case 'Z':
                daos_args->dst_svc = MFU_STRDUP(optarg);
                break;
            case 'X':
                daos_args->dfs_prefix = MFU_STRDUP(optarg);
                break;
#endif
            case 'i':
                inputname = MFU_STRDUP(optarg);
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using input list.");
                }
                break;
            case 'k':
                if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR,
                                "Failed to parse chunk size: '%s'", optarg);
                    }
                    usage = 1;
                } else {
                    mfu_copy_opts->chunk_size = bytes;
                }
                break;
            case 'L':
                /* turn on dereference.
                 * turn off no_dereference */
                mfu_copy_opts->dereference = 1;
                walk_opts->dereference = 1;
                mfu_copy_opts->no_dereference = 0;
                break;
            case 'P':
                /* turn on no_dereference.
                 * turn off dereference */
                mfu_copy_opts->no_dereference = 1;
                mfu_copy_opts->dereference = 0;
                walk_opts->dereference = 0;
                break;
            case 'p':
                mfu_copy_opts->preserve = true;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Preserving file attributes.");
                }
                break;
            case 's':
                mfu_copy_opts->direct = 1;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using O_DIRECT");
                }
                break;
            case 'S':
                mfu_copy_opts->sparse = 1;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using sparse file");
                }
                break;
            case 'R':
                mfu_progress_timeout = atoi(optarg);
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = MFU_LOG_NONE;
                /* since process won't be printed in quiet anyway,
                 * disable the algorithm to save some overhead */
                mfu_progress_timeout = 0;
                break;
            case 'h':
                usage = 1;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if(rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* check that we got a valid progress value */
    if (mfu_progress_timeout < 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Seconds in --progress must be non-negative: %d invalid", mfu_progress_timeout);
        }
        usage = 1;
    }

    /* If we need to print the usage
     * then do so before internal processing */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* Early check to avoid extra processing.
     * Will be further checked below. */
    if ((argc-optind) < 2) {
        MFU_LOG(MFU_LOG_ERR, "A source and destination path is needed: "
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    char** argpaths = (&argv[optind]);

#ifdef DAOS_SUPPORT
    /* Set up DAOS arguments, containers, dfs, etc. */
    rc = daos_setup(rank, argpaths, daos_args, mfu_src_file, mfu_dst_file);
    if (rc != 0) {
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }
    
    /* TODO add support for this */
    if (inputname && mfu_src_file->type == DAOS) {
        MFU_LOG(MFU_LOG_ERR, "--input is not supported with DAOS"
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }
#endif

    /* paths to walk come after the options */
    int numpaths = 0;
    int numpaths_src = 0;
    mfu_param_path* paths = NULL;

    if (optind < argc) {
        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */

        mfu_param_path_set_all(numpaths, (const char**)argpaths, paths);

        /* advance to next set of options */
        optind += numpaths;

        /* the last path is the destination path, all others are source paths */
        numpaths_src = numpaths - 1;
    }

    if (numpaths_src == 0) {
        if(rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "A source and destination path is needed: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        }

        mfu_param_path_free_all(numpaths, paths);
        mfu_free(&paths);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* last item in the list is the destination path */
    const mfu_param_path* destpath = &paths[numpaths - 1];

    /* Parse the source and destination paths. */
    int valid, copy_into_dir;
    mfu_param_path_check_copy(numpaths_src, paths, destpath, mfu_src_file, mfu_dst_file,
                              mfu_copy_opts->no_dereference, &valid, &copy_into_dir);
    mfu_copy_opts->copy_into_dir = copy_into_dir;

    /* exit job if we found a problem */
    if (!valid) {
        if(rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid src/dest paths provided. Exiting run: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        }
        mfu_param_path_free_all(numpaths, paths);
        mfu_free(&paths);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    if (inputname == NULL) {
        /* if daos is set to SRC then use daos_ functions on walk */
        mfu_flist_walk_param_paths(numpaths_src, paths, walk_opts, flist, mfu_src_file);
    } else {
        struct mfu_flist_skip_args skip_args;

        /* otherwise, read list of files from input, but then stat each one */
        mfu_flist input_flist = mfu_flist_new();
        mfu_flist_read_cache(inputname, input_flist);

        skip_args.numpaths = numpaths_src;
        skip_args.paths = paths;
        mfu_flist_stat(input_flist, flist, input_flist_skip, (void *)&skip_args,
                       walk_opts->dereference, mfu_src_file);
        mfu_flist_free(&input_flist);
    }

    /* copy flist into destination */ 
    rc = mfu_flist_copy(flist, numpaths_src, paths,
                            destpath, mfu_copy_opts, mfu_src_file,
                            mfu_dst_file);
    if (rc < 0) {
        /* hit some sort of error during copy */
        rc = 1;
    }

    /* free the file list */
    mfu_flist_free(&flist);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free the input file name */
    mfu_free(&inputname);

    /* free the copy options */
    mfu_copy_opts_delete(&mfu_copy_opts);

    /* free the copy options */
    mfu_walk_opts_delete(&walk_opts);

#ifdef DAOS_SUPPORT
    /* Cleanup DAOS-related variables, etc. */
    daos_cleanup(daos_args, mfu_src_file, mfu_dst_file);
#endif

    /* free the mfu_file object */
    mfu_file_delete(&mfu_src_file);
    mfu_file_delete(&mfu_dst_file);

    /* Alert the user if there were copy errors */
    if (rc != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "One or more errors were detected while copying: "
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_DCP_COPY));
        }
    }

    mfu_finalize();

    /* shut down MPI */
    MPI_Finalize();

    if (rc != 0) {
        return 1;
    }
    return 0;
}
