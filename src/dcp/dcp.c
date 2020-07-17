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
#include <daos_fs.h>
#include <daos_uns.h>
#endif

#include "mpi.h"
#include "libcircle.h"
#include "mfu.h"

#ifdef DAOS_SUPPORT
static void daos_set_paths(
    char** argpaths,
    const char* dfs_prefix,
    uuid_t src_pool_uuid,
    uuid_t src_cont_uuid,
    uuid_t dst_pool_uuid,
    uuid_t dst_cont_uuid,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    int rc = 0;

    char* src_path = argpaths[0];
    char* dst_path = argpaths[1];

    /* find out if a dfs_prefix is being used,
     * if so, then that means that the container
     * is not being copied from the root of the
     * UNS path  */
    if (dfs_prefix == NULL) {
        /* assume both are daos paths for UNS, if resolve path
         * doesn't succeed then set accordingly */
        struct duns_attr_t src_dattr = {0}; 
        struct duns_attr_t dst_dattr = {0}; 
        int src_rc = duns_resolve_path(src_path, &src_dattr);
        int dst_rc = duns_resolve_path(dst_path, &dst_dattr);

        /* Forward slash is "root" of container to walk
         * in daos. Cannot walk from Unified namespace
         * path given /tmp/dsikich/dfs, it is only used
         * to lookup pool/cont uuids, and tells you
         * if that path is mapped to pool/cont uuid in
         * DAOS */
        if (src_rc == 0 && dst_rc == 0) {
            mfu_src_file->type = DAOS;
            mfu_dst_file->type = DAOS;
            uuid_copy(src_pool_uuid, src_dattr.da_puuid);
            uuid_copy(src_cont_uuid, src_dattr.da_cuuid);
            uuid_copy(dst_pool_uuid, dst_dattr.da_puuid);
            uuid_copy(dst_cont_uuid, dst_dattr.da_cuuid);
            argpaths[0] = "/";
            argpaths[1] = "/";
        } else if (src_rc == 0) { 
            mfu_src_file->type = DAOS;
            uuid_copy(src_pool_uuid, src_dattr.da_puuid);
            uuid_copy(src_cont_uuid, src_dattr.da_cuuid);
            argpaths[0] = "/";
        } else if (dst_rc == 0) {
            mfu_dst_file->type = DAOS;
            uuid_copy(dst_pool_uuid, dst_dattr.da_puuid);
            uuid_copy(dst_cont_uuid, dst_dattr.da_cuuid);
            argpaths[1] = "/";
        }

        /* set daos io functions for src/dst paths */
    } else {
        struct duns_attr_t dattr = {0}; 
        rc = duns_resolve_path(dfs_prefix, &dattr);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to resolve DAOS UNS path");
        }

        /* figure out if prefix is on dst or src for 
         * for copying container subsets */
        char* src_ret = strstr(src_path, dfs_prefix);
        if (src_ret != NULL) {
            mfu_src_file->type = DAOS;
            uuid_copy(src_pool_uuid, dattr.da_puuid);
            uuid_copy(src_cont_uuid, dattr.da_cuuid);
            argpaths[0] = src_path + strlen(dfs_prefix);
        } else {
            mfu_dst_file->type = DAOS;
            uuid_copy(dst_pool_uuid, dattr.da_puuid);
            uuid_copy(dst_cont_uuid, dattr.da_cuuid);
            argpaths[1] = dst_path + strlen(dfs_prefix);
        }
    }
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
    printf("  -b, --blocksize     - IO buffer size in bytes (default 1MB)\n");
    printf("      --daos-src-pool      - DAOS source pool \n");
    printf("      --daos-dst-pool      - DAOS destination pool \n");
    printf("      --daos-src-cont      - DAOS source container \n");
    printf("      --daos-dst-cont      - DAOS destination container \n");
    printf("      --daos-svcl          - DAOS service level \n");
    printf("      --daos-prefix        - DAOS prefix for unified namespace path \n");
    printf("  -i, --input <file>  - read source list from file\n");
    printf("  -k, --chunksize     - work size per task in bytes (default 1MB)\n");
    printf("  -p, --preserve      - preserve permissions, ownership, timestamps, extended attributes\n");
    printf("  -s, --synchronous   - use synchronous read/write calls (O_DIRECT)\n");
    printf("  -S, --sparse        - create sparse files when possible\n");
    printf("      --progress <N>  - print progress every N seconds\n");
    printf("  -v, --verbose       - verbose output\n");
    printf("  -q, --quiet         - quiet output\n");
    printf("  -h, --help          - print usage\n");
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
    daos_handle_t src_poh = DAOS_HDL_INVAL;
    daos_handle_t dst_poh = DAOS_HDL_INVAL;
    daos_handle_t src_coh = DAOS_HDL_INVAL;
    daos_handle_t dst_coh = DAOS_HDL_INVAL;
    dfs_t *dfs1           = NULL;
    dfs_t *dfs2           = NULL;
    char* svc             = NULL;
    char* dfs_prefix      = NULL;

    /* initalize value of DAOS UUID's to NULL with uuid_clear */
    uuid_t src_pool_uuid;
    uuid_t dst_pool_uuid;
    uuid_t src_cont_uuid;
    uuid_t dst_cont_uuid;
    uuid_clear(src_pool_uuid);
    uuid_clear(dst_pool_uuid);
    uuid_clear(src_cont_uuid);
    uuid_clear(dst_cont_uuid);
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
        {"daos-svcl"            , required_argument, 0, 'z'},
        {"daos-prefix"          , required_argument, 0, 'X'},
        {"input"                , required_argument, 0, 'i'},
        {"chunksize"            , required_argument, 0, 'k'},
        {"preserve"             , no_argument      , 0, 'p'},
        {"synchronous"          , no_argument      , 0, 's'},
        {"sparse"               , no_argument      , 0, 'S'},
        {"progress"             , required_argument, 0, 'P'},
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
                    argc, argv, "b:d:g:i:k:psSvqh",
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
                rc = uuid_parse(optarg, src_pool_uuid);
                if (rc != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse source pool uuid: '%s'", optarg);
                    }
                    usage = 1;
                }
                break;
            case 'D':
                rc = uuid_parse(optarg, dst_pool_uuid);
                if (rc != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse dst pool uuid: '%s'", optarg);
                    }
                    usage = 1;
                }
                break;
            case 'y':
                rc = uuid_parse(optarg, src_cont_uuid);
                if (rc != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse source cont uuid: '%s'", optarg);
                    }
                    usage = 1;
                }
                mfu_src_file->type = DAOS;
                break;
            case 'Y':
                rc = uuid_parse(optarg, dst_cont_uuid);
                if (rc != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse dst cont uuid: '%s'", optarg);
                    }
                    usage = 1;
                }
                mfu_dst_file->type = DAOS;
                break;
            case 'z':
                svc = MFU_STRDUP(optarg);
                break;
            case 'X':
                dfs_prefix = MFU_STRDUP(optarg);
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
            case 'p':
                mfu_copy_opts->preserve = true;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Preserving file attributes.");
                }
                break;
            case 's':
                mfu_copy_opts->synchronous = 1;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using synchronous read/write (O_DIRECT)");
                }
                break;
            case 'S':
                mfu_copy_opts->sparse = 1;
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using sparse file");
                }
                break;
            case 'P':
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

    char** argpaths = (&argv[optind]);

#ifdef DAOS_SUPPORT
    rc = daos_init();

    /* TODO: Don't exit and fail if daos fails init,
     * could just be regular paths */
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialize daos");
    }

    /* Figure out if daos path is the src or dst,
     * using UNS path, then chop off UNS path
     * prefix since the path is mapped to the root
     * of the container in the DAOS DFS mount */
    if (!daos_uuid_valid(src_pool_uuid) || !daos_uuid_valid(dst_pool_uuid)) {
        daos_set_paths(argpaths, dfs_prefix, src_pool_uuid, src_cont_uuid,
            dst_pool_uuid, dst_cont_uuid, mfu_src_file, mfu_dst_file);
    }

    /* check if DAOS source and destination containers are in the same pool */
    bool same_pool = false;
    if (mfu_src_file->type == DAOS && mfu_dst_file->type == DAOS) {
        if (uuid_compare(src_pool_uuid, dst_pool_uuid) == 0) {
            same_pool = true;
        }
    } 

    /* connect to DAOS source pool if uuid is valid */
    if (mfu_src_file->type == DAOS) {
        /* if DAOS source pool uuid is valid, then set source file type to DAOS */
        daos_connect(rank, svc, src_pool_uuid, src_cont_uuid, &src_poh, &src_coh); 
    }

    if (mfu_dst_file->type == DAOS) {
        if (daos_uuid_valid(dst_pool_uuid) && !same_pool) {
            /* if DAOS is the source and destination type, and containers are in different pools,
             * then connect to the second pool */
            daos_connect(rank, svc, dst_pool_uuid, dst_cont_uuid, &dst_poh, &dst_coh); 
        } else {
            /* if DAOS is source and destination type, and containers are in the same pool,
             * then pool is already connected, so we just need to open and/or create the container */
            if (rank == 0) {
                /* create container in same pool */
                daos_cont_info_t co_info;
                rc = daos_cont_open(src_poh, dst_cont_uuid, DAOS_COO_RW, &dst_coh, &co_info, NULL);

                /* If NOEXIST we create it */
                if (rc != 0) {
                    /* create the container */
                    uuid_t cuuid;
                    rc = dfs_cont_create(src_poh, cuuid, NULL, NULL, NULL);
                    if (rc != 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to create DFS2 container");
                    }

                    /* try to open it again */
                    rc = daos_cont_open(src_poh, cuuid, DAOS_COO_RW, &dst_coh, &co_info, NULL);
                    if (rc != 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to open DFS2 container");
                    }
                }
            }

            /* broadcast container handle from rank 0 */
            daos_bcast_handle(rank, &dst_coh, &src_poh, CONT_HANDLE);
        }
    }

    if (mfu_src_file->type == DAOS) {
        /* DFS is mounted for the source container */
        rc = dfs_mount(src_poh, src_coh, O_RDWR, &dfs1);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to mount DAOS filesystem (DFS)");
        }
    }

    if (mfu_dst_file->type == DAOS) {
        /* DFS is mounted for the destination container */
        if (same_pool) {
            rc = dfs_mount(src_poh, dst_coh, O_RDWR, &dfs2);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to mount DAOS filesystem (DFS)");
            }
        } else {
            rc = dfs_mount(dst_poh, dst_coh, O_RDWR, &dfs2);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to mount DAOS filesystem (DFS)");
            }
        }
    }

    /* set source and destination files to address of their
     * DFS mount within DAOS */
    mfu_src_file->dfs = dfs1;
    mfu_dst_file->dfs = dfs2;
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

        mfu_param_path_set_all(numpaths, argpaths, paths);

        /* advance to next set of options */
        optind += numpaths;

        /* the last path is the destination path, all others are source paths */
        numpaths_src = numpaths - 1;
    }

    if (usage || numpaths_src == 0) {
        if(rank == 0) {
            if (usage != 1) {
                MFU_LOG(MFU_LOG_ERR, "A source and destination path is needed");
            } else {
                print_usage();
            }
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
    mfu_param_path_check_copy(numpaths_src, paths, destpath, mfu_src_file, mfu_dst_file, &valid, &copy_into_dir);
    mfu_copy_opts->copy_into_dir = copy_into_dir; 

    /* exit job if we found a problem */
    if (!valid) {
        if(rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Invalid src/dest paths provided. Exiting run.\n");
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
        mfu_flist_stat(input_flist, flist, input_flist_skip, (void *)&skip_args);
        mfu_flist_free(&input_flist);
    }

    /* copy flist into destination */ 
    int tmp_rc = mfu_flist_copy(flist, numpaths_src, paths,
                                destpath, mfu_copy_opts, mfu_src_file,
                                mfu_dst_file);
    if (tmp_rc < 0) {
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

    /* DAOS: unmount DFS, and close containers and pools */
#ifdef DAOS_SUPPORT
    if (mfu_src_file->type == DAOS) {
        rc = dfs_umount(mfu_src_file->dfs);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to mount DFS namespace");
        }
        rc = daos_cont_close(src_coh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
        }
    }

    if (mfu_dst_file->type == DAOS) {
        rc = dfs_umount(mfu_dst_file->dfs);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed unmount DFS namespace");
        }
        rc = daos_cont_close(dst_coh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
        }
    }

    if (mfu_src_file->type == DAOS) {
        rc = daos_pool_disconnect(src_poh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to disconnect from source pool");
        }
    }

    if (mfu_dst_file->type == DAOS && !same_pool) {
        rc = daos_pool_disconnect(dst_poh, NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to disconnect from destination pool");
        }
    }
#endif

    /* free the mfu_file object */
    mfu_file_delete(&mfu_src_file);
    mfu_file_delete(&mfu_dst_file);

#ifdef DAOS_SUPPORT
    /* finalize daos */
    rc = daos_fini();
#endif

    mfu_finalize();

    /* shut down MPI */
    MPI_Finalize();

    return 0;
}
