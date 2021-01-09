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
#include "mfu_daos.h"
#endif

#include "mpi.h"
#include "libcircle.h"
#include "mfu.h"

#include "mfu_errors.h"

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

    char** argpaths = (&argv[optind]);

    /* var to keep track if this is a posix copy, defaults to true */
    bool is_posix_copy = true;

#ifdef DAOS_SUPPORT
    /* Set up DAOS arguments, containers, dfs, etc. */
    rc = daos_setup(rank, argpaths, daos_args, mfu_src_file, mfu_dst_file, &is_posix_copy);
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

    /* Early check to avoid extra processing.
     * Will be further checked below. */
    if ((argc-optind) < 2 && is_posix_copy) {
        MFU_LOG(MFU_LOG_ERR, "A source and destination path is needed: "
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

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

    if (numpaths_src == 0 && is_posix_copy) {
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

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    /* Parse the source and destination paths. */
    if (is_posix_copy) {
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

        /* perform POSIX copy */
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
    } 
#ifdef DAOS_SUPPORT
    else {
        daos_epoch_t epoch;
        if (rank == 0) {
            rc = daos_obj_list_oids(daos_args, &epoch, flist);
        }
	    if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS failed to list oids: ", MFU_ERRF,
                    MFU_ERRP(-MFU_ERR_DAOS));
            rc = 1;
        }

        /* evenly spread obj ids among each rank */
        mfu_flist_summarize(flist);
        mfu_flist newlist = mfu_flist_spread(flist);

        /* perform copy after oids are spread evenly across all ranks */
        flist_t* local_flist = (flist_t*) newlist;
        rc = daos_obj_copy(daos_args, local_flist);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS object copy failed: ", MFU_ERRF,
                    MFU_ERRP(-MFU_ERR_DAOS));
            rc = 1;
        }

        /* wait here until until all procs are done copying,
         * then destroy snapshot */
        MPI_Barrier(MPI_COMM_WORLD);

        /* destroy snapshot after copy */
        if (rank == 0) {
            daos_epoch_range_t epr;
            epr.epr_lo = epoch;
            epr.epr_hi = epoch;
            rc = daos_cont_destroy_snap(daos_args->src_coh, epr, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "DAOS destroy snapshot failed: ", MFU_ERRF,
                        MFU_ERRP(-MFU_ERR_DAOS));
                rc = 1;
            }
        }

	    /* free newlist that was created for non-posix copy */
	    mfu_flist_free(&newlist);
    }

    // Synchronize rc across all processes
    if (!mfu_alltrue(rc == 0, MPI_COMM_WORLD)) {
        rc = 1;
    }

    // Rank 0 prints success if needed
    if (rc == 0 && rank == 0 && !is_posix_copy) {
        MFU_LOG(MFU_LOG_INFO, "Successfully copied to DAOS Destination Container.");
    }

    /* Cleanup DAOS-related variables, etc. */
    daos_cleanup(daos_args, mfu_src_file, mfu_dst_file, &is_posix_copy);
#else
    MFU_LOG(MFU_LOG_ERR, "Expected POSIX copy.");
    rc = 1;
#endif

    /* free the copy options */
    mfu_copy_opts_delete(&mfu_copy_opts);

    /* free the copy options */
    mfu_walk_opts_delete(&walk_opts);

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
