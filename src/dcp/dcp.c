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
#ifdef DAOS_SUPPORT
    printf("\n");
    printf("DAOS paths can be specified as:\n");
    printf("       daos://<pool>/<cont>[/<path>] | <UNS path>\n");
#endif
    printf("\n");
    printf("Options:\n");
    /* printf("  -d, --debug <level> - specify debug verbosity level (default info)\n"); */
#ifdef LUSTRE_SUPPORT
    /* printf("  -g, --grouplock <id> - use Lustre grouplock when reading/writing file\n"); */
#endif
    printf("  -b, --bufsize <SIZE>     - IO buffer size in bytes (default " MFU_BUFFER_SIZE_STR ")\n");
    printf("  -k, --chunksize <SIZE>   - work size per task in bytes (default " MFU_CHUNK_SIZE_STR ")\n");
    printf("  -X, --xattrs <OPT>       - copy xattrs (none, all, non-lustre, libattr)\n");
#ifdef DAOS_SUPPORT
    printf("      --daos-api           - DAOS API in {DFS, DAOS} (default uses DFS for POSIX containers)\n");
#ifdef HDF5_SUPPORT
    printf("      --daos-preserve      - preserve DAOS container properties and user attributes, a filename "
    					 "to write the metadata to is expected\n");
#endif
#endif
    printf("  -i, --input <file>       - read source list from file\n");
    printf("  -L, --dereference        - copy original files instead of links\n");
    printf("  -P, --no-dereference     - don't follow links in source\n");
    printf("  -p, --preserve           - preserve permissions, ownership, timestamps (see also --xattrs)\n");
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
        {"bufsize"              , required_argument, 0, 'b'},
        {"debug"                , required_argument, 0, 'd'}, // undocumented
        {"grouplock"            , required_argument, 0, 'g'}, // untested
        {"daos-prefix"          , required_argument, 0, 'Y'},
        {"daos-api"             , required_argument, 0, 'y'},
        {"daos-preserve"        , required_argument, 0, 'D'},
        {"input"                , required_argument, 0, 'i'},
        {"chunksize"            , required_argument, 0, 'k'},
        {"xattrs"               , required_argument, 0, 'X'},
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
                    argc, argv, "b:d:g:i:k:LPpsSvqhX:",
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
                    mfu_copy_opts->buf_size = (size_t)bytes;
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
            case 'X':
                mfu_copy_opts->copy_xattrs = parse_copy_xattrs_option(optarg);
                if (mfu_copy_opts->copy_xattrs == XATTR_COPY_INVAL) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Unrecognized option '%s' for --xattrs", optarg);
                    }
                    usage = 1;
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
            case 'Y':
                daos_args->dfs_prefix = MFU_STRDUP(optarg);
                break;
            case 'y':
                if (daos_parse_api_str(optarg, &daos_args->api) != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to parse --daos-api");
                    usage = 1;
                }
                break;
#ifdef HDF5_SUPPORT
            /* daos_preserve needs hdf5 support */
            case 'D':
                daos_args->daos_preserve      = true;
                daos_args->daos_preserve_path = MFU_STRDUP(optarg);
                break;
#endif
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
    
    /* The remaining arguments are treated as src/dst paths */
    int numpaths = argc - optind;

    /* advance to next set of options */
    optind += numpaths;

    /* Before processing, make sure we have at least two paths to begin with */
    if (numpaths < 2) {
        MFU_LOG(MFU_LOG_ERR, "A source and destination path is needed: "
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

#ifdef DAOS_SUPPORT
    /* Set up DAOS arguments, containers, dfs, etc. */
    if (daos_args->api != DAOS_API_HDF5) {
        rc = daos_setup(rank, argpaths, numpaths, daos_args, mfu_src_file, mfu_dst_file);
    }

#ifdef HDF5_SUPPORT
    /* if hdf5 API is specified, then h5repack is used */
    if (daos_args->api == DAOS_API_HDF5) {
        rc = mfu_daos_hdf5_copy(argpaths, daos_args);
        if (rc != 0) {
            rc = 1;
        }
        mfu_finalize();
        MPI_Finalize();
        return rc;
    }
#endif

    /* TODO add support for this */
    if (inputname && mfu_src_file->type == DFS) {
        MFU_LOG(MFU_LOG_ERR, "--input is not supported with DAOS"
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        rc = 1;
    }

    if (rc != 0) {
        daos_cleanup(daos_args, mfu_src_file, mfu_dst_file);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }
#endif

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    /* Perform a POSIX copy for non-DAOS types */
    if (mfu_src_file->type != DAOS && mfu_dst_file->type != DAOS) {
        /* allocate space for each path */
        mfu_param_path* paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* last item in the list is the destination path */
        mfu_param_path* destpath = &paths[numpaths - 1];

        /* Process the source and dest path individually for DFS. */
        if (mfu_src_file->type == DFS || mfu_dst_file->type == DFS) {
            mfu_param_path_set(argpaths[0], &paths[0], mfu_src_file, true);
            mfu_param_path_set(argpaths[1], &paths[1], mfu_dst_file, false);
        } else {
            mfu_param_path_set_all(numpaths-1, (const char**)argpaths, paths, mfu_src_file, true);
            mfu_param_path_set((const char*)(argpaths[numpaths-1]), destpath, mfu_dst_file, false);
        }

        /* Parse the source and destination paths. */
        int valid, copy_into_dir;

        /* the last path is the destination path, all others are source paths */
        int numpaths_src = numpaths - 1;
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
#ifdef DAOS_SUPPORT
            daos_cleanup(daos_args, mfu_src_file, mfu_dst_file);
#endif
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

        /* free the path parameters */
        mfu_param_path_free_all(numpaths, paths);

        /* free memory allocated to hold params */
        mfu_free(&paths);
    } 
#ifdef DAOS_SUPPORT
    /* Perform an object-level copy for DAOS types */
    else {
        /* take a snapshot and walk container to get list of objects,
         * returns epoch number of snapshot */
        int tmp_rc = mfu_daos_flist_walk(daos_args, daos_args->src_coh,
                                         &daos_args->src_epc, flist);
        if (tmp_rc != 0) {
            rc = 1;
            goto daos_cleanup;
        }

        /* Collectively copy all objects */
        tmp_rc = mfu_daos_flist_sync(daos_args, flist, false, true);
        if (tmp_rc != 0) {
            rc = 1;
            goto daos_cleanup;
        }

        /* Rank 0 prints success if needed */
        if (rc == 0 && rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Successfully copied to DAOS Destination Container.");
        }
    }

daos_cleanup:
    /* Cleanup DAOS-related variables, etc. */
    daos_cleanup(daos_args, mfu_src_file, mfu_dst_file);
#endif

    /* free the file list */
    mfu_flist_free(&flist);

    /* free the input file name */
    mfu_free(&inputname);

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
