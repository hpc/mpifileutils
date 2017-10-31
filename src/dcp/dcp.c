/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2015, DataDirect Networks, Inc.
 * 
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/

#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>

#include "mpi.h"
#include "libcircle.h"
#include "mfu.h" 

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
    printf("  -i, --input <file>  - read source list from file\n");
    printf("  -p, --preserve      - preserve permissions, ownership, timestamps, extended attributes\n");
    printf("  -s, --synchronous   - use synchronous read/write calls (O_DIRECT)\n");
    printf("  -S, --sparse        - create sparse files when possible\n");
    printf("  -v, --verbose       - verbose output\n");
    printf("  -h, --help          - print usage\n");
    printf("\n");
    fflush(stdout);
}

int main(int argc, \
         char** argv)
{
    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* pointer to mfu_copy opts */
    mfu_copy_opts_t mfu_cp_opts; 
    mfu_copy_opts_t* mfu_copy_opts = &mfu_cp_opts; 
    
    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;
    mfu_debug_level = MFU_LOG_INFO;

    /* Set default chunk size */
    uint64_t chunk_size = (1*1024*1024);
    mfu_copy_opts->chunk_size = chunk_size ;

    /* By default, don't have iput file. */
    char* inputname = NULL;

    /* By default, don't bother to preserve all attributes. */
    mfu_copy_opts->preserve = 0;

    /* Lustre grouplock ID */
    mfu_copy_opts->grouplock_id; 

    /* By default, don't use O_DIRECT. */
    mfu_copy_opts->synchronous = 0;

    /* By default, don't use sparse file. */
    mfu_copy_opts->sparse = 0; 

    int option_index = 0;
    static struct option long_options[] = {
        {"debug"                , required_argument, 0, 'd'},
        {"grouplock"            , required_argument, 0, 'g'},
        {"input"                , required_argument, 0, 'i'},
        {"preserve"             , no_argument      , 0, 'p'},
        {"synchronous"          , no_argument      , 0, 's'},
        {"sparse"               , no_argument      , 0, 'S'},
        {"verbose"              , no_argument      , 0, 'v'},
        {"help"                 , no_argument      , 0, 'h'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    int usage = 0;
    while(1) {
        int c = getopt_long(
                    argc, argv, "d:g:hi:pusSv",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
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
                    MFU_LOG(MFU_LOG_INFO, "groulock ID: %d.",
                        mfu_copy_opts->grouplock_id);
                }
                break;
#endif
            case 'i':
                inputname = MFU_STRDUP(optarg);
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using input list.");
                }
                break;
            case 'p':
                mfu_copy_opts->preserve = 1;
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
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
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
        char** argpaths = &argv[optind];
        mfu_param_path_set_all(numpaths, argpaths, paths);

        /* advance to next set of options */
        optind += numpaths;
    
        /* the last path is the destination path, all others are source paths */
        numpaths_src = numpaths - 1;
    }

    if (numpaths_src == 0) {
        if(rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "No source path found, at least one");
            print_usage();
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
    mfu_param_path_check_copy(numpaths_src, paths, destpath, &valid, &copy_into_dir);
    mfu_copy_opts->copy_into_dir = copy_into_dir; 
    /* exit job if we found a problem */
    if(!valid) {
        if(rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Exiting run");
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
        /* walk paths and fill in file list */
        int walk_stat = 1;
        int dir_perm  = 0;

        mfu_flist_walk_param_paths(numpaths_src, paths, walk_stat, dir_perm, flist);
    } else {
        struct mfu_flist_skip_args skip_args;

        /* otherwise, read list of files from input, but then stat each one */
        mfu_flist input_flist = mfu_flist_new();
        mfu_flist_read_cache(inputname, input_flist);

        skip_args.numpaths = numpaths_src;
        skip_args.paths = paths;
        mfu_flist_stat(input_flist, flist, mfu_input_flist_skip, (void *)&skip_args);
        mfu_flist_free(&input_flist);
    }

    /* copy flist into destination */ 
    mfu_flist_copy(flist, numpaths_src, paths, destpath, mfu_copy_opts);
    
    /* free the file list */
    mfu_flist_free(&flist);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free the input file name */
    mfu_free(&inputname);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
