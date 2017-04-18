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

/* called by single process upon detection of a problem */
void DCOPY_abort(int code)
{
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

/* called globally by all procs to exit */
void DCOPY_exit(int code)
{
    /* CIRCLE_finalize or will this hang? */
    mfu_finalize();
    MPI_Finalize();
    exit(code);
}

/** Print a usage message. */
void DCOPY_print_usage(void)
{
    /* The compare option isn't really effective because it often
     * reads from the page cache and not the disk, which gives a
     * false sense of validation.  Also, it tends to thrash the
     * metadata server with lots of extra open/close calls.  Plan
     * is to delete it here, and rely on dcmp instead.  For now
     * we just hide it as an option. */

    printf("\n");
    printf("Usage: dcp [options] source target\n");
    printf("       dcp [options] source ... target_dir\n");
    printf("\n");
    printf("Options:\n");
    /* printf("  -c, --compare       - read data back after writing to compare\n"); */
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
    int c;
    int option_index = 0;

    MPI_Init(&argc, &argv);
    mfu_init();

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;
    mfu_debug_level = MFU_LOG_INFO;

    /* By default, don't bother to preserve all attributes. */
    DCOPY_user_opts.preserve = false;

    /* By default, don't use O_DIRECT. */
    DCOPY_user_opts.synchronous = false;

    /* By default, don't use sparse file. */
    DCOPY_user_opts.sparse = false;

    /* Set default chunk size */
    uint64_t chunk_size = (1*1024*1024);
    DCOPY_user_opts.chunk_size = chunk_size ;

    /* By default, don't have iput file. */
    DCOPY_user_opts.input_file = NULL;

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
    while((c = getopt_long(argc, argv, "d:g:hi:pusSv", \
                           long_options, &option_index)) != -1) {
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
                DCOPY_user_opts.grouplock_id = atoi(optarg);

                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "groulock ID: %d.",
                        DCOPY_user_opts.grouplock_id);
                }

                break;
#endif

            case 'i':
                DCOPY_user_opts.input_file = MFU_STRDUP(optarg);
                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using input list.");
                }
                break;

            case 'p':
                DCOPY_user_opts.preserve = true;

                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Preserving file attributes.");
                }

                break;

            case 's':
                DCOPY_user_opts.synchronous = true;

                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using synchronous read/write (O_DIRECT)");
                }

                break;

            case 'S':
                DCOPY_user_opts.sparse = true;

                if(rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using sparse file");
                }

                break;

            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;

                break;

            case 'h':
                if(rank == 0) {
                    DCOPY_print_usage();
                }

                DCOPY_exit(EXIT_SUCCESS);
                break;

            case '?':
            default:
                if(rank == 0) {
                    if(optopt == 'd') {
                        DCOPY_print_usage();
                        fprintf(stderr, "Option -%c requires an argument.\n", \
                                optopt);
                    }
                    else if(isprint(optopt)) {
                        DCOPY_print_usage();
                        fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                    }
                    else {
                        DCOPY_print_usage();
                        fprintf(stderr,
                                "Unknown option character `\\x%x'.\n",
                                optopt);
                    }
                }

                DCOPY_exit(EXIT_FAILURE);
                break;
        }
    }

    /** Parse the source and destination paths. */
    DCOPY_parse_path_args(argv, optind, argc);

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();
    if (DCOPY_user_opts.input_file == NULL) {
        /* walk paths and fill in file list */
        DCOPY_walk_paths(flist);
    } else {
        /* otherwise, read list of files from input, but then stat each one */
        mfu_flist input_flist = mfu_flist_new();
        mfu_flist_read_cache(DCOPY_user_opts.input_file, input_flist);
        mfu_flist_stat(input_flist, flist, DCOPY_input_flist_skip, NULL);
        mfu_flist_free(&input_flist);
    }

    /* copy flist into destination */ 
    mfu_flist_copy(flist, DCOPY_user_opts.preserve, 0); 
    
    /* free our file lists */
    mfu_flist_free(&flist);

    /* free memory allocated to parse user params */
    DCOPY_free_path_args();

    DCOPY_exit(EXIT_SUCCESS);
}

/* EOF */
