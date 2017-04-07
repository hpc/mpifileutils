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

#include "handle_args.h"

#include "mfu_flist.h" 

#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>

#if 0
static void DCOPY_reduce_init(void)
{
    int64_t agg_dirs   = DCOPY_statistics.total_dirs;
    int64_t agg_files  = DCOPY_statistics.total_files;
    int64_t agg_links  = DCOPY_statistics.total_links;
    int64_t agg_size   = DCOPY_statistics.total_size;
    int64_t agg_copied = DCOPY_statistics.total_bytes_copied;

    int64_t values[6];
    values[0] = agg_dirs + agg_files + agg_links;
    values[1] = agg_dirs;
    values[2] = agg_files;
    values[3] = agg_links;
    values[4] = agg_size;
    values[5] = agg_copied;

    CIRCLE_reduce(&values, sizeof(values));
}

static void DCOPY_reduce_op(const void* buf1, size_t size1, const void* buf2, size_t size2)
{
    int64_t values[6];

    const int64_t* a = (const int64_t*) buf1;
    const int64_t* b = (const int64_t*) buf2;

    int i;
    for (i = 0; i < 6; i++) {
        values[i] = a[i] + b[i];
    }

    CIRCLE_reduce(&values, sizeof(values));
}

static void DCOPY_reduce_fini(const void* buf, size_t size)
{
    const int64_t* a = (const int64_t*) buf;

    /* convert size to units */
    uint64_t agg_copied = (uint64_t) a[5];
    double agg_copied_tmp;
    const char* agg_copied_units;
    mfu_format_bytes(agg_copied, &agg_copied_tmp, &agg_copied_units);

    MFU_LOG(MFU_LOG_INFO,
        "Items created %" PRId64 ", Data copied %.3lf %s ...",
        a[0], agg_copied_tmp, agg_copied_units);
//        "Items %" PRId64 ", Dirs %" PRId64 ", Files %" PRId64 ", Links %" PRId64 ", Bytes %.3lf %s",
//        a[0], a[1], a[2], a[3], agg_size_tmp, agg_size_units);
}
#endif

static int64_t DCOPY_sum_int64(int64_t val)
{
    long long val_ull = (long long) val;
    long long sum;
    MPI_Allreduce(&val_ull, &sum, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    return (int64_t) sum;
}

/**
 * Print out information on the results of the file copy.
 */
static void DCOPY_epilogue(int rank)
{
    double rel_time = DCOPY_statistics.wtime_ended - \
                      DCOPY_statistics.wtime_started;
    int64_t agg_dirs   = DCOPY_sum_int64(DCOPY_statistics.total_dirs);
    int64_t agg_files  = DCOPY_sum_int64(DCOPY_statistics.total_files);
    int64_t agg_links  = DCOPY_sum_int64(DCOPY_statistics.total_links);
    int64_t agg_size   = DCOPY_sum_int64(DCOPY_statistics.total_size);
    int64_t agg_copied = DCOPY_sum_int64(DCOPY_statistics.total_bytes_copied);
    double agg_rate = (double)agg_copied / rel_time;

    if(rank == 0) {
        char starttime_str[256];
        struct tm* localstart = localtime(&(DCOPY_statistics.time_started));
        strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S", localstart);

        char endtime_str[256];
        struct tm* localend = localtime(&(DCOPY_statistics.time_ended));
        strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S", localend);

        int64_t agg_items = agg_dirs + agg_files + agg_links;

        /* convert size to units */
        double agg_size_tmp;
        const char* agg_size_units;
        mfu_format_bytes((uint64_t)agg_size, &agg_size_tmp, &agg_size_units);

        /* convert bandwidth to units */
        double agg_rate_tmp;
        const char* agg_rate_units;
        mfu_format_bw(agg_rate, &agg_rate_tmp, &agg_rate_units);

        MFU_LOG(MFU_LOG_INFO, "Started: %s", starttime_str);
        MFU_LOG(MFU_LOG_INFO, "Completed: %s", endtime_str);
        MFU_LOG(MFU_LOG_INFO, "Seconds: %.3lf", rel_time);
        MFU_LOG(MFU_LOG_INFO, "Items: %" PRId64, agg_items);
        MFU_LOG(MFU_LOG_INFO, "  Directories: %" PRId64, agg_dirs);
        MFU_LOG(MFU_LOG_INFO, "  Files: %" PRId64, agg_files);
        MFU_LOG(MFU_LOG_INFO, "  Links: %" PRId64, agg_links);
        MFU_LOG(MFU_LOG_INFO, "Data: %.3lf %s (%" PRId64 " bytes)",
            agg_size_tmp, agg_size_units, agg_size);

        MFU_LOG(MFU_LOG_INFO, "Rate: %.3lf %s " \
            "(%.3" PRId64 " bytes in %.3lf seconds)", \
            agg_rate_tmp, agg_rate_units, agg_copied, rel_time);
    }

    /* free memory allocated to parse user params */
    DCOPY_free_path_args();

    /* free file I/O buffer */
    mfu_free(&DCOPY_user_opts.block_buf2);
    mfu_free(&DCOPY_user_opts.block_buf1);

    return;
}

/**
 * Print a usage message.
 */
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
    int rank;

    MPI_Init(&argc, &argv);
    mfu_init();

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Initialize our processing library and related callbacks. */
    /* This is a bit of chicken-and-egg problem, because we'd like
     * to have our rank to filter output messages below but we might
     * also want to set different libcircle flags based on command line
     * options -- for now just pass in the default flags */
#if 0
    DCOPY_global_rank = CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&DCOPY_add_objects);
    CIRCLE_cb_process(&DCOPY_process_objects);
    CIRCLE_cb_reduce_init(&DCOPY_reduce_init);
    CIRCLE_cb_reduce_op(&DCOPY_reduce_op);
    CIRCLE_cb_reduce_fini(&DCOPY_reduce_fini);
#endif

    /* Initialize statistics */
    DCOPY_statistics.total_dirs  = 0;
    DCOPY_statistics.total_files = 0;
    DCOPY_statistics.total_links = 0;
    DCOPY_statistics.total_size  = 0;
    DCOPY_statistics.total_bytes_copied = 0;

    /* Initialize file cache */
    mfu_copy_src_cache.name = NULL;
    mfu_copy_dst_cache.name = NULL;

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

    /* Set default block size */
    DCOPY_user_opts.block_size = FD_BLOCK_SIZE;

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

    /* allocate buffer to read/write files, aligned on 1MB boundaraies */
    size_t alignment = 1024*1024;
    DCOPY_user_opts.block_buf1 = (char*) MFU_MEMALIGN(
        DCOPY_user_opts.block_size, alignment);
    DCOPY_user_opts.block_buf2 = (char*) MFU_MEMALIGN(
        DCOPY_user_opts.block_size, alignment);

    /* Grab a relative and actual start time for the epilogue. */
    time(&(DCOPY_statistics.time_started));
    DCOPY_statistics.wtime_started = MPI_Wtime();

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

    /* Determine the actual and relative end time for the epilogue. */
    DCOPY_statistics.wtime_ended = MPI_Wtime();
    time(&(DCOPY_statistics.time_ended));

    /* force updates to disk */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Syncing updates to disk.");
    }
    sync();

    /* Print the results to the user. */
    DCOPY_epilogue(rank);

    DCOPY_exit(EXIT_SUCCESS);
}

/* EOF */
