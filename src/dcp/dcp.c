/* See the file "COPYING" for the full license governing this code. */

#include "dcp.h"

#include "handle_args.h"
#include "treewalk.h"
#include "copy.h"
#include "cleanup.h"
#include "compare.h"

#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <inttypes.h>

/** Options specified by the user. */
extern DCOPY_options_t DCOPY_user_opts;

/** Statistics to gather for summary output. */
extern DCOPY_statistics_t DCOPY_statistics;

/** Cache most recent open file descriptors. */
extern DCOPY_file_cache_t DCOPY_file_cache;

/*
 * A table of function pointers used for core operation. These functions
 * perform each stage of the file copy operation: "treewalk", "copy",
 * "cleanup", and "compare". File operations are usually passed through
 * all stages in a linear fashion unless a failure occurs. If a failure
 * occurs, the operation may be passed to a previous stage.
 */
extern void (*DCOPY_jump_table[5])(DCOPY_operation_t* op, \
                                   CIRCLE_handle* handle);

/**
 * The initial seeding callback for items to process on the distributed queue
 * structure. We send all of our source items to the queue here.
 */
static void DCOPY_add_objects(CIRCLE_handle* handle)
{
    DCOPY_enqueue_work_objects(handle);
}

/**
 * The process callback for items found on the distributed queue structure.
 */
static void DCOPY_process_objects(CIRCLE_handle* handle)
{
    char op[CIRCLE_MAX_STRING_LEN];
    /*
        const char* DCOPY_op_string_table[] = {
            "TREEWALK",
            "COPY",
            "CLEANUP",
            "COMPARE"
        };
    */

    /* Pop an item off the queue */
    handle->dequeue(op);
    DCOPY_operation_t* opt = DCOPY_decode_operation(op);

    /*
        BAYER_LOG(BAYER_LOG_DBG, "Performing operation `%s' on operand `%s' (`%d' remain on local queue).", \
            DCOPY_op_string_table[opt->code], opt->operand, handle->local_queue_size());
    */

    DCOPY_jump_table[opt->code](opt, handle);

    DCOPY_opt_free(&opt);
    return;
}

/* iterate through linked list of files and set ownership, timestamps, and permissions
 * starting from deepest level and working backwards */
static void DCOPY_set_metadata()
{
    const DCOPY_stat_elem_t* elem;

    if (DCOPY_global_rank == 0) {
        if(DCOPY_user_opts.preserve) {
            BAYER_LOG(BAYER_LOG_INFO, "Setting ownership, permissions, and timestamps.");
        }
        else {
            BAYER_LOG(BAYER_LOG_INFO, "Fixing permissions.");
        }
    }

    /* get max depth across all procs */
    int max_depth;
    int depth = -1;
    elem = DCOPY_list_head;
    while (elem != NULL) {
        if (elem->depth > depth) {
            depth = elem->depth;
        }
        elem = elem->next;
    }
    MPI_Allreduce(&depth, &max_depth, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    /* now set timestamps on files starting from deepest level */
    for (depth = max_depth; depth > 0; depth--) {
        /* cycle through our list of files and set timestamps
         * for each one at this level */
        elem = DCOPY_list_head;
        while (elem != NULL) {
            if (elem->depth == depth) {
                if(DCOPY_user_opts.preserve) {
                    DCOPY_copy_ownership(elem->sb, elem->file);
                    DCOPY_copy_permissions(elem->sb, elem->file);
                    DCOPY_copy_timestamps(elem->sb, elem->file);
                }
                else {
                    /* TODO: set permissions based on source permissons
                     * masked by umask */
                    DCOPY_copy_permissions(elem->sb, elem->file);
                }
            }
            elem = elem->next;
        }
        
        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }

    return;
}

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
static void DCOPY_epilogue(void)
{
    double rel_time = DCOPY_statistics.wtime_ended - \
                      DCOPY_statistics.wtime_started;
    int64_t agg_dirs   = DCOPY_sum_int64(DCOPY_statistics.total_dirs);
    int64_t agg_files  = DCOPY_sum_int64(DCOPY_statistics.total_files);
    int64_t agg_links  = DCOPY_sum_int64(DCOPY_statistics.total_links);
    int64_t agg_size   = DCOPY_sum_int64(DCOPY_statistics.total_size);
    int64_t agg_copied = DCOPY_sum_int64(DCOPY_statistics.total_bytes_copied);
    double agg_rate = (double)agg_copied / rel_time;

    if(DCOPY_global_rank == 0) {
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
        bayer_format_bytes((uint64_t)agg_size, &agg_size_tmp, &agg_size_units);

        /* convert bandwidth to units */
        double agg_rate_tmp;
        const char* agg_rate_units;
        bayer_format_bw(agg_rate, &agg_rate_tmp, &agg_rate_units);

        BAYER_LOG(BAYER_LOG_INFO, "Started: %s", starttime_str);
        BAYER_LOG(BAYER_LOG_INFO, "Completed: %s", endtime_str);
        BAYER_LOG(BAYER_LOG_INFO, "Seconds: %.3lf", rel_time);
        BAYER_LOG(BAYER_LOG_INFO, "Items: %" PRId64, agg_items);
        BAYER_LOG(BAYER_LOG_INFO, "  Directories: %" PRId64, agg_dirs);
        BAYER_LOG(BAYER_LOG_INFO, "  Files: %" PRId64, agg_files);
        BAYER_LOG(BAYER_LOG_INFO, "  Links: %" PRId64, agg_links);
        BAYER_LOG(BAYER_LOG_INFO, "Data: %.3lf %s (%" PRId64 " bytes)",
            agg_size_tmp, agg_size_units, agg_size);

        BAYER_LOG(BAYER_LOG_INFO, "Rate: %.3lf %s " \
            "(%.3" PRId64 " bytes in %.3lf seconds)", \
            agg_rate_tmp, agg_rate_units, agg_copied, rel_time);
    }

    /* free memory allocated to parse user params */
    DCOPY_free_path_args();

    /* free file I/O buffer */
    bayer_free(&DCOPY_user_opts.block_buf2);
    bayer_free(&DCOPY_user_opts.block_buf1);

    return;
}

/**
 * Print the current version.
 */
static void DCOPY_print_version()
{
    fprintf(stdout, "%s-%s\n", PACKAGE_NAME, PACKAGE_VERSION);
}

/**
 * Print a usage message.
 */
void DCOPY_print_usage(char** argv)
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
    printf("  -d, --debug <level> - specify debug verbosity level (default info)\n");
    printf("  -f, --force         - delete destination file if error on open\n");
    printf("  -p, --preserve      - preserve permissions, ownership, timestamps, extended attributes\n");
    printf("  -s, --synchronous   - use synchronous read/write calls (O_DIRECT)\n");
    printf("  -v, --version       - print version info\n");
    printf("  -h, --help          - print usage\n");
    printf("\n");
    printf("Level: dbg,info,warn,err,fatal\n");
    printf("\n");
    fflush(stdout);
}

int main(int argc, \
         char** argv)
{
    int c;
    int option_index = 0;

    MPI_Init(&argc, &argv);
    bayer_init();

    /* Initialize our processing library and related callbacks. */
    /* This is a bit of chicken-and-egg problem, because we'd like
     * to have our rank to filter output messages below but we might
     * also want to set different libcircle flags based on command line
     * options -- for now just pass in the default flags */
    DCOPY_global_rank = CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&DCOPY_add_objects);
    CIRCLE_cb_process(&DCOPY_process_objects);

    /* Initialize statistics */
    DCOPY_statistics.total_dirs  = 0;
    DCOPY_statistics.total_files = 0;
    DCOPY_statistics.total_links = 0;
    DCOPY_statistics.total_size  = 0;
    DCOPY_statistics.total_bytes_copied = 0;

    /* Initialize file cache */
    DCOPY_src_cache.name  = NULL;
    DCOPY_dst_cache.name = NULL;

    /* By default, skip the compare option. */
    DCOPY_user_opts.compare = false;

    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;
    bayer_debug_level = BAYER_LOG_INFO;

    /* By default, don't unlink destination files if an open() fails. */
    DCOPY_user_opts.force = false;

    /* By default, don't bother to preserve all attributes. */
    DCOPY_user_opts.preserve = false;

    /* By default, assume the filesystem is reliable (exit on errors). */
    DCOPY_user_opts.reliable_filesystem = true;

    /* By default, don't use O_DIRECT. */
    DCOPY_user_opts.synchronous = false;

    /* Set default chunk size */
    DCOPY_user_opts.chunk_size = DCOPY_CHUNK_SIZE;

    /* Set default block size */
    DCOPY_user_opts.block_size = FD_BLOCK_SIZE;

    static struct option long_options[] = {
        {"compare"              , no_argument      , 0, 'c'},
        {"debug"                , required_argument, 0, 'd'},
        {"force"                , no_argument      , 0, 'f'},
        {"help"                 , no_argument      , 0, 'h'},
        {"preserve"             , no_argument      , 0, 'p'},
        {"unreliable-filesystem", no_argument      , 0, 'u'},
        {"synchronous"          , no_argument      , 0, 's'},
        {"version"              , no_argument      , 0, 'v'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    while((c = getopt_long(argc, argv, "cd:fhpusv", \
                           long_options, &option_index)) != -1) {
        switch(c) {

            case 'c':
                DCOPY_user_opts.compare = true;

                if(DCOPY_global_rank == 0) {
                    BAYER_LOG(BAYER_LOG_INFO, "Compare source and destination " \
			"after copy to detect corruption.");
                }

                break;

            case 'd':

                if(strncmp(optarg, "fatal", 5) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_FATAL;
                    bayer_debug_level = BAYER_LOG_FATAL;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: fatal");
                    }

                }
                else if(strncmp(optarg, "err", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_ERR;
                    bayer_debug_level = BAYER_LOG_ERR;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: errors");
                    }

                }
                else if(strncmp(optarg, "warn", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN;
                    bayer_debug_level = BAYER_LOG_WARN;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: warnings");
                    }

                }
                else if(strncmp(optarg, "info", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN; /* we back off a level on CIRCLE verbosity */
                    bayer_debug_level = BAYER_LOG_INFO;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: info");
                    }

                }
                else if(strncmp(optarg, "dbg", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_DBG;
                    bayer_debug_level = BAYER_LOG_DBG;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: debug");
                    }

                }
                else {
                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level `%s' not recognized. " \
                            "Defaulting to `info'.", optarg);
                    }
                }

                break;

            case 'f':
                DCOPY_user_opts.force = true;

                if(DCOPY_global_rank == 0) {
                    BAYER_LOG(BAYER_LOG_INFO, "Deleting destination on errors.");
                }

                break;

            case 'h':

                if(DCOPY_global_rank == 0) {
                    DCOPY_print_usage(argv);
                }

                DCOPY_exit(EXIT_SUCCESS);
                break;

            case 'p':
                DCOPY_user_opts.preserve = true;

                if(DCOPY_global_rank == 0) {
                    BAYER_LOG(BAYER_LOG_INFO, "Preserving file attributes.");
                }

                break;

            case 'u':
                DCOPY_user_opts.reliable_filesystem = false;

                if(DCOPY_global_rank == 0) {
                    BAYER_LOG(BAYER_LOG_INFO, "Unreliable filesystem specified. " \
                        "Retry mode enabled.");
                }

                break;

            case 's':
                DCOPY_user_opts.synchronous = true;

                if(DCOPY_global_rank == 0) {
                    BAYER_LOG(BAYER_LOG_INFO, "Using synchronous read/write (O_DIRECT)");
                }

                break;

            case 'v':

                if(DCOPY_global_rank == 0) {
                    DCOPY_print_version();
                }

                DCOPY_exit(EXIT_SUCCESS);
                break;

            case '?':
            default:

                if(DCOPY_global_rank == 0) {
                    if(optopt == 'd') {
                        DCOPY_print_usage(argv);
                        fprintf(stderr, "Option -%c requires an argument.\n", \
                                optopt);
                    }
                    else if(isprint(optopt)) {
                        DCOPY_print_usage(argv);
                        fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                    }
                    else {
                        DCOPY_print_usage(argv);
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

    /* initialize linked list of stat objects */
    DCOPY_list_head  = NULL;
    DCOPY_list_tail  = NULL;

    /* Initialize our jump table for core operations. */
    DCOPY_jump_table[TREEWALK] = DCOPY_do_treewalk;
    DCOPY_jump_table[COPY]     = DCOPY_do_copy;
    DCOPY_jump_table[CLEANUP]  = DCOPY_do_cleanup;
    DCOPY_jump_table[COMPARE]  = DCOPY_do_compare;

    /* allocate buffer to read/write files, aligned on 1MB boundaraies */
    size_t alignment = 1024*1024;
    DCOPY_user_opts.block_buf1 = (char*) BAYER_MEMALIGN(
        DCOPY_user_opts.block_size, alignment);
    DCOPY_user_opts.block_buf2 = (char*) BAYER_MEMALIGN(
        DCOPY_user_opts.block_size, alignment);

    /* Set the log level for the processing library. */
    CIRCLE_enable_logging(CIRCLE_debug);

    /* Grab a relative and actual start time for the epilogue. */
    time(&(DCOPY_statistics.time_started));
    DCOPY_statistics.wtime_started = MPI_Wtime();

    /* Perform the actual file copy. */
    CIRCLE_begin();

    /* Determine the actual and relative end time for the epilogue. */
    DCOPY_statistics.wtime_ended = MPI_Wtime();
    time(&(DCOPY_statistics.time_ended));

    /* Let the processing library cleanup. */
    CIRCLE_finalize();

    /* close files */
    DCOPY_close_file(&DCOPY_src_cache);
    DCOPY_close_file(&DCOPY_dst_cache);

    /* set permissions, ownership, and timestamps if needed */
    DCOPY_set_metadata();

    /* force updates to disk */
    if (DCOPY_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_INFO, "Syncing updates to disk.");
    }
    sync();

    /* free list of stat objects */
    DCOPY_stat_elem_t* current = DCOPY_list_head;
    while (current != NULL) {
        DCOPY_stat_elem_t* next = current->next;
        free(current->file);
        free(current->sb);
        free(current);
        current = next;
    }

    /* Print the results to the user. */
    DCOPY_epilogue();

    DCOPY_exit(EXIT_SUCCESS);
}

/* EOF */
