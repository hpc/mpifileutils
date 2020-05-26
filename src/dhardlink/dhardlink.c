#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h> /* asctime / localtime */

#include <stdarg.h> /* variable length args */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <assert.h>
#include <inttypes.h>
#include <string.h>
#include <math.h>

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"
#include "mfu_flist.h"

// getpwent getgrent to read user and group entries

/* keep stats during walk */
uint64_t total_dirs    = 0;
uint64_t total_files   = 0;
uint64_t total_links   = 0;
uint64_t total_unknown = 0;
uint64_t total_bytes   = 0;

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dwalk [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file>      - read list from file\n");
    printf("  -o, --output <file>     - write processed list to file in binary format\n");
    printf("  -t, --text              - use with -o; write processed list to file in ascii format\n");
    printf("  -l, --lite              - walk file system without stat\n");
    printf("  -p, --print             - print files to screen\n");
    printf("      --progress <N>      - print progress every N seconds\n");
    printf("  -v, --verbose           - verbose output\n");
    printf("  -q, --quiet             - quiet output\n");
    printf("  -h, --help              - print usage\n");
    printf("\n");
    printf("For more information see https://mpifileutils.readthedocs.io. \n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    int i;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    char* inputname      = NULL;
    char* outputname     = NULL;

    int walk                 = 0;
    int print                = 0;
    int text                 = 0;

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",          1, 0, 'i'},
        {"output",         1, 0, 'o'},
        {"text",           0, 0, 't'},
        {"lite",           0, 0, 'l'},
        {"print",          0, 0, 'p'},
        {"progress",       1, 0, 'P'},
        {"verbose",        0, 0, 'v'},
        {"quiet",          0, 0, 'q'},
        {"help",           0, 0, 'h'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:o:tlpvqh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = MFU_STRDUP(optarg);
                break;
            case 'o':
                outputname = MFU_STRDUP(optarg);
                break;
            case 'l':
                /* don't stat each file on the walk */
                walk_opts->use_stat = 0;
                break;
            case 'p':
                print = 1;
                break;
            case 'P':
                mfu_progress_timeout = atoi(optarg);
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = 0;
                break;
            case 't':
                text = 1;
                break;
            case 'h':
                usage = 1;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if (rank == 0) {
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

    /* paths to walk come after the options */
    int numpaths = 0;
    mfu_param_path* paths = NULL;
    if (optind < argc) {
        /* got a path to walk */
        walk = 1;

        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        char** p = &argv[optind];
        mfu_param_path_set_all((uint64_t)numpaths, (const char**)p, paths);
        optind += numpaths;

        /* don't allow user to specify input file with walk */
        if (inputname != NULL) {
            usage = 1;
        }
    }
    else {
        /* if we're not walking, we must be reading,
         * and for that we need a file */
        if (inputname == NULL) {
            usage = 1;
        }
    }

    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        MPI_Finalize();
        return 0;
    }

    /* create an empty file list with default values */
    mfu_flist flist = mfu_flist_new();

    if (walk) {
        /* walk list of input paths */
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist);
    }
    else {
        /* read data from cache file */
        mfu_flist_read_cache(inputname, flist);
    }

    /* create two empty lists, one to hold hardlinked files and one to hold other files */
    mfu_flist flist_hardlinks = mfu_flist_subset(flist);
    mfu_flist flist_no_hardlinks = mfu_flist_subset(flist);

    /* iterate over each item in our local list and copy to appropriate subset */
    uint64_t idx;
    uint64_t size = mfu_flist_size(flist);
    for (idx = 0; idx < size; idx++) {
        /* only consider regular files */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type != MFU_TYPE_FILE) {
            /* file is something other than regular file */
            continue;
        }

        /* get filename for item */
        const char* file = mfu_flist_file_get_name(flist, idx);

        /* stat item */
        struct stat statbuf;
        if (mfu_lstat(file, &statbuf) != 0) {
            /* ERROR: failed to stat file */
            continue;
        }

        if (statbuf.st_nlink > 1) {
            /* this file has more than one hardlink */
            mfu_flist_file_copy(flist, idx, flist_hardlinks);
        } else {
            /* this file has exactly one hardlink */
            mfu_flist_file_copy(flist, idx, flist_no_hardlinks);
        }
    }

    /* close out our subset lists */
    mfu_flist_summarize(flist_hardlinks);
    mfu_flist_summarize(flist_no_hardlinks);

    /* print details for hardlinked files */
    if (print) {
        mfu_flist_print(flist_hardlinks);
    }

    /* print summary statistics of hardlinked files */
    mfu_flist_print_summary(flist_hardlinks);
    mfu_flist_print_summary(flist_no_hardlinks);

    /* write data to cache file */
    if (outputname != NULL) {
        if (!text) {
            mfu_flist_write_cache(outputname, flist_hardlinks);
        } else {
            mfu_flist_write_text(outputname, flist_hardlinks);
        }
    }

    /* free users, groups, and files objects */
    mfu_flist_free(&flist_no_hardlinks);
    mfu_flist_free(&flist_hardlinks);
    mfu_flist_free(&flist);

    /* free memory allocated for options */
    mfu_free(&outputname);
    mfu_free(&inputname);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
