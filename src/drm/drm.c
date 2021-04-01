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

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#ifdef DAOS_SUPPORT
#include "mfu_daos.h"
#endif

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"
#include "mfu_errors.h"

/*****************************
 * Driver functions
 ****************************/

static void print_usage(void)
{
    printf("\n");
    printf("Usage: drm [options] <path> ...\n");
#ifdef DAOS_SUPPORT
    printf("\n");
    printf("DAOS paths can be specified as:\n");
    printf("       daos://<pool>/<cont>[/<path>] | <UNS path>\n");
#endif
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input   <file>   - read list from file\n");
    printf("  -o, --output <file>    - write list to file in binary format\n");
    printf("  -t, --text             - use with -o; write processed list to file in ascii format\n");
    printf("  -l, --lite             - walk file system without stat\n");
    printf("      --stat             - walk file system with stat\n");
    printf("      --exclude <regex>  - exclude from command entries that match the regex\n");
    printf("      --match   <regex>  - apply command only to entries that match the regex\n");
    printf("      --name             - change regex to apply to entry name rather than full pathname\n");
    printf("      --dryrun           - print out list of files that would be deleted\n");
    printf("      --aggressive       - aggressive mode deletes files during the walk. You CANNOT use dryrun with this option. \n");
    printf("  -T, --traceless        - remove child items without changing parent directory mtime\n");
    printf("      --progress <N>     - print progress every N seconds\n");
    printf("  -v, --verbose          - verbose output\n");
    printf("  -q, --quiet            - quiet output\n");
    printf("  -h, --help             - print usage\n");
    printf("\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    int i;
    int rc = 0;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* parse command line options */
    char* inputname  = NULL;
    char* outputname = NULL;
    char* regex_exp  = NULL;
    int walk         = 0;
    int exclude      = 0;
    int name         = 0;
    int dryrun       = 0;
    int traceless    = 0;
    int text         = 0;

#ifdef DAOS_SUPPORT
    /* DAOS vars */
    daos_args_t* daos_args = daos_args_new();
#endif

    /* with drm, we don't stat files on walk by default,
     * since that info is not needed to remove items and
     * avoiding the stat can significantly speed up the walk */
    walk_opts->use_stat = 0;

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",       1, 0, 'i'},
        {"output",      1, 0, 'o'},
        {"text",        0, 0, 't'},
        {"lite",        0, 0, 'l'},
        {"stat",        0, 0, 's'},
        {"exclude",     1, 0, 'e'},
        {"match",       1, 0, 'a'},
        {"name",        0, 0, 'n'},
        {"dryrun",      0, 0, 'd'},
        {"aggressive",  0, 0, 'A'},
        {"traceless",   0, 0, 'T'},
        {"progress",    1, 0, 'R'},
        {"verbose",     0, 0, 'v'},
        {"quiet",       0, 0, 'q'},
        {"help",        0, 0, 'h'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:o:tlTvqh",
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
            case 't':
                text = 1;
                break;
            case 'l':
                /* don't stat each file during the walk */
                walk_opts->use_stat = 0;
                break;
            case 's':
                /* stat each file during the walk */
                walk_opts->use_stat = 1;
                break;
            case 'e':
                regex_exp = MFU_STRDUP(optarg);
                exclude = 1;
                break;
            case 'a':
                regex_exp = MFU_STRDUP(optarg);
                exclude = 0;
                break;
            case 'n':
                name = 1;
                break;
            case 'd':
                dryrun = 1;
                break;
            case 'A':
                walk_opts->remove = 1;
                /* Turn on walk stat with aggressive option.
                 * Stating items will distribute the remove
                 * workload better in the case that there
                 * are lots (millions, etc) file in one
                 * directory. If stat is turned off in
                 * that case all the work will end up
                 * on one process */
                walk_opts->use_stat = 1;
                break;
            case 'T':
                traceless = 1;
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

    /* print usage if we need to */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* create new mfu_file objects */
    mfu_file_t* mfu_file = mfu_file_new();

    char** argpaths = &argv[optind];

    /* The remaining arguments are treated as paths */
    int numpaths = argc - optind;

    /* advance to next set of options */
    optind += numpaths;

#ifdef DAOS_SUPPORT
    /* Set up DAOS arguments, containers, dfs, etc. */
    rc = daos_setup(rank, argpaths, numpaths, daos_args, mfu_file, NULL);
    if (rc != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Detected one or more DAOS errors: "MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
        }
        rc = 1;
        goto daos_setup_done;
    }

    if (inputname && mfu_file->type == DFS) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "--input is not supported with DAOS"
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        }
        rc = 1;
        goto daos_setup_done;
    }

    /* Not yet supported */
    if (mfu_file->type == DAOS) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "drm only supports DAOS POSIX containers with the DFS API.");
        }
        rc = 1;
        goto daos_setup_done;
    }

daos_setup_done:
    if (rc != 0) {
        daos_cleanup(daos_args, mfu_file, NULL);
        mfu_file_delete(&mfu_file);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }
#endif

    mfu_param_path* paths = NULL;
    if (numpaths > 0) {
        /* got a path to walk */
        walk = 1;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        mfu_param_path_set_all(numpaths, (const char**)argpaths, paths, mfu_file, true);

        /* don't allow input file and walk */
        if (inputname != NULL) {
            usage = 1;
        }
    }
    else {
        /* if we're not walking, we must be reading,
         * and for that we need a file */
        if (inputname == NULL) {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Either a <path> or --input is required.");
            }
            usage = 1;
        }
    }

    /* check that user didn't ask for a dryrun, but then specify
     * an aggressive delete while walking */
    if (dryrun && walk && walk_opts->remove) {
        MFU_LOG(MFU_LOG_ERR, "Cannot perform dryrun with aggressive delete option.  Program is safely exiting.  Please do a dryrun then run an aggressive delete separately.  These two options cannot both be specified for the same program run.");
        usage = 1;
    }

    /* since we don't get a full list back from the walk when using
     * --agressive, we can't write a good output file */
    if (outputname != NULL && walk && walk_opts->remove) {
        MFU_LOG(MFU_LOG_ERR, "Cannot write output file with aggressive delete option.  Program is safely exiting.  These two options cannot both be specified for the same program run.");
        usage = 1;
    }

    /* print usage if we need to */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
#ifdef DAOS_SUPPORT
        daos_cleanup(daos_args, mfu_file, NULL);
#endif
        mfu_file_delete(&mfu_file);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    /* get our list of files, either by walking or reading an
     * input file */
    if (walk) {
        /* walk list of input paths */
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist, mfu_file);
    }
    else {
        /* read list from file */
        mfu_flist_read_cache(inputname, flist);
    }

    /* assume we'll use the full list */
    mfu_flist srclist = flist;

    /* filter the list if needed */
    mfu_flist filtered_flist = MFU_FLIST_NULL;
    if (regex_exp != NULL) {
        /* filter the list based on regex */
        filtered_flist = mfu_flist_filter_regex(flist, regex_exp, exclude, name);

        /* update our source list to use the filtered list instead of the original */
        srclist = filtered_flist;
    }

    /* only actually delete files if the user wasn't doing a dry run */
    if (dryrun) {
        /* just print what we would delete without actually doing anything,
         * this is useful if the user is trying to get a regex right */
        mfu_flist_print(srclist);
    } else {
        /* remove files */
        mfu_flist_unlink(srclist, traceless, mfu_file);
    }

    /* write data to cache file */
    if (outputname != NULL) {
        if (!text) {
            mfu_flist_write_cache(outputname, srclist);
        } else {
            mfu_flist_write_text(outputname, srclist);
        }
    }

#ifdef DAOS_SUPPORT
    daos_cleanup(daos_args, mfu_file, NULL);
#endif

    /* free list if it was used */
    if (filtered_flist != MFU_FLIST_NULL) {
        /* free the filtered flist (if any) */
        mfu_flist_free(&filtered_flist);
    }

    /* free the file list */
    mfu_flist_free(&flist);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free the regex string if we have one */
    mfu_free(&regex_exp);

    /* free the output file name */
    mfu_free(&outputname);

    /* free the input file name */
    mfu_free(&inputname);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* delete file objects */
    mfu_file_delete(&mfu_file);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return rc;
}
