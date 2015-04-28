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

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"

/* TODO: change globals to struct */
static int verbose   = 0;
static int walk_stat = 0;

/*****************************
 * Driver functions
 ****************************/

static void print_usage(void)
{
    printf("\n");
    printf("Usage: drm [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file>  - read list from file\n");
    printf("  -l, --lite          - walk file system without stat\n");
    printf("  -v, --verbose       - verbose output\n");
    printf("  -h, --help          - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    int i;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    bayer_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* parse command line options */
    char* inputname = NULL;
    int walk = 0;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",    1, 0, 'i'},
        {"lite",     0, 0, 'l'},
        {"help",     0, 0, 'h'},
        {"verbose",  0, 0, 'v'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:lhv",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = BAYER_STRDUP(optarg);
                break;
            case 'l':
                walk_stat = 0;
                break;
            case 'h':
                usage = 1;
                break;
            case 'v':
                verbose = 1;
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

    /* paths to walk come after the options */
    int numpaths = 0;
    bayer_param_path* paths = NULL;
    if (optind < argc) {
        /* got a path to walk */
        walk = 1;

        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (bayer_param_path*) BAYER_MALLOC((size_t)numpaths * sizeof(bayer_param_path));

        /* process each path */
        for (i = 0; i < numpaths; i++) {
            const char* path = argv[optind];
            bayer_param_path_set(path, &paths[i]);
            optind++;
        }

        /* don't allow input file and walk */
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

    /* print usage if we need to */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        bayer_finalize();
        MPI_Finalize();
        return 1;
    }

    /* initialize our sorting library */
    DTCMP_Init();

    /* create an empty file list */
    bayer_flist flist = bayer_flist_new();

    /* get our list of files, either by walking or reading an
     * input file */
    if (walk) {
        /* report walk count, time, and rate */
        double start_walk = MPI_Wtime();

        /* TODO: modify walk_path to accept an flist as input,
         * then walk each directory listed in flist */

        /* TODO: the code below will be reused elsewhere */

        /* allocate memory to hold a list of paths */
        const char** path_list = (char**) BAYER_MALLOC(numpaths * sizeof(char*));

        /* fill list of paths and print each one */
        for (i = 0; i < numpaths; i++) {
            /* get path for this step */
            const char* target = paths[i].path;

            /* print message to user that we're starting */
            if (verbose && rank == 0) {
                time_t walk_start_t = time(NULL);
                if (walk_start_t == (time_t)-1) {
                    /* TODO: ERROR! */
                }
                char walk_s[30];
                size_t rc = strftime(walk_s, sizeof(walk_s) - 1, "%FT%T", localtime(&walk_start_t));
                if (rc == 0) {
                    walk_s[0] = '\0';
                }
                printf("%s: Walking %s\n", walk_s, target);
            }

            /* record pointer to path in list */
            path_list[i] = target;
        }
        if (verbose && rank == 0) {
            fflush(stdout);
        }

        /* walk file tree and record stat data for each file */
        bayer_flist_walk_paths((uint64_t) numpaths, path_list, walk_stat, flist);

        /* free the list */
        bayer_free(&path_list);

        double end_walk = MPI_Wtime();

        /* report walk count, time, and rate */
        if (verbose && rank == 0) {
            uint64_t all_count = bayer_flist_global_size(flist);
            double time_diff = end_walk - start_walk;
            double rate = 0.0;
            if (time_diff > 0.0) {
                rate = ((double)all_count) / time_diff;
            }
            printf("Walked %lu files in %f seconds (%f files/sec)\n",
                   all_count, time_diff, rate
                  );
        }
    }
    else {
        /* read list from file */
        double start_read = MPI_Wtime();
        bayer_flist_read_cache(inputname, flist);
        double end_read = MPI_Wtime();

        /* report read count, time, and rate */
        if (verbose && rank == 0) {
            uint64_t all_count = bayer_flist_global_size(flist);
            double time_diff = end_read - start_read;
            double rate = 0.0;
            if (time_diff > 0.0) {
                rate = ((double)all_count) / time_diff;
            }
            printf("Read %lu files in %f seconds (%f files/sec)\n",
                   all_count, time_diff, rate
                  );
        }
    }

    /* remove files without setting writing bit on parent
     * directories, we expect this to clear most files */
    double start_remove = MPI_Wtime();
    bayer_flist_unlink(flist);
    double end_remove = MPI_Wtime();

    /* report remove count, time, and rate */
    if (verbose && rank == 0) {
        uint64_t all_count = bayer_flist_global_size(flist);
        double time_diff = end_remove - start_remove;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        printf("Removed %lu files in %f seconds (%f files/sec)\n",
               all_count, time_diff, rate
              );
    }

    /* free the file list */
    bayer_flist_free(&flist);

    /* shut down the sorting library */
    DTCMP_Finalize();

    /* free the path parameters */
    for (i = 0; i < numpaths; i++) {
        bayer_param_path_free(&paths[i]);
    }
    bayer_free(&paths);

    /* free the input file name */
    bayer_free(&inputname);

    /* shut down MPI */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
