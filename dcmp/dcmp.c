#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <mpi.h>
#include <libcircle.h>
#include <linux/limits.h>
#include <libgen.h>
#include <errno.h>

/* for bool type, true/false macros */
#include <stdbool.h>

#include "bayer.h"

/* globals to hold user-input paths (only valid on rank 0) */
static bayer_param_path src_param_path;
static bayer_param_path dst_param_path;

/* Print a usage message */
static void print_usage()
{
    printf("\n");
    printf("Usage: dcmp [options] source target\n");
    printf("\n");
    printf("Options:\n");
    printf("  -h, --help  - print usage\n");
    printf("\n");
    fflush(stdout);
}

int main(int argc, char **argv)
{
    int c;

    /* initialize MPI and bayer libraries */
    MPI_Init(&argc, &argv);
    bayer_init();

    /* get our rank and number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* TODO: allow user to specify file lists as input files */

    /* TODO: three levels of comparison:
     *   1) file names only
     *   2) stat info + items in #1
     *   3) file contents + items in #2 */

    int option_index = 0;
    static struct option long_options[] = {
        {"help",     0, 0, 'h'},
        {0, 0, 0, 0}
    };

    /* read in command line options */
    int usage = 0;
    while (1) {
        int c = getopt_long(
            argc, argv, "h",
            long_options, &option_index
        );

        if (c == -1) {
            break;
        }

        switch (c) {
        case 'h':
        case '?':
            usage = 1;
            break;
        default:
            usage = 1;
            break;
        }
    }

    /* we should have two arguments left, source and dest paths */
    int numargs = argc - optind;
    if(numargs != 2) {
        BAYER_LOG(BAYER_LOG_ERR, "You must specify a source and destination path.");
        usage = 1;
    }

    /* print usage and exit if necessary */
    if (usage) {
        if (rank == 0) {
            print_usage(argv);
        }
        bayer_finalize();
        MPI_Finalize();
        return 1;
    }

    /* parse the source path */
    const char* srcpath = argv[optind];
    bayer_param_path_set(srcpath, &src_param_path);

    /* parse the destination path */
    const char* dstpath = argv[optind + 1];
    bayer_param_path_set(dstpath, &dst_param_path);

    /* allocate lists for source and destinations */
    bayer_flist flist1 = bayer_flist_new();
    bayer_flist flist2 = bayer_flist_new();

    /* walk source and destination paths */
    const char* path1 = src_param_path.path;
    bayer_flist_walk_path(path1, 0, flist1);

    const char* path2 = dst_param_path.path;
    bayer_flist_walk_path(path2, 0, flist2);

    /* TODO: do stuff ... */

    /* free file lists */
    bayer_flist_free(&flist1);
    bayer_flist_free(&flist2);

    /* free source and dest params */
    bayer_param_path_free(&src_param_path);
    bayer_param_path_free(&dst_param_path);

    /* shut down */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
