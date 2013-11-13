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

/* globals to hold user-input paths */
static bayer_param_path param1;
static bayer_param_path param2;

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

/* Encode the file into a buffer, if the buffer is NULL, return the needed size */
size_t encode(char *buffer, bayer_flist list, uint64_t index)
{
    const char* name = bayer_flist_file_get_name(list, index);
    bayer_filetype type = bayer_flist_file_get_type(list, index);
    size_t count = strlen(name) + 2;

    if (buffer == NULL)
        return count;

    if (type == BAYER_TYPE_DIR) {
        buffer[0] = 'd';
    } else if (type == BAYER_TYPE_FILE || type == BAYER_TYPE_LINK) {
        buffer[0] = 'f';
    } else {
        buffer[0] = 'u';
    }
    strcpy(&buffer[1], name);

    return count;
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
    const char* usrpath1 = argv[optind];
    bayer_param_path_set(usrpath1, &param1);

    /* parse the destination path */
    const char* usrpath2 = argv[optind + 1];
    bayer_param_path_set(usrpath2, &param2);

    /* allocate lists for source and destinations */
    bayer_flist flist1 = bayer_flist_new();
    bayer_flist flist2 = bayer_flist_new();

    /* walk source and destination paths */
    const char* path1 = param1.path;
    bayer_flist_walk_path(path1, 0, flist1);

    const char* path2 = param2.path;
    bayer_flist_walk_path(path2, 0, flist2);

    size_t recvbytes1;
    char* recvbuf1;
    recvbytes1 = bayer_flist_distribute_map(flist1, &recvbuf1, encode);

    char* recvbuf2;
    size_t recvbytes2;
    recvbytes2 = bayer_flist_distribute_map(flist2, &recvbuf2, encode);

    /* TODO: do stuff ... */

    /* free buffers */
    bayer_free(&recvbuf1);
    bayer_free(&recvbuf2);

    /* free file lists */
    bayer_flist_free(&flist1);
    bayer_flist_free(&flist2);

    /* free source and dest params */
    bayer_param_path_free(&param1);
    bayer_param_path_free(&param2);

    /* shut down */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
