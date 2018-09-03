#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sysinfo.h>
#include <string.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "mpi.h"
#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <utime.h>
#include <getopt.h>

#include "mfu.h"

/* All the options available */

static int opts_decompress = 0;
static int opts_compress   = 0;
static int opts_keep       = 0;
static int opts_force      = 0;
static ssize_t opts_memory = -1;
static int opts_blocksize  = 9;
static int opts_verbose    = 0;
static int opts_debug      = 0;

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dbz2 [options] <source>\n");
    printf("\n");
    printf("Options:\n");
    printf("  -c, --compress         - compress file\n");
    printf("  -d, --decompress       - decompress file\n");
    printf("  -k, --keep             - keep existing input file(s)\n");
    printf("  -f, --force            - overwrite output file\n");
    printf("  -b, --blocksize <num>  - block size (1-9)\n");
    printf("  -m, --memory <size>    - memory limit in bytes\n");
    printf("  -v, --verbose          - verbose output\n");
    printf("      --debug            - debug output\n");
    printf("  -h, --help             - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    int option_index = 0;
    static struct option long_options[] = {
        {"compress",   0, 0, 'c'},
        {"decompress", 0, 0, 'd'},
        {"keep",       0, 0, 'k'},
        {"force",      0, 0, 'f'},
        {"blocksize",  1, 0, 'b'},        
        {"memory",     1, 0, 'm'},
        {"verbose",    0, 0, 'v'},
        {"debug",      0, 0, 'y'},
        {"help",       0, 0, 'h'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "cdkfb:m:vyh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        unsigned long long bytes;
        switch (c) {
            case 'c':
                opts_compress = 1;
                break;
            case 'd':
                opts_decompress = 1;
                break;
            case 'k':
                opts_keep = 1;
                break;
            case 'f':
                opts_force = 1;
                break;
            case 'b':
                opts_blocksize = atoi(optarg);
                break;
            case 'm':
                mfu_abtoull(optarg, &bytes);
                opts_memory = (ssize_t) bytes;
                break;            
            case 'v':
                opts_verbose = 1;
                break;
            case 'y':
                opts_debug = 1;
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

    /* Set the log level to control the messages that will be printed */
    if (opts_debug) {
        mfu_debug_level = MFU_LOG_DBG;
    } else if (opts_verbose) {
        mfu_debug_level = MFU_LOG_INFO;
    } else {
        mfu_debug_level = MFU_LOG_ERR;
    }

    /* TODO: also bail if we can't find the file */
    /* print usage if we don't have a file name */
    if (argc != 2) {
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

    char fname[50];
    char fname_out[50];
    strncpy(fname,     argv[1], 50);
    strncpy(fname_out, argv[1], 50);

    if (opts_compress) {
        /* If file exists and we are not supposed to overwrite */
        struct stat st;
        if ((stat(strcat(argv[1], ".bz2"), &st) == 0) && (! opts_force)) {
            MFU_LOG(MFU_LOG_ERR, "Output file already exists\n");
            exit(0);
        }

        int b_size = (int)opts_blocksize;
        mfu_compress_bz2(b_size, fname_out, opts_memory);

        /* If we are to remove the input file */
        if (! opts_keep) {
            mfu_unlink(fname);
        }
    } else if (opts_decompress) {
        /* remove the trailing .bz2 from the string */
        size_t len = strlen(fname_out);
        fname_out[len - 4] = '\0';
        MFU_LOG(MFU_LOG_INFO, "The file name is:%s %s %d", fname, fname_out, (int)len);

        /* If file exists and we are not supposed to overwrite */
        struct stat st;
        if ((stat(fname_out, &st) == 0) && (! opts_force)) {
            MFU_LOG(MFU_LOG_ERR, "Output file already exisis\n");
            exit(0);
        }

        mfu_decompress_bz2(fname, fname_out);

        /* If we are to remove the input file */
        if (! opts_keep) {
            mfu_unlink(fname);
        }
    } else {
        MFU_LOG(MFU_LOG_ERR, "Must use either compression or decompression\n");
        return 1;
    }

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
