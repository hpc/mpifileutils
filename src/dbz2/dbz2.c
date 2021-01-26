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
    printf("  -k, --keep             - keep existing input file\n");
    printf("  -f, --force            - overwrite output file\n");
    printf("  -b, --blocksize <num>  - block size (1-9)\n");
    printf("  -v, --verbose          - verbose output\n");
    printf("  -q, --quiet            - quiet output\n");
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
        {"compress",   0, 0, 'z'},
        {"decompress", 0, 0, 'd'},
        {"keep",       0, 0, 'k'},
        {"force",      0, 0, 'f'},
        {"blocksize",  1, 0, 'b'},
        {"verbose",    0, 0, 'v'},
        {"quiet",      0, 0, 'q'},
        {"help",       0, 0, 'h'},
        {0, 0, 0, 0}
    };

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "zdkfb:vqh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        unsigned long long bytes;
        switch (c) {
            case 'z':
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
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = MFU_LOG_NONE;
                break;
            case 'y':
                mfu_debug_level = MFU_LOG_DBG;
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

    /* TODO: also bail if we can't find the file */
    /* print usage if we don't have a file name */
    if (argc - optind != 1) {
        usage = 1;
    }

    if (!usage && !opts_compress && !opts_decompress) {
        MFU_LOG(MFU_LOG_ERR, "Must use either compression or decompression");
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

    /* create new mfu_file object */    
    mfu_file_t* mfu_file = mfu_file_new();

    /* stat the input file name */
    int numpaths = 0;
    mfu_param_path* paths = NULL;
    if (optind < argc) {
        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        char** p = &argv[optind];
        mfu_param_path_set_all((uint64_t)numpaths, (const char**)p, paths, mfu_file, false);
        optind += numpaths;
    }

    /* check that file named on command line exists */
    if (! paths[0].path_stat_valid) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Input file does not exist: `%s'", paths[0].orig);
        }
        mfu_param_path_free_all(numpaths, paths);
        mfu_file_delete(&mfu_file);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    char* source_file = paths[0].path;

    /* generate target file name based on source file and operation */
    char fname_out[PATH_MAX];
    if (opts_compress) {
        /* generate source file namem with .dbz2 extension */
        strncpy(fname_out, source_file, sizeof(fname_out));
        strcat(fname_out, ".dbz2");
    } else {
        /* generate file name without .dbz2 extension */
        strncpy(fname_out, source_file, sizeof(fname_out));
        size_t len = strlen(fname_out);
        fname_out[len - 5] = '\0';
    }

    /* delete target file if --force thrown */
    if (opts_force) {
        if (rank == 0) {
            mfu_file_unlink(fname_out, mfu_file);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /* If file exists and we are not supposed to overwrite */
    int access_rc;
    if (rank == 0) {
        access_rc = mfu_file_access(fname_out, F_OK, mfu_file);
    }
    MPI_Bcast(&access_rc, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if ((access_rc == 0) && (! opts_force)) {
        MFU_LOG(MFU_LOG_ERR, "Output file already exisis: `%s'", fname_out);
        mfu_param_path_free_all(numpaths, paths);
        mfu_file_delete(&mfu_file);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* compress or decompress file */
    int rc;
    if (opts_compress) {
        int b_size = (int)opts_blocksize;
        rc = mfu_compress_bz2(source_file, fname_out, b_size);
    } else {
        rc = mfu_decompress_bz2(source_file, fname_out);
    }

    /* check whether we created target file */
    if (rc == MFU_SUCCESS) {
        /* created target file successfully,
         * now delete source file if --keep to thrown */
        if (! opts_keep) {
            if (rank == 0) {
                mfu_file_unlink(source_file, mfu_file);
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
    } else {
        /* failed to generate target file, so delete it */
        if (rank == 0) {
            mfu_file_unlink(fname_out, mfu_file);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);
    mfu_free(&paths);

    /* free the mfu_file object */
    mfu_file_delete(&mfu_file);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
