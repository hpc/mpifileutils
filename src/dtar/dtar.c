/**
 * @file dtar.c - parallel tar main file
 *
 * @author - Feiyi Wang
 *
 *
 */

#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <mpi.h>
#include <libcircle.h>
#include <archive.h>
#include <archive_entry.h>
#include <string.h>
#include <getopt.h>

#include "mfu.h"
#include "mfu_flist_archive.h"

/* common structures */

typedef struct {
    size_t  chunk_size;
    size_t  block_size;
    char*   dest_path;
    bool    preserve;
    int     flags;
} mfu_flist_archive_options_t;

/* The opts_blocksize option is optional and is used to specify
 * a blocksize for compression.
 * The opts_memory option is optional and is used to specify
 * memory limit for compression for each process in compression.
 * The opts_compress option is used to specify whether
 * compression/decompression should be used */

static int     opts_create    = 0;
static int     opts_compress  = 0;
static int     opts_verbose   = 0;
static int     opts_debug     = 0;
static int     opts_extract   = 0;
static int     opts_preserve  = 0;
static char*   opts_tarfile   = NULL;
static size_t  opts_chunksize = 1024 * 1024;
static int     opts_blocksize = 9;
static ssize_t opts_memory    = -1;

static void DTAR_abort(int code)
{
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

static void DTAR_exit(int code)
{
    mfu_finalize();
    MPI_Finalize();
    exit(code);
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dtar [options] <source ...>\n");
    printf("\n");
    printf("Options:\n");
    printf("  -c, --create            - create archive\n");
    printf("  -j, --compress          - compress archive\n");
    printf("  -x, --extract           - extract archive\n");
    printf("  -p, --preserve          - preserve attributes\n");
    printf("  -s, --chunksize <bytes> - chunk size (bytes)\n");
    printf("  -f, --file <filename>   - target output file\n");
    printf("  -b, --blocksize <size>  - block size (1-9)\n");
    printf("  -m, --memory <bytes>    - memory limit (bytes)\n");
    printf("  -v, --verbose           - verbose output\n");
    printf("  -y, --debug             - debug output\n");
    printf("  -h, --help              - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    mfu_init();

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size > 1) {
        if (rank == 0) {
            printf(“ERROR: dtar only works with one MPI rank.\n”);
        }
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    int option_index = 0;
    static struct option long_options[] = {
        {"create",    0, 0, 'c'},
        {"compress",  0, 0, 'j'},
        {"extract",   0, 0, 'x'},
        {"preserve",  0, 0, 'p'},
        {"chunksize", 1, 0, 's'},        
        {"file",      1, 0, 'f'},
        {"blocksize", 1, 0, 'b'},
        {"memory",    1, 0, 'm'},
        {"verbose",   0, 0, 'v'},
        {"debug",     0, 0, 'y'},
        {"help",      0, 0, 'h'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "cjxps:f:b:m:vyh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        unsigned long long bytes;
        switch (c) {
            case 'c':
                opts_create = 1;
                break;
            case 'j':
                opts_compress = 1;
                break;
            case 'x':
                opts_extract = 1;
                break;
            case 'p':
                opts_preserve = 1;
                break;
            case 's':
                mfu_abtoull(optarg, &bytes);
                opts_chunksize = (size_t) bytes;
                break;
            case 'f':
                opts_tarfile = MFU_STRDUP(optarg);
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
            case 'd':
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
                usage = 1;
        }
    }

    /* print usage if we need to */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }

    if (opts_debug) {
        mfu_debug_level = MFU_LOG_DBG;
    } else if (opts_verbose) {
        mfu_debug_level = MFU_LOG_INFO;
    } else {
        mfu_debug_level = MFU_LOG_ERR;
    }

    if (!opts_create && !opts_extract && rank == 0) {
        MFU_LOG(MFU_LOG_ERR, "One of extract(x) or create(c) need to be specified");
        DTAR_exit(EXIT_FAILURE);
    }

    if (opts_create && opts_extract && rank == 0) {
        MFU_LOG(MFU_LOG_ERR, "Only one of extraction(x) or create(c) can be specified");
        DTAR_exit(EXIT_FAILURE);
    }

    /* when creating a tarbll, we require a file name */
    if (opts_create && opts_tarfile == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Must specify a file name(-f)");
        DTAR_exit(EXIT_FAILURE);
    }

    /* done by default */
    mfu_archive_options_t archive_opts;
    archive_opts.flags  = ARCHIVE_EXTRACT_TIME;
    archive_opts.flags |= ARCHIVE_EXTRACT_OWNER;
    archive_opts.flags |= ARCHIVE_EXTRACT_PERM;
    archive_opts.flags |= ARCHIVE_EXTRACT_ACL;
    archive_opts.flags |= ARCHIVE_EXTRACT_FFLAGS;

    if (opts_preserve) {
        archive_opts.flags |= ARCHIVE_EXTRACT_XATTR;
        archive_opts.preserve = 1;
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Creating archive with extended attributes");
        }
    }

    archive_opts.chunk_size = opts_chunksize;

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Chunk size = %" PRIu64, archive_opts.chunk_size);
    }

    /* adjust pointers to start of paths */
    int numpaths = argc - optind;
    const char** pathlist = (const char**) &argv[optind];

    if (opts_create) {
        /* allocate space to record info for each source */
        int num_src_params = numpaths;
        size_t src_params_bytes = ((size_t) num_src_params) * sizeof(mfu_param_path);
        mfu_param_path* src_params = (mfu_param_path*) MFU_MALLOC(src_params_bytes);
    
        /* process each source path */
        mfu_param_path_set_all(numpaths, pathlist, src_params);
    
        /* standardize destination path */
        mfu_param_path dest_param;
        mfu_param_path_set(opts_tarfile, &dest_param);
    
        /* check that source and destination are okay */
        int valid;
        mfu_param_path_check_archive(num_src_params, src_params, dest_param, &valid);

        /* walk path to get stats info on all files */
        mfu_flist flist = mfu_flist_new();
        mfu_flist_walk_param_paths(num_src_params, src_params, walk_opts, flist);

        /* create the archive file */
        mfu_flist_archive_create(flist, opts_tarfile, &archive_opts);

        /* compress archive file */
        if (opts_compress) {
            struct stat st;
            char fname[50];
            char fname1[50];
            strncpy(fname1, opts_tarfile, 50);
            strncpy(fname, opts_tarfile, 50);
            if ((stat(strcat(fname, ".bz2"), &st) == 0)) {
                if (rank == 0) {
                    printf("Output file already exists\n");
                }
                exit(0);
            }
            //dbz2_compress(opts_blocksize, opts_tarfile, opts_memory);
            remove(fname1);
        }

        /* free the file list */
        mfu_flist_free(&flist);

        /* free paths */
        mfu_param_path_free_all(num_src_params, src_params);
        mfu_param_path_free(&dest_param);
    } else if (opts_extract) {
        char* tarfile = opts_tarfile;
        if (opts_compress) {
            char fname[50];
            char fname_out[50];
            strncpy(fname, opts_tarfile, 50);
            strncpy(fname_out, opts_tarfile, 50);
            size_t len = strlen(fname_out);
            fname_out[len - 4] = '\0';
            printf("The file name is:%s %s %d", fname, fname_out, (int)len);
            struct stat st;
            if ((stat(fname_out, &st) == 0)) {
                if (rank == 0) {
                    printf("Output file already exists\n");
                }
                exit(0);
            }
            //decompress(fname, fname_out);
            remove(fname);
            tarfile = fname_out;
        }
        mfu_flist_archive_extract(tarfile, opts_verbose, archive_opts.flags);
    } else {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Neither creation or extraction is specified");
            DTAR_exit(EXIT_FAILURE);
        }
    }

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* free context */
    mfu_free(&opts_tarfile);

    DTAR_exit(EXIT_SUCCESS);
}
