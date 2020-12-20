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
#include <string.h>
#include <getopt.h>

#include "mfu.h"

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
//    printf("  -j, --compress          - compress archive\n");
    printf("  -x, --extract           - extract archive\n");
//    printf("  -p, --preserve          - preserve attributes\n");
    printf("  -b, --blocksize <SIZE>  - IO buffer size in bytes (default " MFU_BLOCK_SIZE_STR ")\n");
    printf("  -k, --chunksize <SIZE>  - work size per task in bytes (default " MFU_CHUNK_SIZE_STR ")\n");
    printf("  -f, --file <filename>   - target output file\n");
//    printf("  -m, --memory <bytes>    - memory limit (bytes)\n");
    printf("      --progress <N>      - print progress every N seconds\n");
//    printf("  -v, --verbose           - verbose output\n");
    printf("  -q, --quiet             - quiet output\n");
    printf("  -h, --help              - print usage\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    mfu_init();

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* pointer to mfu_file src object */
    mfu_file_t* mfu_src_file = mfu_file_new();

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* allocate options to configure archive operation */
    mfu_archive_opts_t* archive_opts = mfu_archive_opts_new();

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    int     opts_create   = 0;
    int     opts_extract  = 0;
    int     opts_compress = 0;
    char*   opts_tarfile  = NULL;
    ssize_t opts_memory   = -1;

    int option_index = 0;
    static struct option long_options[] = {
        {"create",    0, 0, 'c'},
        //{"compress",  0, 0, 'j'},
        {"extract",   0, 0, 'x'},
        {"preserve",  0, 0, 'p'},
        {"blocksize", 1, 0, 'b'},
        {"chunksize", 1, 0, 'k'},
        {"file",      1, 0, 'f'},
        //{"memory",    1, 0, 'm'},
        {"progress",  1, 0, 'R'},
        {"verbose",   0, 0, 'v'},
        {"quiet",     0, 0, 'q'},
        {"help",      0, 0, 'h'},
        {0, 0, 0, 0}
    };

    /* Parse options */
    unsigned long long bytes = 0;
    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "cxpb:k:f:vyh",
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
                archive_opts->preserve = true;
                break;
            case 'b':
                if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR,
                                "Failed to parse block size: '%s'", optarg);
                    }
                    usage = 1;
                } else {
                    archive_opts->block_size = (size_t) bytes;
                }
                break;
            case 'k':
                if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR,
                                "Failed to parse chunk size: '%s'", optarg);
                    }
                    usage = 1;
                } else {
                    archive_opts->chunk_size = (size_t) bytes;
                }
                break;
            case 'f':
                opts_tarfile = MFU_STRDUP(optarg);
                break;
            case 'm':
                mfu_abtoull(optarg, &bytes);
                opts_memory = (ssize_t) bytes;
                break;
            case 'R':
                mfu_progress_timeout = atoi(optarg);
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
                usage = 1;
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
        return 0;
    }

    if (!opts_create && !opts_extract) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "One of extract(x) or create(c) needs to be specified");
        }
        DTAR_exit(EXIT_FAILURE);
    }

    if (opts_create && opts_extract) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Only one of extraction(x) or create(c) can be specified");
        }
        DTAR_exit(EXIT_FAILURE);
    }

    /* when creating a tarbll, we require a file name */
    if (opts_create && opts_tarfile == NULL) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Must specify a file name(-f)");
        }
        DTAR_exit(EXIT_FAILURE);
    }

    if (archive_opts->preserve) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Creating archive with extended attributes");
        }
    }

    /* adjust pointers to start of paths */
    int numpaths = argc - optind;
    const char** pathlist = (const char**) &argv[optind];

    /* standardize current working dir */
    mfu_param_path cwd_param;
    char cwd[PATH_MAX];
    mfu_getcwd(cwd, PATH_MAX);
    mfu_param_path_set(cwd, &cwd_param);

    if (opts_create) {
        /* allocate space to record info for each source */
        mfu_param_path* paths = (mfu_param_path*) MFU_MALLOC(numpaths * sizeof(mfu_param_path));

        /* process each source path */
        mfu_param_path_set_all(numpaths, pathlist, paths);

        /* standardize destination path */
        mfu_param_path destpath;
        mfu_param_path_set(opts_tarfile, &destpath);

        /* check that source and destination are okay */
        int valid;
        mfu_param_path_check_archive(numpaths, paths, destpath, archive_opts, &valid);

        /* walk path to get stats info on all files */
        mfu_flist flist = mfu_flist_new();
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist, mfu_src_file);

        /* spread elements evenly among ranks */
        mfu_flist flist2 = mfu_flist_spread(flist);
        mfu_flist_free(&flist);
        flist = flist2;

        /* create the archive file */
        mfu_flist_archive_create(flist, opts_tarfile, numpaths, paths, &cwd_param, archive_opts);

#if 0
        /* compress archive file */
        if (opts_compress) {
            struct stat st;
            char fname[50];
            char fname1[50];
            strncpy(fname1, opts_tarfile, 50);
            strncpy(fname, opts_tarfile, 50);
            if ((stat(strcat(fname, ".bz2"), &st) == 0)) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Output file already exists: '%s'", fname);
                }
                exit(0);
            }
            dbz2_compress(opts_blocksize, opts_tarfile, opts_memory);
            remove(fname1);
        }
#endif

        /* free the file list */
        mfu_flist_free(&flist);

        /* free paths */
        mfu_param_path_free_all(numpaths, paths);
        mfu_param_path_free(&destpath);
    } else if (opts_extract) {
        char* tarfile = opts_tarfile;
#if 0
        if (opts_compress) {
            char fname[50];
            char fname_out[50];
            strncpy(fname, opts_tarfile, 50);
            strncpy(fname_out, opts_tarfile, 50);
            size_t len = strlen(fname_out);
            fname_out[len - 4] = '\0';
            struct stat st;
            if ((stat(fname_out, &st) == 0)) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Output file already exists '%s'", fname_out);
                }
                exit(0);
            }
            //decompress(fname, fname_out);
            remove(fname);
            tarfile = fname_out;
        }
#endif
        mfu_flist_archive_extract(tarfile, &cwd_param, archive_opts);
    } else {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Neither creation or extraction is specified");
        }
        DTAR_exit(EXIT_FAILURE);
    }

    /* free the current working directory param path */
    mfu_param_path_free(&cwd_param);

    /* free the archive options */
    mfu_archive_opts_delete(&archive_opts);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* free the mfu_file object */
    mfu_file_delete(&mfu_src_file);

    /* free context */
    mfu_free(&opts_tarfile);

    DTAR_exit(EXIT_SUCCESS);
}
