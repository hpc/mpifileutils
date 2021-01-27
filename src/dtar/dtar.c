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

/* the user might list several components for each target, in which case
 * we should have walked the target in each case, but we also need to
 * include entries for any parent directories on the way to each target */
static int add_parent_dirs(
    int num,                        /* number of source paths */
    const mfu_param_path* srcpaths, /* list of source param path objects */
    const mfu_param_path* cwdpath,  /* param path of current working dir */
    mfu_flist flist)                /* list in which to insert items for parent dirs */
{
    int rc = MFU_SUCCESS;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* have rank 0 add check for and add any parent directories */
    if (rank == 0) {
        /* create a path object from the current working dir param path */
        mfu_path* cwd = mfu_path_from_str(cwdpath->path);

        /* for each source path, check whether any path components fall
         * between the cwdpath and the srcpath */
        int i;
        for (i = 0; i < num; i++) {
            /* create a path object for this source path */
            const char* s = srcpaths[i].path;
            mfu_path* src = mfu_path_from_str(s);

            /* enforce that all source items are children
             * of the current working dir */
            mfu_path_result pathcmp = mfu_path_cmp(src, cwd);
            if (pathcmp == MFU_PATH_SRC_CHILD || pathcmp == MFU_PATH_EQUAL) {
                /* source path is a child or the same as the cwd path,
                 * compute number of components from cwd to src */
                int src_components = mfu_path_components(src);
                int cwd_components = mfu_path_components(cwd);
                int components = src_components - cwd_components;

                /* step from src back to cwd and add each parent
                 * directory as a list item */
                int j;
                for (j = 0; j < components - 1; j++) {
                    /* get dirname to back up a level */
                    mfu_path_dirname(src);

                    /* get path to this parent directory */
                    const char* parent = mfu_path_strdup(src);

                    /* stat the parent and insert into list */
                    int insert_rc = mfu_flist_file_create_stat(flist, parent);
                    if (insert_rc != MFU_SUCCESS) {
                        /* failed to stat parent directory */
                        MFU_LOG(MFU_LOG_ERR, "Failed to insert path '%s' errno=%d %s",
                            parent, errno, strerror(errno));
                        rc = MFU_FAILURE;
                    }

                    /* done with the parent string */
                    mfu_free(&parent);
                }
            } else {
                /* source path is not a child of cwd,
                 * let's bail with an error for now */
                MFU_LOG(MFU_LOG_ERR, "Source path '%s' is not contained in '%s'",
                    srcpaths[i].orig, cwdpath->path);
                rc = MFU_FAILURE;
            }

            /* free off source path object */
            mfu_path_delete(&src);
        }

        /* free off path object for cwd */
        mfu_path_delete(&cwd);
    }

    /* summarize the list again, since we may have added new elements */
    mfu_flist_summarize(flist);

    /* check that everyone succeeded */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        rc = MFU_FAILURE;
    }

    return rc;
}

/* TODO: add options
 *   --index-skip -- avoid trying to index and extract entries the hard way (round robin)
 *   --index-nowrite -- do not save index after indexing
 *   --index-ignore -- pretend index file does not exist
 *   --index-only -- write index without extracting
 */

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dtar [options] <source ...>\n");
    printf("\n");
    printf("Options:\n");
    printf("  -c, --create            - create archive\n");
    printf("  -x, --extract           - extract archive\n");
    printf("  -f, --file <FILE>       - specify archive file\n");
    printf("  -C, --chdir <DIR>       - change directory to DIR before executing\n");
//    printf("  -j, --compress          - compress archive\n");
//    printf("  -p, --preserve          - preserve attributes\n");
    printf("      --preserve-owner    - preserve owner/group (default effective uid/gid)\n");
    printf("      --preserve-times    - preserve atime/mtime (default current time)\n");
    printf("      --preserve-perms    - preserve permissions (default subtracts umask)\n");
    printf("      --preserve-xattrs   - preserve xattrs (default ignores xattrs)\n");
//    printf("      --preserve-acls     - preserve acls (default ignores acls)\n");
//    printf("      --preserve-flags    - preserve fflags (default ignores ioctl iflags)\n");
    printf("      --fsync             - sync file data to disk on close\n");
    printf("  -b, --bufsize <SIZE>    - IO buffer size in bytes (default " MFU_BUFFER_SIZE_STR ")\n");
    printf("  -k, --chunksize <SIZE>  - work size per task in bytes (default " MFU_CHUNK_SIZE_STR ")\n");
    printf("      --memsize <SIZE>    - memory limit per task for parallel read in bytes (default 256MB)\n");
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

    int     opts_help     = 0;
    int     opts_create   = 0;
    int     opts_extract  = 0;
    int     opts_compress = 0;
    char*   opts_tarfile  = NULL;
    char*   opts_chdir    = NULL;

    int option_index = 0;
    static struct option long_options[] = {
        {"create",    0, 0, 'c'},
        {"extract",   0, 0, 'x'},
        //{"compress",  0, 0, 'j'},
        {"file",      1, 0, 'f'},
        {"chdir",     1, 0, 'C'},
        {"preserve",  0, 0, 'p'},
        {"preserve-owner",  0, 0, 'O'},
        {"preserve-times",  0, 0, 'T'},
        {"preserve-perms",  0, 0, 'P'},
        {"preserve-xattrs", 0, 0, 'X'},
        {"preserve-acls",   0, 0, 'A'},
        {"preserve-flags",  0, 0, 'F'},
        {"fsync",     0, 0, 's'},
        {"bufsize",   1, 0, 'b'},
        {"chunksize", 1, 0, 'k'},
        {"memsize",   1, 0, 'm'},
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
                    argc, argv, "cxf:C:pb:k:vqh",
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
            case 'x':
                opts_extract = 1;
                break;
            case 'f':
                opts_tarfile = MFU_STRDUP(optarg);
                break;
            case 'C':
                opts_chdir = MFU_STRDUP(optarg);
                break;
            case 'j':
                opts_compress = 1;
                break;
            case 'p':
                archive_opts->preserve = true;
                break;
            case 'O':
                archive_opts->preserve_owner = true;
                break;
            case 'T':
                archive_opts->preserve_times = true;
                break;
            case 'P':
                archive_opts->preserve_permissions = true;
                break;
            case 'X':
                archive_opts->preserve_xattrs = true;
                break;
            case 'A':
                archive_opts->preserve_acls = true;
                break;
            case 'F':
                archive_opts->preserve_fflags = true;
                break;
            case 's':
                archive_opts->sync_on_close = true;
                break;
            case 'b':
                if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR,
                                "Failed to parse block size: '%s'", optarg);
                    }
                    usage = 1;
                } else {
                    archive_opts->buf_size = (size_t) bytes;
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
            case 'm':
                if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR,
                                "Failed to parse memory limit: '%s'", optarg);
                    }
                    usage = 1;
                } else {
                    archive_opts->mem_size = (size_t) bytes;
                }
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
                opts_help = 1;
                usage = 1;
                break;
            case '?':
                opts_help = 1;
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

    if (!opts_create && !opts_extract && !opts_help) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "One of extract(x) or create(c) needs to be specified");
        }
        usage = 1;
    }

    if (opts_create && opts_extract) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Only one of extraction(x) or create(c) can be specified");
        }
        usage = 1;
    }

    /* when creating a tarbll, we require a file name */
    if (opts_create && opts_tarfile == NULL) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Must specify a file name(-f)");
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

    if (archive_opts->preserve) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Creating archive with extended attributes");
        }
    }

    /* adjust pointers to start of paths */
    int numpaths = argc - optind;
    const char** pathlist = (const char**) &argv[optind];

    /* change directory if requested */
    if (opts_chdir != NULL) {
        /* change directory, and check that all processes succeeded */
        int chdir_rc = chdir(opts_chdir);
        if (! mfu_alltrue(chdir_rc == 0, MPI_COMM_WORLD)) {
            /* at least one process failed, so bail out */
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to change directory to '%s'", opts_chdir);
            }
            DTAR_exit(EXIT_FAILURE);
        }
    }

    /* standardize current working dir */
    mfu_param_path cwd_param;
    char cwd[PATH_MAX];
    mfu_getcwd(cwd, PATH_MAX);
    mfu_param_path_set(cwd, &cwd_param, mfu_src_file, true);

    int ret = MFU_SUCCESS;
    if (opts_create) {
        /* allocate space to record info for each source */
        mfu_param_path* paths = (mfu_param_path*) MFU_MALLOC(numpaths * sizeof(mfu_param_path));

        /* process each source path */
        mfu_param_path_set_all(numpaths, pathlist, paths, mfu_src_file, true);

        /* standardize destination path */
        mfu_param_path destpath;
        mfu_param_path_set(opts_tarfile, &destpath, mfu_src_file, false);

        /* if we have an existing archive, it is deleted in check_archive so that we don't
         * walk it to be included as an entry of the archive itself in the target archive
         * happens to be in the directory we are walking */

        /* check that source and destination are okay */
        int valid;
        mfu_param_path_check_archive(numpaths, paths, destpath, archive_opts, &valid);

        /* walk path to get stats info on all files */
        mfu_flist flist = mfu_flist_new();
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist, mfu_src_file);

        /* we may need to add parent directories between the cwd
         * and the source paths the user specified */
        int tmp_rc = add_parent_dirs(numpaths, paths, &cwd_param, flist);
        if (tmp_rc != MFU_SUCCESS) {
            mfu_flist_free(&flist);
            mfu_param_path_free(&destpath);
            mfu_param_path_free_all(numpaths, paths);
            mfu_free(&paths);
            mfu_param_path_free(&cwd_param);
            DTAR_exit(EXIT_FAILURE);
        }

        /* spread elements evenly among ranks */
        mfu_flist flist2 = mfu_flist_spread(flist);
        mfu_flist_free(&flist);
        flist = flist2;

        /* create the archive file */
        ret = mfu_flist_archive_create(flist, opts_tarfile, numpaths, paths, &cwd_param, archive_opts);

#if 0
        /* compress archive file */
        if (opts_compress) {
            struct stat st;
            char fname[50];
            char fname1[50];
            strncpy(fname1, opts_tarfile, 50);
            strncpy(fname, opts_tarfile, 50);
            if ((mfu_file_stat(strcat(fname, ".bz2"), &st, mfu_src_file) == 0)) {
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
        mfu_free(&paths);
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
            if ((mfu_file_stat(fname_out, &st, mfu_src_file) == 0)) {
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
        ret = mfu_flist_archive_extract(tarfile, &cwd_param, archive_opts);
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
    mfu_free(&opts_chdir);

    if (ret != MFU_SUCCESS) {
        DTAR_exit(EXIT_FAILURE);
    }
    DTAR_exit(EXIT_SUCCESS);
}
