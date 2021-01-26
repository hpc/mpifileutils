#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>

#include "mpi.h"
#include "mfu.h"

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dreln [options] <oldpath> <newpath> <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file> - read list from file\n");
    printf("  -p, --preserve     - preserve link modification timestamps\n");
    printf("  -r, --relative     - change targets from absolute to relative paths\n");
    printf("      --progress <N> - print progress every N seconds\n");
    printf("  -v, --verbose      - verbose output\n");
    printf("  -q, --quiet        - quiet output\n");
    printf("  -h, --help         - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

static uint64_t reln_count_total;
static uint64_t reln_count;

/* prints progress messages while updating links */
static void reln_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute percentage of items removed */
    double percent = 0.0;
    if (reln_count_total > 0) {
        percent = 100.0 * (double)vals[0] / (double)reln_count_total;
    }

    /* compute average delete rate */
    double rate = 0.0;
    if (secs > 0) {
        rate = (double)vals[0] / secs;
    }

    /* compute estimated time remaining */
    double secs_remaining = -1.0;
    if (rate > 0.0) {
        secs_remaining = (double)(reln_count_total - vals[0]) / rate;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Processed %llu items (%.2f%%) in %f secs (%f items/sec) %d secs remaining ...",
            vals[0], percent, secs, rate, (int)secs_remaining);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Processed %llu items (%.2f%%) in %f secs (%f items/sec)",
            vals[0], percent, secs, rate);
    }
}

int main (int argc, char* argv[])
{
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* whether path to target should be relative from symlink */
    char* inputname = NULL;

    /* whether to preserve timestamps on link */
    int preserve_times = 0;

    /* whether path to target should be relative from symlink */
    int relative_targets = 0;

    /* set debug level to verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    /* define allowed command line optinos */
    int option_index = 0;
    static struct option long_options[] = {
        {"input",        1, 0, 'i'},
        {"preserve",     0, 0, 'p'},
        {"relative",     0, 0, 'r'},
        {"progress",     1, 0, 'R'},
        {"verbose",      0, 0, 'v'},
        {"quiet",        0, 0, 'q'},
        {"help",         0, 0, 'h'},
        {0, 0, 0, 0}
    };

    /* process command line arguments */
    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:prvqh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = MFU_STRDUP(optarg);
                break;
            case 'p':
                preserve_times = 1;
                break;
            case 'r':
                relative_targets = 1;
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

    /* remaining arguments */
    int remaining = argc - optind;

    /* we always require the OLDPATH and NEWPATH args */
    if (remaining < 2) {
        usage = 1;
    }

    /* print usage and bail out if needed */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }

    /* get pointers to old and new paths */
    char* oldpath = argv[optind + 0];
    char* newpath = argv[optind + 1];
    optind += 2;

    /* create new mfu_file objects */
    mfu_file_t* mfu_file = mfu_file_new();

    /* determine whether we're walking or reading from an input file */
    int walk = 0;
    int numpaths = 0;
    mfu_param_path* paths = NULL;
    if (optind < argc) {
        /* got some paths to walk */
        walk = 1;

        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* process paths to be walked */
        const char** p = (const char**)(&argv[optind]);
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));
        mfu_param_path_set_all((uint64_t)numpaths, (const char**)p, paths, mfu_file, true);

        /* TODO: check that walk paths are valid */

        /* don't allow user to specify input file with walk */
        if (inputname != NULL) {
            usage = 1;
        }
    } else {
        /* if we're not walking, we must be reading,
         * and for that we need a file */
        if (inputname == NULL) {
            usage = 1;
        }
    }

    /* print usage and bail out if needed */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_file_delete(&mfu_file);
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    /* get source file list */
    if (walk) {
        /* walk list of input paths */
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist, mfu_file);
    } else {
        /* read cache file */
        mfu_flist_read_cache(inputname, flist);
    }

    /* create path objects of old and new prefix */
    mfu_path* path_old = mfu_path_from_str(oldpath);
    mfu_path* path_new = mfu_path_from_str(newpath);
    mfu_path_reduce(path_old);
    mfu_path_reduce(path_new);

    /* get number of elements in old prefix */
    int components_old = mfu_path_components(path_old);

    /* create a new list by filtering out links */
    mfu_flist linklist_prestat = mfu_flist_subset(flist);
    uint64_t size = mfu_flist_size(flist);
    uint64_t idx;
    for (idx = 0; idx < size; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_LINK) {
            mfu_flist_file_copy(flist, idx, linklist_prestat);
        }
    }
    mfu_flist_summarize(linklist_prestat);
    mfu_flist_free(&flist);

    /* create a third list by stat'ing each link to get timestamps and group info */
    mfu_flist linklist = mfu_flist_new();
    mfu_flist_stat(linklist_prestat, linklist, NULL, NULL, 0, mfu_file);
    mfu_flist_free(&linklist_prestat);

    /* initiate progress messages */
    reln_count_total = mfu_flist_global_size(linklist);
    reln_count = 0;
    mfu_progress* prg = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, reln_progress_fn);

    /* iterate over links, readlink, check against old prefix */
    uint64_t count_changed = 0;
    size = mfu_flist_size(linklist);
    for (idx = 0; idx < size; idx++) {
        /* increment our count of processed links for progress messages */
        reln_count++;

        /* get path to link */
        const char* name = mfu_flist_file_get_name(linklist, idx);

        /* read link target */
        char path[PATH_MAX + 1];
        ssize_t readlink_rc = mfu_file_readlink(name, path, sizeof(path) - 1, mfu_file);
        if(readlink_rc < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to read link `%s' readlink() (errno=%d %s)",
                name, errno, strerror(errno)
            );
            continue;
        }

        /* ensure that string ends with NUL */
        path[readlink_rc] = '\0';

        /* generate path object for target */
        mfu_path* path_target = mfu_path_from_str(path);

        /* check whether target is an absolute path and falls under the old prefix */
        int is_absolute = mfu_path_is_absolute(path_target);
        mfu_path_result path_cmp = mfu_path_cmp(path_old, path_target);
        if (is_absolute && path_cmp == MFU_PATH_DEST_CHILD) {
            /* increment number of changed links */
            count_changed++;

            /* got a link we need to update,
             * delete current link */
            int unlink_rc = mfu_file_unlink(name, mfu_file);
            if (unlink_rc != 0) {
              MFU_LOG(MFU_LOG_WARN,
                      "Failed to delete link: `%s' (errno=%d %s)",
                      name, errno, strerror(errno));
            }

            /* define target path under new prefix,
             * chop old prefix, prepend new prefix */
            mfu_path_slice(path_target, components_old, -1);
            mfu_path_prepend(path_target, path_new);

            /* define target string */
            const char* target_str = NULL;
            if (relative_targets) {
                /* build path to the parent directory of the symlink */
                mfu_path* path_link = mfu_path_from_str(name);
                mfu_path_dirname(path_link);

                /* build relative path from to target */
                mfu_path* path_target_rel = mfu_path_relative(path_link, path_target);
                if (mfu_path_components(path_target_rel) > 0) {
                    target_str = mfu_path_strdup(path_target_rel);
                } else {
                    /* end up with empty string if link points to
                     * its parent directory */
                    target_str = strdup(".");
                }

                /* free paths */
                mfu_path_delete(&path_target_rel);
                mfu_path_delete(&path_link);
            } else {
                /* otherwise, use the full path to the target */
                target_str = mfu_path_strdup(path_target);
            }

            /* create new link */
            int symlink_rc = mfu_file_symlink(target_str, name, mfu_file);
            if (symlink_rc < 0) {
                if(errno == EEXIST) {
                    MFU_LOG(MFU_LOG_WARN,
                            "Original link exists, skip the creation: `%s' (errno=%d %s)",
                            name, errno, strerror(errno));
                } else {
                    MFU_LOG(MFU_LOG_ERR, "Create `%s' symlink() failed, (errno=%d %s)",
                            name, errno, strerror(errno)
                    );
                }
            }

            /* done with the target string */
            mfu_free(&target_str);

            /* TODO: copy xattrs */

            /* get user id and group id of file */
            uid_t uid = (uid_t) mfu_flist_file_get_uid(linklist, idx);
            gid_t gid = (gid_t) mfu_flist_file_get_gid(linklist, idx);

            /* note that we use lchown to change ownership of link itself, it path happens to be a link */
            if (mfu_file_lchown(name, uid, gid, mfu_file) != 0) {
                /* TODO: are there other EPERM conditions we do want to report? */

                /* since the user running dcp may not be the owner of the
                 * file, we could hit an EPERM error here, and the file
                 * will be left with the effective uid and gid of the dcp
                 * process, don't bother reporting an error for that case */
                if (errno != EPERM) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to change ownership on `%s' lchown() (errno=%d %s)",
                        name, errno, strerror(errno)
                       );
                }
            }

            if (preserve_times) {
                /* get atime seconds and nsecs */
                uint64_t atime      = mfu_flist_file_get_atime(linklist, idx);
                uint64_t atime_nsec = mfu_flist_file_get_atime_nsec(linklist, idx);

                /* get mtime seconds and nsecs */
                uint64_t mtime      = mfu_flist_file_get_mtime(linklist, idx);
                uint64_t mtime_nsec = mfu_flist_file_get_mtime_nsec(linklist, idx);

                /* fill in time structures */
                struct timespec times[2];
                times[0].tv_sec  = (time_t) atime;
                times[0].tv_nsec = (long)   atime_nsec;
                times[1].tv_sec  = (time_t) mtime;
                times[1].tv_nsec = (long)   mtime_nsec;

                /* set times with nanosecond precision using utimensat,
                 * assume path is relative to current working directory,
                 * if it's not absolute, and set times on link (not target file)
                 * if dest_path refers to a link */
                if (mfu_file_utimensat(AT_FDCWD, name, times, AT_SYMLINK_NOFOLLOW, mfu_file) != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to change timestamps on `%s' utime() (errno=%d %s)",
                        name, errno, strerror(errno)
                       );
                }
            }
        }

        mfu_path_delete(&path_target);

        /* update our status for progress messages */
        mfu_progress_update(&reln_count, prg);
    }

    /* finalize progress messages */
    mfu_progress_complete(&reln_count, &prg);

    /* TODO: print number of changed links */

    mfu_flist_free(&linklist);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* delete file objects */
    mfu_file_delete(&mfu_file);

    mfu_path_delete(&path_new);
    mfu_path_delete(&path_old);

    mfu_finalize();
    MPI_Finalize();

    return 0;
}
