#include <string.h>
#include <errno.h>

#include "mfu.h"
#include "mfu_flist_internal.h"

static int create_directory(mfu_flist list, uint64_t idx)
{
    /* get name of directory */
    const char* name = mfu_flist_file_get_name(list, idx);

    /* get permissions */
    mode_t mode = (mode_t) mfu_flist_file_get_mode(list, idx);

    /* create the destination directory */
    MFU_LOG(MFU_LOG_DBG, "Creating directory `%s'", name);
    int rc = mfu_mkdir(name, mode);
    if (rc < 0) {
        if (errno == EEXIST) {
            MFU_LOG(MFU_LOG_WARN,
                    "Original directory exists, skip the creation: `%s' (errno=%d %s)",
                    name, errno, strerror(errno)
            );
        } else {
            MFU_LOG(MFU_LOG_ERR, "Create `%s' mkdir() failed (errno=%d %s)",
                    name, errno, strerror(errno)
            );
            return -1;
        }
    }

    return 0;
}

/* create all directories specified in flist */
/* create directories, we work from shallowest level to the deepest
 * with a barrier in between levels, so that we don't try to create
 * a child directory until the parent exists */
void mfu_flist_mkdir(mfu_flist flist)
{
    int rc = 0;

    /* determine whether we should print status messages */
    int verbose = (mfu_debug_level <= MFU_LOG_INFO);

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* indicate to user what phase we're in */
    if (verbose && rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating directories.");
    }

    /* split items in file list into sublists depending on their
     * directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(flist, &levels, &minlevel, &lists);

    /* work from shallowest level to deepest level */
    int level;
    for (level = 0; level < levels; level++) {
        /* time how long this takes */
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* create each directory we have at this level */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* check whether we have a directory */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_DIR) {
                /* create the directory */
                int tmp_rc = create_directory(list, idx);
                if (tmp_rc != 0) {
                    /* set return code to most recent non-zero return code */
                    rc = tmp_rc;
                }

                count++;
            }
        }

        /* wait for all procs to finish before we start
         * creating directories at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        /* stop our timer */
        double end = MPI_Wtime();

         /* print statistics */
        if (verbose) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            double secs = end - start;
            if (secs > 0.0) {
                rate = (double)sum / secs;
            }
            if (rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "  level=%d min=%lu max=%lu sum=%lu rate=%f/sec secs=%f",
                  (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
                );
            }
        }
    }

    /* free our lists of levels */
    mfu_flist_array_free(levels, &lists);

    return;
}

static int create_file(mfu_flist list, uint64_t idx)
{
    /* get source name */
    const char* name = mfu_flist_file_get_name(list, idx);

    /* get permissions */
    mode_t mode = (mode_t) mfu_flist_file_get_mode(list, idx);

    /* create file with mknod
    * for regular files, dev argument is supposed to be ignored,
    * see makedev() to create valid dev */
    dev_t dev;
    memset(&dev, 0, sizeof(dev_t));
    int mknod_rc = mfu_mknod(name, mode | S_IFREG, dev);

    if (mknod_rc < 0) {
        if (errno == EEXIST) {
            MFU_LOG(MFU_LOG_WARN,
                    "Original file exists, skip the creation: `%s' (errno=%d %s)",
                    name, errno, strerror(errno)
            );

            /* TODO: truncate file? */
        } else {
            MFU_LOG(MFU_LOG_ERR, "File `%s' mknod() failed (errno=%d %s)",
                    name, errno, strerror(errno)
            );
            return -1;
        }
    }

    /* TODO: set uid, gid, timestamps? */

    return 0;
}

/* create inodes for all regular files in flist, assumes directories exist */
void mfu_flist_mknod(mfu_flist flist)
{
    int rc = 0;

    /* determine whether we should print status messages */
    int verbose = (mfu_debug_level <= MFU_LOG_INFO);

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* wait for all procs before starting timer */
    MPI_Barrier(MPI_COMM_WORLD);

    /* indicate to user what phase we're in */
    if (verbose && rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating files.");
    }

    /* time how long this takes */
    double start = MPI_Wtime();

    /* iterate over items and set write bit on directories if needed */
    uint64_t idx;
    uint64_t size = mfu_flist_size(flist);
    uint64_t count = 0;
    for (idx = 0; idx < size; idx++) {
        /* get type of item */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);

        /* process files and links */
        if (type == MFU_TYPE_FILE) {
            /* TODO: skip file if it's not readable */
            create_file(flist, idx);
            count++;
        } else if (type == MFU_TYPE_LINK) {
            //create_link(flist, idx);
            //count++;
        }
    }

    /* wait for all procs to finish */
    MPI_Barrier(MPI_COMM_WORLD);

    /* stop our timer */
    double end = MPI_Wtime();

    /* print timing statistics */
    if (verbose) {
        uint64_t min, max, sum;
        MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
        MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
        MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        double rate = 0.0;
        double secs = end - start;
        if (secs > 0.0) {
            rate = (double)sum / secs;
        }
        if (rank == 0) {
            MFU_LOG(MFU_LOG_DBG, "  min=%lu max=%lu sum=%lu rate=%f secs=%f",
              (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
            );
        }
    }

    return;
}

/* progress message to print while setting file metadata */
static void metaapply_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute average delete rate */
    double rate = 0.0;
    if (secs > 0) {
        rate = (double)vals[0] / secs;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Updated %llu items in %f secs (%f items/sec) ...",
            vals[0], secs, rate);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Updated %llu items in %f secs (%f items/sec) done",
            vals[0], secs, rate);
    }
}

static int mfu_set_timestamps(
    mfu_flist flist,
    uint64_t idx)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get atime seconds and nsecs */
    uint64_t atime      = mfu_flist_file_get_atime(flist, idx);
    uint64_t atime_nsec = mfu_flist_file_get_atime_nsec(flist, idx);

    /* get mtime seconds and nsecs */
    uint64_t mtime      = mfu_flist_file_get_mtime(flist, idx);
    uint64_t mtime_nsec = mfu_flist_file_get_mtime_nsec(flist, idx);

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
    const char* name = mfu_flist_file_get_name(flist, idx);
    if(mfu_utimensat(AT_FDCWD, name, times, AT_SYMLINK_NOFOLLOW) != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to change timestamps on `%s' utime() (errno=%d %s)",
            name, errno, strerror(errno)
        );
        rc = -1;
    }

    return rc;
}

/* apply metadata to items in flist
 * work from deepest level to shallowest level in case we're
 * doing things like disabling access on directories */
void mfu_flist_metadata_apply(mfu_flist flist)
{
    int rc = 0;

    /* determine whether we should print status messages */
    int verbose = (mfu_debug_level <= MFU_LOG_INFO);

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* indicate to user what phase we're in */
    if (verbose && rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Setting ownership, permissions, and timestmaps.");
    }

    /* start timer for entie operation */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_start = MPI_Wtime();
    uint64_t total_count = 0;

    /* start progress messages while setting metadata */
    mfu_progress* meta_prog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, metaapply_progress_fn);

    /* split items in file list into sublists depending on their
     * directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(flist, &levels, &minlevel, &lists);

    /* work from shallowest level to deepest level */
    int level;
    for (level = levels - 1; level >= 0; level--) {
        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* create each directory we have at this level */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        for (idx = 0; idx < size; idx++) {
#if 0
            tmp_rc = mfu_set_ownership(list, idx);
            if (tmp_rc < 0) {
                rc = -1;
            }
            tmp_rc = mfu_set_permissions(list, idx);
            if (tmp_rc < 0) {
                rc = -1;
            }
            tmp_rc = mfu_copy_acls(list, idx, dest);
            if (tmp_rc < 0) {
                rc = -1;
            }
#endif
            int tmp_rc = mfu_set_timestamps(list, idx);
            if (tmp_rc < 0) {
                rc = -1;
            }

            /* update our running total */
            total_count++;

            /* update number of items we have completed for progress messages */
            mfu_progress_update(&total_count, meta_prog);
        }

        /* wait for all procs to finish before we start
         * creating directories at next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /* free our lists of levels */
    mfu_flist_array_free(levels, &lists);

    /* finalize progress messages */
    mfu_progress_complete(&total_count, &meta_prog);

    /* stop timer and report total count */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_end = MPI_Wtime();

    /* print timing statistics */
    if (verbose) {
        uint64_t sum;
        MPI_Allreduce(&total_count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        double rate = 0.0;
        double secs = total_end - total_start;
        if (secs > 0.0) {
          rate = (double)sum / secs;
        }
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Updated %lu items in %f seconds (%f items/sec)",
              (unsigned long)sum, secs, rate
            );
        }
    }

    return;
}


