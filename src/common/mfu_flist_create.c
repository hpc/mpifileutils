#include <string.h>
#include <errno.h>

#include "mfu.h"
#include "mfu_flist_internal.h"

/* stores total number of directories to be created to print percent progress in reductions */
static uint64_t mkdir_total_count;

/* progress message to print sum of directories while creating */
static void mkdir_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* get number of items created so far */
    uint64_t items = vals[0];

    /* compute item rate */
    double item_rate = 0.0;
    if (secs > 0) {
        item_rate = (double)items / secs;
    }

    /* compute percentage of items created */
    double percent = 0.0;
    if (mkdir_total_count > 0) {
        percent = (double)items * 100.0 / (double)mkdir_total_count;
    }

    /* estimate seconds remaining */
    double secs_remaining = 0.0;
    if (item_rate > 0.0) {
        secs_remaining = (double)(mkdir_total_count - items) / item_rate;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO,
            "Created %llu directories (%.0f%%) in %.3lf secs (%.3lf dirs/sec) %.0f secs left ...",
            items, percent, secs, item_rate, secs_remaining
        );
    } else {
        MFU_LOG(MFU_LOG_INFO,
            "Created %llu directories (%.0f%%) in %.3lf secs (%.3lf dirs/sec) done",
            items, percent, secs, item_rate
        );
    }
}

static int create_directory(mfu_flist list, uint64_t idx)
{
    /* get name of directory */
    const char* name = mfu_flist_file_get_name(list, idx);

    /* get permissions */
    //mode_t mode = (mode_t) mfu_flist_file_get_mode(list, idx);
    mode_t mode = DCOPY_DEF_PERMS_DIR;

    /* create the destination directory */
    int rc = mfu_mkdir(name, mode);
    if (rc < 0) {
        if (errno == EEXIST) {
#if 0
            MFU_LOG(MFU_LOG_WARN,
                "Original directory exists, skipping create: `%s' (errno=%d %s)",
                name, errno, strerror(errno)
            );
#endif
            return 0;
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
void mfu_flist_mkdir(mfu_flist flist, mfu_create_opts_t* opts)
{
    int rc = 0;

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* count total number of directories to be created */
    uint64_t idx;
    uint64_t size = mfu_flist_size(flist);
    uint64_t count = 0;
    for (idx = 0; idx < size; idx++) {
       /* check whether we have a directory */
       mfu_filetype type = mfu_flist_file_get_type(flist, idx);
       if (type == MFU_TYPE_DIR) {
           count++;
       }
    }

    /* get total for print percent progress while creating */
    mkdir_total_count = 0;
    MPI_Allreduce(&count, &mkdir_total_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* bail early if there is no work to do */
    if (mkdir_total_count == 0) {
        return;
    }

    /* indicate to user what phase we're in */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating %llu directories", mkdir_total_count);
    }

    /* start progress messages while setting metadata */
    mfu_progress* mkdir_prog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, mkdir_progress_fn);

    /* split items in file list into sublists depending on their
     * directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(flist, &levels, &minlevel, &lists);

    /* work from shallowest level to deepest level */
    int level;
    uint64_t reduce_count = 0;
    for (level = 0; level < levels; level++) {
        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* create each directory we have at this level */
        size = mfu_flist_size(list);
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

                /* update our running count for progress messages */
                reduce_count++;
                mfu_progress_update(&reduce_count, mkdir_prog);
            }
        }

        /* wait for all procs to finish before we start
         * creating directories at next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /* finalize progress messages */
    mfu_progress_complete(&reduce_count, &mkdir_prog);

    /* free our lists of levels */
    mfu_flist_array_free(levels, &lists);

    return;
}

/* stores total number of items to be created to print percent progress in reductions */
static uint64_t mknod_total_count;

/* progress message to print sum of items while creating */
static void mknod_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* get number of items created so far */
    uint64_t items = vals[0];

    /* compute item rate */
    double item_rate = 0.0;
    if (secs > 0) {
        item_rate = (double)items / secs;
    }

    /* compute percentage of items created */
    double percent = 0.0;
    if (mknod_total_count > 0) {
        percent = (double)items * 100.0 / (double)mknod_total_count;
    }

    /* estimate seconds remaining */
    double secs_remaining = 0.0;
    if (item_rate > 0.0) {
        secs_remaining = (double)(mknod_total_count - items) / item_rate;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO,
            "Created %llu items (%.0f%%) in %.3lf secs (%.3lf items/sec) %.0f secs left ...",
            items, percent, secs, item_rate, secs_remaining
        );
    } else {
        MFU_LOG(MFU_LOG_INFO,
            "Created %llu items (%.0f%%) in %.3lf secs (%.3lf items/sec) done",
            items, percent, secs, item_rate
        );
    }
}

static int create_file(mfu_flist list, uint64_t idx, mfu_create_opts_t* opts)
{
    /* get source name */
    const char* name = mfu_flist_file_get_name(list, idx);

    /* get permissions */
    //mode_t mode = (mode_t) mfu_flist_file_get_mode(list, idx);
    mode_t mode = DCOPY_DEF_PERMS_FILE;

    /* apply lustre striping to item if user requested it */
    if (opts->lustre_stripe) {
        /* can only stripe regular files (this true?) */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);
        if (type == MFU_TYPE_FILE) {
            /* got a regular file, check its size */
            uint64_t filesize = mfu_flist_file_get_size(list, idx);
            if (filesize >= opts->lustre_stripe_minsize) {
                /* If we are overwriting files, preemptively delete any existing entry.
                 * Once a file exists, its striping parameters can't be changed. */
                if (opts->overwrite) {
                    mfu_unlink(name);
                }

                /* file size is big enough, let's stripe */
                uint64_t stripe_width = opts->lustre_stripe_width;
                int stripe_count = (int) opts->lustre_stripe_count;
                mfu_stripe_set(name, stripe_width, stripe_count);
            }
        }

        return 0;
    }

    /* create file with mknod
     * for regular files, dev argument is supposed to be ignored,
     * see makedev() to create valid dev */
    dev_t dev;
    memset(&dev, 0, sizeof(dev_t));
    int mknod_rc = mfu_mknod(name, mode | S_IFREG, dev);
    if (mknod_rc < 0) {
        if (errno == EEXIST) {
            /* failed to create because something already exists at this path,
             * try to delete it */
            if (opts->overwrite) {
                /* user selected over write, so try to delete the item */
                int unlink_rc = mfu_unlink(name);
                if (unlink_rc == 0) {
                    /* delete succeeded, try to create it agai */
                    mknod_rc = mfu_mknod(name, mode | S_IFREG, dev);
                    if (mknod_rc < 0) {
                        MFU_LOG(MFU_LOG_WARN,
                            "Failed to create: `%s' (errno=%d %s)",
                            name, errno, strerror(errno)
                        );
                        return -1;
                    }
                } else {
                    /* failed to delete existing item */
                    MFU_LOG(MFU_LOG_WARN,
                        "Original file exists and failed to delete: `%s' (errno=%d %s)",
                        name, errno, strerror(errno)
                    );
                    return -1;
                }
            } else {
                /* failed to delete existing item */
                MFU_LOG(MFU_LOG_ERR,
                    "Original file exists: `%s' (errno=%d %s)",
                    name, errno, strerror(errno)
                );
                return -1;
            }
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
void mfu_flist_mknod(mfu_flist flist, mfu_create_opts_t* opts)
{
    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* first, count number of items to create in the list of the current process */
    uint64_t idx;
    uint64_t size = mfu_flist_size(flist);
    uint64_t count = 0;
    for (idx = 0; idx < size; idx++) {
        /* create regular files */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_FILE) {
            count++;
        }
    }

    /* get total for print percent progress while creating */
    mknod_total_count = 0;
    MPI_Allreduce(&count, &mknod_total_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* bail early if there is no work to do */
    if (mknod_total_count == 0) {
        return;
    }

    /* wait for all procs before starting timer */
    MPI_Barrier(MPI_COMM_WORLD);

    /* indicate to user what phase we're in */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating %llu files", mknod_total_count);
    }

    /* start progress messages while setting metadata */
    mfu_progress* mknod_prog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, mknod_progress_fn);

    /* iterate over items and set write bit on directories if needed */
    count = 0;
    for (idx = 0; idx < size; idx++) {
        /* create regular files */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_FILE) {
            /* TODO: skip file if it's not readable */
            create_file(flist, idx, opts);

            /* update our running count for progress messages */
            count++;
            mfu_progress_update(&count, mknod_prog);
        }
    }

    /* finalize progress messages */
    mfu_progress_complete(&count, &mknod_prog);

    /* wait for all procs to finish */
    MPI_Barrier(MPI_COMM_WORLD);

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
        MFU_LOG(MFU_LOG_INFO, "Updated %llu items in %.3lf secs (%.3lf items/sec) ...",
            vals[0], secs, rate);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Updated %llu items in %.3lf secs (%.3lf items/sec) done",
            vals[0], secs, rate);
    }
}

static int mfu_set_ownership(
    mfu_flist flist,
    uint64_t idx)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get user id and group id of file */
    uid_t uid = (uid_t) mfu_flist_file_get_uid(flist, idx);
    gid_t gid = (gid_t) mfu_flist_file_get_gid(flist, idx);

    /* note that we use lchown to change ownership of link itself, it path happens to be a link */
    const char* name = mfu_flist_file_get_name(flist, idx);
    if(mfu_lchown(name, uid, gid) != 0) {
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
        rc = -1;
    }

    return rc;
}

/* TODO: condionally set setuid and setgid bits? */
static int mfu_set_permissions(
    mfu_flist flist,
    uint64_t idx,
    mfu_create_opts_t* opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* change mode on everything but symlinks */
    mfu_filetype type = mfu_flist_file_get_type(flist, idx);
    if(type != MFU_TYPE_LINK) {
        /* get mode of this item */
        mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

        /* only consider the mode bits chmod can set */
        mode = mode & 07777;

        /* conditionally subtract off umask */
        if (! opts->set_permissions) {
            mode = mode & ~opts->umask;
        }

        /* chmod of the item */
        const char* name = mfu_flist_file_get_name(flist, idx);
        if(mfu_chmod(name, mode) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to change permissions on `%s' chmod() (errno=%d %s)",
                name, errno, strerror(errno));
            rc = -1;
        }
    }
    return rc;
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
void mfu_flist_metadata_apply(mfu_flist flist, mfu_create_opts_t* opts)
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
            if (opts->set_owner) {
                int tmp_rc = mfu_set_ownership(list, idx);
                if (tmp_rc < 0) {
                    rc = -1;
                }
            }

            int tmp_rc = mfu_set_permissions(list, idx, opts);
            if (tmp_rc < 0) {
                rc = -1;
            }

#if 0
            tmp_rc = mfu_copy_acls(list, idx, dest);
            if (tmp_rc < 0) {
                rc = -1;
            }
#endif
            if (opts->set_timestamps) {
                int tmp_rc = mfu_set_timestamps(list, idx);
                if (tmp_rc < 0) {
                    rc = -1;
                }
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
            MFU_LOG(MFU_LOG_INFO, "Updated %lu items in %.3lf seconds (%.3lf items/sec)",
              (unsigned long)sum, secs, rate
            );
        }
    }

    return;
}

/* return a newly allocated create_opts structure, set default values on its fields,
 * this is used when creating directories and files as well as updating metadata like
 * timestamps after creating items */
mfu_create_opts_t* mfu_create_opts_new(void)
{
    mfu_create_opts_t* opts = (mfu_create_opts_t*) MFU_MALLOC(sizeof(mfu_create_opts_t));

    /* whether to delete existing items (non-directories) */
    opts->overwrite = NULL;

    /* whether to set uid */
    opts->set_owner = false;

    /* whether to set atime/mtime */
    opts->set_timestamps = false;

    /* whether to set permission bits */
    opts->set_permissions = false;

    /* record current umask value which we'll use when setting permission bits
     * in metadata_apply */
    opts->umask = umask(0022);
    umask(opts->umask);

    /* whether to apply lustre striping */
    opts->lustre_stripe = false;

    /* if applying lustre striping parameteres, only consider files greater than minsize bytes */
    opts->lustre_stripe_minsize = 0;

    /* if applying lustre striping parameteres, size of stripe width in bytes */
    opts->lustre_stripe_width = 1024 * 1024;

    /* if applying lustre striping parameteres, number of stripes to use */
    opts->lustre_stripe_count = -1;

    return opts;
}

void mfu_create_opts_delete(mfu_create_opts_t** popts)
{
  if (popts != NULL) {
    mfu_create_opts_t* opts = *popts;

    /* free fields allocated on opts */
    if (opts != NULL) {
      //mfu_free(&opts->dest_path);
    }

    mfu_free(popts);
  }
}
