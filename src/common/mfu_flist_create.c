#include <string.h>
#include <errno.h>

#include "mfu.h"
#include "mfu_flist_internal.h"

static int create_directory(mfu_flist list, uint64_t idx)
{
    /* get name of directory */
    const char* name = mfu_flist_file_get_name(list, idx);
    printf("In create directory\n");

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
