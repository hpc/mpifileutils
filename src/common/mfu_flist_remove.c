#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"
#include "mfu_flist_internal.h"

/*****************************
 * Global functions used by remove routines
 ****************************/

/* holds total number of items to be deleted */
uint64_t remove_count;
uint64_t remove_count_total;

/* remove progress request */
mfu_progress* rmprog;

/* prints progress messages while deleting items */
static void remove_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute percentage of items removed */
    double percent = 0.0;
    if (remove_count_total > 0) {
        percent = 100.0 * (double)vals[0] / (double)remove_count_total;
    }

    /* compute average delete rate */
    double rate = 0.0;
    if (secs > 0) {
        rate = (double)vals[0] / secs;
    }

    /* compute estimated time remaining */
    double secs_remaining = -1.0;
    if (rate > 0.0) {
        secs_remaining = (double)(remove_count_total - vals[0]) / rate;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Removed %llu items (%.2f%%) in %f secs (%f items/sec) %d secs remaining ...",
            vals[0], percent, secs, rate, (int)secs_remaining);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Removed %llu items (%.2f%%) in %f secs (%f items/sec)",
            vals[0], percent, secs, rate);
    }
}

/* removes name by calling rmdir, unlink, or remove depending
 * on item type */
static void remove_type(char type, const char* name)
{
    /* TODO: don't print message if errno == ENOENT (file already gone) */
    if (type == 'd') {
        int rc = mfu_rmdir(name);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to rmdir `%s' (errno=%d %s)",
                      name, errno, strerror(errno)
                     );
        }
    }
    else if (type == 'f') {
        int rc = mfu_unlink(name);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to unlink `%s' (errno=%d %s)",
                      name, errno, strerror(errno)
                     );
        }
    }
    else if (type == 'u') {
        int rc = remove(name);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to remove `%s' (errno=%d %s)",
                      name, errno, strerror(errno)
                     );
        }
    }
    else {
        /* print error */
        MFU_LOG(MFU_LOG_ERR, "Unknown type=%c name=%s",
                  type, name
                 );
    }

    return;
}

/*****************************
 * Directly remove items in local portion of distributed list
 ****************************/

/* for given depth, just remove the files we know about */
static void remove_direct(mfu_flist list, uint64_t* rmcount)
{
    /* each process directly removes its elements */
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);

    /* keep track of files deleted so far */
    for (idx = 0; idx < size; idx++) {
        /* get name and type of item */
        const char* name = mfu_flist_file_get_name(list, idx);
        mfu_filetype type = mfu_flist_file_get_type(list, idx);

        /* delete item */
        if (type == MFU_TYPE_DIR) {
            remove_type('d', name);
        }
        else if (type == MFU_TYPE_FILE || type == MFU_TYPE_LINK) {
            remove_type('f', name);
        }
        else {
            remove_type('u', name);
        }

        /* increment number of items we have deleted
         * and check on progress message */
        remove_count++;
        mfu_progress_update(&remove_count, rmprog);
    }

    /* report the number of items we deleted */
    *rmcount = size;
    return;
}

/*****************************
 * Distribute items evenly across processes, then remove
 ****************************/

/* for given depth, evenly spread the files among processes for
 * improved load balancing */
static void remove_spread(mfu_flist flist, uint64_t* rmcount)
{
    /* evenly spread flist among processes,
     * execute direct delete, and free temp list */
    mfu_flist newlist = mfu_flist_spread(flist);
    remove_direct(newlist, rmcount);
    mfu_flist_free(&newlist);
    return;
}

/*****************************
 * Map all items in same parent directory to a single rank
 ****************************/

/* we hash file names based on its parent directory to map all
 * files in the same directory to the same process */
static int map_name(mfu_flist flist, uint64_t idx, int ranks, const void* args)
{
    /* get name of item */
    const char* name = mfu_flist_file_get_name(flist, idx);

    /* get parent directory of item */
    char* dir = MFU_STRDUP(name);
    dirname(dir);

    /* identify rank to send this file to */
    size_t dir_len = strlen(dir);
    uint32_t hash = mfu_hash_jenkins(dir, dir_len);
    int rank = (int)(hash % (uint32_t)ranks);

    /* free directory string */
    mfu_free(&dir);

    /* return rank to map item to */
    return rank;
}

static void remove_map(mfu_flist list, uint64_t* rmcount)
{
    /* remap files based on parent directory */
    mfu_flist newlist = mfu_flist_remap(list, map_name, NULL);

    /* at this point, we can directly remove files in our list */
    remove_direct(newlist, rmcount);

    /* free list of remapped files */
    mfu_flist_free(&newlist);

    return;
}

/*****************************
 * Globally sort items by filename, then remove,
 * may reduce locking if need to lock by directories
 ****************************/

/* for each depth, sort files by filename and then remove, to test
 * whether it matters to limit the number of directories each process
 * has to reference (e.g., locking) */
static void remove_sort(mfu_flist list, uint64_t* rmcount)
{
    /* bail out if total count is 0 */
    uint64_t all_count = mfu_flist_global_size(list);
    if (all_count == 0) {
        return;
    }

    /* get maximum file name and number of items */
    int chars = (int) mfu_flist_file_max_name(list);
    uint64_t my_count = mfu_flist_size(list);

    /* create key datatype (filename) and comparison op */
    MPI_Datatype dt_key;
    DTCMP_Op op_str;
    DTCMP_Str_create_ascend(chars, &dt_key, &op_str);

    /* create keysat datatype (filename + type) */
    MPI_Datatype types[2], dt_keysat;
    types[0] = dt_key;
    types[1] = MPI_CHAR;
    DTCMP_Type_create_series(2, types, &dt_keysat);

    /* allocate send buffer */
    int sendcount = (int) my_count;
    size_t sendbufsize = (size_t)(sendcount * (chars + 1));
    char* sendbuf = (char*) MFU_MALLOC(sendbufsize);

    /* copy data into buffer */
    char* ptr = sendbuf;
    uint64_t idx;
    for (idx = 0; idx < my_count; idx++) {
        /* encode the filename first */
        const char* name = mfu_flist_file_get_name(list, idx);
        strcpy(ptr, name);
        ptr += chars;

        /* last character encodes item type */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);
        if (type == MFU_TYPE_DIR) {
            ptr[0] = 'd';
        }
        else if (type == MFU_TYPE_FILE || type == MFU_TYPE_LINK) {
            ptr[0] = 'f';
        }
        else {
            ptr[0] = 'u';
        }
        ptr++;
    }

    /* sort items */
    void* recvbuf;
    int recvcount;
    DTCMP_Handle handle;
    DTCMP_Sortz(
        sendbuf, sendcount, &recvbuf, &recvcount,
        dt_key, dt_keysat, op_str, DTCMP_FLAG_NONE, MPI_COMM_WORLD, &handle
    );

    /* delete data */
    int delcount = 0;
    ptr = (char*)recvbuf;
    while (delcount < recvcount) {
        /* get item name */
        char* name = ptr;
        ptr += chars;

        /* get item type */
        char type = ptr[0];
        ptr++;

        /* delete item */
        remove_type(type, name);
        delcount++;
    }

    /* record number of items we deleted */
    *rmcount = (uint64_t) delcount;

    /* free output data */
    DTCMP_Free(&handle);

    /* free our send buffer */
    mfu_free(&sendbuf);

    /* free key comparison operation */
    DTCMP_Op_free(&op_str);

    /* free datatypes */
    MPI_Type_free(&dt_keysat);
    MPI_Type_free(&dt_key);

    return;
}

#if 0
/*****************************
 * Remove items using libcircle for dynamic load balancing
 ****************************/

/* globals needed for libcircle callback routines */
static mfu_flist circle_list; /* list of items we're deleting */
static uint64_t circle_count;   /* number of items local process has removed */

static void remove_create(CIRCLE_handle* handle)
{
    char path[CIRCLE_MAX_STRING_LEN];

    /* enqueues all items at rm_depth to be deleted */
    uint64_t idx;
    uint64_t size = mfu_flist_size(circle_list);
    for (idx = 0; idx < size; idx++) {
        /* get name and type of item */
        const char* name = mfu_flist_file_get_name(circle_list, idx);
        mfu_filetype type = mfu_flist_file_get_type(circle_list, idx);

        /* encode type */
        if (type == MFU_TYPE_DIR) {
            path[0] = 'd';
        }
        else if (type == MFU_TYPE_FILE || type == MFU_TYPE_LINK) {
            path[0] = 'f';
        }
        else {
            path[0] = 'u';
        }

        /* encode name */
        size_t len = strlen(name) + 2;
        if (len <= CIRCLE_MAX_STRING_LEN) {
            strcpy(&path[1], name);
            handle->enqueue(path);
        }
        else {
            MFU_LOG(MFU_LOG_ERR, "Filename longer than %lu",
                      (unsigned long)CIRCLE_MAX_STRING_LEN
                     );
        }
    }

    return;
}

static void remove_process(CIRCLE_handle* handle)
{
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);

    char item = path[0];
    char* name = &path[1];
    remove_type(item, name);
    circle_count++;

    return;
}

/* insert all items to be removed into libcircle for
 * dynamic load balancing */
static void remove_libcircle(mfu_flist list, uint64_t* rmcount)
{
    /* set globals for libcircle callbacks */
    circle_list  = list;
    circle_count = 0;

    /* initialize libcircle */
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL | CIRCLE_TERM_TREE);

    /* set libcircle verbosity level */
    enum CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    if (mfu_debug_level >= MFU_LOG_VERBOSE) {
        //        loglevel = CIRCLE_LOG_INFO;
    }
    CIRCLE_enable_logging(loglevel);

    /* register callbacks */
    CIRCLE_cb_create(&remove_create);
    CIRCLE_cb_process(&remove_process);

    /* run the libcircle job */
    CIRCLE_begin();
    CIRCLE_finalize();

    /* record number of items we deleted */
    *rmcount = circle_count;

    return;
}
#endif

/* TODO: sort w/ spread and synchronization */
/* allreduce to get total count of items */
/* sort by name */
/* alltoall to determine which processes to send / recv from */
/* alltoallv to exchange data */
/* pt2pt with left and right neighbors to determine if they have the same dirname */
/* delete what we can witout waiting */
/* if my right neighbor has same dirname, send it msg when we're done */
/* if my left neighbor has same dirname, wait for msg */

/*****************************
 * Driver functions
 ****************************/

/* removes list of items, sets write bits on directories from
 * top-to-bottom, then removes items one level at a time starting
 * from the deepest */
void mfu_flist_unlink(mfu_flist flist, bool traceless)
{
    int level;

    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double start_remove = MPI_Wtime();

    /* print a message to inform user what we're doing */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        uint64_t all_count = mfu_flist_global_size(flist);
        MFU_LOG(MFU_LOG_INFO, "Removing %lu items", all_count);

        /* store number of items in global for progress function */
        remove_count_total = all_count;
    }

    /* split files into separate lists by directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(flist, &levels, &minlevel, &lists);
    mfu_flist pstatlist;
    uint64_t size = mfu_flist_size(flist);
    char** strings = NULL;

    /* if traceless, dump the stat of each item's pdir */
    if (traceless) {
        uint64_t idx;

        strings = (char **) MFU_MALLOC(size * sizeof(char *));
        for (idx = 0; idx < size; idx++) {
            /* stat the item */
            struct stat st;
            /* get name of item */
            const char* name = mfu_flist_file_get_name(flist, idx);
            char *pdir = MFU_STRDUP(name);

            dirname(pdir);

            size_t len = strlen(pdir);

            strings[idx] = (char *) MFU_MALLOC(len + 1);

            strcpy(strings[idx], pdir);

            mfu_free(&pdir);
        }

        pstatlist = mfu_flist_new();

        mfu_flist_set_detail(pstatlist, 1);

        /* allocate arrays to hold result from DTCMP_Rankv_strings call to
        * assign group and rank values to each item */
        uint64_t output_bytes = size * sizeof(uint64_t);
        uint64_t groups;
        uint64_t* group_ids   = (uint64_t*) MFU_MALLOC(output_bytes);
        uint64_t* group_ranks = (uint64_t*) MFU_MALLOC(output_bytes);
        uint64_t* group_rank  = (uint64_t*) MFU_MALLOC(output_bytes);

        DTCMP_Rankv_strings((int)size, (const char**)strings, &groups, group_ids, group_ranks,
                           group_rank, DTCMP_FLAG_NONE, MPI_COMM_WORLD);

        for (idx = 0; idx < size; idx++) {
            if(group_rank[idx] == 0) {
                /* stat the item */
                struct stat st;
                char *pdir = strings[idx];
                int status = mfu_lstat(pdir, &st);

                if (status != 0) {
                    MFU_LOG(MFU_LOG_DBG, "mfu_lstat(%s): %d", pdir, status);
                    continue;
                }

                /* insert item into output list */
                mfu_flist_insert_stat(pstatlist, strings[idx], st.st_mode, &st);
            }
        }

        for (idx = 0; idx < size; idx++) {
            mfu_free(&strings[idx]);
        }

        mfu_free(&strings);
        mfu_free(&group_rank);
        mfu_free(&group_ranks);
        mfu_free(&group_ids);

        /* compute global summary */
        mfu_flist_summarize(pstatlist);

        /* To be sure that all procs have executed their stat calls before
         * moving on to deleting things.
         */
        MPI_Barrier(MPI_COMM_WORLD);
    }

#if 0
    /* dive from shallow to deep, ensure all directories have write bit set */
    for (level = 0; level < levels; level++) {
        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* determine whether we have details at this level */
        int detail = mfu_flist_have_detail(list);

        /* iterate over items and set write bit on directories if needed */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        for (idx = 0; idx < size; idx++) {
            /* check whether we have a directory */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_DIR) {
                /* assume we have to set the bit */
                int set_write_bit = 1;
                if (detail) {
                    mode_t mode = (mode_t) mfu_flist_file_get_mode(list, idx);
                    if (mode & S_IWUSR) {
                        /* we have the mode of the file, and the bit is already set */
                        set_write_bit = 0;
                    }
                }

                /* set the bit if needed */
                if (set_write_bit) {
                    const char* name = mfu_flist_file_get_name(list, idx);
                    int rc = chmod(name, S_IRWXU);
                    if (rc != 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to chmod directory `%s' (errno=%d %s)",
                                  name, errno, strerror(errno)
                                 );
                    }
                }
            }
        }

        /* wait for all procs to finish before we start next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }
#endif

    /* start timer and broadcast for progress messages */
    remove_count = 0;
    rmprog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, remove_progress_fn);

    /* now remove files starting from deepest level */
    for (level = levels - 1; level >= 0; level--) {
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        uint64_t count = 0;
        //remove_direct(list, &count);
        remove_spread(list, &count);
//        remove_map(list, &count);
//        remove_sort(list, &count);
//        remove_libcircle(list, &count);
//        TODO: remove sort w/ spread

        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        double end = MPI_Wtime();

        if (mfu_debug_level >= MFU_LOG_VERBOSE) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            if (end - start > 0.0) {
                rate = (double)sum / (end - start);
            }
            double time_diff = end - start;
            if (mfu_rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "level=%d min=%lu max=%lu sum=%lu rate=%f secs=%f",
                       (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, time_diff
                      );
            }
        }
    }

    mfu_progress_complete(&remove_count, &rmprog);

    /* if traceless, restore the stat of each item's pdir */
    if (traceless) {
        mfu_flist newlist = mfu_flist_spread(pstatlist);
        size = mfu_flist_size(newlist);
        uint64_t idx;

        for (idx = 0; idx < size; idx++) {
            struct timespec times[2];
            /* get name of item */
            const char* pdir = mfu_flist_file_get_name(newlist, idx);

            /* TODO: skip removed dir, if it happens to become a problem*/

            times[0].tv_sec  = mfu_flist_file_get_atime(newlist, idx);
            times[1].tv_sec  = mfu_flist_file_get_mtime(newlist, idx);

            times[0].tv_nsec = mfu_flist_file_get_atime_nsec(newlist, idx);
            times[1].tv_nsec = mfu_flist_file_get_mtime_nsec(newlist, idx);

            if(mfu_utimensat(AT_FDCWD, pdir, times, AT_SYMLINK_NOFOLLOW) != 0) {
                MFU_LOG(MFU_LOG_DBG,
                        "Failed to changeback timestamps with utimesat() `%s' (errno=%d %s)",
                        pdir, errno, strerror(errno));
            }
        }

        mfu_flist_free(&newlist);
        mfu_flist_free(&pstatlist);
    }

    mfu_flist_array_free(levels, &lists);

    /* wait for all tasks and stop timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double end_remove = MPI_Wtime();

    /* report remove count, time, and rate */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        uint64_t all_count = mfu_flist_global_size(flist);
        double time_diff = end_remove - start_remove;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        MFU_LOG(MFU_LOG_INFO, "Removed %lu items in %f seconds (%f items/sec)",
               all_count, time_diff, rate
              );
    }

    /* wait for summary to be printed */
    MPI_Barrier(MPI_COMM_WORLD);

    return;
}
