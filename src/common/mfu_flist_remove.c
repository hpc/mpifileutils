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
#include <time.h> /* asctime / localtime */

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

    /* start timer and broadcast for progress messages */
    mfu_progress_msgs_t* msgs = mfu_progress_start(10, MPI_COMM_WORLD);

    /* keep track of files deleted so far */
    uint64_t file_count = 0;
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
        file_count++;
        mfu_progress_update(msgs, file_count);
    }

    mfu_progress_complete(&msgs, file_count);

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
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL);

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

/* return a newly allocated progress_msgs structure, set default values on its fields */
static mfu_progress_msgs_t* mfu_progress_msgs_new(void)
{
    /* allocate a new structure */
    mfu_progress_msgs_t* msgs = (mfu_progress_msgs_t*) MFU_MALLOC(sizeof(mfu_progress_msgs_t));

    /* we'll dup input communicator on start to conduct bcast/reduce calls
     * we'll free it after complete */
    msgs->comm = MPI_COMM_NULL;

    /* initialize broadcast request to NULL */
    msgs->bcast_req = MPI_REQUEST_NULL;

    /* initialize reduce request to NULL */
    msgs->reduce_req = MPI_REQUEST_NULL;

    /* to record most timestamp of printing most recent message */
    msgs->current = 0;

    /* initialize keep_going flag to 0 */
    msgs->keep_going = false;

    /* initialize local and global arrays to 0,
     * first index is status flag and the second index
     * in the arrays is for the number of items
     * removed */
    msgs->values[0] = 0;
    msgs->values[1] = 0;
    msgs->global_vals[0] = 0;
    msgs->global_vals[1] = 0;

    /* initialize count to 0 */
    msgs->count = 0;

    return msgs;
}

/* cleanup memory allocated by mfu_progress struct */
static void mfu_progress_msgs_delete(mfu_progress_msgs_t** pmsgs)
{
  if (pmsgs != NULL) {
    mfu_progress_msgs_t* msgs = *pmsgs;
    mfu_free(pmsgs);
  }
}

/* start progress timer */
mfu_progress_msgs_t* mfu_progress_start(int secs, MPI_Comm comm)
{
    /* allocate and initialize a new structure */
    mfu_progress_msgs_t* msgs = mfu_progress_msgs_new();

    /* dup input communicator so our non-blocking collectives
     * don't interfere with caller's MPI communication */
    MPI_Comm_dup(comm, &msgs->comm);

    /* we'll keep executing bcast/reduce iterations until
     * all processes call complete */
    msgs->keep_going = 1;

    int comm_rank;
    MPI_Comm_rank(msgs->comm, &comm_rank);

    if (comm_rank == 0) {
        /* set current time & timeout on rank 0 */
        msgs->current   = time(NULL);
        msgs->timeout   = secs;
        msgs->bcast_req = MPI_REQUEST_NULL;
    } else {
        /* if rank != 0 recv bcast */
        MPI_Ibcast(&(msgs->keep_going), 1, MPI_INT, 0, msgs->comm, &(msgs->bcast_req));
    }

    return msgs;
}

/* update progress across all processes in work loop */
void mfu_progress_update(mfu_progress_msgs_t* msgs,
                         uint64_t file_count)
{
    int comm_rank, comm_size;
    MPI_Comm_rank(msgs->comm, &comm_rank);
    MPI_Comm_size(msgs->comm, &comm_size);

    int bcast_done  = 0;
    int reduce_done = 0;

    if (comm_rank == 0) {
        /* get current time and compute number of seconds since
         * we last printed a message */
        time_t now = time(NULL);
        double time_diff = difftime(now, msgs->current);

        /* if timeout hasn't expired do nothing, return from function */
        if (time_diff < msgs->timeout) {
            return;
        }

        /* if there are no bcast or reduce requests outstanding send a
         * bcast/reduce pair */
        if (msgs->bcast_req == MPI_REQUEST_NULL && msgs->reduce_req == MPI_REQUEST_NULL) {
            /* signal other procs that it's time for a reduction */
            MPI_Ibcast(&(msgs->keep_going), 1, MPI_INT, 0, msgs->comm, &(msgs->bcast_req));

            /* update our local count value to contribute in reduction */
            msgs->values[0] = file_count;

            /* set our complete flag to 0 to indicate that we have not finished */
            msgs->values[1] = 0;

            /* initiate the reduction */
            MPI_Ireduce(msgs->values, msgs->global_vals, 2,
                        MPI_UINT64_T, MPI_SUM, 0, msgs->comm, &(msgs->reduce_req));
        } else {
            /* check if bcast and/or reduce is done */
            MPI_Test(&(msgs->bcast_req), &bcast_done, MPI_STATUS_IGNORE);
            MPI_Test(&(msgs->reduce_req), &reduce_done, MPI_STATUS_IGNORE);

            /* print new progress message when bcast and reduce have completed */
            if (bcast_done && reduce_done) {
                /* TODO: print progress message */

                /* update/reset the timer after reporting progress */
                msgs->current = time(NULL);
            }
        }
    } else {
        /* we may have a reduce already outstanding,
         * wait for it to complete before we start a new one,
         * if there is no outstanding reduce, this sets the flag to 1 */
        MPI_Test(&(msgs->reduce_req), &reduce_done, MPI_STATUS_IGNORE);
        if (!reduce_done) {
            /* not done, keep waiting */
            return;
        }

        /* wait for rank 0 to signal us with a bcast */
        MPI_Test(&(msgs->bcast_req), &bcast_done, MPI_STATUS_IGNORE);
        if (!bcast_done) {
            /* not done, keep waiting */
            return;
        }

        /* to get here, the bcast must have completed,
         * so call reduce to contribute our current values */

        /* update our local count value to contribute in reduction */
        msgs->values[0] = file_count;

        /* set our complete flag to 0 to indicate that we have not finished */
        msgs->values[1] = 0;

        /* contribute our current values */
        MPI_Ireduce(msgs->values, msgs->global_vals, 2,
                    MPI_UINT64_T, MPI_SUM, 0, msgs->comm, &(msgs->reduce_req));

        /* since we are not in complete,
         * we can infer that keep_going must be 1,
         * so initiate new bcast for another bcast/reduce iteration */
        MPI_Ibcast(&(msgs->keep_going), 1, MPI_INT, 0, msgs->comm, &(msgs->bcast_req));
    }
}

/* continue broadcasting progress until all processes have completed */
void mfu_progress_complete(mfu_progress_msgs_t** pmsgs,
                           uint64_t file_count)
{
    mfu_progress_msgs_t* msgs = *pmsgs;

    int comm_rank, comm_size;
    MPI_Comm_rank(msgs->comm, &comm_rank);
    MPI_Comm_size(msgs->comm, &comm_size);

    if (comm_rank == 0) {
        while (1) {
            /* if timeout is up then send a bcast/request pair */
            if (msgs->bcast_req == MPI_REQUEST_NULL && msgs->reduce_req == MPI_REQUEST_NULL) {
                /* get current time and compute number of seconds since
                 * we last printed a message */
                time_t now = time(NULL);
                double time_diff = difftime(now, msgs->current);

                /* if timeout hasn't expired do nothing, return from function */
                if (time_diff < msgs->timeout) {
                    continue;
                }

                /* timeout has expired, initiate a new bcast/reduce iteration */
                MPI_Ibcast(&(msgs->keep_going), 1, MPI_INT, 0, msgs->comm, &(msgs->bcast_req));

                /* update our local count value to contribute in reduction */
                msgs->values[0] = file_count;

                /* we have reached complete, so set our complete flag to 1 */
                msgs->values[1] = 1;

                MPI_Ireduce(msgs->values, msgs->global_vals, 2,
                            MPI_UINT64_T, MPI_SUM, 0, msgs->comm, &(msgs->reduce_req));
            } else {
                /* if there are outstanding reqs then wait for bcast
                 * and reduce to finish */
                MPI_Wait(&(msgs->bcast_req), MPI_STATUS_IGNORE);
                MPI_Wait(&(msgs->reduce_req), MPI_STATUS_IGNORE);

                /* TODO: print progress message */

                /* once outstanding bcast finishes in which we
                 * set keep_going == 0, we can stop */
                if (msgs->keep_going == 0) {
                    break;
                }

                /* update curren't time */
                msgs->current = time(NULL);

                /* when all processes are complete, this will sum
                 * to comm_size */
                if (msgs->global_vals[1] == comm_size) {
                    /* all procs are done, tell them we can
                     * stop with next bcast/reduce iteration */
                    msgs->keep_going = 0;

                    /* TODO: send bcast immediately so we don't need to wait on timeout? */
                }
            }
        }
    } else {
        /* when rank != 0 */
        while (1) {
            /* if have an outstanding reduce, wait for that to finish
             * if not, this will return immediately */
            MPI_Wait(&(msgs->reduce_req), MPI_STATUS_IGNORE);

            /* wait for bcast to finish */
            MPI_Wait(&(msgs->bcast_req), MPI_STATUS_IGNORE);

            /* update our local count value to contribute in reduction */
            msgs->values[0] = file_count;

            /* we have reached complete, so set our complete flag to 1 */
            msgs->values[1] = 1;

            /* bcast completed, so contribute our reduction value */
            MPI_Ireduce(msgs->values, msgs->global_vals, 2,
                        MPI_UINT64_T, MPI_SUM, 0, msgs->comm, &(msgs->reduce_req));

            /* if keep_going flag is set then wait for another bcast */
            if (msgs->keep_going) {
                MPI_Ibcast(&(msgs->keep_going), 1, MPI_INT, 0, msgs->comm, &(msgs->bcast_req));
            } else {
                /* everyone is finished, wait on the reduce we just started */
                MPI_Wait(&(msgs->reduce_req), MPI_STATUS_IGNORE);
                break;
            }
        }
    }

    /* release communicator we dup'ed during start */
    MPI_Comm_free(&msgs->comm);

    /* free our structure */
    mfu_progress_msgs_delete(pmsgs);
}

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
