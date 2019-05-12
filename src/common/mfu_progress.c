//#include <dirent.h>
//#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
//#include <getopt.h>

//#include <pwd.h> /* for getpwent */
//#include <grp.h> /* for getgrent */
//#include <errno.h>
//#include <string.h>

//#include <libgen.h> /* dirname */

//#include "libcircle.h"
//#include "dtcmp.h"
#include "mfu.h"
//#include "mfu_flist_internal.h"

/* start progress timer */
mfu_progress* mfu_progress_start(int secs, int count, MPI_Comm comm, mfu_progress_fn progfn)
{
    /* allocate a new structure */
    mfu_progress* prg = (mfu_progress*) MFU_MALLOC(sizeof(mfu_progress));

    /* dup input communicator so our non-blocking collectives
     * don't interfere with caller's MPI communication */
    MPI_Comm_dup(comm, &prg->comm);

    /* initialize broadcast and reduce requests to NULL */
    prg->bcast_req  = MPI_REQUEST_NULL;
    prg->reduce_req = MPI_REQUEST_NULL;

    /* we'll keep executing bcast/reduce iterations until
     * all processes call complete */
    prg->keep_going = 1;

    /* record number of items to sum in progress updates */
    prg->count = count;

    /* allocate space to hold local and global values in reduction,
     * grab one extra space to hold completion status flags across procs */
    size_t bytes = (count + 1) * sizeof(uint64_t);
    prg->values      = (uint64_t*) MFU_MALLOC(bytes);
    prg->global_vals = (uint64_t*) MFU_MALLOC(bytes);

    /* record function to call to print progress */
    prg->progfn = progfn;

    /* set start time, initialize last time we reported, and timeout */
    prg->time_start = MPI_Wtime();
    prg->time_last  = prg->time_start;
    prg->timeout    = (double) secs;

    /* post buffer for incoming bcast */
    int rank;
    MPI_Comm_rank(prg->comm, &rank);
    if (rank != 0) {
        MPI_Ibcast(&(prg->keep_going), 1, MPI_INT, 0, prg->comm, &(prg->bcast_req));
    }

    return prg;
}

static void mfu_progress_reduce(uint64_t complete, uint64_t* vals, mfu_progress* prg)
{
    /* set our complete flag to indicate whether we have finished */
    prg->values[0] = complete;

    /* update our local count value to contribute in reduction */
    memcpy(&prg->values[1], vals, prg->count * sizeof(uint64_t));

    /* initiate the reduction */
    MPI_Ireduce(prg->values, prg->global_vals, prg->count + 1,
                MPI_UINT64_T, MPI_SUM, 0, prg->comm, &(prg->reduce_req));
}

/* update progress across all processes in work loop */
void mfu_progress_update(uint64_t* vals, mfu_progress* prg)
{
    int rank, ranks;
    MPI_Comm_rank(prg->comm, &rank);
    MPI_Comm_size(prg->comm, &ranks);

    int bcast_done  = 0;
    int reduce_done = 0;

    if (rank == 0) {
        /* if there are no bcast or reduce requests outstanding,
         * check whether it is time to send one */
        if (prg->bcast_req == MPI_REQUEST_NULL && prg->reduce_req == MPI_REQUEST_NULL) {
            /* get current time and compute number of seconds since
             * we last printed a message */
            double now = MPI_Wtime();
            double time_diff = now - prg->time_last;

            /* if timeout hasn't expired do nothing, return from function */
            if (time_diff < prg->timeout) {
                return;
            }

            /* signal other procs that it's time for a reduction */
            MPI_Ibcast(&(prg->keep_going), 1, MPI_INT, 0, prg->comm, &(prg->bcast_req));

            /* set our complete flag to 0 to indicate that we have not finished,
             * and contribute our current values */
            mfu_progress_reduce(0, vals, prg);
        } else {
            /* got an outstanding bcast or reduce, check to see if it's done */
            MPI_Test(&(prg->bcast_req), &bcast_done, MPI_STATUS_IGNORE);
            MPI_Test(&(prg->reduce_req), &reduce_done, MPI_STATUS_IGNORE);

            /* print new progress message when bcast and reduce have completed */
            if (bcast_done && reduce_done) {
                /* print progress message */
                if (prg->progfn) {
                    double now = MPI_Wtime();
                    double secs = now - prg->time_start;
                    (*prg->progfn)(&prg->global_vals[1], prg->count, (int)prg->global_vals[0], ranks, secs);
                }

                /* update/reset the timer after reporting progress */
                prg->time_last = MPI_Wtime();
            }
        }
    } else {
         /* get current time and compute number of seconds since
          * we last reported a message */
         double now = MPI_Wtime();
         double time_diff = now - prg->time_last;

         /* if timeout hasn't expired do nothing, return from function */
         if (time_diff < prg->timeout) {
             return;
         }

        /* we may have a reduce already outstanding,
         * wait for it to complete before we start a new one,
         * if there is no outstanding reduce, this sets the flag to 1 */
        MPI_Test(&(prg->reduce_req), &reduce_done, MPI_STATUS_IGNORE);
        if (!reduce_done) {
            /* not done, keep waiting */
            return;
        }

        /* wait for rank 0 to signal us with a bcast */
        MPI_Test(&(prg->bcast_req), &bcast_done, MPI_STATUS_IGNORE);
        if (!bcast_done) {
            /* not done, keep waiting */
            return;
        }

        /* to get here, the bcast must have completed,
         * so call reduce to contribute our current values */

        /* set our complete flag to 0 to indicate that we have not finished,
         * and contribute our current values */
        mfu_progress_reduce(0, vals, prg);

        /* update/reset the timer after reporting progress */
        prg->time_last = MPI_Wtime();

        /* since we are not in complete,
         * we can infer that keep_going must be 1,
         * so initiate new bcast for another bcast/reduce iteration */
        MPI_Ibcast(&(prg->keep_going), 1, MPI_INT, 0, prg->comm, &(prg->bcast_req));
    }
}

/* continue broadcasting progress until all processes have completed */
void mfu_progress_complete(uint64_t* vals, mfu_progress** pprg)
{
    mfu_progress* prg = *pprg;

    int rank, ranks;
    MPI_Comm_rank(prg->comm, &rank);
    MPI_Comm_size(prg->comm, &ranks);

    if (rank == 0) {
        while (1) {
            /* send a bcast/request pair */
            if (prg->bcast_req == MPI_REQUEST_NULL && prg->reduce_req == MPI_REQUEST_NULL) {
                /* initiate a new bcast/reduce iteration */
                MPI_Ibcast(&(prg->keep_going), 1, MPI_INT, 0, prg->comm, &(prg->bcast_req));

                /* we have reached complete, so set our complete flag to 1,
                 * and contribute our current values */
                mfu_progress_reduce(1, vals, prg);
            } else {
                /* if there are outstanding reqs then wait for bcast
                 * and reduce to finish */
                MPI_Wait(&(prg->bcast_req), MPI_STATUS_IGNORE);
                MPI_Wait(&(prg->reduce_req), MPI_STATUS_IGNORE);

                /* print progress message */
                if (prg->progfn) {
                    double now = MPI_Wtime();
                    double secs = now - prg->time_start;
                    (*prg->progfn)(&prg->global_vals[1], prg->count, (int)prg->global_vals[0], ranks, secs);
                }

                /* once outstanding bcast finishes in which we
                 * set keep_going == 0, we can stop */
                if (prg->keep_going == 0) {
                    break;
                }

                /* update curren't time */
                prg->time_last = MPI_Wtime();

                /* when all processes are complete, this will sum
                 * to the number of ranks */
                if (prg->global_vals[0] == ranks) {
                    /* all procs are done, tell them we can
                     * stop with next bcast/reduce iteration */
                    prg->keep_going = 0;

                    /* send bcast immediately so we don't need to wait on timeout */
                //    MPI_Ibcast(&(prg->keep_going), 1, MPI_INT, 0, prg->comm, &(prg->bcast_req));

                    /* we have reached complete, so set our complete flag to 1,
                     * and contribute our current values */
                //    mfu_progress_reduce(1, vals, prg);
                }
            }
        }
    } else {
        /* when rank != 0 */
        while (1) {
            /* if have an outstanding reduce, wait for that to finish
             * if not, this will return immediately */
            MPI_Wait(&(prg->reduce_req), MPI_STATUS_IGNORE);

            /* wait for bcast to finish */
            MPI_Wait(&(prg->bcast_req), MPI_STATUS_IGNORE);

            /* we have reached complete, so set our complete flag to 1,
             * and contribute our current values */
            mfu_progress_reduce(1, vals, prg);

            /* if keep_going flag is set then wait for another bcast */
            if (prg->keep_going) {
                MPI_Ibcast(&(prg->keep_going), 1, MPI_INT, 0, prg->comm, &(prg->bcast_req));
            } else {
                /* everyone is finished, wait on the reduce we just started */
                MPI_Wait(&(prg->reduce_req), MPI_STATUS_IGNORE);
                break;
            }
        }
    }

    /* release communicator we dup'ed during start */
    MPI_Comm_free(&prg->comm);

    /* free memory allocated to hold reduction data */
    mfu_free(&prg->values);
    mfu_free(&prg->global_vals);

    /* free our structure */
    mfu_free(pprg);
}
