/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_PROGRESS_H
#define MFU_PROGRESS_H

#include "mpi.h"

/* function prototype for progress callback */
typedef void (*mfu_progress_fn)(const uint64_t* vals, int count, int complete, int ranks, double secs);

/* struct that holds global variables for progress message reporting */
typedef struct {
    MPI_Comm comm; /* dup'ed communicator to execute bcast/reduce */
    MPI_Request bcast_req;
    MPI_Request reduce_req;
    double time_start;
    double time_last;
    double timeout;
    int keep_going;
    int count;
    uint64_t* values;      /* local array */
    uint64_t* global_vals; /* global array across all ranks */
    mfu_progress_fn progfn;
} mfu_progress;

/* start progress timer and return newly allocated structure
 * to track its state */
mfu_progress* mfu_progress_start(int secs, int count, MPI_Comm dupcomm, mfu_progress_fn progfn);

/* update progress across all processes in work loop */
void mfu_progress_update(uint64_t* vals, mfu_progress* msgs);

/* continue broadcasting progress until all processes have completed,
 * and free structure allocated in start */
void mfu_progress_complete(uint64_t* vals, mfu_progress** pmsgs);

#endif /* MFU_PROGRESS_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
