/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_PROGRESS_H
#define MFU_PROGRESS_H

#include "mpi.h"

/* function prototype for progress callback
 *   vals     - array of current values summed across ranks
 *   count    - number of elements in vals array
 *   complete - number of ranks that are complete
 *   ranks    - number of ranks involved
 *   secs     - number of seconds since start was called */
typedef void (*mfu_progress_fn)(const uint64_t* vals, int count, int complete, int ranks, double secs);

/* (opaque) struct that holds state for progress message reporting */
typedef struct {
    MPI_Comm comm;          /* dup'ed communicator to execute bcast/reduce */
    MPI_Request bcast_req;  /* request for outstanding bcast */
    MPI_Request reduce_req; /* request for outstanding reduce */
    double time_start;      /* time when start was called */
    double time_last;       /* time when last report was requested */
    double timeout;         /* number of seconds between reports */
    int keep_going;         /* flag indicating whether any process is still working */
    int count;              /* number of items in values arrays */
    uint64_t* values;       /* array holding contribution to global sum from local proc */
    uint64_t* global_vals;  /* array to hold global sum across ranks */
    mfu_progress_fn progfn; /* callback function to execute to print progress message */
} mfu_progress;

/* start progress timer and return newly allocated structure
 * to track its state
 *   secs   - IN number of seconds between progress messages
 *   count  - IN number of uint64_t values to sum in each message
 *   comm   - IN communicator to dup on which to execute ibcast/ireduce
 *   progfn - IN callback to invoke to print progress message */
mfu_progress* mfu_progress_start(int secs, int count, MPI_Comm comm, mfu_progress_fn progfn);

/* update progress across all processes in work loop,
 *   vals - IN update contribution of this process to global sum
 *   prg  - IN pointer to struct returned in start */
void mfu_progress_update(uint64_t* vals, mfu_progress* prg);

/* continue broadcasting progress until all processes have completed,
 * and free structure allocated in start
 *   vals  - IN array with latest contribution from this process to global sum
 *   pprg  - IN address of pointer to struct returned in start */
void mfu_progress_complete(uint64_t* vals, mfu_progress** pprg);

#endif /* MFU_PROGRESS_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
