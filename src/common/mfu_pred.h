/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_PRED_H
#define MFU_PRED_H

#include "mfu.h"

/* defines a structure that can hold the time since Linux epoch
 * with nanosecond resolution */
typedef struct mfu_pred_times_t {
    uint64_t secs;  /* seconds since Linux epoch */
    uint64_t nsecs; /* nanoseconds since last second */
} mfu_pred_times;

/* defines a structure that can hold the time since Linux epoch
 * with nanosecond resolution as well as a comparison value */
typedef struct mfu_pred_times_rel_t {
    int direction;      /* -1 (less than), 0 (equal), 1 (greater than) */
    uint64_t magnitude; /* value */
    mfu_pred_times t;   /* base time to be compared against */
} mfu_pred_times_rel;

/* allocate a new predicate list */
mfu_pred* mfu_pred_new(void);

/* free a predicate list, sets pointer to mfu_pred to NULL on return */
void mfu_pred_free(mfu_pred**);

/* add predicate function to chain,
 * arg is optional and its meaning depends on the test being added,
 * if not NULL, it should be allocated such that it can be
 * freed via mfu_free() during call to mfu_pred_free() */
void mfu_pred_add(mfu_pred* pred, mfu_pred_fn predicate, void* arg);

/* execute predicate chain against specified flist item,
 * returns 1 if item satisfies predicate, 0 if not, and -1 if error */
int mfu_pred_execute(mfu_flist flist, uint64_t idx, const mfu_pred*);

/* captures current time and returns it in an mfu_pred_times structure,
 * must be freed by caller with mfu_free */
mfu_pred_times* mfu_pred_now(void);

/* given a find-like number string (N, +N, -N) and a base times structure,
 * allocate and return a structure that records a comparison to this base time,
 * N (exactly N), +N (greater than N), -N (less than N) */
mfu_pred_times_rel* mfu_pred_relative(const char* str, const mfu_pred_times* t);

/* --------------------------
 * Predefined predicates
 * -------------------------- */

/* find-like tests against file list elements */

/* tests file name of element, using shell pattern matching,
 * arg to mfu_pred_add should be a copy of the pattern string */
int MFU_PRED_NAME(mfu_flist flist, uint64_t idx, void* arg);

/* tests full path of element, using shell pattern matching,
 * arg to mfu_pred_add should be a copy of the pattern string */
int MFU_PRED_PATH(mfu_flist flist, uint64_t idx, void* arg);

/* tests full path of element, using regexec(),
 * arg to mfu_pred_add should be a copy of the compiled regex */
int MFU_PRED_REGEX(mfu_flist flist, uint64_t idx, void* arg);

/* tests the numeric group id of an element,
 * arg to mfu_pred_add should be a copy of the number as a string,
 * +N and -N notation can be used to check open ended ranges */
int MFU_PRED_GID(mfu_flist flist, uint64_t idx, void* arg);

/* tests the group name of an element,
 * arg to mfu_pred_add should be a copy of the name as a string */
int MFU_PRED_GROUP(mfu_flist flist, uint64_t idx, void* arg);

/* tests the numeric user id of an element,
 * arg to mfu_pred_add should be a copy of the number as a string,
 * +N and -N notation can be used to check open ended ranges */
int MFU_PRED_UID(mfu_flist flist, uint64_t idx, void* arg);

/* tests the user name of an element,
 * arg to mfu_pred_add should be a copy of the name as a string */
int MFU_PRED_USER  (mfu_flist flist, uint64_t idx, void* arg);

/* tests the size of an element, if element is a regular file,
 * arg to mfu_pred_add should be a copy of the size as a string,
 * +N and -N notation can be used to check open ended ranges,
 * units may also be appended, e.g., "+2GB" */
int MFU_PRED_SIZE  (mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the element was accessed in the last N minutes,
 * arg to mfu_pred_add should be a struct returned from mfu_pred_relative */
int MFU_PRED_AMIN(mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the element was modified in the last N minutes,
 * arg to mfu_pred_add should be a struct returned from mfu_pred_relative */
int MFU_PRED_MMIN(mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the element status was changed in the last N minutes,
 * arg to mfu_pred_add should be a struct returned from mfu_pred_relative */
int MFU_PRED_CMIN(mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the element was accessed in the last N days (24 hours),
 * arg to mfu_pred_add should be a struct returned from mfu_pred_relative */
int MFU_PRED_ATIME(mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the element was modified in the last N days (24 hours),
 * arg to mfu_pred_add should be a struct returned from mfu_pred_relative */
int MFU_PRED_MTIME(mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the element status was changed in the last N days (24 hours),
 * arg to mfu_pred_add should be a struct returned from mfu_pred_relative */
int MFU_PRED_CTIME(mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the element was accessed more recently than the specified time,
 * arg to mfu_pred_add should be a copy of a mfu_pred_times struct */
int MFU_PRED_ANEWER(mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the element was modified more recently than the specified time,
 * arg to mfu_pred_add should be a copy of a mfu_pred_times struct */
int MFU_PRED_MNEWER(mfu_flist flist, uint64_t idx, void* arg);

/* tests whether the status of the element was changed more recently than the specified time,
 * arg to mfu_pred_add should be a copy of a mfu_pred_times struct */
int MFU_PRED_CNEWER(mfu_flist flist, uint64_t idx, void* arg);

/* tests the type of the element,
 * arg to mfu_pred_add should be a copy of a type string,
 * where the type string is:
 *   "d" - directory
 *   "f" - regular file
 *   "l" - symlink */
int MFU_PRED_TYPE(mfu_flist flist, uint64_t idx, void* arg);

#endif /* MFU_PRED_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
