#ifndef _PRED_H
#define _PRED_H

#include "mfu.h"

/* function prototype for predicate */
typedef int (*mfu_pred_fn)(mfu_flist flist, uint64_t idx, void* arg);

/* defines element type for predicate list */
typedef struct mfu_pred_item_t {
    mfu_pred_fn f;                /* function to be executed */
    void* arg;                    /* argument to be passed to function */
    struct mfu_pred_item_t* next; /* pointer to next element in list */
} mfu_pred;

/* allocate a new predicate list */
mfu_pred* mfu_pred_new(void);

/* free a predicate list */
void mfu_pred_free(mfu_pred**);

/* add predicate function to chain,
 * if arg is not NULL, it should be allocated */
void mfu_pred_add(mfu_pred* pred, mfu_pred_fn predicate, void* arg);

/* execute predicate against specified flist item,
 * returns 1 if item satisfies predicate, 0 if not, and -1 if error */
int  mfu_pred_execute(mfu_flist flist, uint64_t idx, const mfu_pred*);

/* --------------------------
 * Predefined predicates
 * -------------------------- */

int mfu_pred_name  (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_path  (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_regex (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_gid   (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_group (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_uid   (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_user  (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_size  (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_amin  (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_mmin  (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_cmin  (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_atime (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_mtime (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_ctime (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_anewer(mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_mnewer(mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_cnewer(mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_type  (mfu_flist flist, uint64_t idx, void* arg);

int mfu_pred_exec  (mfu_flist flist, uint64_t idx, void* arg);
int mfu_pred_print (mfu_flist flist, uint64_t idx, void* arg);

#endif
