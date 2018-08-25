#ifndef _PRED_H
#define _PRED_H

#include "mfu.h"

typedef int (*pred_t)(mfu_flist flist, uint64_t idx, void* arg);

struct pred_item_t
{
    pred_t f;
    void* arg;
    struct pred_item_t* next;
};

typedef struct pred_item_t pred_item;
pred_item* pred_head;
void pred_add(pred_t, void*);
void pred_commit(void);

int pred_name  (mfu_flist flist, uint64_t idx, void*);
int pred_gid   (mfu_flist flist, uint64_t idx, void*);
int pred_group (mfu_flist flist, uint64_t idx, void*);
int pred_uid   (mfu_flist flist, uint64_t idx, void*);
int pred_user  (mfu_flist flist, uint64_t idx, void*);
int pred_newer (mfu_flist flist, uint64_t idx, void*);
int pred_type  (mfu_flist flist, uint64_t idx, void*);

int pred_exec  (mfu_flist flist, uint64_t idx, void*);
int pred_print (mfu_flist flist, uint64_t idx, void*);

int execute (mfu_flist flist, uint64_t idx, pred_item*);

#endif
