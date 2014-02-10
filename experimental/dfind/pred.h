#ifndef _PRED_H
#define _PRED_H

struct target {
    char* fname;
    struct stat* statbuf;
};

typedef int (*pred_t)(struct target, void* arg);

struct pred_item_t {
    pred_t f;
    void* arg;
    struct pred_item_t* next;
};

typedef struct pred_item_t pred_item;
pred_item* pred_head;
void pred_add(pred_t, void*);
void pred_commit(void);

int name(struct target, void*);
int pred_exec(struct target, void*);
int pred_print(struct target, void*);
int pred_newer(struct target, void*);
int pred_type(struct target, void*);

int execute(char*, pred_item*);

#endif
