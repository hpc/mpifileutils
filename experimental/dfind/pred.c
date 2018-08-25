#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <libgen.h>
#include <fnmatch.h>

#include "mfu.h"

#include "common.h"
#include "pred.h"

static void parse_number(const char* str, int* cmp, uint64_t* val)
{
    if (str[0] == '+') {
        /* check whether id is greater than target */
        *cmp = 1;
        *val = (uint64_t) atoi(&str[1]);
    } else if (str[0] == '-') {
        /* check whether id is less than target */
        *cmp = -1;
        *val = (uint64_t) atoi(&str[1]);
    } else {
        /* check whether id is equal to target */
        *cmp = 0;
        *val = (uint64_t) atoi(str);
    }
}

void pred_add(pred_t predicate, void* arg)
{
    if (! pred_head) {
        pred_head       = (pred_item*) MFU_MALLOC(sizeof(pred_item));
        pred_head->f    = predicate;
        pred_head->arg  = arg;
        pred_head->next = NULL;
        return;
    }
    
    pred_item* p = pred_head;
    
    while (p->next) {
        p = p->next;
    }
    
    p->next = (pred_item*) MFU_MALLOC(sizeof(pred_item));
    p       = p->next;
    p->f    = predicate;
    p->arg  = arg;
    p->next = NULL;
}

void pred_commit (void)
{
    int need_print = 1;

    pred_item* cur = pred_head;
    while (cur) {
        if (cur->f == pred_print || cur->f == pred_exec) {
            need_print = 0;
            break;
        }
        cur = cur->next;
    }
    
    if (need_print) {
//        pred_add(pred_print, NULL);
    }
}

int execute (mfu_flist flist, uint64_t idx, pred_item* root)
{
    pred_item* p = root;
    
    while (p) {
        if (p->f(flist, idx, p->arg) <= 0) {
            return -1;
        }
        p = p->next;
    }
    
    return 0;
}

int pred_type (mfu_flist flist, uint64_t idx, void* arg)
{
    mode_t m = (mode_t) arg;
    
    mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

    if ((mode & m) == m) {
        return 1;
    } else {
        return 0;
    }
}

int pred_name (mfu_flist flist, uint64_t idx, void* arg)
{
    char* pattern = (char*) arg;

    const char* name = mfu_flist_file_get_name(flist, idx);

    char* tmpname = MFU_STRDUP(name);
    int ret = fnmatch(pattern, basename(tmpname), FNM_PERIOD) ? 0 : 1;
    mfu_free(&tmpname);

    return ret;
}

int pred_gid (mfu_flist flist, uint64_t idx, void* arg)
{
    uint64_t id = mfu_flist_file_get_gid(flist, idx);

    int cmp;
    uint64_t val;
    parse_number((char*)arg, &cmp, &val);

    int ret = 0;
    if (cmp > 0) {
        /* check whether id is greater than target */
        if (id > val) {
            ret = 1;
        }
    } else if (cmp < 0) {
        /* check whether id is less than target */
        if (id < val) {
            ret = 1;
        }
    } else {
        /* check whether id is equal to target */
        if (id == val) {
            ret = 1;
        }
    }

    return ret;
}

int pred_group (mfu_flist flist, uint64_t idx, void* arg)
{
    char* pattern = (char*) arg;
    const char* str = mfu_flist_file_get_groupname(flist, idx);
    int ret = 0;
    if (strcmp(str, pattern) == 0) {
        ret = 1;
    }
    return ret;
}

int pred_uid (mfu_flist flist, uint64_t idx, void* arg)
{
    uint64_t id = mfu_flist_file_get_uid(flist, idx);

    int cmp;
    uint64_t val;
    parse_number((char*)arg, &cmp, &val);

    int ret = 0;
    if (cmp > 0) {
        /* check whether id is greater than target */
        if (id > val) {
            ret = 1;
        }
    } else if (cmp < 0) {
        /* check whether id is less than target */
        if (id < val) {
            ret = 1;
        }
    } else {
        /* check whether id is equal to target */
        if (id == val) {
            ret = 1;
        }
    }

    return ret;
}

int pred_user (mfu_flist flist, uint64_t idx, void* arg)
{
    char* pattern = (char*) arg;
    const char* str = mfu_flist_file_get_username(flist, idx);
    int ret = 0;
    if (strcmp(str, pattern) == 0) {
        ret = 1;
    }
    return ret;
}

int pred_newer (mfu_flist flist, uint64_t idx, void * arg)
{
    uint64_t mtime = mfu_flist_file_get_mtime(flist, idx);
    if (mtime > (uint64_t)arg) {
        return 1;
    } else {
        return 0;
    }
}

int pred_exec (mfu_flist flist, uint64_t idx, void* arg)
{
    int argmax = sysconf(_SC_ARG_MAX);
    int written = 0;
    int ret;
    char* command = MFU_STRDUP((char*) arg);
    char* cmdline = (char*) MFU_MALLOC(argmax);
    char* subst = strstr(command, "{}");
    
    if (subst) {
        subst[0] = '\0';
        subst += 2; /* Point to the first char after '{}' */
    }

    const char* name = mfu_flist_file_get_name(flist, idx);

    written = snprintf(cmdline, argmax/sizeof(char), "%s%s%s", command, name, subst);
    if (written > argmax/sizeof(char)) {
        fprintf(stderr, "argument %s to exec too long.\n", cmdline);
        mfu_free(&cmdline);
        mfu_free(&command);
        return -1;
    }
    
    ret = system(cmdline);

    mfu_free(&cmdline);
    mfu_free(&command);

    return ret ? 0 : 1;
}

int pred_print (mfu_flist flist, uint64_t idx, void* arg)
{
    const char* name = mfu_flist_file_get_name(flist, idx);
    printf("%s\n", name);
    return 1;
}
