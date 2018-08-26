#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <libgen.h>
#include <fnmatch.h>

#include <regex.h>

#include "mfu.h"

#include "common.h"
#include "pred.h"

static uint64_t NSECS_IN_MIN = (uint64_t) (1000000000ULL * 60ULL);
static uint64_t NSECS_IN_DAY = (uint64_t) (1000000000ULL * 60ULL * 60ULL * 24ULL);

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

int pred_path (mfu_flist flist, uint64_t idx, void* arg)
{
    char* pattern = (char*) arg;
    const char* name = mfu_flist_file_get_name(flist, idx);
    int ret = fnmatch(pattern, name, FNM_PERIOD) ? 0 : 1;
    return ret;
}

int pred_regex (mfu_flist flist, uint64_t idx, void* arg)
{
    /* run regex on full path */
    regex_t* regex = (regex_t*) arg;
    const char* name = mfu_flist_file_get_name(flist, idx);
    int regex_return = regexec(regex, name, 0, NULL, 0);
    int ret = (regex_return == 0) ? 1 : 0;
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

int pred_size (mfu_flist flist, uint64_t idx, void* arg)
{
    int ret = 0;

    uint64_t size = mfu_flist_file_get_size(flist, idx);

    char* str = (char*) arg;
    unsigned long long bytes;
    if (str[0] == '+') {
        /* check whether size is greater than target */
        mfu_abtoull(&str[1], &bytes);
        if (size > (uint64_t)bytes) {
            ret = 1;
        }
    } else if (str[0] == '-') {
        /* check whether size is less than target */
        mfu_abtoull(&str[1], &bytes);
        if (size < (uint64_t)bytes) {
            ret = 1;
        }
    } else {
        /* check whether size is equal to target */
        mfu_abtoull(str, &bytes);
        if (size == (uint64_t)bytes) {
            ret = 1;
        }
    }

    return ret;
}

static int check_time (uint64_t secs, uint64_t nsecs, uint64_t units, void* arg)
{
    /* compute age of item in integer number of days */
    uint64_t item_nsecs = secs     * 1000000000 + nsecs;
    uint64_t now_nsecs  = now_secs * 1000000000 + now_usecs * 1000;
    uint64_t age_nsecs = 0;
    if (item_nsecs < now_nsecs) {
        age_nsecs = now_nsecs - item_nsecs;
    }
    uint64_t age = age_nsecs / units;

    /* parse parameter from user */
    int cmp;
    uint64_t val;
    parse_number((char*)arg, &cmp, &val);

    int ret = 0;
    if (cmp > 0) {
        /* check whether age is greater than target */
        if (age > val) {
            ret = 1;
        }
    } else if (cmp < 0) {
        /* check whether age is less than target */
        if (age < val) {
            ret = 1;
        }
    } else {
        /* check whether age is equal to target */
        if (age == val) {
            ret = 1;
        }
    }

    return ret;
}

int pred_amin (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_atime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_atime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_MIN, arg);
}

int pred_mmin (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_mtime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_mtime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_MIN, arg);
}

int pred_cmin (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_ctime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_ctime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_MIN, arg);
}

int pred_atime (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_atime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_atime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_DAY, arg);
}

int pred_mtime (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_mtime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_mtime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_DAY, arg);
}

int pred_ctime (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_ctime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_ctime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_DAY, arg);
}

int pred_anewer (mfu_flist flist, uint64_t idx, void * arg)
{
    uint64_t secs  = mfu_flist_file_get_atime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_atime_nsec(flist, idx);
    struct stattimes* times = (struct stattimes*) arg;
    if (secs > times->secs ||
       (secs == times->secs && nsecs > times->nsecs))
    {
        return 1;
    } else {
        return 0;
    }
}

int pred_mnewer (mfu_flist flist, uint64_t idx, void * arg)
{
    uint64_t secs  = mfu_flist_file_get_mtime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_mtime_nsec(flist, idx);
    struct stattimes* times = (struct stattimes*) arg;
    if (secs > times->secs ||
       (secs == times->secs && nsecs > times->nsecs))
    {
        return 1;
    } else {
        return 0;
    }
}

int pred_cnewer (mfu_flist flist, uint64_t idx, void * arg)
{
    uint64_t secs  = mfu_flist_file_get_ctime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_ctime_nsec(flist, idx);
    struct stattimes* times = (struct stattimes*) arg;
    if (secs > times->secs ||
       (secs == times->secs && nsecs > times->nsecs))
    {
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
