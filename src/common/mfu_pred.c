#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <libgen.h>
#include <fnmatch.h>
#include <sys/time.h>

#include <regex.h>

#include "mfu.h"
#include "mfu_flist_internal.h"
#include "mfu_pred.h"

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

mfu_pred* mfu_pred_new(void)
{
    mfu_pred* p = (mfu_pred*) MFU_MALLOC(sizeof(mfu_pred));
    p->f    = NULL;
    p->arg  = NULL;
    p->next = NULL;
    return p;
}

void mfu_pred_add(mfu_pred* head, mfu_pred_fn predicate, void* arg)
{
    if (head) {
        mfu_pred* p = head;
        
        while (p->next) {
            p = p->next;
        }
        
        p->next = (mfu_pred*) MFU_MALLOC(sizeof(mfu_pred));
        p       = p->next;
        p->f    = predicate;
        p->arg  = arg;
        p->next = NULL;
    }
}

/* free memory allocated in list of predicates */
void mfu_pred_free (mfu_pred** phead)
{
    if (phead != NULL) {
        mfu_pred* cur = *phead;
        while (cur) {
            mfu_pred* next = cur->next;
            if (cur->arg != NULL) {
                mfu_free(&cur->arg);
            }
            mfu_free(&cur);
            cur = next;
        }
        *phead = NULL;
    }
}

int mfu_pred_execute (mfu_flist flist, uint64_t idx, const mfu_pred* root)
{
    const mfu_pred* p = root;
    
    while (p) {
        if (p->f != NULL) {
            int ret = p->f(flist, idx, p->arg);
            if (ret <= 0) {
                return ret;
            }
        }
        p = p->next;
    }
    
    return 1;
}

/* captures current time and returns it in an mfu_pred_times structure,
 * must be freed by caller with mfu_free */
mfu_pred_times* mfu_pred_now(void)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* capture current time for any time based queries,
     * to get a consistent value, capture and bcast from rank 0 */
    uint64_t times[2];
    if (rank == 0) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        times[0] = (uint64_t) tv.tv_sec;
        times[1] = (uint64_t) tv.tv_usec;
    }
    MPI_Bcast(times, 2, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* copy time values into a newly allocated mfu_pred_times struct */
    mfu_pred_times* t = (mfu_pred_times*) MFU_MALLOC(sizeof(mfu_pred_times));
    t->secs  = times[0];
    t->nsecs = times[1] * 1000;
    return t;
}

/* given a find-like number string (N, +N, -N) and a base times structure,
 * allocate and return a structure that records a comparison to this base time */
mfu_pred_times_rel* mfu_pred_relative(const char* str, const mfu_pred_times* t)
{
    /* parse time string */
    int cmp;
    uint64_t val;
    parse_number((const char*)str, &cmp, &val);

    mfu_pred_times_rel* r = (mfu_pred_times_rel*) MFU_MALLOC(sizeof(mfu_pred_times_rel));
    r->direction = cmp;
    r->magnitude = val;
    r->t.secs  = t->secs;
    r->t.nsecs = t->nsecs;
    return r;
}

int MFU_PRED_TYPE (mfu_flist flist, uint64_t idx, void* arg)
{
    mode_t type = *((mode_t*)arg);

    mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

    return (mode & S_IFMT) == type;
}

int MFU_PRED_NAME (mfu_flist flist, uint64_t idx, void* arg)
{
    char* pattern = (char*) arg;

    const char* name = mfu_flist_file_get_name(flist, idx);

    char* tmpname = MFU_STRDUP(name);
    int ret = fnmatch(pattern, basename(tmpname), FNM_PERIOD) ? 0 : 1;
    mfu_free(&tmpname);

    return ret;
}

int MFU_PRED_PATH (mfu_flist flist, uint64_t idx, void* arg)
{
    char* pattern = (char*) arg;
    const char* name = mfu_flist_file_get_name(flist, idx);
    int ret = fnmatch(pattern, name, FNM_PERIOD) ? 0 : 1;
    return ret;
}

int MFU_PRED_REGEX (mfu_flist flist, uint64_t idx, void* arg)
{
    /* run regex on full path */
    regex_t* regex = (regex_t*) arg;
    const char* name = mfu_flist_file_get_name(flist, idx);
    int regex_return = regexec(regex, name, 0, NULL, 0);
    int ret = (regex_return == 0) ? 1 : 0;
    return ret;
}

int MFU_PRED_GID (mfu_flist flist, uint64_t idx, void* arg)
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

int MFU_PRED_GROUP (mfu_flist flist, uint64_t idx, void* arg)
{
    char* pattern = (char*) arg;
    const char* str = mfu_flist_file_get_groupname(flist, idx);
    int ret = 0;
    if (strcmp(str, pattern) == 0) {
        ret = 1;
    }
    return ret;
}

int MFU_PRED_UID (mfu_flist flist, uint64_t idx, void* arg)
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

int MFU_PRED_USER (mfu_flist flist, uint64_t idx, void* arg)
{
    char* pattern = (char*) arg;
    const char* str = mfu_flist_file_get_username(flist, idx);
    int ret = 0;
    if (strcmp(str, pattern) == 0) {
        ret = 1;
    }
    return ret;
}

int MFU_PRED_SIZE (mfu_flist flist, uint64_t idx, void* arg)
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
    mfu_pred_times_rel* r = (mfu_pred_times_rel*) arg;

    /* compute age of item in integer number of days */
    uint64_t item_nsecs = secs      * 1000000000 + nsecs;
    uint64_t now_nsecs  = r->t.secs * 1000000000 + r->t.nsecs;
    uint64_t age_nsecs = 0;
    if (item_nsecs < now_nsecs) {
        age_nsecs = now_nsecs - item_nsecs;
    }
    uint64_t age = age_nsecs / units;

    /* parse parameter from user */
    int cmp = r->direction;
    uint64_t val = r->magnitude;

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

int MFU_PRED_AMIN (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_atime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_atime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_MIN, arg);
}

int MFU_PRED_MMIN (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_mtime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_mtime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_MIN, arg);
}

int MFU_PRED_CMIN (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_ctime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_ctime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_MIN, arg);
}

int MFU_PRED_ATIME (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_atime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_atime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_DAY, arg);
}

int MFU_PRED_MTIME (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_mtime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_mtime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_DAY, arg);
}

int MFU_PRED_CTIME (mfu_flist flist, uint64_t idx, void* arg)
{
    /* get timestamp from item */
    uint64_t secs  = mfu_flist_file_get_ctime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_ctime_nsec(flist, idx);
    return check_time(secs, nsecs, NSECS_IN_DAY, arg);
}

int MFU_PRED_ANEWER (mfu_flist flist, uint64_t idx, void * arg)
{
    uint64_t secs  = mfu_flist_file_get_atime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_atime_nsec(flist, idx);
    mfu_pred_times* times = (mfu_pred_times*) arg;
    if (secs > times->secs ||
       (secs == times->secs && nsecs > times->nsecs))
    {
        return 1;
    } else {
        return 0;
    }
}

int MFU_PRED_MNEWER (mfu_flist flist, uint64_t idx, void * arg)
{
    uint64_t secs  = mfu_flist_file_get_mtime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_mtime_nsec(flist, idx);
    mfu_pred_times* times = (mfu_pred_times*) arg;
    if (secs > times->secs ||
       (secs == times->secs && nsecs > times->nsecs))
    {
        return 1;
    } else {
        return 0;
    }
}

int MFU_PRED_CNEWER (mfu_flist flist, uint64_t idx, void * arg)
{
    uint64_t secs  = mfu_flist_file_get_ctime(flist, idx);
    uint64_t nsecs = mfu_flist_file_get_ctime_nsec(flist, idx);
    mfu_pred_times* times = (mfu_pred_times*) arg;
    if (secs > times->secs ||
       (secs == times->secs && nsecs > times->nsecs))
    {
        return 1;
    } else {
        return 0;
    }
}
