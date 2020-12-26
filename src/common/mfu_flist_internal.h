/* defines internal types and function prototypes for mfu_flist
 * implementations */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_FLIST_INTERNAL_H
#define MFU_FLIST_INTERNAL_H

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "mpi.h"

#include "mfu.h"
#include "strmap.h"

/****************************************
 * Define types
 ***************************************/

/* linked list element of stat data used during walk */
typedef struct list_elem {
    char* file;             /* file name (strdup'd) */
    int depth;              /* depth within directory tree */
    mfu_filetype type;    /* type of file object */
    int detail;             /* flag to indicate whether we have stat data */
    uint64_t mode;          /* stat mode */
    uint64_t uid;           /* user id */
    uint64_t gid;           /* group id */
    uint64_t atime;         /* access time */
    uint64_t atime_nsec;    /* access time nanoseconds */
    uint64_t mtime;         /* modify time */
    uint64_t mtime_nsec;    /* modify time nanoseconds */
    uint64_t ctime;         /* create time */
    uint64_t ctime_nsec;    /* create time nanoseconds */
    uint64_t size;          /* file size in bytes */
    struct list_elem* next; /* pointer to next item */
    /* vars for a non-posix DAOS copy */
    uint64_t obj_id_lo;
    uint64_t obj_id_hi;
} elem_t;

/* holds an array of objects: users, groups, or file data */
typedef struct {
    void* buf;       /* pointer to memory buffer holding data */
    size_t bufsize;  /* number of bytes in buffer */
    uint64_t count;  /* number of items */
    uint64_t chars;  /* max name of item */
    MPI_Datatype dt; /* MPI datatype for sending/receiving/writing to file */
} buf_t;

/* abstraction for distributed file list */
typedef struct flist {
    int detail;              /* set to 1 if we have stat, 0 if just file name */
    uint64_t offset;         /* global offset of our file across all procs */
    uint64_t total_files;    /* total item count in list across all procs */
    uint64_t total_users;    /* number of users (valid if detail is 1) */
    uint64_t total_groups;   /* number of groups (valid if detail is 1) */
    uint64_t max_file_name;  /* maximum filename strlen()+1 in global list */
    uint64_t max_user_name;  /* maximum username strlen()+1 */
    uint64_t max_group_name; /* maximum groupname strlen()+1 */
    int min_depth;           /* minimum file depth */
    int max_depth;           /* maximum file depth */

    /* variables to track linked list of stat data during walk */
    uint64_t list_count; /* number of items in list */
    elem_t*  list_head;  /* points to item at head of list */
    elem_t*  list_tail;  /* points to item at tail of list */
    elem_t** list_index; /* an array with pointers to each item in list */
    uint64_t list_cap;   /* current capacity of list_index */

    /* buffers of users, groups, and files */
    buf_t users;
    buf_t groups;
    int have_users;        /* set to 1 if user map is valid */
    int have_groups;       /* set to 1 if group map is valid */
    strmap* user_id2name;  /* map linux uid to user name */
    strmap* group_id2name; /* map linux gid to group name */
} flist_t;

/* create a type consisting of chars number of characters
 * immediately followed by a uint32_t */
void mfu_flist_usrgrp_create_stridtype(int chars, MPI_Datatype* dt);

/* build a name-to-id map and an id-to-name map */
void mfu_flist_usrgrp_create_map(const buf_t* items, strmap* id2name);

/* given an id, lookup its corresponding name, returns id converted
 * to a string if no matching name is found */
const char* mfu_flist_usrgrp_get_name_from_id(strmap* id2name, uint64_t id);

/* read user array from file system using getpwent() */
void mfu_flist_usrgrp_get_users(flist_t* flist);

/* read group array from file system using getgrent() */
void mfu_flist_usrgrp_get_groups(flist_t* flist);

/* initialize structures for user and group names and id-to-name maps */
void mfu_flist_usrgrp_init(flist_t* flist);

/* free user and group structures */
void mfu_flist_usrgrp_free(flist_t* flist);

/* copy user and group structures from srclist to flist */
void mfu_flist_usrgrp_copy(flist_t* srclist, flist_t* flist);

/* append element to tail of linked list */
void mfu_flist_insert_elem(flist_t* flist, elem_t* elem);

/* insert a file given its mode and optional stat data */
void mfu_flist_insert_stat(flist_t* flist, const char* fpath, mode_t mode, const struct stat* sb);

/* given path, return level within directory tree,
 * counts '/' characters assuming path is standardized
 * and absolute */
int mfu_flist_compute_depth(const char* path);

#endif /* MFU_FLIST_INTERNAL_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
