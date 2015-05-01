#define _GNU_SOURCE
#include <dirent.h>
#include <fcntl.h>
#include <sys/syscall.h>

#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h> /* asctime / localtime */

/* These headers are needed to query the Lustre MDS for stat
 * information.  This information may be incomplete, but it
 * is faster than a normal stat, which requires communication
 * with the MDS plus every OST a file is striped across. */
//#define LUSTRE_STAT
#ifdef LUSTRE_STAT
#include <sys/ioctl.h>
#include <lustre/lustre_user.h>
#endif /* LUSTRE_STAT */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"
#include "strmap.h"

/****************************************
 * Define types
 ***************************************/

/* linked list element of stat data used during walk */
typedef struct list_elem {
    char* file;             /* file name (strdup'd) */
    int depth;              /* depth within directory tree */
    bayer_filetype type;    /* type of file object */
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
    uint64_t total_files;    /* total file count in list across all procs */
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

    /* buffers of users, groups, and files */
    buf_t users;
    buf_t groups;
    int have_users;        /* set to 1 if user map is valid */
    int have_groups;       /* set to 1 if group map is valid */
    strmap* user_id2name;  /* map linux uid to user name */
    strmap* group_id2name; /* map linux gid to group name */
} flist_t;

/****************************************
 * Globals
 ***************************************/

/* Need global variables during walk to record top directory
 * and file list */
static uint64_t CURRENT_NUM_DIRS;
static char** CURRENT_DIRS;
static flist_t* CURRENT_LIST;

/****************************************
 * Functions on types
 ***************************************/

static void buft_init(buf_t* items)
{
    items->buf     = NULL;
    items->bufsize = 0;
    items->count   = 0;
    items->chars   = 0;
    items->dt      = MPI_DATATYPE_NULL;
}

static void buft_copy(buf_t* src, buf_t* dst)
{
    dst->bufsize = src->bufsize;
    dst->buf = BAYER_MALLOC(dst->bufsize);
    memcpy(dst->buf, src->buf, dst->bufsize);

    dst->count = src->count;
    dst->chars = src->chars;

    if (src->dt != MPI_DATATYPE_NULL) {
        MPI_Type_dup(src->dt, &dst->dt);
    }
    else {
        dst->dt = MPI_DATATYPE_NULL;
    }
}

static void buft_free(buf_t* items)
{
    bayer_free(&items->buf);
    items->bufsize = 0;

    if (items->dt != MPI_DATATYPE_NULL) {
        MPI_Type_free(&(items->dt));
    }

    items->count = 0;
    items->chars = 0;

    return;
}

/* given a mode_t from stat, return the corresponding BAYER filetype */
static bayer_filetype get_bayer_filetype(mode_t mode)
{
    /* set file type */
    bayer_filetype type;
    if (S_ISDIR(mode)) {
        type = BAYER_TYPE_DIR;
    }
    else if (S_ISREG(mode)) {
        type = BAYER_TYPE_FILE;
    }
    else if (S_ISLNK(mode)) {
        type = BAYER_TYPE_LINK;
    }
    else {
        /* unknown file type */
        type = BAYER_TYPE_UNKNOWN;
    }
    return type;
}

/* given path, return level within directory tree,
 * counts '/' characters assuming path is standardized
 * and absolute */
static int get_depth(const char* path)
{
    const char* c;
    int depth = 0;
    for (c = path; *c != '\0'; c++) {
        if (*c == '/') {
            depth++;
        }
    }
    return depth;
}

/* create a datatype to hold file name and stat info */
static void create_stattype(int detail, int chars, MPI_Datatype* dt_stat)
{
    /* build type for file path */
    MPI_Datatype dt_filepath;
    MPI_Type_contiguous(chars, MPI_CHAR, &dt_filepath);

    /* build keysat type */
    int fields;
    MPI_Datatype types[11];
    if (detail) {
        fields = 11;
        types[0]  = dt_filepath;  /* file name */
        types[1]  = MPI_UINT64_T; /* mode */
        types[2]  = MPI_UINT64_T; /* uid */
        types[3]  = MPI_UINT64_T; /* gid */
        types[4]  = MPI_UINT64_T; /* atime secs */
        types[5]  = MPI_UINT64_T; /* atime nsecs */
        types[6]  = MPI_UINT64_T; /* mtime secs */
        types[7]  = MPI_UINT64_T; /* mtime nsecs */
        types[8]  = MPI_UINT64_T; /* ctime secs */
        types[9]  = MPI_UINT64_T; /* ctime nsecs */
        types[10] = MPI_UINT64_T; /* size */
    }
    else {
        fields = 2;
        types[0] = dt_filepath;  /* file name */
        types[1] = MPI_UINT32_T; /* file type */
    }
    DTCMP_Type_create_series(fields, types, dt_stat);

    MPI_Type_free(&dt_filepath);
    return;
}

/* return number of bytes needed to pack element */
static size_t list_elem_pack_size(int detail, uint64_t chars, const elem_t* elem)
{
    size_t size;
    if (detail) {
        size = chars + 0 * 4 + 10 * 8;
    }
    else {
        size = chars + 1 * 4;
    }
    return size;
}

/* pack element into buffer and return number of bytes written */
static size_t list_elem_pack(void* buf, int detail, uint64_t chars, const elem_t* elem)
{
    /* set pointer to start of buffer */
    char* start = (char*) buf;
    char* ptr = start;

    /* copy in file name */
    char* file = elem->file;
    strcpy(ptr, file);
    ptr += chars;

    if (detail) {
        bayer_pack_uint64(&ptr, elem->mode);
        bayer_pack_uint64(&ptr, elem->uid);
        bayer_pack_uint64(&ptr, elem->gid);
        bayer_pack_uint64(&ptr, elem->atime);
        bayer_pack_uint64(&ptr, elem->atime_nsec);
        bayer_pack_uint64(&ptr, elem->mtime);
        bayer_pack_uint64(&ptr, elem->mtime_nsec);
        bayer_pack_uint64(&ptr, elem->ctime);
        bayer_pack_uint64(&ptr, elem->ctime_nsec);
        bayer_pack_uint64(&ptr, elem->size);
    }
    else {
        /* just have the file type */
        bayer_pack_uint32(&ptr, elem->type);
    }

    size_t bytes = (size_t)(ptr - start);
    return bytes;
}

/* unpack element from buffer and return number of bytes read */
static size_t list_elem_unpack(const void* buf, int detail, uint64_t chars, elem_t* elem)
{
    const char* start = (const char*) buf;
    const char* ptr = start;

    /* get name and advance pointer */
    const char* file = ptr;
    ptr += chars;

    /* copy path */
    elem->file = BAYER_STRDUP(file);

    /* set depth */
    elem->depth = get_depth(file);

    elem->detail = detail;

    if (detail) {
        /* extract fields */
        bayer_unpack_uint64(&ptr, &elem->mode);
        bayer_unpack_uint64(&ptr, &elem->uid);
        bayer_unpack_uint64(&ptr, &elem->gid);
        bayer_unpack_uint64(&ptr, &elem->atime);
        bayer_unpack_uint64(&ptr, &elem->atime_nsec);
        bayer_unpack_uint64(&ptr, &elem->mtime);
        bayer_unpack_uint64(&ptr, &elem->mtime_nsec);
        bayer_unpack_uint64(&ptr, &elem->ctime);
        bayer_unpack_uint64(&ptr, &elem->ctime_nsec);
        bayer_unpack_uint64(&ptr, &elem->size);

        /* use mode to set file type */
        elem->type = get_bayer_filetype((mode_t)elem->mode);
    }
    else {
        bayer_unpack_uint32(&ptr, &elem->type);
    }

    size_t bytes = (size_t)(ptr - start);
    return bytes;
}

/* return number of bytes needed to pack element */
static size_t list_elem_pack2_size(int detail, uint64_t chars, const elem_t* elem)
{
    size_t size;
    if (detail) {
        size = 2 * 4 + chars + 0 * 4 + 10 * 8;
    }
    else {
        size = 2 * 4 + chars + 1 * 4;
    }
    return size;
}

/* pack element into buffer and return number of bytes written */
static size_t list_elem_pack2(void* buf, int detail, uint64_t chars, const elem_t* elem)
{
    /* set pointer to start of buffer */
    char* start = (char*) buf;
    char* ptr = start;

    /* copy in detail flag */
    bayer_pack_uint32(&ptr, (uint32_t) detail);

    /* copy in length of file name field */
    bayer_pack_uint32(&ptr, (uint32_t) chars);

    /* copy in file name */
    char* file = elem->file;
    strcpy(ptr, file);
    ptr += chars;

    if (detail) {
        /* copy in fields */
        bayer_pack_uint64(&ptr, elem->mode);
        bayer_pack_uint64(&ptr, elem->uid);
        bayer_pack_uint64(&ptr, elem->gid);
        bayer_pack_uint64(&ptr, elem->atime);
        bayer_pack_uint64(&ptr, elem->atime_nsec);
        bayer_pack_uint64(&ptr, elem->mtime);
        bayer_pack_uint64(&ptr, elem->mtime_nsec);
        bayer_pack_uint64(&ptr, elem->ctime);
        bayer_pack_uint64(&ptr, elem->ctime_nsec);
        bayer_pack_uint64(&ptr, elem->size);
    }
    else {
        /* just have the file type */
        bayer_pack_uint32(&ptr, elem->type);
    }

    size_t bytes = (size_t)(ptr - start);
    return bytes;
}

/* unpack element from buffer and return number of bytes read */
static size_t list_elem_unpack2(const void* buf, elem_t* elem)
{
    /* set pointer to start of buffer */
    const char* start = (const char*) buf;
    const char* ptr = start;

    /* extract detail field */
    uint32_t detail;
    bayer_unpack_uint32(&ptr, &detail);

    /* extract length of file name field */
    uint32_t chars;
    bayer_unpack_uint32(&ptr, &chars);

    /* get name and advance pointer */
    const char* file = ptr;
    ptr += chars;

    /* copy path */
    elem->file = BAYER_STRDUP(file);

    /* set depth */
    elem->depth = get_depth(file);

    elem->detail = (int) detail;

    if (detail) {
        /* extract fields */
        bayer_unpack_uint64(&ptr, &elem->mode);
        bayer_unpack_uint64(&ptr, &elem->uid);
        bayer_unpack_uint64(&ptr, &elem->gid);
        bayer_unpack_uint64(&ptr, &elem->atime);
        bayer_unpack_uint64(&ptr, &elem->atime_nsec);
        bayer_unpack_uint64(&ptr, &elem->mtime);
        bayer_unpack_uint64(&ptr, &elem->mtime_nsec);
        bayer_unpack_uint64(&ptr, &elem->ctime);
        bayer_unpack_uint64(&ptr, &elem->ctime_nsec);
        bayer_unpack_uint64(&ptr, &elem->size);

        /* use mode to set file type */
        elem->type = get_bayer_filetype((mode_t)elem->mode);
    }
    else {
        /* only have type */
        uint32_t type;
        bayer_unpack_uint32(&ptr, &type);
        elem->type = (bayer_filetype) type;
    }

    size_t bytes = (size_t)(ptr - start);
    return bytes;
}

static size_t list_elem_encode_size(const elem_t* elem)
{
    size_t reclen = strlen(elem->file); /* filename */
    reclen += 2; /* | + type letter */
    reclen += 1; /* trailing newline */
    return reclen;
}

static size_t list_elem_encode(void* buf, const elem_t* elem)
{
    char* ptr = (char*) buf;

    size_t len = strlen(elem->file);
    strncpy(ptr, elem->file, len);
    ptr += len;

    *ptr = '|';
    ptr++;

    bayer_filetype type = elem->type;
    if (type == BAYER_TYPE_FILE) {
        *ptr = 'F';
    } else if (type == BAYER_TYPE_DIR) {
        *ptr = 'D';
    } else if (type == BAYER_TYPE_LINK) {
        *ptr = 'L';
    } else {
        *ptr = 'U';
    }
    ptr++;

    *ptr = '\n';
 
    size_t reclen = len + 3;
    return reclen;
}

/* given a buffer, decode element and store values in elem */
static void list_elem_decode(char* buf, elem_t* elem)
{
    /* get name and advance pointer */
    const char* file = strtok(buf, "|");

    /* copy path */
    elem->file = BAYER_STRDUP(file);

    /* set depth */
    elem->depth = get_depth(file);

    elem->detail = 0;

    const char* type = strtok(NULL, "|");
    char c = type[0];
    if (c == 'F') {
        elem->type = BAYER_TYPE_FILE;
    } else if (c == 'D') {
        elem->type = BAYER_TYPE_DIR;
    } else if (c == 'L') {
        elem->type = BAYER_TYPE_LINK;
    } else {
        elem->type = BAYER_TYPE_UNKNOWN;
    }

    return;
}

/* append element to tail of linked list */
static void list_insert_elem(flist_t* flist, elem_t* elem)
{
    /* set head if this is the first item */
    if (flist->list_head == NULL) {
        flist->list_head = elem;
    }

    /* update last element to point to this new element */
    elem_t* tail = flist->list_tail;
    if (tail != NULL) {
        tail->next = elem;
    }

    /* make this element the new tail */
    flist->list_tail = elem;
    elem->next = NULL;

    /* increase list count by one */
    flist->list_count++;

    /* delete the index if we have one, it's out of date */
    bayer_free(&flist->list_index);

    return;
}

/* insert a file given its mode and optional stat data */
static void list_insert_stat(flist_t* flist, const char* fpath, mode_t mode, const struct stat* sb)
{
    /* create new element to record file path, file type, and stat info */
    elem_t* elem = (elem_t*) BAYER_MALLOC(sizeof(elem_t));

    /* copy path */
    elem->file = BAYER_STRDUP(fpath);

    /* set depth */
    elem->depth = get_depth(fpath);

    /* set file type */
    elem->type = get_bayer_filetype(mode);

    /* copy stat info */
    if (sb != NULL) {
        elem->detail = 1;
        elem->mode  = (uint64_t) sb->st_mode;
        elem->uid   = (uint64_t) sb->st_uid;
        elem->gid   = (uint64_t) sb->st_gid;
        elem->atime = (uint64_t) sb->st_atime;
        elem->mtime = (uint64_t) sb->st_mtime;
        elem->ctime = (uint64_t) sb->st_ctime;
        elem->size  = (uint64_t) sb->st_size;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
        elem->atime_nsec = (uint64_t) sb->st_atimespec.tv_nsec;
        elem->ctime_nsec = (uint64_t) sb->st_ctimespec.tv_nsec;
        elem->mtime_nsec = (uint64_t) sb->st_mtimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
        elem->atime_nsec = (uint64_t) sb->st_atim.tv_nsec;
        elem->ctime_nsec = (uint64_t) sb->st_ctim.tv_nsec;
        elem->mtime_nsec = (uint64_t) sb->st_mtim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
        elem->atime_nsec = (uint64_t) sb->st_atime_n;
        elem->ctime_nsec = (uint64_t) sb->st_ctime_n;
        elem->mtime_nsec = (uint64_t) sb->st_mtime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
        elem->atime_nsec = (uint64_t) sb->st_uatime * 1000;
        elem->ctime_nsec = (uint64_t) sb->st_uctime * 1000;
        elem->mtime_nsec = (uint64_t) sb->st_umtime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
        elem->atime_nsec = (uint64_t) sb->st_atime_usec * 1000;
        elem->ctime_nsec = (uint64_t) sb->st_ctime_usec * 1000;
        elem->mtime_nsec = (uint64_t) sb->st_mtime_usec * 1000;
#else
        elem->atime_nsec = 0;
        elem->ctime_nsec = 0;
        elem->mtime_nsec = 0;
#endif

        /* TODO: link to user and group names? */
    }
    else {
        elem->detail = 0;
    }

    /* append element to tail of linked list */
    list_insert_elem(flist, elem);

    return;
}

/* insert copy of specified element into list */
static void list_insert_copy(flist_t* flist, elem_t* src)
{
    /* create new element */
    elem_t* elem = (elem_t*) BAYER_MALLOC(sizeof(elem_t));

    /* copy values from source */
    elem->file       = BAYER_STRDUP(src->file);
    elem->depth      = src->depth;
    elem->type       = src->type;
    elem->detail     = src->detail;
    elem->mode       = src->mode;
    elem->uid        = src->uid;
    elem->gid        = src->gid;
    elem->atime      = src->atime;
    elem->atime_nsec = src->atime_nsec;
    elem->mtime      = src->mtime;
    elem->mtime_nsec = src->mtime_nsec;
    elem->ctime      = src->ctime;
    elem->ctime_nsec = src->ctime_nsec;
    elem->size       = src->size;

    /* append element to tail of linked list */
    list_insert_elem(flist, elem);

    return;
}

/* insert a file given a pointer to packed data */
static void list_insert_decode(flist_t* flist, char* buf)
{
    /* create new element to record file path, file type, and stat info */
    elem_t* elem = (elem_t*) BAYER_MALLOC(sizeof(elem_t));

    /* decode buffer and store values in element */
    list_elem_decode(buf, elem);

    /* append element to tail of linked list */
    list_insert_elem(flist, elem);

    return;
}

/* insert a file given a pointer to packed data */
static size_t list_insert_ptr(flist_t* flist, char* ptr, int detail, uint64_t chars)
{
    /* create new element to record file path, file type, and stat info */
    elem_t* elem = (elem_t*) BAYER_MALLOC(sizeof(elem_t));

    /* get name and advance pointer */
    size_t bytes = list_elem_unpack(ptr, detail, chars, elem);

    /* append element to tail of linked list */
    list_insert_elem(flist, elem);

    return bytes;
}

/* delete linked list of stat items */
static void list_delete(flist_t* flist)
{
    elem_t* current = flist->list_head;
    while (current != NULL) {
        elem_t* next = current->next;
        bayer_free(&current->file);
        bayer_free(&current);
        current = next;
    }
    flist->list_count = 0;
    flist->list_head  = NULL;
    flist->list_tail  = NULL;

    /* delete the cached index */
    bayer_free(&flist->list_index);

    return;
}

/* given an index, return pointer to that file element,
 * NULL if index is not in range */
static elem_t* list_get_elem(flist_t* flist, uint64_t idx)
{
    uint64_t max = flist->list_count;

    /* build index of list elements if we don't already have one */
    if (flist->list_index == NULL) {
        /* allocate array to record pointer to each element */
        size_t index_size = max * sizeof(elem_t*);
        flist->list_index = (elem_t**) BAYER_MALLOC(index_size);

        /* get pointer to each element */
        uint64_t i = 0;
        elem_t* current = flist->list_head;
        while (i < max && current != NULL) {
            flist->list_index[i] = current;
            current = current->next;
            i++;
        }
    }

    /* return pointer to element if index is within range */
    if (idx < max) {
        elem_t* elem = flist->list_index[idx];
        return elem;
    }
    return NULL;
}

static void list_compute_summary(flist_t* flist)
{
    /* initialize summary values */
    flist->max_file_name  = 0;
    flist->max_user_name  = 0;
    flist->max_group_name = 0;
    flist->min_depth      = 0;
    flist->max_depth      = 0;
    flist->total_files    = 0;
    flist->offset         = 0;

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* get total number of files in list */
    uint64_t total;
    uint64_t count = flist->list_count;
    MPI_Allreduce(&count, &total, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    flist->total_files = total;

    /* bail out early if no one has anything */
    if (total <= 0) {
        return;
    }

    /* compute the global offset of our first item */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }
    flist->offset = offset;

    /* compute local min/max values */
    int min_depth = -1;
    int max_depth = -1;
    uint64_t max_name = 0;
    elem_t* current = flist->list_head;
    while (current != NULL) {
        uint64_t len = (uint64_t)(strlen(current->file) + 1);
        if (len > max_name) {
            max_name = len;
        }

        int depth = current->depth;
        if (depth < min_depth || min_depth == -1) {
            min_depth = depth;
        }
        if (depth > max_depth || max_depth == -1) {
            max_depth = depth;
        }

        /* go to next item */
        current = current->next;
    }

    /* get global maximums */
    int global_max_depth;
    MPI_Allreduce(&max_depth, &global_max_depth, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    uint64_t global_max_name;
    MPI_Allreduce(&max_name, &global_max_name, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* since at least one rank has an item and max will be -1 on ranks
     * without an item, set our min to global max if we have no items,
     * this will ensure that our contribution is >= true global min */
    int global_min_depth;
    if (count == 0) {
        min_depth = global_max_depth;
    }
    MPI_Allreduce(&min_depth, &global_min_depth, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

    /* set summary values */
    flist->max_file_name = global_max_name;
    flist->min_depth = global_min_depth;
    flist->max_depth = global_max_depth;

    /* set summary on users and groups */
    if (flist->detail) {
        flist->total_users    = flist->users.count;
        flist->total_groups   = flist->groups.count;
        flist->max_user_name  = flist->users.chars;
        flist->max_group_name = flist->groups.chars;
    }

    return;
}

static int list_convert_to_dt(flist_t* flist, buf_t* items)
{
    /* initialize output params */
    items->buf   = NULL;
    items->count = 0;
    items->chars = 0;
    items->dt    = MPI_DATATYPE_NULL;

    /* find smallest length that fits max and consists of integer
     * number of 8 byte segments */
    int max = (int) flist->max_file_name;
    int chars = max / 8;
    if (chars * 8 < max) {
        chars++;
    }
    chars *= 8;

    /* nothing to do if no one has anything */
    if (chars <= 0) {
        return 0;
    }

    /* build stat type */
    MPI_Datatype dt;
    create_stattype(flist->detail, chars, &dt);

    /* get extent of stat type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* allocate buffer */
    uint64_t count = flist->list_count;
    size_t bufsize = (size_t)extent * count;
    void* buf = BAYER_MALLOC(bufsize);

    /* copy stat data into stat datatypes */
    char* ptr = (char*) buf;
    const elem_t* current = flist->list_head;
    while (current != NULL) {
        /* pack item into buffer and advance pointer */
        size_t bytes = list_elem_pack(ptr, flist->detail, (uint64_t)chars, current);
        ptr += bytes;
        current = current->next;
    }

    /* set output params */
    items->buf     = buf;
    items->bufsize = bufsize;
    items->count   = count;
    items->chars   = (uint64_t)chars;
    items->dt      = dt;

    return 0;
}

/* build a name-to-id map and an id-to-name map */
static void create_map(const buf_t* items, strmap* id2name)
{
    uint64_t i;
    const char* ptr = (const char*)items->buf;
    for (i = 0; i < items->count; i++) {
        const char* name = ptr;
        ptr += items->chars;

        uint64_t id;
        bayer_unpack_uint64(&ptr, &id);

        /* convert id number to string */
        char id_str[20];
        int len_int = snprintf(id_str, sizeof(id_str), "%llu", (unsigned long long) id);
        if (len_int < 0) {
            /* TODO: ERROR! */
            printf("ERROR!!!\n");
        }

        size_t len = (size_t) len_int;
        if (len > (sizeof(id_str) - 1)) {
            /* TODO: ERROR! */
            printf("ERROR!!!\n");
        }

        strmap_set(id2name, id_str, name);
    }
    return;
}

/* given an id, lookup its corresponding name, returns id converted
 * to a string if no matching name is found */
static const char* get_name_from_id(strmap* id2name, uint64_t id)
{
    /* convert id number to string representation */
    char id_str[20];
    int len_int = snprintf(id_str, sizeof(id_str), "%llu", (unsigned long long) id);
    if (len_int < 0) {
        /* TODO: ERROR! */
        printf("ERROR!!!\n");
    }

    size_t len = (size_t) len_int;
    if (len > (sizeof(id_str) - 1)) {
        /* TODO: ERROR! */
        printf("ERROR!!!\n");
    }

    /* lookup name by id */
    const char* name = strmap_get(id2name, id_str);

    /* if not found, store id as name and return that */
    if (name == NULL) {
        strmap_set(id2name, id_str, id_str);
        name = strmap_get(id2name, id_str);
    }

    return name;
}

/****************************************
 * File list user API
 ***************************************/

/* create object that BAYER_FLIST_NULL points to */
static flist_t flist_null;
bayer_flist BAYER_FLIST_NULL = &flist_null;

/* allocate and initialize a new file list object */
bayer_flist bayer_flist_new()
{
    /* allocate memory for file list, cast it to handle, initialize and return */
    flist_t* flist = (flist_t*) BAYER_MALLOC(sizeof(flist_t));

    flist->detail = 0;
    flist->total_files = 0;

    /* initialize linked list */
    flist->list_count = 0;
    flist->list_head  = NULL;
    flist->list_tail  = NULL;
    flist->list_index = NULL;

    /* initialize user, group, and file buffers */
    buft_init(&flist->users);
    buft_init(&flist->groups);

    /* allocate memory for maps */
    flist->have_users  = 0;
    flist->have_groups = 0;
    flist->user_id2name  = strmap_new();
    flist->group_id2name = strmap_new();

    bayer_flist bflist = (bayer_flist) flist;
    return bflist;
}

/* free resouces in file list */
void bayer_flist_free(bayer_flist* pbflist)
{
    /* convert handle to flist_t */
    flist_t* flist = *(flist_t**)pbflist;

    /* delete linked list */
    list_delete(flist);

    buft_free(&flist->users);
    buft_free(&flist->groups);

    strmap_delete(&flist->user_id2name);
    strmap_delete(&flist->group_id2name);

    bayer_free(&flist);

    /* set caller's pointer to NULL */
    *pbflist = BAYER_FLIST_NULL;

    return;
}

/* given an input list, split items into separate lists depending
 * on their depth, returns number of levels, minimum depth, and
 * array of lists as output */
void bayer_flist_array_by_depth(
    bayer_flist srclist,
    int* outlevels,
    int* outmin,
    bayer_flist** outlists)
{
    /* check that our pointers are valid */
    if (outlevels == NULL || outmin == NULL || outlists == NULL) {
        return;
    }

    /* initialize return values */
    *outlevels = 0;
    *outmin    = -1;
    *outlists  = NULL;

    /* get total file count */
    uint64_t total = bayer_flist_global_size(srclist);
    if (total == 0) {
        return;
    }

    /* get min and max depths, determine number of levels,
     * allocate array of lists */
    int min = bayer_flist_min_depth(srclist);
    int max = bayer_flist_max_depth(srclist);
    int levels = max - min + 1;
    bayer_flist* lists = (bayer_flist*) BAYER_MALLOC((size_t)levels * sizeof(bayer_flist));

    /* create a list for each level */
    int i;
    for (i = 0; i < levels; i++) {
        lists[i] = bayer_flist_subset(srclist);
    }

    /* copy each item from source list to its corresponding level */
    uint64_t idx = 0;
    uint64_t size = bayer_flist_size(srclist);
    while (idx < size) {
        int depth = bayer_flist_file_get_depth(srclist, idx);
        int depth_index = depth - min;
        bayer_flist dstlist = lists[depth_index];
        bayer_flist_file_copy(srclist, idx, dstlist);
        idx++;
    }

    /* summarize each list */
    for (i = 0; i < levels; i++) {
        bayer_flist_summarize(lists[i]);
    }

    /* set return parameters */
    *outlevels = levels;
    *outmin    = min;
    *outlists  = lists;

    return;
}

/* frees array of lists created in call to
 * bayer_flist_split_by_depth */
void bayer_flist_array_free(int levels, bayer_flist** outlists)
{
    /* check that our pointer is valid */
    if (outlists == NULL) {
        return;
    }

    /* free each list */
    int i;
    bayer_flist* lists = *outlists;
    for (i = 0; i < levels; i++) {
        bayer_flist_free(&lists[i]);
    }

    /* free the array of lists and set caller's pointer to NULL */
    bayer_free(outlists);
    return;
}

/* return number of files across all procs */
uint64_t bayer_flist_global_size(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->total_files;
    return val;
}

/* returns the global index of first item on this rank,
 * when placing items in rank order */
uint64_t bayer_flist_global_offset(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->offset;
    return val;
}

/* return number of files in local list */
uint64_t bayer_flist_size(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->list_count;
    return val;
}

/* return number of users */
uint64_t bayer_flist_user_count(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->total_users;
    return val;
}

/* return number of groups */
uint64_t bayer_flist_group_count(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->total_groups;
    return val;
}

/* return maximum length of file names */
uint64_t bayer_flist_file_max_name(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->max_file_name;
    return val;
}

/* return maximum length of user names */
uint64_t bayer_flist_user_max_name(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->max_user_name;
    return val;
}

/* return maximum length of group names */
uint64_t bayer_flist_group_max_name(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->max_group_name;
    return val;
}

/* return min depth */
int bayer_flist_min_depth(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    int val = flist->min_depth;
    return val;
}

/* return max depth */
int bayer_flist_max_depth(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    int val = flist->max_depth;
    return val;
}

int bayer_flist_have_detail(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    int val = flist->detail;
    return val;
}

const char* bayer_flist_file_get_name(bayer_flist bflist, uint64_t idx)
{
    const char* name = NULL;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        name = elem->file;
    }
    return name;
}

int bayer_flist_file_get_depth(bayer_flist bflist, uint64_t idx)
{
    int depth = -1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        depth = elem->depth;
    }
    return depth;
}

bayer_filetype bayer_flist_file_get_type(bayer_flist bflist, uint64_t idx)
{
    bayer_filetype type = BAYER_TYPE_NULL;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        type = elem->type;
    }
    return type;
}

uint64_t bayer_flist_file_get_mode(bayer_flist bflist, uint64_t idx)
{
    uint64_t mode = 0;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail > 0) {
        mode = elem->mode;
    }
    return mode;
}

uint64_t bayer_flist_file_get_uid(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->uid;
    }
    return ret;
}

uint64_t bayer_flist_file_get_gid(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->gid;
    }
    return ret;
}

uint64_t bayer_flist_file_get_atime(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->atime;
    }
    return ret;
}

uint64_t bayer_flist_file_get_atime_nsec(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->atime_nsec;
    }
    return ret;
}

uint64_t bayer_flist_file_get_mtime(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->mtime;
    }
    return ret;
}

uint64_t bayer_flist_file_get_mtime_nsec(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->mtime_nsec;
    }
    return ret;
}

uint64_t bayer_flist_file_get_ctime(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->ctime;
    }
    return ret;
}

uint64_t bayer_flist_file_get_ctime_nsec(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->ctime_nsec;
    }
    return ret;
}

uint64_t bayer_flist_file_get_size(bayer_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->size;
    }
    return ret;
}

const char* bayer_flist_file_get_username(bayer_flist bflist, uint64_t idx)
{
    const char* ret = NULL;
    flist_t* flist = (flist_t*) bflist;
    if (flist->detail) {
        uint64_t id = bayer_flist_file_get_uid(bflist, idx);
        ret = get_name_from_id(flist->user_id2name, id);
    }
    return ret;
}

const char* bayer_flist_file_get_groupname(bayer_flist bflist, uint64_t idx)
{
    const char* ret = NULL;
    flist_t* flist = (flist_t*) bflist;
    if (flist->detail) {
        uint64_t id = bayer_flist_file_get_gid(bflist, idx);
        ret = get_name_from_id(flist->group_id2name, id);
    }
    return ret;
}

/****************************************
 * Global counter and callbacks for LIBCIRCLE reductions
 ***************************************/

uint64_t reduce_items;

static void reduce_init(void)
{
    CIRCLE_reduce(&reduce_items, sizeof(uint64_t));
}

static void reduce_exec(const void* buf1, size_t size1, const void* buf2, size_t size2)
{
    const uint64_t* a = (const uint64_t*) buf1;
    const uint64_t* b = (const uint64_t*) buf2;
    uint64_t val = a[0] + b[0];
    CIRCLE_reduce(&val, sizeof(uint64_t));
}

static void reduce_fini(const void* buf, size_t size)
{
    /* get current time */
    time_t walk_start_t = time(NULL);
    if (walk_start_t == (time_t) - 1) {
        /* TODO: ERROR! */
    }

    /* format timestamp string */
    char walk_s[30];
    size_t rc = strftime(walk_s, sizeof(walk_s) - 1, "%FT%T", localtime(&walk_start_t));
    if (rc == 0) {
        walk_s[0] = '\0';
    }

    /* get result of reduction */
    const uint64_t* a = (const uint64_t*) buf;
    unsigned long long val = (unsigned long long) a[0];

    /* print status to stdout */
    printf("%s: Items walked %llu ...\n", walk_s, val);
    fflush(stdout);
}

#ifdef LUSTRE_STAT
/****************************************
 * Walk directory tree using Lustre's MDS stat
 ***************************************/

static void lustre_stripe_info(void* buf)
{
    struct lov_user_md* md = &((struct lov_user_mds_data*) buf)->lmd_lmm;

    uint32_t pattern = (uint32_t) md->lmm_pattern;
    if (pattern != LOV_PATTERN_RAID0) {
        /* we don't know how to interpret this pattern */
        return;
    }

    /* get stripe info for file */
    uint32_t size   = (uint32_t) md->lmm_stripe_size;
    uint16_t count  = (uint16_t) md->lmm_stripe_count;
    uint16_t offset = (uint16_t) md->lmm_stripe_offset;

    uint16_t i;
    if (md->lmm_magic == LOV_USER_MAGIC_V1) {
        struct lov_user_md_v1* md1 = (struct lov_user_md_v1*) md;
        for (i = 0; i < count; i++) {
            uint32_t idx = md1->lmm_objects[i].l_ost_idx;
        }
    }
    else if (md->lmm_magic == LOV_USER_MAGIC_V3) {
        struct lov_user_md_v3* md3 = (struct lov_user_md_v3*) md;
        for (i = 0; i < count; i++) {
            uint32_t idx = md3->lmm_objects[i].l_ost_idx;
        }
    }
    else {
        /* unknown magic number */
    }

    return;
}

static int lustre_mds_stat(int fd, char* fname, struct stat* sb)
{
    /* allocate a buffer */
    size_t pathlen = strlen(fname) + 1;
    size_t bufsize = pathlen;
    //size_t datasize = sizeof(lstat_t) + lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
    size_t datasize = sizeof(struct lov_user_mds_data) + LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
    if (datasize > bufsize) {
        bufsize = datasize;
    }
    char* buf = (char*) BAYER_MALLOC(bufsize);

    /* Usage: ioctl(fd, IOC_MDC_GETFILEINFO, buf)
     * IN: fd open file descriptor of file's parent directory
     * IN: buf file name (no path)
     * OUT: buf lstat_t */
    strcpy(buf, fname);
    //  strncpy(buf, fname, bufsize);

    int ret = ioctl(fd, IOC_MDC_GETFILEINFO, buf);

    /* Copy lstat_t to struct stat */
    if (ret != -1) {
        lstat_t* ls = (lstat_t*) &((struct lov_user_mds_data*) buf)->lmd_st;
        sb->st_dev     = ls->st_dev;
        sb->st_ino     = ls->st_ino;
        sb->st_mode    = ls->st_mode;
        sb->st_nlink   = ls->st_nlink;
        sb->st_uid     = ls->st_uid;
        sb->st_gid     = ls->st_gid;
        sb->st_rdev    = ls->st_rdev;
        sb->st_size    = ls->st_size;
        sb->st_blksize = ls->st_blksize;
        sb->st_blocks  = ls->st_blocks;
        sb->st_atime   = ls->st_atime;
        sb->st_mtime   = ls->st_mtime;
        sb->st_ctime   = ls->st_ctime;

        lustre_stripe_info(buf);
    }
    else {
        printf("ioctl errno=%d %s\n", errno, strerror(errno));
    }

    /* free the buffer */
    bayer_free(&buf);

    return ret;
}

static void walk_lustrestat_process_dir(char* dir, CIRCLE_handle* handle)
{
    /* TODO: may need to try these functions multiple times */
    DIR* dirp = bayer_opendir(dir);

    if (! dirp) {
        /* TODO: print error */
    }
    else {
        /* get file descriptor for open directory */
        int fd = dirfd(dirp);
        if (fd < 0) {
            /* TODO: print error */
            goto done;
        }

        /* Read all directory entries */
        while (1) {
            /* read next directory entry */
            struct dirent* entry = bayer_readdir(dirp);
            if (entry == NULL) {
                break;
            }

            /* process component, unless it's "." or ".." */
            char* name = entry->d_name;
            if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                size_t len = strlen(dir) + 1 + strlen(name) + 1;
                if (len < sizeof(newpath)) {
                    /* build full path to item */
                    strcpy(newpath, dir);
                    strcat(newpath, "/");
                    strcat(newpath, name);

                    /* stat item */
                    mode_t mode;
                    int have_mode = 0;
                    struct stat st;
                    int status = lustre_mds_stat(fd, name, &st);
                    if (status != -1) {
                        have_mode = 1;
                        mode = st.st_mode;
                        list_insert_stat(CURRENT_LIST, newpath, mode, &st);
                    }
                    else {
                        /* error */
                    }

                    /* increment our item count */
                    reduce_items++;

                    /* recurse into directories */
                    if (have_mode && S_ISDIR(mode)) {
                        handle->enqueue(newpath);
                    }
                }
                else {
                    /* TODO: print error in correct format */
                    /* name is too long */
                    printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
                    fflush(stdout);
                }
            }
        }
    }

done:
    bayer_closedir(dirp);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_lustrestat_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        const char* path = CURRENT_DIRS[i];

        /* stat top level item */
        struct stat st;
        int status = bayer_lstat(path, &st);
        if (status != 0) {
            /* TODO: print error */
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        list_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

        /* recurse into directory */
        if (S_ISDIR(st.st_mode)) {
            walk_lustrestat_process_dir(path, handle);
        }
    }

    return;
}

/** Callback given to process the dataset. */
static void walk_lustrestat_process(CIRCLE_handle* handle)
{
    /* in this case, only items on queue are directories */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);
    walk_lustrestat_process_dir(path, handle);
    return;
}

#endif /* LUSTRE_STAT */

/****************************************
 * Walk directory tree using stat at top level and getdents system call
 ***************************************/

struct linux_dirent {
    long           d_ino;
    off_t          d_off;
    unsigned short d_reclen;
    char           d_name[];
};

//#define BUF_SIZE 10*1024*1024
#define BUF_SIZE 128*1024U

static void walk_getdents_process_dir(char* dir, CIRCLE_handle* handle)
{
    char buf[BUF_SIZE];

    /* TODO: may need to try these functions multiple times */
    int fd = bayer_open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        /* print error */
        BAYER_LOG(BAYER_LOG_ERR, "Failed to open directory for reading: %s", dir);
        return;
    }

    /* Read all directory entries */
    while (1) {
        /* execute system call to get block of directory entries */
        int nread = syscall(SYS_getdents, fd, buf, (int) BUF_SIZE);
        if (nread == -1) {
            BAYER_LOG(BAYER_LOG_ERR, "syscall to getdents failed when reading %s (errno=%d %s)", dir, errno, strerror(errno));
            break;
        }

        /* bail out if we're done */
        if (nread == 0) {
            break;
        }

        /* otherwise, we read some bytes, so process each record */
        int bpos = 0;
        while (bpos < nread) {
            /* get pointer to current record */
            struct linux_dirent* d = (struct linux_dirent*)(buf + bpos);

            /* get name of directory item, skip d_ino== 0, ".", and ".." entries */
            char* name = d->d_name;
            if (d->d_ino != 0 && (strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* check whether we can define path to item:
                 * <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                size_t len = strlen(dir) + 1 + strlen(name) + 1;
                if (len < sizeof(newpath)) {
                    /* build full path to item */
                    strcpy(newpath, dir);
                    strcat(newpath, "/");
                    strcat(newpath, name);

                    /* get type of item */
                    char d_type = *(buf + bpos + d->d_reclen - 1);

#if 0
                    printf("%-10s ", (d_type == DT_REG) ?  "regular" :
                           (d_type == DT_DIR) ?  "directory" :
                           (d_type == DT_FIFO) ? "FIFO" :
                           (d_type == DT_SOCK) ? "socket" :
                           (d_type == DT_LNK) ?  "symlink" :
                           (d_type == DT_BLK) ?  "block dev" :
                           (d_type == DT_CHR) ?  "char dev" : "???");

                    printf("%4d %10lld  %s\n", d->d_reclen,
                           (long long) d->d_off, (char*) d->d_name);
#endif

                    /* TODO: this is hacky, would be better to create list elem directly */
                    /* determine type of item (just need to set bits in mode
                     * that get_bayer_filetype checks for) */
                    mode_t mode = 0;
                    if (d_type == DT_REG) {
                        mode |= S_IFREG;
                    }
                    else if (d_type == DT_DIR) {
                        mode |= S_IFDIR;
                    }
                    else if (d_type == DT_LNK) {
                        mode |= S_IFLNK;
                    }

                    /* insert a record for this item into our list */
                    list_insert_stat(CURRENT_LIST, newpath, mode, NULL);

                    /* increment our item count */
                    reduce_items++;

                    /* recurse on directory if we have one */
                    if (d_type == DT_DIR) {
                        handle->enqueue(newpath);
                    }
                }
                else {
                    BAYER_LOG(BAYER_LOG_ERR, "Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
                }
            }

            /* advance to next record */
            bpos += d->d_reclen;
        }
    }

    bayer_close(dir, fd);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_getdents_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        const char* path = CURRENT_DIRS[i];

        /* stat top level item */
        struct stat st;
        int status = bayer_lstat(path, &st);
        if (status != 0) {
            /* TODO: print error */
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        list_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

        /* recurse into directory */
        if (S_ISDIR(st.st_mode)) {
            walk_getdents_process_dir(path, handle);
        }
    }

    return;
}

/** Callback given to process the dataset. */
static void walk_getdents_process(CIRCLE_handle* handle)
{
    /* in this case, only items on queue are directories */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);
    walk_getdents_process_dir(path, handle);
    return;
}

/****************************************
 * Walk directory tree using stat at top level and readdir
 ***************************************/

static void walk_readdir_process_dir(char* dir, CIRCLE_handle* handle)
{
    /* TODO: may need to try these functions multiple times */
    DIR* dirp = bayer_opendir(dir);

    if (! dirp) {
        /* TODO: print error */
    }
    else {
        /* Read all directory entries */
        while (1) {
            /* read next directory entry */
            struct dirent* entry = bayer_readdir(dirp);
            if (entry == NULL) {
                break;
            }

            /* process component, unless it's "." or ".." */
            char* name = entry->d_name;
            if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                size_t len = strlen(dir) + 1 + strlen(name) + 1;
                if (len < sizeof(newpath)) {
                    /* build full path to item */
                    strcpy(newpath, dir);
                    strcat(newpath, "/");
                    strcat(newpath, name);

#ifdef _DIRENT_HAVE_D_TYPE
                    /* record info for item */
                    mode_t mode;
                    int have_mode = 0;
                    if (entry->d_type != DT_UNKNOWN) {
                        /* we can read object type from directory entry */
                        have_mode = 1;
                        mode = DTTOIF(entry->d_type);
                        list_insert_stat(CURRENT_LIST, newpath, mode, NULL);
                    }
                    else {
                        /* type is unknown, we need to stat it */
                        struct stat st;
                        int status = bayer_lstat(newpath, &st);
                        if (status == 0) {
                            have_mode = 1;
                            mode = st.st_mode;
                            list_insert_stat(CURRENT_LIST, newpath, mode, &st);
                        }
                        else {
                            /* error */
                        }
                    }

                    /* increment our item count */
                    reduce_items++;

                    /* recurse into directories */
                    if (have_mode && S_ISDIR(mode)) {
                        handle->enqueue(newpath);
                    }
#endif
                }
                else {
                    /* TODO: print error in correct format */
                    /* name is too long */
                    printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
                    fflush(stdout);
                }
            }
        }
    }

    bayer_closedir(dirp);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_readdir_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        char* path = CURRENT_DIRS[i];

        /* stat top level item */
        struct stat st;
        int status = bayer_lstat(path, &st);
        if (status != 0) {
            /* TODO: print error */
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        list_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

        /* recurse into directory */
        if (S_ISDIR(st.st_mode)) {
            walk_readdir_process_dir(path, handle);
        }
    }

    return;
}

/** Callback given to process the dataset. */
static void walk_readdir_process(CIRCLE_handle* handle)
{
    /* in this case, only items on queue are directories */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);
    walk_readdir_process_dir(path, handle);
    return;
}

/****************************************
 * Walk directory tree using stat on every object
 ***************************************/

static void walk_stat_process_dir(char* dir, CIRCLE_handle* handle)
{
    /* TODO: may need to try these functions multiple times */
    DIR* dirp = bayer_opendir(dir);

    if (! dirp) {
        /* TODO: print error */
    }
    else {
        while (1) {
            /* read next directory entry */
            struct dirent* entry = bayer_readdir(dirp);
            if (entry == NULL) {
                break;
            }

            /* We don't care about . or .. */
            char* name = entry->d_name;
            if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                size_t len = strlen(dir) + 1 + strlen(name) + 1;
                if (len < sizeof(newpath)) {
                    /* build full path to item */
                    strcpy(newpath, dir);
                    strcat(newpath, "/");
                    strcat(newpath, name);

                    /* add item to queue */
                    handle->enqueue(newpath);
                }
                else {
                    /* TODO: print error in correct format */
                    /* name is too long */
                    printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
                    fflush(stdout);
                }
            }
        }
    }

    bayer_closedir(dirp);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_stat_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        /* we'll call stat on every item */
        const char* path = CURRENT_DIRS[i];
        handle->enqueue(path);
    }
}

/** Callback given to process the dataset. */
static void walk_stat_process(CIRCLE_handle* handle)
{
    /* get path from queue */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);

    /* stat item */
    struct stat st;
    int status = bayer_lstat(path, &st);
    if (status != 0) {
        /* print error */
        return;
    }

    /* increment our item count */
    reduce_items++;

    /* TODO: filter items by stat info */

    /* record info for item in list */
    list_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

    /* recurse into directory */
    if (S_ISDIR(st.st_mode)) {
        /* TODO: check that we can recurse into directory */
        walk_stat_process_dir(path, handle);
    }

    return;
}

/****************************************
 * Functions to read user and group info
 ***************************************/

/* create a type consisting of chars number of characters
 * immediately followed by a uint32_t */
static void create_stridtype(int chars, MPI_Datatype* dt)
{
    /* build type for string */
    MPI_Datatype dt_str;
    MPI_Type_contiguous(chars, MPI_CHAR, &dt_str);

    /* build keysat type */
    MPI_Datatype types[2];
    types[0] = dt_str;       /* file name */
    types[1] = MPI_UINT64_T; /* id */
    DTCMP_Type_create_series(2, types, dt);

    MPI_Type_free(&dt_str);
    return;
}

/* element for a linked list of name/id pairs */
typedef struct strid {
    char* name;
    uint64_t id;
    struct strid* next;
} strid_t;

/* insert specified name and id into linked list given by
 * head, tail, and count, also increase maxchars if needed */
static void strid_insert(
    const char* name,
    uint64_t id,
    strid_t** head,
    strid_t** tail,
    int* count,
    int* maxchars)
{
    /* allocate and fill in new element */
    strid_t* elem = (strid_t*) malloc(sizeof(strid_t));
    elem->name = BAYER_STRDUP(name);
    elem->id   = id;
    elem->next = NULL;

    /* insert element into linked list */
    if (*head == NULL) {
        *head = elem;
    }
    if (*tail != NULL) {
        (*tail)->next = elem;
    }
    *tail = elem;
    (*count)++;

    /* increase maximum name if we need to */
    size_t len = strlen(name) + 1;
    if (*maxchars < (int)len) {
        /* round up to nearest multiple of 8 */
        size_t len8 = len / 8;
        if (len8 * 8 < len) {
            len8++;
        }
        len8 *= 8;

        *maxchars = (int)len8;
    }

    return;
}

/* copy data from linked list to array */
static void strid_serialize(strid_t* head, int chars, void* buf)
{
    char* ptr = (char*)buf;
    strid_t* current = head;
    while (current != NULL) {
        char* name  = current->name;
        uint64_t id = current->id;

        strcpy(ptr, name);
        ptr += chars;

        bayer_pack_uint64(&ptr, id);

        current = current->next;
    }
    return;
}

/* delete linked list and reset head, tail, and count values */
static void strid_delete(strid_t** head, strid_t** tail, int* count)
{
    /* free memory allocated in linked list */
    strid_t* current = *head;
    while (current != NULL) {
        strid_t* next = current->next;
        bayer_free(&current->name);
        bayer_free(&current);
        current = next;
    }

    /* set list data structure values back to NULL */
    *head  = NULL;
    *tail  = NULL;
    *count = 0;

    return;
}

/* read user array from file system using getpwent() */
static void get_users(buf_t* items)
{
    /* initialize output parameters */
    buft_init(items);

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* rank 0 iterates over users with getpwent */
    strid_t* head = NULL;
    strid_t* tail = NULL;
    int count = 0;
    int chars = 0;
    if (rank == 0) {
        struct passwd* p;
        while (1) {
            /* get next user, this function can fail so we retry a few times */
            int retries = 3;
retry:
            p = getpwent();
            if (p == NULL) {
                if (errno == EIO) {
                    retries--;
                }
                else {
                    /* TODO: ERROR! */
                    retries = 0;
                }
                if (retries > 0) {
                    goto retry;
                }
            }

            if (p != NULL) {
                /*
                printf("User=%s Pass=%s UID=%d GID=%d Name=%s Dir=%s Shell=%s\n",
                  p->pw_name, p->pw_passwd, p->pw_uid, p->pw_gid, p->pw_gecos, p->pw_dir, p->pw_shell
                );
                printf("User=%s UID=%d GID=%d\n",
                  p->pw_name, p->pw_uid, p->pw_gid
                );
                */
                char* name  = p->pw_name;
                uint64_t id = p->pw_uid;
                strid_insert(name, id, &head, &tail, &count, &chars);
            }
            else {
                /* hit the end of the user list */
                endpwent();
                break;
            }
        }

        //    printf("Max username %d, count %d\n", (int)chars, count);
    }

    /* bcast count and number of chars */
    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&chars, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* create datatype to represent a username/id pair */
    MPI_Datatype dt;
    create_stridtype(chars, &dt);

    /* get extent of type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* allocate an array to hold all user names and ids */
    size_t bufsize = (size_t)count * (size_t)extent;
    char* buf = (char*) BAYER_MALLOC(bufsize);

    /* copy items from list into array */
    if (rank == 0) {
        strid_serialize(head, chars, buf);
    }

    /* broadcast the array of usernames and ids */
    MPI_Bcast(buf, count, dt, 0, MPI_COMM_WORLD);

    /* set output parameters */
    items->buf     = buf;
    items->bufsize = bufsize;
    items->count   = (uint64_t) count;
    items->chars   = (uint64_t) chars;
    items->dt      = dt;

    /* delete the linked list */
    if (rank == 0) {
        strid_delete(&head, &tail, &count);
    }

    return;
}

/* read group array from file system using getgrent() */
static void get_groups(buf_t* items)
{
    /* initialize output parameters */
    buft_init(items);

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* rank 0 iterates over users with getpwent */
    strid_t* head = NULL;
    strid_t* tail = NULL;
    int count = 0;
    int chars = 0;
    if (rank == 0) {
        struct group* p;
        while (1) {
            /* get next user, this function can fail so we retry a few times */
            int retries = 3;
retry:
            p = getgrent();
            if (p == NULL) {
                if (errno == EIO || errno == EINTR) {
                    retries--;
                }
                else {
                    /* TODO: ERROR! */
                    retries = 0;
                }
                if (retries > 0) {
                    goto retry;
                }
            }

            if (p != NULL) {
                /*
                        printf("Group=%s GID=%d\n",
                          p->gr_name, p->gr_gid
                        );
                */
                char* name  = p->gr_name;
                uint64_t id = p->gr_gid;
                strid_insert(name, id, &head, &tail, &count, &chars);
            }
            else {
                /* hit the end of the group list */
                endgrent();
                break;
            }
        }

        //    printf("Max groupname %d, count %d\n", chars, count);
    }

    /* bcast count and number of chars */
    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&chars, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* create datatype to represent a username/id pair */
    MPI_Datatype dt;
    create_stridtype(chars, &dt);

    /* get extent of type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* allocate an array to hold all user names and ids */
    size_t bufsize = (size_t)count * (size_t)extent;
    char* buf = (char*) BAYER_MALLOC(bufsize);

    /* copy items from list into array */
    if (rank == 0) {
        strid_serialize(head, chars, buf);
    }

    /* broadcast the array of usernames and ids */
    MPI_Bcast(buf, count, dt, 0, MPI_COMM_WORLD);

    /* set output parameters */
    items->buf     = buf;
    items->bufsize = bufsize;
    items->count   = (uint64_t) count;
    items->chars   = (uint64_t) chars;
    items->dt      = dt;

    /* delete the linked list */
    if (rank == 0) {
        strid_delete(&head, &tail, &count);
    }

    return;
}

bayer_flist bayer_flist_subset(bayer_flist src)
{
    /* allocate a new file list */
    bayer_flist bflist = bayer_flist_new();

    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;
    flist_t* srclist = (flist_t*)src;

    /* copy user and groups if we have them */
    flist->detail = srclist->detail;
    if (srclist->detail) {
        buft_copy(&srclist->users, &flist->users);
        buft_copy(&srclist->groups, &flist->groups);
        strmap_merge(flist->user_id2name, srclist->user_id2name);
        strmap_merge(flist->group_id2name, srclist->group_id2name);
        flist->have_users  = 1;
        flist->have_groups = 1;
    }

    return bflist;
}

/* Set up and execute directory walk */
void bayer_flist_walk_path(const char* dirpath, int use_stat, bayer_flist bflist)
{
    bayer_flist_walk_paths(1, &dirpath, use_stat, bflist);
    return;
}

/* Set up and execute directory walk */
void bayer_flist_walk_paths(uint64_t num_paths, const char** paths, int use_stat, bayer_flist bflist)
{
    /* report walk count, time, and rate */
    double start_walk = MPI_Wtime();

    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* hard code this for now, until we get options structure */
    int verbose = 1;

    /* print message to user that we're starting */
    if (verbose && rank == 0) {
        uint64_t i;
        for (i = 0; i < num_paths; i++) {
            time_t walk_start_t = time(NULL);
            if (walk_start_t == (time_t)-1) {
                /* TODO: ERROR! */
            }
            char walk_s[30];
            size_t rc = strftime(walk_s, sizeof(walk_s) - 1, "%FT%T", localtime(&walk_start_t));
            if (rc == 0) {
                walk_s[0] = '\0';
            }
            printf("%s: Walking %s\n", walk_s, paths[i]);
        }
        fflush(stdout);
    }

    /* initialize libcircle */
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL);

    /* set libcircle verbosity level */
    enum CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    CIRCLE_enable_logging(loglevel);

    /* TODO: check that paths is not NULL */
    /* TODO: check that each path is within limits */

    /* set some global variables to do the file walk */
    CURRENT_NUM_DIRS = num_paths;
    CURRENT_DIRS     = paths;
    CURRENT_LIST     = flist;

    /* we lookup users and groups first in case we can use
     * them to filter the walk */
    flist->detail = 0;
    if (use_stat) {
        flist->detail = 1;
        if (flist->have_users == 0) {
            get_users(&flist->users);
            create_map(&flist->users, flist->user_id2name);
            flist->have_users = 1;
        }
        if (flist->have_groups == 0) {
            get_groups(&flist->groups);
            create_map(&flist->groups, flist->group_id2name);
            flist->have_groups = 1;
        }
    }

    /* register callbacks */
    if (use_stat) {
        /* walk directories by calling stat on every item */
        CIRCLE_cb_create(&walk_stat_create);
        CIRCLE_cb_process(&walk_stat_process);
//        CIRCLE_cb_create(&walk_lustrestat_create);
//        CIRCLE_cb_process(&walk_lustrestat_process);
    }
    else {
        /* walk directories using file types in readdir */
        CIRCLE_cb_create(&walk_readdir_create);
        CIRCLE_cb_process(&walk_readdir_process);
//        CIRCLE_cb_create(&walk_getdents_create);
//        CIRCLE_cb_process(&walk_getdents_process);
    }

    /* prepare callbacks and initialize variables for reductions */
    reduce_items = 0;
    CIRCLE_cb_reduce_init(&reduce_init);
    CIRCLE_cb_reduce_op(&reduce_exec);
    CIRCLE_cb_reduce_fini(&reduce_fini);

    /* run the libcircle job */
    CIRCLE_begin();
    CIRCLE_finalize();

    /* compute global summary */
    list_compute_summary(flist);

    double end_walk = MPI_Wtime();

    /* report walk count, time, and rate */
    if (verbose && rank == 0) {
        uint64_t all_count = bayer_flist_global_size(flist);
        double time_diff = end_walk - start_walk;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        printf("Walked %lu files in %f seconds (%f files/sec)\n",
               all_count, time_diff, rate
              );
    }

    return;
}

/****************************************
 * Read file list from file
 ***************************************/

static uint64_t get_filesize(const char* name)
{
    uint64_t size = 0;
    struct stat sb;
    int rc = bayer_lstat(name, &sb);
    if (rc == 0) {
        size = (uint64_t) sb.st_size;
    }
    return size;
}

/* reads a file assuming variable length records stored one per line,
 * data encoded in ASCII, fields separated by '|' characters,
 * we divide the file into sections and each process is responsible
 * for records in its section, tricky part is to handle records
 * that spill from one section into the next */
static void read_cache_variable(
    const char* name,
    MPI_File fh,
    char* datarep,
    flist_t* flist)
{
    MPI_Status status;

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* assume data records start at byte 0,
     * need to update this code if this is not the case */
    MPI_Offset disp = 0;

    /* indicate that we just have file names */
    flist->detail = 0;

    /* get file size to determine how much each process should read */
    uint64_t filesize = get_filesize(name);

    /* TODO: consider stripe width */

    /* compute number of chunks in file */
    uint64_t chunk_size = 1024 * 10;
    uint64_t chunks = filesize / chunk_size;
    if (chunks * chunk_size < filesize) {
        chunks++;
    }

    /* compute chunk count for each process */
    uint64_t chunk_count = chunks / (uint64_t) ranks;
    uint64_t remainder = chunks - chunk_count * ranks;
    if ((uint64_t) rank < remainder) {
        chunk_count++;
    }

    /* get our chunk offset */
    uint64_t chunk_offset;
    MPI_Exscan(&chunk_count, &chunk_offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        chunk_offset = 0;
    }

    /* in order to avoid blowing out memory, we read into a fixed-size
     * buffer and unpack */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = chunk_size;
    void* buf1 = BAYER_MALLOC(bufsize);
    void* buf2 = BAYER_MALLOC(bufsize);
    void* buf  = buf1;
    
    /* set file view to be sequence of characters past header */
    MPI_File_set_view(fh, disp, MPI_CHAR, MPI_CHAR, datarep, MPI_INFO_NULL);
    
    /* compute offset of first byte we'll read */
    MPI_Offset read_offset = disp + (MPI_Offset) (chunk_offset * chunk_size);

    /* compute offset of last byte we need to read,
     * note we may actually read further if our last record spills
     * into next chunk */
    uint64_t last_offset = disp + (chunk_offset + chunk_count) * chunk_size;
    if (last_offset > filesize) {
        last_offset = filesize;
    }
    
    /* read last character from chunk before our first,
     * if this char is not a newline, then last record in the
     * previous chunk spills into ours, in which case we need
     * to scan past first newline */

    /* assume we don't have to scan passt first newline */
    int scan = 0;
    if (read_offset > 0) {
        /* read last byte in chunk before our first */
        MPI_Offset pos = read_offset - 1;
        MPI_File_read_at(fh, pos, buf, 1, MPI_CHAR, &status);

        /* if last character is not newline, we need to scan past
         * first new line in our chunk */
        char* ptr = (char*) buf;
        if (*ptr != '\n') {
            scan = 1;
        }
    }

    /* read data from file in chunks, decode records and insert in list */
    uint64_t bufoffset = 0; /* offset within buffer where we should read data */
    int done = 0;
    while (! done) {
        /* we're done if we there's nothing to read */
        if ((uint64_t) read_offset >= filesize) {
            break;
        }

        /* determine number to read, try to read a full buffer's worth,
         * but reduce this if that overruns the end of the file */
        int read_count = (int) (bufsize - bufoffset);
        uint64_t remaining = filesize - (uint64_t) read_offset;
        if (remaining < (uint64_t) read_count) {
            read_count = (int) remaining;
        }
    
        /* read in our chunk */
        char* bufstart = (char*) buf + bufoffset;
        MPI_File_read_at(fh, read_offset, bufstart, read_count, MPI_CHAR, &status);

        /* TODO: check number of items read in status, in case file
         * size changed somehow since we first read the file size */

        /* update read offset for next time */
        read_offset += (MPI_Offset) read_count;
    
        /* setup pointers to work with read buffer */
        char* ptr = (char*) buf;
        char* end = ptr + bufoffset + read_count;

        /* scan past first newline (first part of block is a partial
         * record handled by another process) */
        if (scan) {
            /* advance to the next newline character */
            while(*ptr != '\n' && ptr != end) {
                ptr++;
            }

            /* go one past newline character */
            if (ptr != end) {
                ptr++;
            }

            /* no need to do that again */
            scan = 0;
        }

        /* process records */
        char* start = ptr;
        while (start != end) {
            /* start points to beginning of a record, scan to
             * search for end of record, advance ptr past next
             * newline character or to end of buffer */
            while(*ptr != '\n' && ptr != end) {
                ptr++;
            }

            /* process record if we hit a newline,
             * otherwise copy partial record to other buffer */
            if (*ptr == '\n') {
                 /* we've got a full record,
                 * terminate record string with NUL */
                *ptr = '\0';

                /* process record */
                list_insert_decode(flist, start);

                /* go one past newline character */
                ptr++;
                start = ptr;

                /* compute position of last byte we read,
                 * stop if we have reached or exceeded the limit that
                 * need to read */
                uint64_t pos = ((uint64_t)(read_offset - read_count)) - bufoffset + (ptr - (char*)buf);
                if (pos >= last_offset) {
                    done = 1;
                    break;
                }
            } else {
                /* hit end of buffer but not end of record,
                 * copy partial record to start of next buffer */

                /* swap buffers */
                if (buf == buf1) {
                    buf = buf2;
                } else {
                    buf = buf1;
                }

                /* copy remainder to next buffer */
                size_t len = (size_t) (ptr - start);
                memcpy(buf, start, len);
                bufoffset = (uint64_t) len;

                /* done with this buffer */
                break;
            }
        }
    }

    /* free buffer */
    bayer_free(&buf2);
    bayer_free(&buf1);
    buf = NULL;

    return;
}

/* file format:
 *   uint64_t timestamp when walk started
 *   uint64_t timestamp when walk ended
 *   uint64_t total number of files
 *   uint64_t max filename length
 *   list of <filenames(str), filetype(uint32_t)> */
static void read_cache_v2(
    const char* name,
    MPI_Offset* outdisp,
    MPI_File fh,
    char* datarep,
    uint64_t* outstart,
    uint64_t* outend,
    flist_t* flist)
{
    MPI_Status status;

    MPI_Offset disp = *outdisp;

    /* indicate that we just have file names */
    flist->detail = 0;

    /* get our rank */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* rank 0 reads and broadcasts header */
    uint64_t header[4];
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_read_at(fh, disp, header, 4, MPI_UINT64_T, &status);
    }
    MPI_Bcast(header, 4, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    disp += 4 * 8; /* 4 consecutive uint64_t types in external32 */

    uint64_t all_count;
    *outstart      = header[0];
    *outend        = header[1];
    all_count      = header[2];
    uint64_t chars = header[3];

    /* compute count for each process */
    uint64_t count = all_count / (uint64_t)ranks;
    uint64_t remainder = all_count - count * (uint64_t)ranks;
    if ((uint64_t)rank < remainder) {
        count++;
    }

    /* get our offset */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }

    /* create file datatype and read in file info if there are any */
    if (all_count > 0 && chars > 0) {
        /* create types */
        MPI_Datatype dt;
        create_stattype(flist->detail, (int)chars,   &dt);

        /* get extents */
        MPI_Aint lb_file, extent_file;
        MPI_Type_get_extent(dt, &lb_file, &extent_file);

        /* in order to avoid blowing out memory, we'll pack into a smaller
         * buffer and iteratively make many collective reads */

        /* allocate a buffer, ensure it's large enough to hold at least one
         * complete record */
        size_t bufsize = 1024*1024;
        if (bufsize < (size_t) extent_file) {
            bufsize = (size_t) extent_file;
        }
        void* buf = BAYER_MALLOC(bufsize);
    
        /* compute number of items we can fit in each read iteration */
        uint64_t bufcount = (uint64_t)bufsize / (uint64_t)extent_file;
    
        /* determine number of iterations we need to read all items */
        uint64_t iters = count / bufcount;
        if (iters * bufcount < count) {
            iters++;
        }
    
        /* compute max iterations across all procs */
        uint64_t all_iters;
        MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    
        /* set file view to be sequence of datatypes past header */
        MPI_File_set_view(fh, disp, dt, dt, datarep, MPI_INFO_NULL);
    
        /* compute byte offset to read our element */
        MPI_Offset read_offset = disp + (MPI_Offset)offset * extent_file;
    
        /* iterate with multiple reads until all records are read */
        uint64_t totalcount = 0;
        while (all_iters > 0) {
            /* determine number to read */
            int read_count = (int) bufcount;
            uint64_t remaining = count - totalcount;
            if (remaining < bufcount) {
                read_count = (int) remaining;
            }

            /* issue a collective read */
            MPI_File_read_at_all(fh, read_offset, buf, read_count, dt, &status);

            /* update our offset with the number of bytes we just read */
            read_offset += (MPI_Offset)read_count * (MPI_Offset)extent_file;
            totalcount += (uint64_t) read_count;

            /* unpack data from buffer into list */
            char* ptr = (char*) buf;
            uint64_t packcount = 0;
            while (packcount < (uint64_t) read_count) {
                /* unpack item from buffer and advance pointer */
                list_insert_ptr(flist, ptr, 0, chars);
                ptr += extent_file;
                packcount++;
            }
    
            /* one less iteration */
            all_iters--;
        }

        /* free buffer */
        bayer_free(&buf);

        /* free off our datatype */
        MPI_Type_free(&dt);
    }

    *outdisp = disp;
    return;
}

/* file format:
 *   uint64_t timestamp when walk started
 *   uint64_t timestamp when walk ended
 *   uint64_t total number of users
 *   uint64_t max username length
 *   uint64_t total number of groups
 *   uint64_t max groupname length
 *   uint64_t total number of files
 *   uint64_t max filename length
 *   list of <username(str), userid(uint64_t)>
 *   list of <groupname(str), groupid(uint64_t)>
 *   list of <files(str)>
 *   */
static void read_cache_v3(
    const char* name,
    MPI_Offset* outdisp,
    MPI_File fh,
    char* datarep,
    uint64_t* outstart,
    uint64_t* outend,
    flist_t* flist)
{
    MPI_Status status;

    MPI_Offset disp = *outdisp;

    /* indicate that we have stat data */
    flist->detail = 1;

    /* pointer to users, groups, and file buffer data structure */
    buf_t* users  = &flist->users;
    buf_t* groups = &flist->groups;

    /* get our rank */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* rank 0 reads and broadcasts header */
    uint64_t header[8];
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_read_at(fh, disp, header, 8, MPI_UINT64_T, &status);
    }
    MPI_Bcast(header, 8, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    disp += 8 * 8; /* 8 consecutive uint64_t types in external32 */

    uint64_t all_count;
    *outstart        = header[0];
    *outend          = header[1];
    users->count     = header[2];
    users->chars     = header[3];
    groups->count    = header[4];
    groups->chars    = header[5];
    all_count        = header[6];
    uint64_t chars   = header[7];

    /* compute count for each process */
    uint64_t count = all_count / (uint64_t)ranks;
    uint64_t remainder = all_count - count * (uint64_t)ranks;
    if ((uint64_t)rank < remainder) {
        count++;
    }

    /* get our offset */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }

    /* read users, if any */
    if (users->count > 0 && users->chars > 0) {
        /* create type */
        create_stridtype((int)users->chars,  &(users->dt));

        /* get extent */
        MPI_Aint lb_user, extent_user;
        MPI_Type_get_extent(users->dt, &lb_user, &extent_user);

        /* allocate memory to hold data */
        size_t bufsize_user = users->count * (size_t)extent_user;
        users->buf = (void*) BAYER_MALLOC(bufsize_user);
        users->bufsize = bufsize_user;

        /* read data */
        MPI_File_set_view(fh, disp, users->dt, users->dt, datarep, MPI_INFO_NULL);
        if (rank == 0) {
            MPI_File_read_at(fh, disp, users->buf, (int)users->count, users->dt, &status);
        }
        MPI_Bcast(users->buf, (int)users->count, users->dt, 0, MPI_COMM_WORLD);
        disp += (MPI_Offset) bufsize_user;
    }

    /* read groups, if any */
    if (groups->count > 0 && groups->chars > 0) {
        /* create type */
        create_stridtype((int)groups->chars, &(groups->dt));

        /* get extent */
        MPI_Aint lb_group, extent_group;
        MPI_Type_get_extent(groups->dt, &lb_group, &extent_group);

        /* allocate memory to hold data */
        size_t bufsize_group = groups->count * (size_t)extent_group;
        groups->buf = (void*) BAYER_MALLOC(bufsize_group);
        groups->bufsize = bufsize_group;

        /* read data */
        MPI_File_set_view(fh, disp, groups->dt, groups->dt, datarep, MPI_INFO_NULL);
        if (rank == 0) {
            MPI_File_read_at(fh, disp, groups->buf, (int)groups->count, groups->dt, &status);
        }
        MPI_Bcast(groups->buf, (int)groups->count, groups->dt, 0, MPI_COMM_WORLD);
        disp += (MPI_Offset) bufsize_group;
    }

    /* read files, if any */
    if (all_count > 0 && chars > 0) {
        /* create types */
        MPI_Datatype dt;
        create_stattype(flist->detail, (int)chars, &dt);

        /* get extents */
        MPI_Aint lb_file, extent_file;
        MPI_Type_get_extent(dt, &lb_file, &extent_file);

        /* in order to avoid blowing out memory, we'll pack into a smaller
         * buffer and iteratively make many collective reads */

        /* allocate a buffer, ensure it's large enough to hold at least one
         * complete record */
        size_t bufsize = 1024*1024;
        if (bufsize < (size_t) extent_file) {
            bufsize = (size_t) extent_file;
        }
        void* buf = BAYER_MALLOC(bufsize);
    
        /* compute number of items we can fit in each read iteration */
        uint64_t bufcount = (uint64_t)bufsize / (uint64_t)extent_file;
    
        /* determine number of iterations we need to read all items */
        uint64_t iters = count / bufcount;
        if (iters * bufcount < count) {
            iters++;
        }
    
        /* compute max iterations across all procs */
        uint64_t all_iters;
        MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    
        /* set file view to be sequence of datatypes past header */
        MPI_File_set_view(fh, disp, dt, dt, datarep, MPI_INFO_NULL);
    
        /* compute byte offset to read our element */
        MPI_Offset read_offset = disp + (MPI_Offset)offset * extent_file;
    
        /* iterate with multiple reads until all records are read */
        uint64_t totalcount = 0;
        while (all_iters > 0) {
            /* determine number to read */
            int read_count = (int) bufcount;
            uint64_t remaining = count - totalcount;
            if (remaining < bufcount) {
                read_count = (int) remaining;
            }

            /* issue a collective read */
            MPI_File_read_at_all(fh, read_offset, buf, read_count, dt, &status);

            /* update our offset with the number of bytes we just read */
            read_offset += (MPI_Offset)read_count * (MPI_Offset)extent_file;
            totalcount += (uint64_t) read_count;

            /* unpack data from buffer into list */
            char* ptr = (char*) buf;
            uint64_t packcount = 0;
            while (packcount < (uint64_t) read_count) {
                /* unpack item from buffer and advance pointer */
                list_insert_ptr(flist, ptr, 1, chars);
                ptr += extent_file;
                packcount++;
            }
    
            /* one less iteration */
            all_iters--;
        }

        /* free buffer */
        bayer_free(&buf);

        /* free off our datatype */
        MPI_Type_free(&dt);
    }

    /* create maps of users and groups */
    create_map(&flist->users, flist->user_id2name);
    create_map(&flist->groups, flist->group_id2name);

    *outdisp = disp;
    return;
}

void bayer_flist_read_cache(
    const char* name,
    bayer_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;

    /* start timer */
    double start_read = MPI_Wtime();

    /* hard code this for now, until we get options structure */
    int verbose = 1;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* open file */
    int rc;
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    int amode = MPI_MODE_RDONLY;
    rc = MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, MPI_INFO_NULL, &fh);
    if (rc != MPI_SUCCESS) {
        if (rank == 0) {
            printf("Failed to open file %s", name);
        }
        return;
    }

    /* set file view */
    MPI_Offset disp = 0;

    /* rank 0 reads and broadcasts version */
    uint64_t version;
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_read_at(fh, disp, &version, 1, MPI_UINT64_T, &status);
    }
    MPI_Bcast(&version, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    disp += 1 * 8; /* 9 consecutive uint64_t types in external32 */

    /* need a couple of dummy params to record walk start and end times */
    uint64_t outstart = 0;
    uint64_t outend = 0;

    /* read data from file */
    if (version == 2) {
        read_cache_v2(name, &disp, fh, datarep, &outstart, &outend, flist);
    }
    else if (version == 3) {
        read_cache_v3(name, &disp, fh, datarep, &outstart, &outend, flist);
    }
    else {
        /* TODO: unknown file format */
        read_cache_variable(name, fh, datarep, flist);
    }

    /* close file */
    MPI_File_close(&fh);

    /* compute global summary */
    list_compute_summary(flist);

    /* end timer */
    double end_read = MPI_Wtime();

    /* report read count, time, and rate */
    if (verbose && rank == 0) {
        uint64_t all_count = bayer_flist_global_size(flist);
        double time_diff = end_read - start_read;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        printf("Read %lu files in %f seconds (%f files/sec)\n",
               all_count, time_diff, rate
              );
    }

    return;
}

/****************************************
 * Write file list to file
 ***************************************/

/* file version
 * 1: version, start, end, files, file chars, list (file)
 * 2: version, start, end, files, file chars, list (file, type)
 * 3: version, start, end, files, users, user chars, groups, group chars,
 *    files, file chars, list (user, userid), list (group, groupid),
 *    list (stat) */

/* write each record in ASCII format, terminated with newlines */
static void write_cache_readdir_variable(
    const char* name,
    flist_t* flist)
{
    /* get our rank in job */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* walk the list to determine the number of bytes we'll write */
    uint64_t bytes = 0;
    uint64_t recmax = 0;
    const elem_t* current = flist->list_head;
    while (current != NULL) {
        /* <name>|<type={D,F,L}>\n */
        uint64_t reclen = (uint64_t) list_elem_encode_size(current);
        if (recmax < reclen) {
            recmax = reclen;
        }
        bytes += reclen;
        current = current->next;
    }

    /* compute byte offset for each task */
    uint64_t offset;
    MPI_Scan(&bytes, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }

    /* open file */
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
    int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;
    MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, MPI_INFO_NULL, &fh);

    /* truncate file to 0 bytes */
    MPI_File_set_size(fh, 0);

    MPI_Offset disp = 0;
#if 0
    /* prepare header */
    uint64_t header[5];
    header[0] = 2;               /* file version */
    header[1] = walk_start;      /* time_t when file walk started */
    header[2] = walk_end;        /* time_t when file walk stopped */
    header[3] = all_count;       /* total number of stat entries */
    header[4] = (uint64_t)chars; /* number of chars in file name */

    /* write the header */
    MPI_Offset disp = 0;
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_write_at(fh, disp, header, 5, MPI_UINT64_T, &status);
    }
    disp += 5 * 8;
#endif

    /* in order to avoid blowing out memory, we'll pack into a smaller
     * buffer and iteratively make many collective writes */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = 1024*1024;
    if (bufsize < recmax) {
        bufsize = recmax;
    }
    void* buf = BAYER_MALLOC(bufsize);

    /* set file view to be sequence of datatypes past header */
    MPI_File_set_view(fh, disp, MPI_CHAR, MPI_CHAR, datarep, MPI_INFO_NULL);

    /* compute byte offset to write our element */
    MPI_Offset write_offset = disp + (MPI_Offset)offset;

    /* iterate with multiple writes until all records are written */
    current = flist->list_head;
    while (current != NULL) {
        /* copy stat data into write buffer */
        char* ptr = (char*) buf;
        size_t packsize = 0;
        size_t recsize = list_elem_encode_size(current);
        while (current != NULL && (packsize + recsize) <= bufsize) {
            /* pack item into buffer and advance pointer */
            size_t bytes = list_elem_encode(ptr, current);
            ptr += bytes;
            packsize += bytes;

            /* get pointer to next element and update our recsize */
            current = current->next;
            if (current != NULL) {
                recsize = list_elem_encode_size(current);
            }
        }

        /* write file info */
        int write_count = (int) packsize;
        MPI_File_write_at(fh, write_offset, buf, write_count, MPI_CHAR, &status);

        /* update our offset with the number of bytes we just wrote */
        write_offset += (MPI_Offset) packsize;
    }

    /* free write buffer */
    bayer_free(&buf);

    /* close file */
    MPI_File_close(&fh);

    return;
}

/* file format:
 *   uint64_t timestamp when walk started
 *   uint64_t timestamp when walk ended
 *   uint64_t total number of files
 *   uint64_t max filename length
 *   list of <filenames(str), filetype(uint32_t)> */

static void write_cache_readdir(
    const char* name,
    uint64_t walk_start,
    uint64_t walk_end,
    flist_t* flist)
{
    /* get our rank in job */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* get number of items in our list and total file count */
    uint64_t count     = flist->list_count;
    uint64_t all_count = flist->total_files;
    uint64_t offset    = flist->offset;

    /* find smallest length that fits max and consists of integer
     * number of 8 byte segments */
    int max = (int) flist->max_file_name;
    int chars = max / 8;
    if (chars * 8 < max) {
        chars++;
    }
    chars *= 8;

    /* build datatype to hold file info */
    MPI_Datatype dt;
    create_stattype(flist->detail, chars, &dt);

    /* get extent of stat type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* determine number of bytes we'll write */
    uint64_t bytes = (uint64_t)extent * count;

    /* open file */
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
    int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;
    MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, MPI_INFO_NULL, &fh);

    /* truncate file to 0 bytes */
    MPI_File_set_size(fh, 0);

    /* prepare header */
    uint64_t header[5];
    header[0] = 2;               /* file version */
    header[1] = walk_start;      /* time_t when file walk started */
    header[2] = walk_end;        /* time_t when file walk stopped */
    header[3] = all_count;       /* total number of stat entries */
    header[4] = (uint64_t)chars; /* number of chars in file name */

    /* write the header */
    MPI_Offset disp = 0;
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_write_at(fh, disp, header, 5, MPI_UINT64_T, &status);
    }
    disp += 5 * 8;

    /* in order to avoid blowing out memory, we'll pack into a smaller
     * buffer and iteratively make many collective writes */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = 1024*1024;
    if (bufsize < (size_t) extent) {
        bufsize = (size_t) extent;
    }
    void* buf = BAYER_MALLOC(bufsize);

    /* compute number of items we can fit in each write iteration */
    uint64_t bufcount = (uint64_t)bufsize / (uint64_t)extent;

    /* determine number of iterations we need to write all items */
    uint64_t iters = count / bufcount;
    if (iters * bufcount < count) {
        iters++;
    }

    /* compute max iterations across all procs */
    uint64_t all_iters;
    MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* set file view to be sequence of datatypes past header */
    MPI_File_set_view(fh, disp, dt, dt, datarep, MPI_INFO_NULL);

    /* compute byte offset to write our element */
    MPI_Offset write_offset = disp + (MPI_Offset)offset * extent;

    /* iterate with multiple writes until all records are written */
    const elem_t* current = flist->list_head;
    while (all_iters > 0) {
        /* copy stat data into write buffer */
        char* ptr = (char*) buf;
        uint64_t packcount = 0;
        while (current != NULL && packcount < bufcount) {
            /* pack item into buffer and advance pointer */
            size_t bytes = list_elem_pack(ptr, flist->detail, (uint64_t)chars, current);
            ptr += bytes;
            packcount++;
            current = current->next;
        }

        /* collective write of file info */
        int write_count = (int) packcount;
        MPI_File_write_at_all(fh, write_offset, buf, write_count, dt, &status);

        /* update our offset with the number of bytes we just wrote */
        write_offset += (MPI_Offset)packcount * (MPI_Offset)extent;

        /* one less iteration */
        all_iters--;
    }

    /* free write buffer */
    bayer_free(&buf);

    /* close file */
    MPI_File_close(&fh);

    /* free the datatype */
    MPI_Type_free(&dt);

    return;
}

static void write_cache_stat(
    const char* name,
    uint64_t walk_start,
    uint64_t walk_end,
    flist_t* flist)
{
    buf_t* users  = &flist->users;
    buf_t* groups = &flist->groups;

    /* get our rank in job */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* get number of items in our list and total file count */
    uint64_t count     = flist->list_count;
    uint64_t all_count = flist->total_files;
    uint64_t offset    = flist->offset;

    /* find smallest length that fits max and consists of integer
     * number of 8 byte segments */
    int max = (int) flist->max_file_name;
    int chars = max / 8;
    if (chars * 8 < max) {
        chars++;
    }
    chars *= 8;

    /* build datatype to hold file info */
    MPI_Datatype dt;
    create_stattype(flist->detail, chars, &dt);

    /* get extent of stat type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* determine number of bytes we'll write */
    uint64_t bytes = (uint64_t)extent * count;

    /* open file */
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
    int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;
    MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, MPI_INFO_NULL, &fh);

    /* truncate file to 0 bytes */
    MPI_File_set_size(fh, 0);

    /* prepare header */
    uint64_t header[9];
    header[0] = 3;               /* file version */
    header[1] = walk_start;      /* time_t when file walk started */
    header[2] = walk_end;        /* time_t when file walk stopped */
    header[3] = users->count;    /* number of user records */
    header[4] = users->chars;    /* number of chars in user name */
    header[5] = groups->count;   /* number of group records */
    header[6] = groups->chars;   /* number of chars in group name */
    header[7] = all_count;       /* total number of stat entries */
    header[8] = (uint64_t)chars; /* number of chars in file name */

    /* write the header */
    MPI_Offset disp = 0;
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_write_at(fh, disp, header, 9, MPI_UINT64_T, &status);
    }
    disp += 9 * 8;

    if (users->dt != MPI_DATATYPE_NULL) {
        /* get extent user */
        MPI_Aint lb_user, extent_user;
        MPI_Type_get_extent(users->dt, &lb_user, &extent_user);

        /* write out users */
        MPI_File_set_view(fh, disp, users->dt, users->dt, datarep, MPI_INFO_NULL);
        if (rank == 0) {
            int write_count = (int) users->count;
            MPI_File_write_at(fh, disp, users->buf, write_count, users->dt, &status);
        }
        disp += (MPI_Offset)users->count * extent_user;
    }

    if (groups->dt != MPI_DATATYPE_NULL) {
        /* get extent group */
        MPI_Aint lb_group, extent_group;
        MPI_Type_get_extent(groups->dt, &lb_group, &extent_group);

        /* write out groups */
        MPI_File_set_view(fh, disp, groups->dt, groups->dt, datarep, MPI_INFO_NULL);
        if (rank == 0) {
            int write_count = (int) groups->count;
            MPI_File_write_at(fh, disp, groups->buf, write_count, groups->dt, &status);
        }
        disp += (MPI_Offset)groups->count * extent_group;
    }

    /* in order to avoid blowing out memory, we'll pack into a smaller
     * buffer and iteratively make many collective writes */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = 1024*1024;
    if (bufsize < (size_t) extent) {
        bufsize = (size_t) extent;
    }
    void* buf = BAYER_MALLOC(bufsize);

    /* compute number of items we can fit in each write iteration */
    uint64_t bufcount = (uint64_t)bufsize / (uint64_t)extent;

    /* determine number of iterations we need to write all items */
    uint64_t iters = count / bufcount;
    if (iters * bufcount < count) {
        iters++;
    }

    /* compute max iterations across all procs */
    uint64_t all_iters;
    MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* set file view to be sequence of datatypes past header */
    MPI_File_set_view(fh, disp, dt, dt, datarep, MPI_INFO_NULL);

    /* compute byte offset to write our element */
    MPI_Offset write_offset = disp + (MPI_Offset)offset * extent;

    /* iterate with multiple writes until all records are written */
    const elem_t* current = flist->list_head;
    while (all_iters > 0) {
        /* copy stat data into write buffer */
        char* ptr = (char*) buf;
        uint64_t packcount = 0;
        while (current != NULL && packcount < bufcount) {
            /* pack item into buffer and advance pointer */
            size_t bytes = list_elem_pack(ptr, flist->detail, (uint64_t)chars, current);
            ptr += bytes;
            packcount++;
            current = current->next;
        }

        /* collective write of file info */
        int write_count = (int) packcount;
        MPI_File_write_at_all(fh, write_offset, buf, write_count, dt, &status);

        /* update our offset with the number of bytes we just wrote */
        write_offset += (MPI_Offset)packcount * (MPI_Offset)extent;

        /* one less iteration */
        all_iters--;
    }

    /* free write buffer */
    bayer_free(&buf);

    /* close file */
    MPI_File_close(&fh);

    /* free the datatype */
    MPI_Type_free(&dt);

    return;
}

void bayer_flist_write_cache(
    const char* name,
    bayer_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;

    /* start timer */
    double start_write = MPI_Wtime();

    /* hard code this for now, until we get options structure */
    int verbose = 1;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* report the filename we're writing to */
    if (verbose && rank == 0) {
        printf("Writing to output file: %s\n", name);
        fflush(stdout);
    }

    if (flist->detail) {
        write_cache_stat(name, 0, 0, flist);
    }
    else {
        //write_cache_readdir(name, 0, 0, flist);
        write_cache_readdir_variable(name, flist);
    }

    /* end timer */
    double end_write = MPI_Wtime();

    /* report write count, time, and rate */
    if (verbose && rank == 0) {
        uint64_t all_count = bayer_flist_global_size(flist);
        double secs = end_write - start_write;
        double rate = 0.0;
        if (secs > 0.0) {
            rate = ((double)all_count) / secs;
        }
        printf("Wrote %lu files in %f seconds (%f files/sec)\n",
               all_count, secs, rate
              );
    }

    return;
}

void bayer_flist_file_copy(bayer_flist bsrc, uint64_t idx, bayer_flist bdst)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bsrc;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        flist_t* dstlist = (flist_t*) bdst;
        list_insert_copy(dstlist, elem);
    }
    return;
}

size_t bayer_flist_file_pack_size(bayer_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    size_t size = list_elem_pack2_size(flist->detail, flist->max_file_name, NULL);
    return size;
}

size_t bayer_flist_file_pack(void* buf, bayer_flist bflist, uint64_t idx)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        size_t size = list_elem_pack2(buf, flist->detail, flist->max_file_name, elem);
        return size;
    }
    return 0;
}

size_t bayer_flist_file_unpack(const void* buf, bayer_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = (elem_t*) BAYER_MALLOC(sizeof(elem_t));
    size_t size = list_elem_unpack2(buf, elem);
    list_insert_elem(flist, elem);
    return size;
}

int bayer_flist_summarize(bayer_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;
    list_compute_summary(flist);
    return BAYER_SUCCESS;
}

/*****************************
 * Randomly hash items to processes by filename, then remove
 ****************************/

/* for given depth, hash directory name and map to processes to
 * test whether having all files in same directory on one process
 * matters */
size_t bayer_flist_distribute_map(bayer_flist list, char** buffer,
                                  bayer_flist_name_encode_fn encode,
                                  bayer_flist_map_fn map, void* args)
{
    uint64_t idx;

    /* get our rank and number of ranks in job */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* allocate arrays for alltoall */
    size_t bufsize = (size_t)ranks * sizeof(int);
    int* sendsizes  = (int*) BAYER_MALLOC(bufsize);
    int* senddisps  = (int*) BAYER_MALLOC(bufsize);
    int* sendoffset = (int*) BAYER_MALLOC(bufsize);
    int* recvsizes  = (int*) BAYER_MALLOC(bufsize);
    int* recvdisps  = (int*) BAYER_MALLOC(bufsize);

    /* initialize sendsizes and offsets */
    int i;
    for (i = 0; i < ranks; i++) {
        sendsizes[i]  = 0;
        sendoffset[i] = 0;
    }

    /* compute number of bytes we'll send to each rank */
    size_t sendbytes = 0;
    uint64_t size = bayer_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        int dest = map(list, idx, ranks, args);

        /* TODO: check that pack size doesn't overflow int */
        /* total number of bytes we'll send to each rank and the total overall */
        size_t count = encode(NULL, list, idx, args);
        sendsizes[dest] += (int) count;
        sendbytes += count;
    }

    /* compute displacements */
    senddisps[0] = 0;
    for (i = 1; i < ranks; i++) {
        senddisps[i] = senddisps[i - 1] + sendsizes[i - 1];
    }

    /* allocate space */
    char* sendbuf = (char*) BAYER_MALLOC(sendbytes);

    /* copy data into buffer */
    for (idx = 0; idx < size; idx++) {
        int dest = map(list, idx, ranks, args);

        /* identify region to be sent to rank */
        char* path = sendbuf + senddisps[dest] + sendoffset[dest];
        size_t count = encode(path, list, idx, args);

        /* TODO: check that pack size doesn't overflow int */
        /* bump up the offset for this rank */
        sendoffset[dest] += (int) count;
    }

    /* alltoall to specify incoming counts */
    MPI_Alltoall(sendsizes, 1, MPI_INT, recvsizes, 1, MPI_INT, MPI_COMM_WORLD);

    /* compute size of recvbuf and displacements */
    size_t recvbytes = 0;
    recvdisps[0] = 0;
    for (i = 0; i < ranks; i++) {
        recvbytes += (size_t) recvsizes[i];
        if (i > 0) {
            recvdisps[i] = recvdisps[i - 1] + recvsizes[i - 1];
        }
    }

    /* allocate recvbuf */
    char* recvbuf = (char*) BAYER_MALLOC(recvbytes);

    /* alltoallv to send data */
    MPI_Alltoallv(
        sendbuf, sendsizes, senddisps, MPI_CHAR,
        recvbuf, recvsizes, recvdisps, MPI_CHAR, MPI_COMM_WORLD
    );

    /* free memory */
    bayer_free(&recvdisps);
    bayer_free(&recvsizes);
    bayer_free(&sendbuf);
    bayer_free(&sendoffset);
    bayer_free(&senddisps);
    bayer_free(&sendsizes);

    *buffer = recvbuf;
    return recvbytes;
}

/* given an input list and a map function pointer, call map function
 * for each item in list, identify new rank to send item to and then
 * exchange items among ranks and return new output list */
bayer_flist bayer_flist_remap(bayer_flist list, bayer_flist_map_fn map, void* args)
{
    uint64_t idx;

    /* create new list as subset (actually will be a remapping of
     * input list */
    bayer_flist newlist = bayer_flist_subset(list);

    /* get our rank and number of ranks in job */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* allocate arrays for alltoall */
    size_t bufsize = (size_t)ranks * sizeof(int);
    int* sendsizes  = (int*) BAYER_MALLOC(bufsize);
    int* senddisps  = (int*) BAYER_MALLOC(bufsize);
    int* sendoffset = (int*) BAYER_MALLOC(bufsize);
    int* recvsizes  = (int*) BAYER_MALLOC(bufsize);
    int* recvdisps  = (int*) BAYER_MALLOC(bufsize);

    /* initialize sendsizes and offsets */
    int i;
    for (i = 0; i < ranks; i++) {
        sendsizes[i]  = 0;
        sendoffset[i] = 0;
    }

    /* get number of elements in our local list */
    uint64_t size = bayer_flist_size(list);

    /* allocate space to record file-to-rank mapping */
    int* file2rank = (int*) BAYER_MALLOC(size * sizeof(int));

    /* call map function for each item to identify its new rank,
     * and compute number of bytes we'll send to each rank */
    size_t sendbytes = 0;
    for (idx = 0; idx < size; idx++) {
        /* determine which rank we'll map this file to */
        int dest = map(list, idx, ranks, args);

        /* cache mapping so we don't have to compute it again
         * below while packing items for send */
        file2rank[idx] = dest;

        /* TODO: check that pack size doesn't overflow int */
        /* total number of bytes we'll send to each rank and the total overall */
        size_t count = bayer_flist_file_pack_size(list);
        sendsizes[dest] += (int) count;
        sendbytes += count;
    }

    /* compute send buffer displacements */
    senddisps[0] = 0;
    for (i = 1; i < ranks; i++) {
        senddisps[i] = senddisps[i - 1] + sendsizes[i - 1];
    }

    /* allocate space for send buffer */
    char* sendbuf = (char*) BAYER_MALLOC(sendbytes);

    /* copy data into send buffer */
    for (idx = 0; idx < size; idx++) {
        /* determine which rank we mapped this file to */
        int dest = file2rank[idx];

        /* get pointer into send buffer and pack item */
        char* ptr = sendbuf + senddisps[dest] + sendoffset[dest];
        size_t count = bayer_flist_file_pack(ptr, list, idx);

        /* TODO: check that pack size doesn't overflow int */
        /* bump up the offset for this rank */
        sendoffset[dest] += (int) count;
    }

    /* alltoall to get our incoming counts */
    MPI_Alltoall(sendsizes, 1, MPI_INT, recvsizes, 1, MPI_INT, MPI_COMM_WORLD);

    /* compute size of recvbuf and displacements */
    size_t recvbytes = 0;
    recvdisps[0] = 0;
    for (i = 0; i < ranks; i++) {
        recvbytes += (size_t) recvsizes[i];
        if (i > 0) {
            recvdisps[i] = recvdisps[i - 1] + recvsizes[i - 1];
        }
    }

    /* allocate recvbuf */
    char* recvbuf = (char*) BAYER_MALLOC(recvbytes);

    /* alltoallv to send data */
    MPI_Alltoallv(
        sendbuf, sendsizes, senddisps, MPI_CHAR,
        recvbuf, recvsizes, recvdisps, MPI_CHAR, MPI_COMM_WORLD
    );

    /* unpack items into new list */
    char* ptr = recvbuf;
    char* recvend = recvbuf + recvbytes;
    while (ptr < recvend) {
        size_t count = bayer_flist_file_unpack(ptr, newlist);
        ptr += count;
    }
    bayer_flist_summarize(newlist);

    /* free memory */
    bayer_free(&file2rank);
    bayer_free(&recvbuf);
    bayer_free(&recvdisps);
    bayer_free(&recvsizes);
    bayer_free(&sendbuf);
    bayer_free(&sendoffset);
    bayer_free(&senddisps);
    bayer_free(&sendsizes);

    /* return list to caller */
    return newlist;
}
