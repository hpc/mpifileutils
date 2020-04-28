/* See the file "COPYING" for the full license governing this code. */

#ifndef __COMMON_H_
#define __COMMON_H_

/* Make sure we're using 64 bit file handling. */
#ifdef _FILE_OFFSET_BITS
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE 1
#endif

#ifndef __USE_LARGEFILE64
#define __USE_LARGEFILE64
#endif

#ifndef _LARGEFILE_SOURCE
#define _LARGEFILE_SOURCE
#endif

/* Enable posix extensions (popen). */
#ifndef _BSD_SOURCE
#define _BSD_SOURCE 1
#endif

/* For O_NOATIME support */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "mpi.h"

#include "mfu.h"
#include "mfu_util.h"

#include <libcircle.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>

#if DCOPY_USE_XATTRS
#include <sys/xattr.h>
/*
 * Newer versions of attr deprecated attr/xattr.h which defines ENOATTR as a
 * ENODATA. Add the definition to keep compatibility.
 */
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif
#endif /* DCOPY_USE_XATTRS */

/* default mode to create new files or directories */
#define DCOPY_DEF_PERMS_FILE (S_IRUSR | S_IWUSR)
#define DCOPY_DEF_PERMS_DIR  (S_IRWXU)

/*
 * This is the size of each chunk to be processed (in bytes).
 */
#define DCOPY_CHUNK_SIZE (1*1024*1024)

/* buffer size to read/write data to file system */
#define FD_BLOCK_SIZE (1*1024*1024)

/*
 * FIXME: Is this description correct?
 *
 * This is the size of the buffer used to copy from the fd page cache to L1
 * cache before the buffer is copied back down into the destination fd page
 * cache.
 */
#define FD_PAGE_CACHE_SIZE (32768)

#ifndef PATH_MAX
#define PATH_MAX (4096)
#endif

#ifndef _POSIX_ARG_MAX
#define MAX_ARGS 4096
#else
#define MAX_ARGS _POSIX_ARG_MAX
#endif

typedef enum {
    TREEWALK, COPY, CLEANUP, COMPARE
} DCOPY_operation_code_t;

typedef struct {
    /*
     * The total file size.
     */
    int64_t file_size;

    /*
     * The chunk number this operation is associated with.
     */
    int64_t chunk;

    /*
     * This offset represents the index into the operand path that gives the
     * starting index of the root path to copy from.
     */
    uint16_t source_base_offset;

    /* The operation type. */
    DCOPY_operation_code_t code;

    /* The full source path. */
    char* operand;

    /*
     * If the destination already existed before this copy started, we want to
     * copy the files to a location inside the destination. This is to keep
     * track of the path inside the destination.
     */
    char* dest_base_appendix;

    /* the full dest path */
    char* dest_full_path;
} DCOPY_operation_t;

typedef struct {
    int64_t  total_dirs;         /* sum of all directories */
    int64_t  total_files;        /* sum of all files */
    int64_t  total_links;        /* sum of all symlinks */
    int64_t  total_size;         /* sum of all file sizes */
    int64_t  total_bytes_copied; /* total bytes written */
    time_t   time_started;
    time_t   time_ended;
    double   wtime_started;
    double   wtime_ended;
} DCOPY_statistics_t;

typedef struct {
    int    copy_into_dir;
    char*  dest_path;
    bool   compare;
    bool   force;
    bool   preserve;
    bool   reliable_filesystem;
    bool   synchronous;
    size_t chunk_size; /* size to chunk files by */
    size_t block_size; /* block size to read/write to file system */
    char*  block_buf1;
    char*  block_buf2;
} DCOPY_options_t;

/* cache open file descriptor to avoid
 * opening / closing the same file */
typedef struct {
    char* name;
    int   read;
    int   fd;
} DCOPY_file_cache_t;

/** Cache most recent open file descriptor to avoid opening / closing the same file */
extern DCOPY_file_cache_t DCOPY_src_cache;
extern DCOPY_file_cache_t DCOPY_dst_cache;

/* struct for elements in linked list */
typedef struct list_elem {
  char* file;             /* file name */
  struct stat64* sb;      /* stat info */
  int depth;
  struct list_elem* next; /* pointer to next item */
} DCOPY_stat_elem_t;

extern int DCOPY_global_rank;
extern size_t DCOPY_chunksize;
extern size_t DCOPY_blocksize;

extern DCOPY_stat_elem_t* DCOPY_list_head;
extern DCOPY_stat_elem_t* DCOPY_list_tail;

DCOPY_operation_t* DCOPY_decode_operation(char* op);

void DCOPY_opt_free(DCOPY_operation_t** opt);

char* DCOPY_encode_operation(DCOPY_operation_code_t code, \
                             int64_t chunk, \
                             char* operand, \
                             uint16_t source_base_offset, \
                             char* dest_base_appendix, \
                             int64_t file_size);

void DCOPY_retry_failed_operation(DCOPY_operation_code_t target, \
                                  CIRCLE_handle* handle, \
                                  DCOPY_operation_t* op);

int DCOPY_open_source(
    const char* file
);

int DCOPY_open_file(
    const char* file,
    int read,
    DCOPY_file_cache_t* cache
);

int DCOPY_close_file(
    DCOPY_file_cache_t* cache
);

void DCOPY_copy_xattrs(
    DCOPY_operation_t* op,
    const struct stat64* statbuf,
    const char* dest_path
);

void DCOPY_copy_ownership(
    const struct stat64* statbuf,
    const char* dest_path
);

void DCOPY_copy_permissions(
    const struct stat64* statbuf,
    const char* dest_path
);

void DCOPY_copy_timestamps(
    const struct stat64* statbuf,
    const char* dest_path
);

/* called by single process upon detection of a problem */
void DCOPY_abort(
    int code
) __attribute__((noreturn));

/* called globally by all procs to exit */
void DCOPY_exit(
    int code
);

#endif /* __COMMON_H_ */
