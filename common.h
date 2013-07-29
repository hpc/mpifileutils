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

/* Autotool defines. */
#include "../config.h"

/* Common logging. */
#include "log.h"

#include <mpi.h>

#include <libcircle.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>
#include <attr/xattr.h>

/*
 * This is the size of each chunk to be processed (in bytes).
 */
/* #define DCOPY_CHUNK_SIZE (1073741824) 1GB chunk */
#define DCOPY_CHUNK_SIZE (536870912) /* 512MB chunk */
/* #define DCOPY_CHUNK_SIZE (33554432)  32MB chunk */
/* #define DCOPY_CHUNK_SIZE (33554432) 16MB chunk */

/* default mode to create new files or directories */
#define DCOPY_DEF_PERMS_FILE (S_IRUSR | S_IWUSR)
#define DCOPY_DEF_PERMS_DIR  (S_IRWXU)

/*
 * FIXME: Is this description correct?
 *
 * This is the size of the buffer used to copy from the fd page cache to L1
 * cache before the buffer is copied back down into the destination fd page
 * cache.
 */
#define FD_PAGE_CACHE_SIZE (32768)

/*
 * block size to read and write file data,
 * should evenly divide DCOPY_CHUNK_SIZE
 * */
#define FD_BLOCK_SIZE (1048576)

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
     * track of the path inside the destination (especially in the recursive
     * case).
     */
    char* dest_base_appendix;

    /* the full dest path */
    char* dest_full_path;
} DCOPY_operation_t;

typedef struct {
    int64_t  total_bytes_copied;
    time_t   time_started;
    time_t   time_ended;
    double   wtime_started;
    double   wtime_ended;
} DCOPY_statistics_t;

typedef struct {
    char*  dest_path;
    int    num_src_paths;
    char** src_path;
    bool   conditional;
    bool   skip_compare;
    bool   force;
    bool   preserve;
    bool   recursive;
    bool   recursive_unspecified;
    bool   reliable_filesystem;
} DCOPY_options_t;

/* struct for elements in linked list */
typedef struct list_elem {
  char* file;             /* file name */
  struct stat64* sb;      /* stat info */
  int depth;
  struct list_elem* next; /* pointer to next item */
} DCOPY_stat_elem_t;

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

void DCOPY_add_objects(CIRCLE_handle* handle);

void DCOPY_process_objects(CIRCLE_handle* handle);

void DCOPY_unlink_destination(DCOPY_operation_t* op);

FILE* DCOPY_open_input_stream(DCOPY_operation_t* op);

int DCOPY_open_input_fd(DCOPY_operation_t* op, \
                        off64_t offset, \
                        off64_t len);

FILE* DCOPY_open_output_stream(DCOPY_operation_t* op);

int DCOPY_open_output_fd(DCOPY_operation_t* op);

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
void DCOPY_abort(int code) __attribute__((noreturn));

/* called globally by all procs to exit */
void DCOPY_exit(int code);

#endif /* __COMMON_H_ */
