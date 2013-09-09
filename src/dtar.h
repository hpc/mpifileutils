#ifndef __DTAR_H_
#define __DTAR_H_

#ifndef O_BINARY
#define O_BINARY        0
#endif
#ifndef O_CLOEXEC
#define O_CLOEXEC       0
#endif

/* #define DTAR_CHUNK_SIZE (1073741824) 1GB chunk */
/* #define DTAR_CHUNK_SIZE (536870912)  512MB chunk */
/* #define DTAR_CHUNK_SIZE (33554432)  32MB chunk */
#define DTAR_CHUNK_SIZE (33554432)  /*16MB chunk */
#define FD_BLOCK_SIZE (1048576)

#ifndef PATH_MAX
#define PATH_MAX (4096)
#endif

#ifndef _POSIX_ARG_MAX
#define MAX_ARGS 4096
#else
#define MAX_ARGS _POSIX_ARG_MAX
#endif

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

#include "log.h"
#include <libcircle.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <utime.h>
#include <mpi.h>
#include <dirent.h>
#include <xattr.h>

typedef enum {
    TREEWALK, COPY
} DTAR_operation_code_t;

typedef struct {
    char* dest_path;
    int num_src_paths;
    char** src_path;
    char compress;
} DTAR_options_t;

typedef struct {
//    struct archive *a;
//    struct archive *disk;
//    struct archive_entry *entry;
//    ssize_t len;
//    int     fd_src;
    int fd_tar;
    int flags;
} DTAR_writer_t;

typedef struct {

    int64_t file_size;
    int64_t chunk;
    int64_t offset;
    DTAR_operation_code_t code;
    char* operand;
    char* dir;
} DTAR_operation_t;

extern MPI_Comm new_comm;
extern MPI_Comm inter_comm;

extern int64_t g_tar_offset;
extern int verbose;

extern DTAR_options_t DTAR_user_opts;
extern DTAR_writer_t DTAR_writer;
extern DTAR_loglevel DTAR_debug_level;
extern FILE* DTAR_debug_stream;

extern void (*DTAR_jump_table[3])(DTAR_operation_t* op, CIRCLE_handle* handle);

void DTAR_writer_init(void);
void DTAR_add_objects(CIRCLE_handle * handle);
void DTAR_process_objects(CIRCLE_handle * handle);
void DTAR_enqueue_work_objects(CIRCLE_handle* handle);

char* DTAR_encode_operation(DTAR_operation_code_t code, int64_t chunk,
        char* operand, uint64_t offset, int64_t file_size, char* dir);

DTAR_operation_t* DTAR_decode_operation(char* op);

void DTAR_parse_path_args(char * filename, char compress, char ** argv);

void DTAR_abort(int code);
void DTAR_exit(int code);

void DTAR_do_copy(DTAR_operation_t* op, CIRCLE_handle* handle);

int DTAR_perform_copy(DTAR_operation_t* op, int in_fd, int out_fd,
        off64_t offset);

int DTAR_open_input_fd(DTAR_operation_t* op, off64_t offset, off64_t len);

void DTAR_do_treewalk(DTAR_operation_t* op, CIRCLE_handle* handle);

void DTAR_stat_process_link(DTAR_operation_t* op, const struct stat64* statbuf,
        CIRCLE_handle* handle);

void DTAR_stat_process_file(DTAR_operation_t* op, const struct stat64* statbuf,
        CIRCLE_handle* handle);

void DTAR_stat_process_dir(DTAR_operation_t* op, const struct stat64* statbuf,
        CIRCLE_handle* handle);

#endif
