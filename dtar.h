#ifndef __DTAR_H_
#define __DTAR_H_

#include <mpi.h>

#include <libcircle.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>
#include <attr/xattr.h>

#ifndef O_BINARY
#define O_BINARY        0
#endif
#ifndef O_CLOEXEC
#define O_CLOEXEC       0
#endif

typedef enum {
    TREEWALK, COPY, CLEANUP
} DTAR_operation_code_t;

typedef struct {
    char*  dest_path;
    int    num_src_paths;
    char** src_path;
    char   compress;
} DTAR_options_t;

typedef struct {
    struct archive *a;
    struct archive *disk;
    struct archive_entry *entry;
    ssize_t len;
    int     fd_src;
    int     fd_tar;
    int     flags;
}DTAR_writer_t;

typedef struct {

    int64_t file_size;
    int64_t chunk;
    uint16_t source_base_offset;
    DTAR_operation_code_t code;
    char* operand;
    char* dest_base_appendix;
    char* dest_full_path;

} DTAR_operation_t;   /*copy from dcp */


#endif
