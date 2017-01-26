/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2015, DataDirect Networks, Inc.
 * 
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/

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
#include "../../config.h"

#include "mpi.h"

#include "mfu.h"

#include <libcircle.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>

#if DCOPY_USE_XATTRS
#include <attr/xattr.h>
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

typedef struct {
    int64_t  total_dirs;         /* sum of all directories */
    int64_t  total_files;        /* sum of all files */
    int64_t  total_links;        /* sum of all symlinks */
    int64_t  total_size;         /* sum of all file sizes */
    int64_t  total_bytes_copied; /* total bytes written */
    time_t   time_started;       /* time when dcp command started */
    time_t   time_ended;         /* time when dcp command ended */
    double   wtime_started;      /* time when dcp command started */
    double   wtime_ended;        /* time when dcp command ended */
} DCOPY_statistics_t;

typedef struct {
    int    copy_into_dir; /* flag indicating whether copying into existing dir */
    char*  dest_path;     /* prefex of destination directory */
    char*  input_file;    /* file name of input list*/
    bool   preserve;      /* wether to preserve timestamps, ownership, permissions, etc. */
    bool   synchronous;   /* whether to use O_DIRECT */
    bool   sparse;        /* whether to create sparse files */
    size_t chunk_size;    /* size to chunk files by */
    size_t block_size;    /* block size to read/write to file system */
    char*  block_buf1;    /* buffer to read / write data */
    char*  block_buf2;    /* another buffer to read / write data */
#ifdef LUSTRE_SUPPORT
    int    grouplock_id;      /* Lustre grouplock ID */
#endif
} DCOPY_options_t;

/* cache open file descriptor to avoid
 * opening / closing the same file */
typedef struct {
    char* name; /* name of open file (NULL if none) */
    int   read; /* whether file is open for read-only (1) or write (0) */
    int   fd;   /* file descriptor */
} DCOPY_file_cache_t;

/** Cache most recent open file descriptor to avoid opening / closing the same file */
extern DCOPY_file_cache_t DCOPY_src_cache;
extern DCOPY_file_cache_t DCOPY_dst_cache;

extern int DCOPY_global_rank;

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
    mfu_flist flist,
    uint64_t index,
    const char* dest_path
);

void DCOPY_copy_ownership(
    mfu_flist flist,
    uint64_t index,
    const char* dest_path
);

void DCOPY_copy_permissions(
    mfu_flist flist,
    uint64_t index,
    const char* dest_path
);

void DCOPY_copy_timestamps(
    mfu_flist flist,
    uint64_t index,
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
