/* defines utility functions like memory allocation
 * and error / abort routines */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_PARAM_PATH_H
#define MFU_PARAM_PATH_H

#include <stdlib.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include "mpi.h"
#include "mfu_io.h"

/* for struct stat */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/* Collective routine called "mfu_param_path_set" to process input
 * paths specified by the user.  This routine takes a path as a string,
 * processes it in different ways, and fills in a data structure.
 *
 * The data structure contains a copy of the original string as
 * specified by the user (orig), a standardized version (path) that
 * uses an absolute path and removes things like ".", "..", consecutive
 * and trailing '/' chars, and finally a version that resolves any
 * symlinks (target).  For path and target, it also executes and
 * records the stat info corresponding to those paths (path_stat and
 * target_stat), and there is a flag set to 0 or 1 to indicate whether
 * the stat fields are valid (path_stat_valid and target_stat_valid).
 *
 * To avoid hitting the file system with a bunch of redundant stat
 * calls, only one rank executes the stat call for each path, and
 * the information is shared through MPI.
 *
 * After calling mfu_param_path_set, you must eventually call
 * mfu_param_path_free to release resources allocated in
 * mfu_param_path_set. */

typedef struct mfu_param_path_t {
    char* orig;              /* original path as specified by user */
    char* path;              /* reduced path, but still includes symlinks */
    int   path_stat_valid;   /* flag to indicate whether path_stat is valid */
    struct stat path_stat;   /* stat of path */
    char* target;            /* fully resolved path, no more symlinks */
    int   target_stat_valid; /* flag to indicate whether target_stat is valid */
    struct stat target_stat; /* stat of target path */
} mfu_param_path;

/* options passed to I/O functions that tell them which backend filesystem to use */
typedef struct {
    enum                 {POSIX, DFS, DAOS} type;
    int                  fd;
#ifdef DAOS_SUPPORT
    /* DAOS specific variables for I/O */
    daos_off_t           offset;  /* file offset */
    dfs_obj_t*           obj;     /* open object handle */
    dfs_sys_t*           dfs_sys; /* handle for high-level file operations */
    dfs_t*               dfs;     /* handle for lower-level file operations */
#endif
} mfu_file_t;

/* set fields in param according to path */
void mfu_param_path_set(
    const char* path,      /* IN  - path to be queried */
    mfu_param_path* param, /* OUT - param_path structure to fill in after querying input path */
    mfu_file_t* mfu_file,  /* IN  - file system type holding the specified path */
    bool warn              /* IN  - if path does not exist, whether to print warning (true) or not (false) */
);

/* free memory associated with param */
void mfu_param_path_free(mfu_param_path* param);

/* set fields in params according to paths,
 * the number of paths is specified in num,
 * paths is an array of char* of length num pointing to the input paths,
 * params is an array of length num to hold output */
void mfu_param_path_set_all(
    uint64_t num,           /* IN  - number of paths to be queried */
    const char** paths,     /* IN  - list of paths to be queried, of length num */
    mfu_param_path* params, /* OUT - list of param_paths to fill in with results of querying input paths */
    mfu_file_t* mfu_file,   /* IN  - file system type holding the specified paths */
    bool warn               /* IN  - if path does not exist, whether to print warning (true) or not (false) */
);

/* free resources allocated in call to mfu_param_path_set_all */
void mfu_param_path_free_all(uint64_t num, mfu_param_path* params);

/* given a list of source param_paths and single destinaton path,
 * identify whether sources can be copied to destination, returns
 * valid=1 if copy is valid and returns copy_into_dir=1 if
 * destination is a directory and items should be copied into
 * it rather than on top of it */
void mfu_param_path_check_copy(
    uint64_t num,                   /* IN  - number of source paths */
    const mfu_param_path* paths,    /* IN  - array of source param paths */
    const mfu_param_path* destpath, /* IN  - dest param path */
    mfu_file_t* mfu_src_file,       /* IN  - mfu_file for source that specifies which I/O calls to make */
    mfu_file_t* mfu_dst_file,       /* IN  - mfu_file for destination that specifies which I/O calls to make */
    int no_dereference,             /* IN  - if true, don't dereference source symbolic links */
    int* flag_valid,                /* OUT - flag indicating whether combination of source and dest param paths are valid (1) or not (0) */
    int* flag_copy_into_dir         /* OUT - flag indicating whether source items should be copied into destination directory (1) or not (0) */
);

/* options passed to walk that effect how the walk is executed */
typedef struct {
    int dir_perms;      /* flag option to update dir perms during walk */
    int remove;         /* flag option to remove files during walk */
    int use_stat;       /* flag option on whether or not to stat files during walk */
    int dereference;    /* flag option to dereference symbolic links */
} mfu_walk_opts_t;

typedef enum {
    XATTR_COPY_INVAL,
    XATTR_COPY_NONE,
    XATTR_SKIP_LUSTRE,
    XATTR_USE_LIBATTR,
    XATTR_COPY_ALL,
} attr_copy_t;

/* options passed to mfu_ */
typedef struct {
    int          copy_into_dir;    /* flag indicating whether copying into existing dir */
    int          do_sync;          /* flag option to sync src dir with dest dir */
    char*        dest_path;        /* prefex of destination directory */
    char*        input_file;       /* file name of input list */
    bool         preserve;         /* whether to preserve timestamps, ownership, permissions, etc. */
    attr_copy_t  copy_xattrs;      /* which xattrs to copy; important for Lustre */
    int          dereference;      /* if true, dereference symbolic links in the source.
                                    * this is not a perfect opposite of no_dereference */
    int          no_dereference;   /* if true, don't dereference source symbolic links */
    bool         direct;           /* whether to use O_DIRECT */
    bool         sparse;           /* whether to create sparse files */
    size_t       chunk_size;       /* size to chunk files by */
    size_t       buf_size;         /* buffer size to read/write to file system */
    char*        block_buf1;       /* buffer to read / write data */
    char*        block_buf2;       /* another buffer to read / write data */
    int          grouplock_id;     /* Lustre grouplock ID */
    uint64_t     batch_files;      /* max batch size to copy files, 0 implies no limit */
} mfu_copy_opts_t;

/*
 * Parse an option string provided by the user to determine
 * which xattrs to copy from source to destination.
 */
attr_copy_t parse_copy_xattrs_option(char *optarg);

/* Given a source item name, determine which source path this item
 * is contained within, extract directory components from source
 * path to this item and then prepend destination prefix.
 * Returns NULL if destination path could not be computed.
 * Otherwise allocates and returns a string giving the computed destination path.
 * Caller is responsible for freeing returned string with mfu_free(). */
char* mfu_param_path_copy_dest(
    const char* name,               /* IN  - path of item being considered */
    int numpaths,                   /* IN  - number of source paths */
    const mfu_param_path* paths,    /* IN  - array of source param paths */
    const mfu_param_path* destpath, /* IN  - dest param path */
    mfu_copy_opts_t* mfu_copy_opts, /* IN  - options to be used during copy */
    mfu_file_t* mfu_src_file,       /* IN  - I/O filesystem functions to use for copy of src */
    mfu_file_t* mfu_dst_file        /* IN  - I/O filesystem functions to use for copy of dst */
);

#endif /* MFU_PARAM_PATH_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
