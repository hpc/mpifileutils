/* defines utility functions like memory allocation
 * and error / abort routines */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef BAYER_PARAM_PATH_H
#define BAYER_PARAM_PATH_H

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include "mpi.h"

/* for struct stat */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/* Collective routine called "bayer_param_path_set" to process input
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
 * calls, only rank 0 executes the stat calls, and it broadcasts the
 * results to all other ranks in the job.
 *
 * After calling bayer_param_path_set, you must eventually call
 * bayer_param_path_free to release resources allocated in
 * bayer_param_path_set. */

typedef struct bayer_param_path_t {
    char* orig;              /* original path as specified by user */
    char* path;              /* reduced path, but still includes symlinks */
    int   path_stat_valid;   /* flag to indicate whether path_stat is valid */
    struct stat path_stat;   /* stat of path */
    char* target;            /* fully resolved path, no more symlinks */
    int   target_stat_valid; /* flag to indicate whether target_stat is valid */
    struct stat target_stat; /* stat of target path */
} bayer_param_path;

/* set fields in param according to path */
void bayer_param_path_set(const char* path, bayer_param_path* param);

/* free memory associated with param */
void bayer_param_path_free(bayer_param_path* param);

#endif /* BAYER_PARAM_PATH_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
