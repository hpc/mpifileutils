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
#include <stdint.h>
#include "mpi.h"

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
 * calls, only rank 0 executes the stat calls, and it broadcasts the
 * results to all other ranks in the job.
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

/* set fields in params according to paths,
 * the number of paths is specified in num,
 * paths is an array of char* of length num pointing to the input paths,
 * params is an array of length num to hold output */
void mfu_param_path_set_all(uint64_t num, const char** paths, mfu_param_path* params);

/* free resources allocated in call to mfu_param_path_set_all */
void mfu_param_path_free_all(uint64_t num, mfu_param_path* params);

/* set fields in param according to path */
void mfu_param_path_set(const char* path, mfu_param_path* param);

/* free memory associated with param */
void mfu_param_path_free(mfu_param_path* param);

/* given a list of param_paths, walk each one and add to flist */
void mfu_param_path_walk(uint64_t num, const mfu_param_path* params, int walk_stat, mfu_flist flist, int dir_perms);

/* given a list of source param_paths and single destinaton path,
 * identify whether sources can be copied to destination, returns
 * valid=1 if copy is valid and returns copy_into_dir=1 if
 * destination is a directory and items should be copied into
 * it rather than on top of it */
void mfu_param_path_check_copy(uint64_t num, const mfu_param_path* paths, const mfu_param_path* destpath, int* flag_valid, int* flag_copy_into_dir);

#endif /* MFU_PARAM_PATH_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
