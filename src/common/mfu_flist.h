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

#ifndef MFU_FLIST_H
#define MFU_FLIST_H

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

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <ctype.h>
#include <stdbool.h>
#include "mpi.h"

#if DCOPY_USE_XATTRS
#include <attr/xattr.h>
#endif /* DCOPY_USE_XATTRS */

/* default mode to create new files or directories */
#define DCOPY_DEF_PERMS_FILE (S_IRUSR | S_IWUSR)
#define DCOPY_DEF_PERMS_DIR  (S_IRWXU)

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


/****************************************
 * Define types
 ***************************************/

/* TODO: these types may be encoded in files */
typedef enum mfu_filetypes_e {
    MFU_TYPE_NULL    = 0,
    MFU_TYPE_UNKNOWN = 1,
    MFU_TYPE_FILE    = 2,
    MFU_TYPE_DIR     = 3,
    MFU_TYPE_LINK    = 4,
} mfu_filetype;

/* define handle type to a file list */
typedef void* mfu_flist;

/* create a NULL handle */
extern mfu_flist MFU_FLIST_NULL;

/* create new, empty file list */
mfu_flist mfu_flist_new(void);

/* create list as subset of another list
 * (returns emtpy list with same user and group maps) */
mfu_flist mfu_flist_subset(
    mfu_flist srclist
);

/* given an input flist, return a newly allocated flist consisting of 
 * a filtered set by finding all items that match/don't match a given
 * regular expression
 *
 *   exclude=0 - take matching items
 *   exclude=1 - exclude matching items
 *
 *   name=0 - match against full path of item
 *   name=1 - match against basename of item */
mfu_flist mfu_flist_filter_regex(
    mfu_flist flist,
    const char* regex_exp,
    int exclude,
    int name
);

/* create file list by walking directory */
void mfu_flist_walk_path(
    const char* path,
    int use_stat,
    int dir_permissions,
    mfu_flist flist
);

/* create file list by walking list of directories */
void mfu_flist_walk_paths(
    uint64_t num_paths,
    const char** paths,
    int use_stat,
    int dir_permissions,
    mfu_flist flist
);

/* given a list of param_paths, walk each one and add to flist */
void mfu_flist_walk_param_paths(
    uint64_t num,
    const mfu_param_path* params,
    int use_stat,
    int dir_perms,
    mfu_flist flist
);

/* skip function pointer: given a path input, along with user-provided
 * arguments, compute whether to enqueue this file in output list of
 * mfu_flist_stat, return 1 if file should be skipped, 0 if not. */
typedef int (*mfu_flist_skip_fn) (const char* path, void *args);

/* skip function args */
struct mfu_flist_skip_args {
    int numpaths;
    const mfu_param_path* paths;
};

/* Given an input file list, stat each file and enqueue details
 * in output file list, skip entries excluded by skip function
 * and skip args */
void mfu_flist_stat(
    mfu_flist input_flist,
    mfu_flist flist,
    mfu_flist_skip_fn skip_fn,
    void *skip_args
);

/* read file list from file */
void mfu_flist_read_cache(
    const char* name,
    mfu_flist flist
);

/* write file list to file */
void mfu_flist_write_cache(
    const char* name,
    mfu_flist flist
);

/* free resouces in file list */
void mfu_flist_free(mfu_flist* flist);

/* given an input list, split items into separate lists depending
 * on their depth, returns number of levels, minimum depth, and
 * array of lists as output */
void mfu_flist_array_by_depth(
    mfu_flist srclist,   /* IN  - input list */
    int* outlevels,        /* OUT - number of depth levels */
    int* outmin,           /* OUT - minimum depth number */
    mfu_flist** outlists /* OUT - array of lists split by depth */
);

/* frees array of lists created in call to
 * mfu_flist_split_by_depth */
void mfu_flist_array_free(int levels, mfu_flist** outlists);

/* copy specified source file into destination list */
void mfu_flist_file_copy(mfu_flist src, uint64_t index, mfu_flist dest);

/* get number of bytes to pack a file from the specified list */
size_t mfu_flist_file_pack_size(mfu_flist flist);

/* pack specified file into buf, return number of bytes used */
size_t mfu_flist_file_pack(void* buf, mfu_flist flist, uint64_t index);

/* unpack file from buf and insert into list, return number of bytes read */
size_t mfu_flist_file_unpack(const void* buf, mfu_flist flist);

/* run this to enable query functions on list after adding elements */
int mfu_flist_summarize(mfu_flist flist);

/* return number of files across all procs */
uint64_t mfu_flist_global_size(mfu_flist flist);

/* returns the global index of first item on this rank,
 * when placing items in rank order */
uint64_t mfu_flist_global_offset(mfu_flist flist);

/* return number of files in local list */
uint64_t mfu_flist_size(mfu_flist flist);

/* return number of users */
uint64_t mfu_flist_user_count(mfu_flist flist);

/* return number of groups */
uint64_t mfu_flist_group_count(mfu_flist flist);

/* return maximum length of file names */
uint64_t mfu_flist_file_max_name(mfu_flist flist);

/* return maximum length of user names */
uint64_t mfu_flist_user_max_name(mfu_flist flist);

/* return maximum length of group names */
uint64_t mfu_flist_group_max_name(mfu_flist flist);

/* return minimum depth of all files */
int mfu_flist_min_depth(mfu_flist flist);

/* return maximum depth of all files */
int mfu_flist_max_depth(mfu_flist flist);

/* determines which properties are readable */
int mfu_flist_have_detail(mfu_flist flist);

/* always set */
const char* mfu_flist_file_get_name(mfu_flist flist, uint64_t index);
int mfu_flist_file_get_depth(mfu_flist flist, uint64_t index);
mfu_filetype mfu_flist_file_get_type(mfu_flist flist, uint64_t index);

/* valid if detail == 1 */
uint64_t mfu_flist_file_get_mode(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_uid(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_gid(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_atime(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_atime_nsec(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_mtime(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_mtime_nsec(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_ctime(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_ctime_nsec(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_size(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_perm(mfu_flist flist, uint64_t index);
const char* mfu_flist_file_get_username(mfu_flist flist, uint64_t index);
const char* mfu_flist_file_get_groupname(mfu_flist flist, uint64_t index);

/* Encode the file into a buffer, if the buffer is NULL, return the needed size */
typedef size_t (*mfu_flist_name_encode_fn)(char* buf, mfu_flist flist, uint64_t index, void* args);

/* map function pointer: given a list and index as input, along with
 * number of ranks and pointer to user-provided arguments, compute
 * rank number specified item should be assigned to */
typedef int (*mfu_flist_map_fn)(mfu_flist flist, uint64_t index, int ranks, void* args);

//typedef size_t (*mfu_flist_map) (mfu_flist flist, char **buf, mfu_flist_name_encode encode);
size_t mfu_flist_distribute_map(
    mfu_flist list,
    char** buffer,
    mfu_flist_name_encode_fn encode,
    mfu_flist_map_fn map,
    void* args
);

/* given an input list and a map function pointer, call map function
 * for each item in list, identify new rank to send item to and then
 * exchange items among ranks and return new output list */
mfu_flist mfu_flist_remap(mfu_flist list, mfu_flist_map_fn map, const void* args);

/* takes a list, spreads it out evenly, and then returns the newly created list 
* to the caller */
mfu_flist mfu_flist_spread(mfu_flist flist);

/* copy items in list */
void mfu_flist_copy(mfu_flist src_cp_list, int numpaths,
        const mfu_param_path* paths, const mfu_param_path* destpath, 
        mfu_copy_opts_t* mfu_copy_opts);

/* unlink all items in flist */
void mfu_flist_unlink(mfu_flist flist);

int mfu_input_flist_skip(const char* name, void *args);

/* sort flist by specified fields, given as common-delimitted list
 * precede field name with '-' character to reverse sort order:
 *   name,user,group,uid,gid,atime,mtime,ctime,size
 * For example to sort by size in descending order, followed by name
 *   char fields[] = "size,-name"; */
int mfu_flist_sort(const char* fields, mfu_flist* flist);

/* given a list of files print from start and end of the list */
void mfu_flist_print(mfu_flist flist);

/* TODO: integrate this into the file list proper, or otherwise move it to another file */
/* used to create a linked list of copy_elem's */
typedef struct mfu_file_chunk_struct {
  const char* name; /* file name */
  uint64_t offset;
  uint64_t length;
  uint64_t file_size;
  uint64_t rank_of_owner;
  uint64_t index_of_owner;
  struct mfu_file_chunk_struct* next;
} mfu_file_chunk;

/* given a file list and a chunk size, split files at chunk boundaries and evenly
 * spread chunks to processes, returns a linked list of file sections each process
 * is responsbile for */
mfu_file_chunk* mfu_file_chunk_list_alloc(mfu_flist list, uint64_t chunk_size);

/* free the linked list allocated with mfu_file_chunk_list_alloc */
void mfu_file_chunk_list_free(mfu_file_chunk** phead);
#endif /* MFU_FLIST_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
