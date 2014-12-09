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

#ifndef BAYER_FLIST_H
#define BAYER_FLIST_H

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include "mpi.h"

/****************************************
 * Define types
 ***************************************/

/* TODO: these types may be encoded in files */
typedef enum bayer_filetypes_e {
    BAYER_TYPE_NULL    = 0,
    BAYER_TYPE_UNKNOWN = 1,
    BAYER_TYPE_FILE    = 2,
    BAYER_TYPE_DIR     = 3,
    BAYER_TYPE_LINK    = 4,
} bayer_filetype;

/* define handle type to a file list */
typedef void* bayer_flist;

/* create a NULL handle */
extern bayer_flist BAYER_FLIST_NULL;

/* create new, empty file list */
bayer_flist bayer_flist_new(void);

/* create list as subset of another list
 * (returns emtpy list with same user and group maps) */
bayer_flist bayer_flist_subset(
    bayer_flist srclist
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
bayer_flist bayer_flist_filter_regex(
    bayer_flist flist,
    const char* regex_exp,
    int exclude,
    int name
);

/* create file list by walking directory */
void bayer_flist_walk_path(
    const char* path,
    int use_stat,
    int dir_permissions,
    bayer_flist flist
);

/* create file list by walking list of directories */
void bayer_flist_walk_paths(
    uint64_t num_paths,
    const char** paths,
    int use_stat,
    int dir_permissions,
    bayer_flist flist
);

/* skip function pointer: given a path input, along with user-provided
 * arguments, compute whether to skip enqueue this path.
 * return 1 if skip enqueue, 0 if not.
 */
typedef int (*bayer_flist_skip_fn) (const char* path, void* args);

/* create file list by stating files */
void bayer_flist_stat(
    bayer_flist input_flist,
    bayer_flist flist,
    bayer_flist_skip_fn skip_fn,
    void *skip_args

/* read file list from file */
void bayer_flist_read_cache(
    const char* name,
    bayer_flist flist
);

/* write file list to file */
void bayer_flist_write_cache(
    const char* name,
    bayer_flist flist
);

/* free resouces in file list */
void bayer_flist_free(bayer_flist* flist);

/* given an input list, split items into separate lists depending
 * on their depth, returns number of levels, minimum depth, and
 * array of lists as output */
void bayer_flist_array_by_depth(
    bayer_flist srclist,   /* IN  - input list */
    int* outlevels,        /* OUT - number of depth levels */
    int* outmin,           /* OUT - minimum depth number */
    bayer_flist** outlists /* OUT - array of lists split by depth */
);

/* frees array of lists created in call to
 * bayer_flist_split_by_depth */
void bayer_flist_array_free(int levels, bayer_flist** outlists);

/* copy specified source file into destination list */
void bayer_flist_file_copy(bayer_flist src, uint64_t index, bayer_flist dest);

/* get number of bytes to pack a file from the specified list */
size_t bayer_flist_file_pack_size(bayer_flist flist);

/* pack specified file into buf, return number of bytes used */
size_t bayer_flist_file_pack(void* buf, bayer_flist flist, uint64_t index);

/* unpack file from buf and insert into list, return number of bytes read */
size_t bayer_flist_file_unpack(const void* buf, bayer_flist flist);

/* run this to enable query functions on list after adding elements */
int bayer_flist_summarize(bayer_flist flist);

/* return number of files across all procs */
uint64_t bayer_flist_global_size(bayer_flist flist);

/* returns the global index of first item on this rank,
 * when placing items in rank order */
uint64_t bayer_flist_global_offset(bayer_flist flist);

/* return number of files in local list */
uint64_t bayer_flist_size(bayer_flist flist);

/* return number of users */
uint64_t bayer_flist_user_count(bayer_flist flist);

/* return number of groups */
uint64_t bayer_flist_group_count(bayer_flist flist);

/* return maximum length of file names */
uint64_t bayer_flist_file_max_name(bayer_flist flist);

/* return maximum length of user names */
uint64_t bayer_flist_user_max_name(bayer_flist flist);

/* return maximum length of group names */
uint64_t bayer_flist_group_max_name(bayer_flist flist);

/* return minimum depth of all files */
int bayer_flist_min_depth(bayer_flist flist);

/* return maximum depth of all files */
int bayer_flist_max_depth(bayer_flist flist);

/* determines which properties are readable */
int bayer_flist_have_detail(bayer_flist flist);

/* always set */
const char* bayer_flist_file_get_name(bayer_flist flist, uint64_t index);
int bayer_flist_file_get_depth(bayer_flist flist, uint64_t index);
bayer_filetype bayer_flist_file_get_type(bayer_flist flist, uint64_t index);

/* valid if detail == 1 */
uint64_t bayer_flist_file_get_mode(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_uid(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_gid(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_atime(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_atime_nsec(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_mtime(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_mtime_nsec(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_ctime(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_ctime_nsec(bayer_flist flist, uint64_t index);
uint64_t bayer_flist_file_get_size(bayer_flist flist, uint64_t index);
const char* bayer_flist_file_get_username(bayer_flist flist, uint64_t index);
const char* bayer_flist_file_get_groupname(bayer_flist flist, uint64_t index);

/* Encode the file into a buffer, if the buffer is NULL, return the needed size */
typedef size_t (*bayer_flist_name_encode_fn)(char* buf, bayer_flist flist, uint64_t index, void* args);

/* map function pointer: given a list and index as input, along with
 * number of ranks and pointer to user-provided arguments, compute
 * rank number specified item should be assigned to */
typedef int (*bayer_flist_map_fn)(bayer_flist flist, uint64_t index, int ranks, void* args);

//typedef size_t (*bayer_flist_map) (bayer_flist flist, char **buf, bayer_flist_name_encode encode);
size_t bayer_flist_distribute_map(
    bayer_flist list,
    char** buffer,
    bayer_flist_name_encode_fn encode,
    bayer_flist_map_fn map,
    void* args
);

/* given an input list and a map function pointer, call map function
 * for each item in list, identify new rank to send item to and then
 * exchange items among ranks and return new output list */
bayer_flist bayer_flist_remap(bayer_flist list, bayer_flist_map_fn map, const void* args);

/* takes a list, spreads it out evenly, and then returns the newly created list 
* to the caller */
bayer_flist bayer_flist_spread(bayer_flist flist);

/* unlink all items in flist */
void bayer_flist_unlink(bayer_flist flist);

/* sort flist by specified fields, given as common-delimitted list
 * precede field name with '-' character to reverse sort order:
 *   name,user,group,uid,gid,atime,mtime,ctime,size
 * For example to sort by size in descending order, followed by name
 *   char fields[] = "size,-name"; */
int bayer_flist_sort(const char* fields, bayer_flist* flist);

/* given a list of files print from start and end of the list */
void bayer_flist_print(bayer_flist flist);

/* TODO: integrate this into the file list proper, or otherwise move it to another file */
/* used to create a linked list of copy_elem's */
typedef struct bayer_file_chunk_struct {
  const char* name; /* file name */
  uint64_t offset;
  uint64_t length;
  uint64_t file_size;
  uint64_t rank_of_owner;
  uint64_t index_of_owner;
  struct bayer_file_chunk_struct* next;
} bayer_file_chunk;

/* given a file list and a chunk size, split files at chunk boundaries and evenly
 * spread chunks to processes, returns a linked list of file sections each process
 * is responsbile for */
bayer_file_chunk* bayer_file_chunk_list_alloc(bayer_flist list, uint64_t chunk_size);

/* free the linked list allocated with bayer_file_chunk_list_alloc */
void bayer_file_chunk_list_free(bayer_file_chunk** phead);

#endif /* BAYER_FLIST_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
