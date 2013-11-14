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
bayer_flist bayer_flist_new();

/* create list as subset of another list
 * (returns emtpy list with same user and group maps) */
bayer_flist bayer_flist_subset(
  bayer_flist srclist
);

/* create file list by walking directory */
void bayer_flist_walk_path(
  const char* path,
  int use_stat,
  bayer_flist flist
);

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
typedef size_t (*bayer_flist_name_encode) (char *buf, bayer_flist flist, uint64_t index);
typedef size_t (*bayer_flist_distribute) (bayer_flist flist, char **buf, bayer_flist_name_encode encode);
size_t bayer_flist_distribute_map(bayer_flist flist, char **buffer, bayer_flist_name_encode encode);

#endif /* BAYER_FLIST_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
