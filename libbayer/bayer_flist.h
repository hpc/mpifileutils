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
  TYPE_NULL    = 0,
  TYPE_UNKNOWN = 1,
  TYPE_FILE    = 2,
  TYPE_DIR     = 3,
  TYPE_LINK    = 4,
} bayer_filetype;

/* define handle type to a file list */
typedef void* bayer_flist;

/* create a NULL handle */
extern bayer_flist BAYER_FLIST_NULL;

/* initialize file list */
//void flist_init(bayer_list flist);

/* create list as subset of another list
 * (returns emtpy list with same user and group maps) */
void bayer_flist_subset(
  bayer_flist srclist,
  bayer_flist* bflist
);

/* create file list by walking directory */
void bayer_flist_walk_path(
  const char* path,
  int use_stat,
  bayer_flist* bflist
);

/* read file list from file */
void bayer_flist_read_cache(
  const char* name,
  bayer_flist* bflist
);

/* write file list to file */
void bayer_flist_write_cache(
  const char* name,
  bayer_flist bflist
);

/* free resouces in file list */
void bayer_flist_free(bayer_flist* flist);

size_t bayer_flist_file_pack_size(bayer_flist flist);
size_t bayer_flist_file_pack(void* buf, bayer_flist flist, int index);
size_t bayer_flist_file_unpack(const void* buf, bayer_flist flist, int detail, uint64_t chars);

/* run this to enable query functions on list after adding elements */
int bayer_flist_summarize(bayer_flist flist);

/* return number of files across all procs */
uint64_t bayer_flist_global_size(bayer_flist flist);

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

int bayer_flist_have_detail(bayer_flist flist);

/* get pointer to name of file at specified index,
 * returns 0 on success -1 otherwise */
int bayer_flist_file_name(bayer_flist flist, int index, char** name);

/* get depth of file at specified index,
 * returns 0 on success -1 otherwise */
int bayer_flist_file_depth(bayer_flist flist, int index, int* depth);

/* get type of file at specified index,
 * returns 0 on success -1 otherwise */
int bayer_flist_file_type(bayer_flist flist, int index, bayer_filetype* type);

/* get mode of file at specified index,
 * returns 0 on success -1 otherwise */
int bayer_flist_file_mode(bayer_flist flist, int index, mode_t* mode);

uint32_t bayer_flist_file_get_uid(bayer_flist flist, int index);
uint32_t bayer_flist_file_get_gid(bayer_flist flist, int index);
uint32_t bayer_flist_file_get_atime(bayer_flist flist, int index);
uint32_t bayer_flist_file_get_mtime(bayer_flist flist, int index);
uint32_t bayer_flist_file_get_ctime(bayer_flist flist, int index);
uint64_t bayer_flist_file_get_size(bayer_flist flist, int index);
const char* bayer_flist_file_get_username(bayer_flist flist, int index);
const char* bayer_flist_file_get_groupname(bayer_flist flist, int index);

#endif /* BAYER_FLIST_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
