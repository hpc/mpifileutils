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

/* TODO: these types may be encoded in files,
 * so changing their values can break backwards compatibility
 * in reading any such files */
typedef enum mfu_filetypes_e {
    MFU_TYPE_NULL    = 0, /* type not set */
    MFU_TYPE_UNKNOWN = 1, /* type not known */
    MFU_TYPE_FILE    = 2, /* regular file */
    MFU_TYPE_DIR     = 3, /* directory */
    MFU_TYPE_LINK    = 4, /* symlink */
} mfu_filetype;

/* define handle type to a file list */
typedef void* mfu_flist;

/* define a value to represent a NULL handle */
extern mfu_flist MFU_FLIST_NULL;

/****************************************
 * Functions to create and free lists
 ****************************************/

/* create new, empty file list */
mfu_flist mfu_flist_new(void);

/* free resouces in file list */
void mfu_flist_free(mfu_flist* flist);

/****************************************
 * Functions to add items by walking file system
 ****************************************/

/* create file list by walking directory,
 * optionally stat each item, and optionally
 * set directory permission bits in order to walk into a directory,
 * whose permission bits are otherwise restrictive (e.g., useful for recursive unlink) */
void mfu_flist_walk_path(
    const char* path,    /* IN  - path to be walked */
    int use_stat,        /* IN  - whether to stat each item (1) or not (0) */
    int dir_permissions, /* IN  - whether to set directory permission bits (1) or not (0) during walk */
    mfu_flist flist      /* OUT - flist to insert walked items into */
);

/* create file list by walking list of directories */
void mfu_flist_walk_paths(
    uint64_t num_paths,  /* IN  - number of paths in array */
    const char** paths,  /* IN  - array of paths to be walkted */
    int use_stat,        /* IN  - whether to stat each item (1) or not (0) */
    int dir_permissions, /* IN  - whether to set directory permission bits (1) or not (0) during walk */
    mfu_flist flist      /* OUT - flist to insert walked items into */
);

/* given a list of param_paths, walk each one and add to flist */
void mfu_flist_walk_param_paths(
    uint64_t num,                 /* IN  - number of paths in array */
    const mfu_param_path* params, /* IN  - array of paths to be walkted */
    int use_stat,                 /* IN  - whether to stat each item (1) or not (0) */
    int dir_perms,                /* IN  - whether to set directory permission bits (1) or not (0) during walk */
    mfu_flist flist               /* OUT - flist to insert walked items into */
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
    mfu_flist input_flist,     /* IN  - input flist to source items */
    mfu_flist flist,           /* OUT - output flist to copy items into */
    mfu_flist_skip_fn skip_fn, /* IN  - pointer to skip function */
    void *skip_args            /* IN  - arguments to be passed to skip function */
);

/****************************************
 * Functions to filter list in different ways
 ****************************************/

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

/* given an input list, split items into separate lists depending
 * on their depth, returns number of levels, minimum depth, and
 * array of lists as output */
void mfu_flist_array_by_depth(
    mfu_flist srclist,   /* IN  - input list */
    int* outlevels,      /* OUT - number of depth levels */
    int* outmin,         /* OUT - minimum depth number */
    mfu_flist** outlists /* OUT - array of lists split by depth */
);

/* frees array of lists created in call to
 * mfu_flist_split_by_depth */
void mfu_flist_array_free(int levels, mfu_flist** outlists);

/****************************************
 * Functions to read/write list to file or print to screen
 ****************************************/

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

/* write file list to text file */
void mfu_flist_write_text(
    const char* name,
    mfu_flist flist
);

/* given a list of files print from start and end of the list */
void mfu_flist_print(mfu_flist flist);

/****************************************
 * Functions to add new items to a list
 * and summarize global properties after adding new items
 ****************************************/

/* run this to enable query functions on list after adding elements */
int mfu_flist_summarize(mfu_flist flist);

/* create list as subset of another list
 * (returns emtpy list with same user and group maps) */
mfu_flist mfu_flist_subset(mfu_flist srclist);

/* copy specified source file into destination list */
void mfu_flist_file_copy(mfu_flist src, uint64_t index, mfu_flist dest);

/* get number of bytes to pack a file from the specified list */
size_t mfu_flist_file_pack_size(mfu_flist flist);

/* pack specified file into buf, return number of bytes used */
size_t mfu_flist_file_pack(void* buf, mfu_flist flist, uint64_t index);

/* unpack file from buf and insert into list, return number of bytes read */
size_t mfu_flist_file_unpack(const void* buf, mfu_flist flist);

/* fake insert, just increase the count, used for counting */
void mfu_flist_increase(mfu_flist* pblist);

/* create a new empty entry in the file list and return its index */
uint64_t mfu_flist_file_create(mfu_flist flist);

/****************************************
 * Functions to get/set properties of list
 ****************************************/

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

/* set flist deatils flag */
void mfu_flist_set_detail(mfu_flist flist, int detail);

/****************************************
 * Functions to get/set properties of individual list elements
 ****************************************/

/* read properties on specified item in local flist */
/* always set */
const char* mfu_flist_file_get_name(mfu_flist flist, uint64_t index);
int mfu_flist_file_get_depth(mfu_flist flist, uint64_t index);
mfu_filetype mfu_flist_file_get_type(mfu_flist flist, uint64_t index);

/* these properties are set if detail == 1 */
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
#if DCOPY_USE_XATTRS
void *mfu_flist_file_get_acl(mfu_flist bflist, uint64_t idx, ssize_t *acl_size, char *type);
#endif
const char* mfu_flist_file_get_username(mfu_flist flist, uint64_t index);
const char* mfu_flist_file_get_groupname(mfu_flist flist, uint64_t index);

/* set properties on specified item in local flist */
void mfu_flist_file_set_name(mfu_flist flist, uint64_t index, const char* name);
void mfu_flist_file_set_type(mfu_flist flist, uint64_t index, mfu_filetype type);
void mfu_flist_file_set_detail(mfu_flist flist, uint64_t index, int detail);
void mfu_flist_file_set_mode(mfu_flist flist, uint64_t index, uint64_t mode);
void mfu_flist_file_set_uid(mfu_flist flist, uint64_t index, uint64_t uid);
void mfu_flist_file_set_gid(mfu_flist flist, uint64_t index, uint64_t gid);
void mfu_flist_file_set_atime(mfu_flist flist, uint64_t index, uint64_t atime);
void mfu_flist_file_set_atime_nsec(mfu_flist flist, uint64_t index, uint64_t atime_nsec);
void mfu_flist_file_set_mtime(mfu_flist flist, uint64_t index, uint64_t mtime);
void mfu_flist_file_set_mtime_nsec(mfu_flist flist, uint64_t index, uint64_t mtime_nsec);
void mfu_flist_file_set_ctime(mfu_flist flist, uint64_t index, uint64_t ctime);
void mfu_flist_file_set_ctime_nsec(mfu_flist flist, uint64_t index, uint64_t ctime_nsec);
void mfu_flist_file_set_size(mfu_flist flist, uint64_t index, uint64_t size);
#if DCOPY_USE_XATTRS
//void *mfu_flist_file_set_acl(mfu_flist bflist, uint64_t idx, ssize_t *acl_size, char *type);
#endif

/****************************************
 * Functions to change how items are distributed over ranks
 ****************************************/

/* map function pointer: given a list and index as input, along with
 * number of ranks and pointer to user-provided arguments, compute
 * rank number specified item should be assigned to */
typedef int (*mfu_flist_map_fn)(mfu_flist flist, uint64_t index, int ranks, void* args);

/* given an input list and a map function pointer, call map function
 * for each item in list, identify new rank to send item to and then
 * exchange items among ranks and return new output list */
mfu_flist mfu_flist_remap(mfu_flist list, mfu_flist_map_fn map, const void* args);

/* takes a list, spreads it evenly among processes with respect to item count,
 * and then returns the newly created list to the caller */
mfu_flist mfu_flist_spread(mfu_flist flist);

/* sort flist by specified fields, given as common-delimitted list
 * precede field name with '-' character to reverse sort order:
 *   name,user,group,uid,gid,atime,mtime,ctime,size
 * For example to sort by size in descending order, followed by name
 *   char fields[] = "size,-name"; */
int mfu_flist_sort(const char* fields, mfu_flist* flist);

/****************************************
 * Functions to create / remove data on file system based on input list
 ****************************************/

/* copy items in list from source paths to destination,
 * each item in source list must come from one of the
 * given source paths */
void mfu_flist_copy(
    mfu_flist src_cp_list,          /* IN - flist providing source items */
    int numpaths,                   /* IN - number of source paths */
    const mfu_param_path* paths,    /* IN - array of source pathts */
    const mfu_param_path* destpath, /* IN - destination path */
    mfu_copy_opts_t* mfu_copy_opts  /* IN - options to be used during copy */
);

/* create all directories in flist */
void mfu_flist_mkdir(mfu_flist flist);

/* create inodes for all regular files in flist, assumes directories exist */
void mfu_flist_mknod(mfu_flist flist);

/* unlink all items in flist,
 * if traceless=1, restore timestamps on parent directories after unlinking children */
void mfu_flist_unlink(mfu_flist flist, bool traceless);

/* TODO: integrate this into the file list proper, or otherwise move it to another file */
/* element structure in linked list returned by mfu_file_chunk_list_alloc */
typedef struct mfu_file_chunk_struct {
  const char* name;        /* full path to file name */
  uint64_t offset;         /* starting byte offset in file */
  uint64_t length;         /* length of bytes process is responsible for */
  uint64_t file_size;      /* full size of target file */
  uint64_t rank_of_owner;  /* MPI rank acting as the owner of this file */
  uint64_t index_of_owner; /* index value of file in original flist on its owner rank */
  struct mfu_file_chunk_struct* next; /* pointer to next chunk element */
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
