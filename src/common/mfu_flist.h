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
#include <sys/xattr.h>
/*
 * Newer versions of attr deprecated attr/xattr.h which defines ENOATTR as a
 * ENODATA. Add the definition to keep compatibility.
 */
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif
#endif /* DCOPY_USE_XATTRS */

/* default mode to create new files or directories */
#define DCOPY_DEF_PERMS_FILE (S_IRUSR | S_IWUSR)
#define DCOPY_DEF_PERMS_DIR  (S_IRWXU)

/* default chunk size to split files into work units */
#define MFU_CHUNK_SIZE_STR "4MB"
#define MFU_CHUNK_SIZE (4*1024*1024)

/* default buffer size to read/write data to file system */
#define MFU_BUFFER_SIZE_STR "4MB"
#define MFU_BUFFER_SIZE (4*1024*1024)

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

/* function prototype for predicate, return values:
 *   1 - if element satisfies test
 *   0 - if element does not satisfy test
 *  -1 - if test of element encountered an error condition */
typedef int (*mfu_pred_fn)(mfu_flist flist, uint64_t idx, void* arg);

/* defines element type for predicate list */
typedef struct mfu_pred_item_t {
    mfu_pred_fn f;                /* function to be executed */
    void* arg;                    /* argument to be passed to function */
    struct mfu_pred_item_t* next; /* pointer to next element in list */
} mfu_pred;

/* we parse the mode string given by the user and build a linked list of
 * permissions operations, this defines one element in that list.  This
 * enables the user to specify a sequence of operations separated with
 * commas like "u+r,g+x" */
typedef struct mfu_perms_t {
    int octal;           /* set to 1 if mode_octal is valid */
    long mode_octal;     /* records octal mode (converted to an integer) */
    int usr;             /* set to 1 if user (owner) bits should be set (e.g. u+r) */
    int group;           /* set to 1 if group bits should be set (e.g. g+r) */
    int other;           /* set to 1 if other bits should be set (e.g. o+r) */
    int all;             /* set to 1 if all bits should be set (e.g. a+r) */
    int assume_all;      /* if this flag is set umask is taken into account */
    int plus;            /* set to 1 if mode has plus, set to 0 for minus */
    int read;            /* set to 1 if 'r' is given */
    int write;           /* set to 1 if 'w' is given */
    int execute;         /* set to 1 if 'x' is given */
    int capital_execute; /* set to 1 if 'X' is given */
    int assignment;      /* set to 1 if operation is an assignment (e.g. g=u) */
    char source;         /* records source of target: 'u', 'g', 'a' */
    struct mfu_perms_t* next;  /* pointer to next perms struct in linked list */
} mfu_perms;

/****************************************
 * Functions to create, free, and inspect mode strings
 ****************************************/

/* given a mode string like "u+r,g-x", fill in a linked list of permission
 * struct pointers returns 1 on success, 0 on failure */
int mfu_perms_parse(const char* modestr, mfu_perms** pperms);

/* given a linked list of permissions structures, check whether user has given us
 * something like "u+rx", "u+rX", or "u+r,u+X" since we need to set bits on
 * directories during the walk in this case. Also, check for turning on read and
 * execute for the "all" bits as well because those can also turn on the user's
 * read and execute bits, sets dir_perms to 1 if "rx" should be set on directories
 * and set to 0 otherwise */
void mfu_perms_need_dir_rx(const mfu_perms* head, mfu_walk_opts_t* walk_opts);

/* free the permissions linked list allocated in mfu_perms_parse,
 * sets pointer to NULL on return */
void mfu_perms_free(mfu_perms** pperms);

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
    const char* path,           /* IN  - path to be walked */
    mfu_walk_opts_t* walk_opts, /* IN  - functions to perform during the walk */
    mfu_flist flist,            /* OUT - flist to insert walked items into */
    mfu_file_t* mfu_file        /* IN  - I/O filesystem functions to use during the walk */
);

/* create file list by walking list of directories */
void mfu_flist_walk_paths(
    uint64_t num_paths,         /* IN  - number of paths in array */
    const char** paths,         /* IN  - array of paths to be walkted */
    mfu_walk_opts_t* walk_opts, /* IN  - functions to perform during the walk */
    mfu_flist flist,            /* OUT - flist to insert walked items into */
    mfu_file_t* mfu_file        /* IN  - I/O filesystem functions to use during the walk */
);

/* given a list of param_paths, walk each one and add to flist */
void mfu_flist_walk_param_paths(
    uint64_t num,                 /* IN  - number of paths in array */
    const mfu_param_path* params, /* IN  - array of paths to be walkted */
    mfu_walk_opts_t* walk_opts,   /* IN  - functions to perform during the walk */
    mfu_flist flist,              /* OUT - flist to insert walked items into */
    mfu_file_t* mfu_file          /* IN  - I/O filesystem functions to use during the walk */
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
    void *skip_args,           /* IN  - arguments to be passed to skip function */
    int dereference,           /* IN  - whether to dereference symbolic links */
    mfu_file_t* mfu_file       /* IN  - I/O filesystem functions to use */
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

/* given an input flist, return a newly allocated flist consisting of
 * a filtered set by finding all items that match the given predicate */
mfu_flist mfu_flist_filter_pred(mfu_flist flist, mfu_pred* p);

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

/* print count of items, directories, files, links, and bytes */
void mfu_flist_print_summary(mfu_flist flist);

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

/* insert a new entry by stat'ing a path and return its index,
 * returns MFU_SUCCESS on success */
int mfu_flist_file_create_stat(mfu_flist flist, const char* path);

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

/* given a mode_t from stat, return the corresponding MFU filetype */
mfu_filetype mfu_flist_mode_to_filetype(mode_t mode);

/****************************************
 * Functions to get/set properties of individual list elements
 ****************************************/

/* read properties on specified item in local flist */
/* always set */
uint64_t mfu_flist_file_get_oid_low(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_oid_high(mfu_flist flist, uint64_t index);
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
#ifdef DAOS_SUPPORT
void mfu_flist_file_set_oid(mfu_flist flist, uint64_t index, daos_obj_id_t oid);
void mfu_flist_file_set_cont(mfu_flist flist, uint64_t index, const char* name);
#endif
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
typedef int (*mfu_flist_map_fn)(mfu_flist flist, uint64_t index, int ranks, const void* args);

/* given an input list and a map function pointer, call map function
 * for each item in list, identify new rank to send item to and then
 * exchange items among ranks and return new output list */
mfu_flist mfu_flist_remap(mfu_flist list, mfu_flist_map_fn map, const void* args);

/* takes a list, spreads it evenly among processes with respect to item count,
 * and then returns the newly created list to the caller */
mfu_flist mfu_flist_spread(mfu_flist flist);

/* sort flist by specified fields, given as common-delimitted list
 * returns a newly allocated sorted list
 * precede field name with '-' character to reverse sort order:
 *   name,user,group,uid,gid,atime,mtime,ctime,size
 * For example to sort by size in descending order, followed by name
 *   char fields[] = "size,-name"; */
mfu_flist mfu_flist_sort(const char* fields, mfu_flist flist);

/****************************************
 * Functions to create / remove data on file system based on input list
 ****************************************/

/* allocate a new mfu_file_t structure,
 * and set its fields with default values */
mfu_file_t* mfu_file_new(void);

/* free object allocated in mfu_file_new */
void mfu_file_delete(mfu_file_t** mfile);

/* allocate a new mfu_copy_opts structure,
 * and set its fields with default values */
mfu_copy_opts_t* mfu_copy_opts_new(void);

/* free object allocated in mfu_copy_opts_new */
void mfu_copy_opts_delete(mfu_copy_opts_t** opts);

/* copy items in list from source paths to destination,
 * each item in source list must come from one of the
 * given source paths, returns 0 on success -1 on error */
int mfu_flist_copy(
    mfu_flist src_cp_list,          /* IN - flist providing source items */
    int numpaths,                   /* IN - number of source paths */
    const mfu_param_path* paths,    /* IN - array of source pathts */
    const mfu_param_path* destpath, /* IN - destination path */
    mfu_copy_opts_t* mfu_copy_opts, /* IN - options to be used during copy */
    mfu_file_t* mfu_src_file,       /* IN - I/O filesystem functions to use for copy of src */
    mfu_file_t* mfu_dst_file        /* IN - I/O filesystem functions to use for copy of dst */
);

/* link items in list from source paths to destination,
 * each item in source list must come from the
 * source path, returns 0 on success -1 on error */
int mfu_flist_hardlink(
    mfu_flist src_link_list,         /* IN - flist providing source items */
    const mfu_param_path* srcpath,   /* IN - the source patht */
    const mfu_param_path* destpath,  /* IN - destination path */
    mfu_copy_opts_t* mfu_copy_opts,  /* IN - options to be used during copy */
    mfu_file_t* mfu_src_file,        /* IN - I/O filesystem functions for src */
    mfu_file_t* mfu_dst_file         /* IN - I/O filesystem functions for dst */
);

/* fill files in list with data
 * returns 0 on success -1 on error */
int mfu_flist_fill(
    mfu_flist list,                 /* IN - flist providing items */
    mfu_copy_opts_t* mfu_copy_opts, /* IN - options to be used during fill */
    mfu_file_t* mfu_file            /* IN - I/O filesystem functions */
);

/* allocate a new mfu_walk_opts structure,
 * and set its fields with default values */
mfu_walk_opts_t* mfu_walk_opts_new(void);

/* free object allocated in mfu_walk_opts_new */
void mfu_walk_opts_delete(mfu_walk_opts_t** opts);

/* options to configure creation of directories and files */
typedef struct {
    bool overwrite;       /* whether to replace unlink existing items (non-directories) */
    bool set_owner;       /* whether to copy uid/gid from flist to item */
    bool set_timestamps;  /* whether to copy timestamps from flist to item */
    bool set_permissions; /* whether to copy permission bits from flist to item */
    mode_t umask;         /* umask to apply when setting permissions (default current umask) */
    bool lustre_stripe;   /* whether to apply lustre striping parameters */
    uint64_t lustre_stripe_minsize; /* min file size in bytes for which to stripe file */
    uint64_t lustre_stripe_width;   /* size of a single stripe in bytes */
    uint64_t lustre_stripe_count;   /* number of stripes */
} mfu_create_opts_t;

/* return a newly allocated create opts structure */
mfu_create_opts_t* mfu_create_opts_new(void);

/* free create options allocated from mfu_create_opts_new */
void mfu_create_opts_delete(mfu_create_opts_t** popts);

/* create all directories in flist */
void mfu_flist_mkdir(
    mfu_flist flist,
    mfu_create_opts_t* opts
);

/* create inodes for all regular files in flist, assumes directories exist */
void mfu_flist_mknod(
    mfu_flist flist,
    mfu_create_opts_t* opts
);

/* apply metadata updates to items in list */
void mfu_flist_metadata_apply(
    mfu_flist flist,
    mfu_create_opts_t* opts
);

/* unlink all items in flist,
 * if traceless=1, restore timestamps on parent directories after unlinking children */
void mfu_flist_unlink(mfu_flist flist, bool traceless, mfu_file_t* mfu_file);

typedef struct {
    uid_t getuid;   /* result from getuid */
    uid_t geteuid;  /* result from geteuid */
    uid_t uid;      /* new user id for item's owner, -1 for no change */
    gid_t gid;      /* new group id for item's group, -1 for no change  */
    mode_t umask;   /* umask to apply when setting item permissions */
    bool capchown;  /* whether process has CAP_CHOWN capability */
    bool capfowner; /* whether process has CAP_FOWNER capability */
    bool force;     /* always call chmod/chgrp on every item */
    bool silence;   /* avoid printing EPERM errors */
} mfu_chmod_opts_t;

/* return a newly allocated chmod structure */
mfu_chmod_opts_t* mfu_chmod_opts_new(void);

/* free chmod options allocated from mfu_chmod_opts_new */
void mfu_chmod_opts_delete(mfu_chmod_opts_t** popts);

/* given an input flist,
 * change owner on items if usrname != NULL,
 * change group on items if grname != NULL
 * set permissions on items according to perms list if head != NULL */
void mfu_flist_chmod(
  mfu_flist flist,
  const char* usrname,
  const char* grname,
  const mfu_perms* head,
  mfu_chmod_opts_t* opts
);

/* given a source and destination file, update destination metadata
 * to match source if needed, returns 0 on success -1 on error */
int mfu_flist_file_sync_meta(mfu_flist src_list, uint64_t src_index,
                             mfu_flist dst_list, uint64_t dst_index,
                             mfu_file_t* mfu_file);

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

/* return number of items in chunk list */
uint64_t mfu_file_chunk_list_size(const mfu_file_chunk* list);

/* given an flist, a file chunk list generated from that flist,
 * and an input array of flags with one element per chunk,
 * execute a LOR per item in the flist, and return the result
 * to the process owning that item in the flist */
void mfu_file_chunk_list_lor(
    mfu_flist list,             /* IN  - input flist */
    const mfu_file_chunk* head, /* IN  - chunk list generated from flist */
    const int* vals,            /* IN  - array of flags, one element for each chunk in the chunk list */
    int* results                /* OUT - array of output, storing logical OR across all chunks for each item in flist */
);

/****************************************
 * Functions to read/write list to file or print to screen
 ****************************************/

typedef struct {
    char*   dest_path;
    bool    sync_on_close;
    bool    preserve_owner;
    bool    preserve_times;
    bool    preserve_permissions;
    bool    preserve_xattrs;
    bool    preserve_acls;
    bool    preserve_fflags;
    bool    preserve;
    int     flags;
    size_t  chunk_size;
    size_t  buf_size;
    size_t  mem_size;
    size_t  header_size;
    int     create_libcircle;
    int     extract_libarchive;
} mfu_archive_opts_t;

/* return a newly allocated archive_opts structure, set default values on its fields */
mfu_archive_opts_t* mfu_archive_opts_new(void);

/* free archive opts structure allocated with mfu_archive_opts_new */
void mfu_archive_opts_delete(mfu_archive_opts_t** popts);

/* check that source paths exist and that parent directory for destination
 * is writable, sets dest_path field in opts, must be called before calling
 * mfu_flist_archive_create */
void mfu_param_path_check_archive(
    int numparams,             /* number of parameter paths in srcparams list */
    mfu_param_path* srcparams, /* list of source paths to be included in archived */
    mfu_param_path destparam,  /* parameter path for archive name */
    mfu_archive_opts_t* opts,  /* archive options, call set dest_path field */
    int* valid                 /* valid = 1 if all paths check out, 0 otherwise */
);

/* write items in file list to tar archive */
int mfu_flist_archive_create(
    mfu_flist flist,               /* list of items to be written to archive */
    const char* filename,          /* name of target archive file */
    int numpaths,                  /* number of source paths */
    const mfu_param_path* paths,   /* list of source paths */
    const mfu_param_path* cwdpath, /* current working directory used to construct relative path to each item in flist */
    mfu_archive_opts_t* opts       /* options to configure archive operation */
);

/* extract named archive file to disk into the given current working directory */
int mfu_flist_archive_extract(
    const char* filename,          /* name of archive file to be extracted */
    const mfu_param_path* cwdpath, /* current working dir used to construct absolute path of each item */
    mfu_archive_opts_t* opts       /* options to configure archive extraction operation */
);

#endif /* MFU_FLIST_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
