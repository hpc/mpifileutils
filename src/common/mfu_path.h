#ifndef MFU_PATH_H
#define MFU_PATH_H

#include <stdarg.h>
#include <sys/types.h>

/* TODO: for formatted strings, use special %| character (or something
 * similar) to denote directories in portable way */

/* Stores path as a linked list, breaking path at each directory marker
 * and terminating NUL.  Can append and insert paths or cut and slice
 * them.  Can initialize a path from a string and extract a path into
 * a string.  Path consists of a number of components indexed from 0.
 *
 * Examples:
 * * root directory "/" consists of a path with two components both of
 *   which are empty strings */

/*
=========================================
This file defines the data structure for a path,
which is an double-linked list of elements,
where each element contains a component (char string).
=========================================
*/

/*
=========================================
Define hash and element structures
=========================================
*/

struct mfu_path_elem_struct;

/* define the structure for a path element */
typedef struct mfu_path_elem_struct {
  char* component; /* pointer to strdup'd component string */
  size_t chars;    /* number of chars in component */
  struct mfu_path_elem_struct* next; /* pointer to next element */
  struct mfu_path_elem_struct* prev; /* pointer to previous element */
} mfu_path_elem;

/* define the structure for a path object */
typedef struct {
  int components;        /* number of components in path */
  size_t chars;          /* number of chars in path */
  mfu_path_elem* head; /* pointer to first element */
  mfu_path_elem* tail; /* pointer to last element */
} mfu_path;

/*
=========================================
Allocate and delete path objects
=========================================
*/

/* allocates a new path */
mfu_path* mfu_path_new(void);

/* allocates a path from string */
mfu_path* mfu_path_from_str(const char* str);

/* allocates a path from formatted string */
mfu_path* mfu_path_from_strf(const char* format, ...);

/* allocates and returns a copy of path */
mfu_path* mfu_path_dup(const mfu_path* path);

/* frees a path and sets path pointer to NULL */
int mfu_path_delete(mfu_path** ptr_path);

/*
=========================================
get size and string functions
=========================================
*/

/* returns 1 if path has 0 components, 0 otherwise */
int mfu_path_is_null(const mfu_path* path);

/* return number of components in path */
int mfu_path_components(const mfu_path* path);

/* return number of characters needed to store path
 * (excludes terminating NUL) */
size_t mfu_path_strlen(const mfu_path* path);

/* copy string into user buffer, abort if buffer is too small,
 * return number of bytes written */
size_t mfu_path_strcpy(char* buf, size_t n, const mfu_path* path);

/* allocate memory and return path in string form,
 * caller is responsible for freeing string with mfu_free() */
char* mfu_path_strdup(const mfu_path* path);

/*
=========================================
insert, append, prepend functions
=========================================
*/

/* inserts path2 so head element in path2 starts at specified offset
 * in path1, e.g.,
 *   0   - before first element of path1
 *   N-1 - before last element of path1
 *   N   - after last element of path1 */
int mfu_path_insert(mfu_path* path1, int offset, const mfu_path* ptr_path2);

/* prepends path2 to path1 */
int mfu_path_prepend(mfu_path* path1, const mfu_path* ptr_path2);

/* appends path2 to path1 */
int mfu_path_append(mfu_path* path1, const mfu_path* ptr_path2);

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int mfu_path_insert_str(mfu_path* path, int offset, const char* str);

/* prepends components in string to path */
int mfu_path_prepend_str(mfu_path* path, const char* str);

/* appends components in string to path */
int mfu_path_append_str(mfu_path* path, const char* str);

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int mfu_path_insert_strf(mfu_path* path, int offset, const char* format, ...);

/* prepends components in string to path */
int mfu_path_prepend_strf(mfu_path* path, const char* format, ...);

/* adds new components to end of path using printf-like formatting */
int mfu_path_append_strf(mfu_path* path, const char* format, ...);

/*
=========================================
cut, slice, and subpath functions
=========================================
*/

/* keeps upto length components of path starting at specified location
 * and discards the rest, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
int mfu_path_slice(mfu_path* path, int offset, int length);

/* drops last component from path */
int mfu_path_dirname(mfu_path* path);

/* only leaves last component of path */
int mfu_path_basename(mfu_path* path);

/* copies upto length components of path starting at specified location
 * and returns subpath as new path, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
mfu_path* mfu_path_sub(mfu_path* path, int offset, int length);

/* chops path at specified location and returns remainder as new path,
 * offset can be negative to count from back of path */
mfu_path* mfu_path_cut(mfu_path* path, int offset);

/*
=========================================
simplify and resolve functions
=========================================
*/

/* removes consecutive '/', '.', '..', and trailing '/' */
int mfu_path_reduce(mfu_path* path);

/* creates path from string, calls reduce, calls path_strdup,
 * and deletes path, caller must free returned string with mfu_free */
char* mfu_path_strdup_reduce_str(const char* str);

/* same as above, but prepend curr working dir if path not absolute */
char* mfu_path_strdup_abs_reduce_str(const char* str);

/* return 1 if path starts with an empty string, 0 otherwise */
int mfu_path_is_absolute(const mfu_path* path);

/* value returned by mfu_path_cmp */
typedef enum {
    MFU_PATH_EQUAL   = 0, /* src and dest paths are same */
    MFU_PATH_SRC_CHILD,   /* src is contained within dest */
    MFU_PATH_DEST_CHILD,  /* dest is contained within child */
    MFU_PATH_DIFF,        /* src and dest are different and one does not contain the other */
} mfu_path_result;

/* compare two paths and return one of above results */
mfu_path_result mfu_path_cmp(const mfu_path* src, const mfu_path* dest);

/* compute and return relative path from src to dst */
mfu_path* mfu_path_relative(const mfu_path* src, const mfu_path* dst);

#endif /* MFU_PATH_H */
