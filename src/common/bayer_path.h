#ifndef BAYER_PATH_H
#define BAYER_PATH_H

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

struct bayer_path_elem_struct;

/* define the structure for a path element */
typedef struct bayer_path_elem_struct {
  char* component; /* pointer to strdup'd component string */
  size_t chars;    /* number of chars in component */
  struct bayer_path_elem_struct* next; /* pointer to next element */
  struct bayer_path_elem_struct* prev; /* pointer to previous element */
} bayer_path_elem;

/* define the structure for a path object */
typedef struct {
  int components;      /* number of components in path */
  size_t chars;        /* number of chars in path */
  bayer_path_elem* head; /* pointer to first element */
  bayer_path_elem* tail; /* pointer to last element */
} bayer_path;

/*
=========================================
Allocate and delete path objects
=========================================
*/

/* allocates a new path */
bayer_path* bayer_path_new(void);

/* allocates a path from string */
bayer_path* bayer_path_from_str(const char* str);

/* allocates a path from formatted string */
bayer_path* bayer_path_from_strf(const char* format, ...);

/* allocates and returns a copy of path */
bayer_path* bayer_path_dup(const bayer_path* path);

/* frees a path and sets path pointer to NULL */
int bayer_path_delete(bayer_path** ptr_path);

/*
=========================================
get size and string functions
=========================================
*/

/* returns 1 if path has 0 components, 0 otherwise */
int bayer_path_is_null(const bayer_path* path);

/* return number of components in path */
int bayer_path_components(const bayer_path* path);

/* return number of characters needed to store path
 * (excludes terminating NUL) */
size_t bayer_path_strlen(const bayer_path* path);

/* copy string into user buffer, abort if buffer is too small,
 * return number of bytes written */
size_t bayer_path_strcpy(char* buf, size_t n, const bayer_path* path);

/* allocate memory and return path in string form,
 * caller is responsible for freeing string with bayer_free() */
char* bayer_path_strdup(const bayer_path* path);

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
int bayer_path_insert(bayer_path* path1, int offset, const bayer_path* ptr_path2);

/* prepends path2 to path1 */
int bayer_path_prepend(bayer_path* path1, const bayer_path* ptr_path2);

/* appends path2 to path1 */
int bayer_path_append(bayer_path* path1, const bayer_path* ptr_path2);

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int bayer_path_insert_str(bayer_path* path, int offset, const char* str);

/* prepends components in string to path */
int bayer_path_prepend_str(bayer_path* path, const char* str);

/* appends components in string to path */
int bayer_path_append_str(bayer_path* path, const char* str);

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int bayer_path_insert_strf(bayer_path* path, int offset, const char* format, ...);

/* prepends components in string to path */
int bayer_path_prepend_strf(bayer_path* path, const char* format, ...);

/* adds new components to end of path using printf-like formatting */
int bayer_path_append_strf(bayer_path* path, const char* format, ...);

/*
=========================================
cut, slice, and subpath functions
=========================================
*/

/* keeps upto length components of path starting at specified location
 * and discards the rest, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
int bayer_path_slice(bayer_path* path, int offset, int length);

/* drops last component from path */
int bayer_path_dirname(bayer_path* path);

/* only leaves last component of path */
int bayer_path_basename(bayer_path* path);

/* copies upto length components of path starting at specified location
 * and returns subpath as new path, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
bayer_path* bayer_path_sub(bayer_path* path, int offset, int length);

/* chops path at specified location and returns remainder as new path,
 * offset can be negative to count from back of path */
bayer_path* bayer_path_cut(bayer_path* path, int offset);

/*
=========================================
simplify and resolve functions
=========================================
*/

/* removes consecutive '/', '.', '..', and trailing '/' */
int bayer_path_reduce(bayer_path* path);

/* creates path from string, calls reduce, calls path_strdup,
 * and deletes path, caller must free returned string with bayer_free */
char* bayer_path_strdup_reduce_str(const char* str);

/* same as above, but prepend curr working dir if path not absolute */
char* bayer_path_strdup_abs_reduce_str(const char* str);

/* return 1 if path starts with an empty string, 0 otherwise */
int bayer_path_is_absolute(const bayer_path* path);

/* return 1 if child is contained in tree starting at parent, 0 otherwise */
int bayer_path_is_child(const bayer_path* parent, const bayer_path* child);

/* compute and return relative path from src to dst */
bayer_path* bayer_path_relative(const bayer_path* src, const bayer_path* dst);

#endif /* BAYER_PATH_H */
