#ifndef STRMAP_H
#define STRMAP_H

/* Stores a set of key/value pairs, where key and value are both
 * stored as strings. */

#include <stdarg.h>
#include <sys/types.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define STRMAP_SUCCESS (0)

/*
=========================================
Define AVL tree data structures
=========================================
*/

/* Even though the structure is defined here,
 * consider these types to be opaque and only
 * use functions in this file to modify them. */

/* define the structure for an element of a hash */
typedef struct strmap_node_struct {
  const char* key;   /* pointer to key string */
  size_t key_len;    /* number of characters in key string (including terminating NUL) */
  const char* value; /* pointer to value string */
  size_t value_len;  /* number of characters in value string (including terminating NUL) */
  int    height;     /* max height of subtree rooted at this node */
  struct strmap_node_struct* parent; /* pointer to parent node */
  struct strmap_node_struct* left;   /* pointer to left child */
  struct strmap_node_struct* right;  /* pointer to right child */
} strmap_node;

/* structure to track root of a tree */
typedef struct strmap_struct {
  strmap_node* root; /* pointer to the root node in the tree */
  size_t len;        /* sum of characters in all key/value strings (including terminating NULs) */
  uint64_t size;     /* number of nodes in the tree */
} strmap;

/*
=========================================
Allocate and delete map objects
=========================================
*/

/* allocates a new map */
strmap* strmap_new(void);

/* copies entries from src into dst */
void strmap_merge(strmap* dst, const strmap* src);

/* frees a map */
void strmap_delete(strmap** map);

/*
=========================================
iterate over key/value pairs
=========================================
*/

/* return first node in map */
const strmap_node* strmap_node_first(const strmap* map);

/* return last node in map */
const strmap_node* strmap_node_last(const strmap* map);

/* get the previous node in map */
const strmap_node* strmap_node_previous(const strmap_node* node);

/* the next node in map */
const strmap_node* strmap_node_next(const strmap_node* node);

/* returns pointer to key string */
const char* strmap_node_key(const strmap_node* node);

/* returns pointer to value string */
const char* strmap_node_value(const strmap_node* node);

/*
=========================================
set, get, and unset key/value pairs
=========================================
*/

/* return number of key/value pairs in map */
uint64_t strmap_size(const strmap* map);

/* insert key/value into map, overwrites existing key */
int strmap_set(strmap* map, const char* key, const char* value);

/* insert key/value into map as "key=value" with printf formatting,
 * overwrites existing key */
int strmap_setf(strmap* map, const char* format, ...);

/* returns pointer to value string if found, NULL otherwise */
const char* strmap_get(const strmap* map, const char* key);

/* returns pointer to value string if found, NULL otherwise,
 * key can use printf formatting */
const char* strmap_getf(strmap* map, const char* format, ...);

/* deletes specified key */
int strmap_unset(strmap* map, const char* key);

/* deletes specified key using printf formatting */
int strmap_unsetf(strmap* map, const char* format, ...);

#define strmap_foreach(strmap, node)       \
  for ((node) = strmap_node_first(strmap); \
    (node) != NULL;                        \
    (node) = strmap_node_next(node))

/*
=========================================
pack and unpack data structure into array of bytes
=========================================
*/

#if 0
/* returns number of bytes needed to pack map */
size_t strmap_pack_size(const strmap* map);

/* pack map into buffer starting at specified memory location */
size_t strmap_pack(void* buf, const strmap* map);

/* unpack map stored at buf into tree, returns number of bytes read */
size_t strmap_unpack(const void* buf, strmap* map);

/* print map to stdout for debugging */
void strmap_print(const strmap* map);
#endif

#ifdef __cplusplus
}
#endif
#endif /* STRMAP_H */
