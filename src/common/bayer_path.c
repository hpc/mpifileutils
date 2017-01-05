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

/* Defines a double linked list representing a file path. */

#include "bayer.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include <stdint.h>

/*
=========================================
Private functions
=========================================
*/

static inline int bayer_path_elem_init(bayer_path_elem* elem)
{
    elem->component = NULL;
    elem->chars     = 0;
    elem->next      = NULL;
    elem->prev      = NULL;
    return BAYER_SUCCESS;
}

static inline int bayer_path_init(bayer_path* path)
{
    path->components = 0;
    path->chars      = 0;
    path->head       = NULL;
    path->tail       = NULL;
    return BAYER_SUCCESS;
}

/* allocate and initialize a new path element */
static bayer_path_elem* bayer_path_elem_alloc(void)
{
    bayer_path_elem* elem = (bayer_path_elem*) malloc(sizeof(bayer_path_elem));
    if (elem != NULL) {
        bayer_path_elem_init(elem);
    }
    else {
        BAYER_ABORT(-1, "Failed to allocate memory for path element");
    }
    return elem;
}

/* free a path element */
static int bayer_path_elem_free(bayer_path_elem** ptr_elem)
{
    if (ptr_elem != NULL) {
        /* got an address to the pointer of an element,
         * dereference to get pointer to elem */
        bayer_path_elem* elem = *ptr_elem;
        if (elem != NULL) {
            /* free the component which was strdup'ed */
            bayer_free(&(elem->component));
        }
    }

    /* free the element structure itself */
    bayer_free(ptr_elem);

    return BAYER_SUCCESS;
}

/* allocate a new path */
static bayer_path* bayer_path_alloc(void)
{
    bayer_path* path = (bayer_path*) malloc(sizeof(bayer_path));
    if (path != NULL) {
        bayer_path_init(path);
    }
    else {
        BAYER_ABORT(-1, "Failed to allocate memory for path object");
    }
    return path;
}

/* allocate and return a duplicate of specified elememnt,
 * only copies value not next and previoud pointers */
static bayer_path_elem* bayer_path_elem_dup(const bayer_path_elem* elem)
{
    /* check that element is not NULL */
    if (elem == NULL) {
        return NULL;
    }

    /* allocate new element */
    bayer_path_elem* dup_elem = bayer_path_elem_alloc();
    if (dup_elem == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path element");
    }

    /* set component and chars fields (next and prev will be set later) */
    dup_elem->component = strdup(elem->component);
    dup_elem->chars     = elem->chars;

    return dup_elem;
}

/* return element at specified offset in path
 *   0   - points to first element
 *   N-1 - points to last element */
static bayer_path_elem* bayer_path_elem_index(const bayer_path* path, int idx)
{
    /* check that we got a path */
    if (path == NULL) {
        BAYER_ABORT(-1, "Assert that path are not NULL");
    }

    /* check that index is in range */
    if (idx < 0 || idx >= path->components) {
        BAYER_ABORT(-1, "Offset %d is out of range [0,%d)",
                    idx, path->components
                   );
    }

    /* scan until we find element at specified index */
    bayer_path_elem* current = NULL;
    if (path->components > 0) {
        int i;
        int from_head = idx;
        int from_tail = path->components - idx - 1;
        if (from_head <= from_tail) {
            /* shorter to start at head and go forwards */
            current = path->head;
            for (i = 0; i < from_head; i++) {
                current = current->next;
            }
        }
        else {
            /* shorter to start at tail and go backwards */
            current = path->tail;
            for (i = 0; i < from_tail; i++) {
                current = current->prev;
            }
        }
    }

    return current;
}

/* insert element at specified offset in path
 *   0   - before first element
 *   N-1 - before last element
 *   N   - after last element */
static int bayer_path_elem_insert(bayer_path* path, int offset, bayer_path_elem* elem)
{
    /* check that we got a path and element */
    if (path == NULL || elem == NULL) {
        BAYER_ABORT(-1, "Assert that path and elem are not NULL");
    }

    /* check that offset is in range */
    if (offset < 0 || offset > path->components) {
        BAYER_ABORT(-1, "Offset %d is out of range",
                    offset, path->components
                   );
    }

    /* if offset equals number of components, insert after last element */
    if (offset == path->components) {
        /* attach to path */
        path->components++;
        path->chars += elem->chars;

        /* get pointer to tail element and point to element as new tail */
        bayer_path_elem* tail = path->tail;
        path->tail = elem;

        /* tie element to tail */
        elem->prev = tail;
        elem->next = NULL;

        /* fix up old tail element */
        if (tail != NULL) {
            /* tie last element to new element */
            tail->next = elem;
        }
        else {
            /* if tail is NULL, this is the only element in path, so set head */
            path->head = elem;
        }

        return BAYER_SUCCESS;
    }

    /* otherwise, insert element before current element */

    /* lookup element at specified offset */
    bayer_path_elem* current = bayer_path_elem_index(path, offset);

    /* attach to path */
    path->components++;
    path->chars += elem->chars;

    /* insert element before current */
    if (current != NULL) {
        /* get pointer to element before current */
        bayer_path_elem* prev = current->prev;
        elem->prev = prev;
        elem->next = current;
        if (prev != NULL) {
            /* tie previous element to new element */
            prev->next = elem;
        }
        else {
            /* if prev is NULL, this element is the new head of the path */
            path->head = elem;
        }
        current->prev = elem;
    }
    else {
        /* if current is NULL, this is the only element in the path */
        path->head = elem;
        path->tail = elem;
        elem->prev = NULL;
        elem->next = NULL;
    }

    return BAYER_SUCCESS;
}

/* extract specified element from path */
static int bayer_path_elem_extract(bayer_path* path, bayer_path_elem* elem)
{
    /* check that we got a path and element */
    if (path == NULL || elem == NULL) {
        /* nothing to do in this case */
        BAYER_ABORT(-1, "Assert that path and elem are not NULL");
    }

    /* TODO: would be nice to verify that elem is part of path */

    /* subtract component and number of chars from path */
    path->components--;
    path->chars -= elem->chars;

    /* lookup address of elements of next and previous items */
    bayer_path_elem* prev = elem->prev;
    bayer_path_elem* next = elem->next;

    /* fix up element that comes before */
    if (prev != NULL) {
        /* there's an item before this one, tie it to next item */
        prev->next = next;
    }
    else {
        /* we're the first item, point head to next item */
        path->head = next;
    }

    /* fix up element that comes after */
    if (next != NULL) {
        /* there's an item after this one, tie it to previous item */
        next->prev = prev;
    }
    else {
        /* we're the last item, point tail to previous item */
        path->tail = prev;
    }

    return BAYER_SUCCESS;
}

/* allocates and returns a string filled in with formatted text,
 * assumes that caller has called va_start before and will call va_end
 * after */
static char* bayer_path_alloc_strf(const char* format, va_list args1, va_list args2)
{
    /* get length of component string */
    size_t chars = (size_t) vsnprintf(NULL, 0, format, args1);

    /* allocate space to hold string, add one for the terminating NUL */
    size_t len = chars + 1;
    char* str = (char*) malloc(len);
    if (str == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* copy formatted string into new memory */
    vsnprintf(str, len, format, args2);

    /* return string */
    return str;
}

/*
=========================================
Allocate and delete path objects
=========================================
*/

/* allocate a new path */
bayer_path* bayer_path_new()
{
    bayer_path* path = bayer_path_alloc();
    if (path == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path object");
    }
    return path;
}

/* allocates a path from string */
bayer_path* bayer_path_from_str(const char* str)
{
    /* allocate a path object */
    bayer_path* path = bayer_path_alloc();
    if (path == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path object");
    }

    /* check that str is not NULL */
    if (str != NULL) {
        /* iterate through components of string */
        const char* start = str;
        const char* end   = str;
        while (1) {
            /* scan end until we stop on a '/' or '\0' character */
            while (*end != '/' && *end != '\0') {
                end++;
            }

            /* compute number of bytes to copy this component
             * (including terminating NULL) */
            size_t buflen = (size_t)(end - start + 1);
            char* buf = (char*) malloc(buflen);
            if (buf == NULL) {
                BAYER_ABORT(-1, "Failed to allocate memory for component string");
            }

            /* copy characters into string buffer and add terminating NUL */
            size_t chars = buflen - 1;
            if (chars > 0) {
                strncpy(buf, start, chars);
            }
            buf[chars] = '\0';

            /* allocate new element */
            bayer_path_elem* elem = bayer_path_elem_alloc();
            if (elem == NULL) {
                BAYER_ABORT(-1, "Failed to allocate memory for path component");
            }

            /* record string in element */
            elem->component = buf;
            elem->chars     = chars;

            /* add element to path */
            bayer_path_elem_insert(path, path->components, elem);

            if (*end != '\0') {
                /* advance to next character */
                end++;
                start = end;
            }
            else {
                /* stop, we've hit the end of the input string */
                break;
            }
        }
    }

    return path;
}

/* allocates a path from formatted string */
bayer_path* bayer_path_from_strf(const char* format, ...)
{
    /* allocate formatted string */
    va_list args1, args2;
    va_start(args1, format);
    va_start(args2, format);
    char* str = bayer_path_alloc_strf(format, args1, args2);
    va_end(args2);
    va_end(args1);
    if (str == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* create path from string */
    bayer_path* path = bayer_path_from_str(str);

    /* free the string */
    bayer_free(&str);

    return path;
}

/* duplicate a path */
bayer_path* bayer_path_dup(const bayer_path* path)
{
    /* easy if path is NULL */
    if (path == NULL) {
        return NULL;
    }

    /* allocate a new path */
    bayer_path* dup_path = bayer_path_new();
    if (dup_path == NULL) {
        BAYER_ABORT(-1, "Failed to allocate path object");
    }

    /* get pointer to first element and delete elements in list */
    bayer_path_elem* current = path->head;
    while (current != NULL) {
        /* get pointer to element after current, delete current,
         * and set current to next */
        bayer_path_elem* dup_elem = bayer_path_elem_dup(current);
        if (dup_elem == NULL) {
            BAYER_ABORT(-1, "Failed to allocate path element object");
        }

        /* insert new element at end of path */
        bayer_path_elem_insert(dup_path, dup_path->components, dup_elem);

        /* advance to next element */
        current = current->next;
    }

    return dup_path;
}

/* free a path */
int bayer_path_delete(bayer_path** ptr_path)
{
    if (ptr_path != NULL) {
        /* got an address to the pointer of a path object,
         * dereference to get pointer to path */
        bayer_path* path = *ptr_path;
        if (path != NULL) {
            /* get pointer to first element and delete elements in list */
            bayer_path_elem* current = path->head;
            while (current != NULL) {
                /* get pointer to element after current, delete current,
                 * and set current to next */
                bayer_path_elem* next = current->next;
                bayer_path_elem_free(&current);
                current = next;
            }
        }
    }

    /* free the path object itself */
    bayer_free(ptr_path);

    return BAYER_SUCCESS;
}

/*
=========================================
get size and string functions
=========================================
*/

/* returns 1 if path has 0 components, 0 otherwise */
int bayer_path_is_null(const bayer_path* path)
{
    if (path != NULL) {
        int components = path->components;
        if (components > 0) {
            return 0;
        }
    }
    return 1;
}

/* return number of components in path */
int bayer_path_components(const bayer_path* path)
{
    if (path != NULL) {
        int components = path->components;
        return components;
    }
    return 0;
}

/* return number of characters needed to store path
 * (not including terminating NUL) */
size_t bayer_path_strlen(const bayer_path* path)
{
    if (path != NULL) {
        int components = path->components;
        if (components > 0) {
            /* special case for root directory, we want to print "/"
             * not the empty string */
            bayer_path_elem* head = path->head;
            if (components == 1 && strcmp(head->component, "") == 0) {
                /* if we only one component and it is the empty string,
                 * print this as the root directory */
                return 1;
            }

            /* otherwise, need a '/' between components so include
             * this in our count */
            size_t slashes = (size_t)(components - 1);
            size_t chars   = path->chars;
            size_t len     = slashes + chars;
            return len;
        }
    }
    return 0;
}

/* copies path into buf, caller must ensure buf is large enough */
static int bayer_path_strcpy_internal(char* buf, const bayer_path* path)
{
    /* special case for root directory,
     * we want to print "/" not the empty string */
    int components = path->components;
    if (components == 1 && strcmp(path->head->component, "") == 0) {
        /* got the root directory, just print "/" */
        strcpy(buf, "/");
        return BAYER_SUCCESS;
    }

    /* copy contents into string buffer */
    char* ptr = buf;
    bayer_path_elem* current = path->head;
    while (current != NULL) {
        /* copy component to buffer */
        char* component = current->component;
        size_t chars    = current->chars;
        memcpy((void*)ptr, (void*)component, chars);
        ptr += chars;

        /* if there is another component, add a slash */
        bayer_path_elem* next = current->next;
        if (next != NULL) {
            *ptr = '/';
            ptr++;
        }

        /* move to next component */
        current = next;
    }

    /* terminate the string */
    *ptr = '\0';

    return BAYER_SUCCESS;
}

/* copy string into user buffer, abort if buffer is too small */
size_t bayer_path_strcpy(char* buf, size_t n, const bayer_path* path)
{
    /* check that we have a pointer to a path */
    if (path == NULL) {
        BAYER_ABORT(-1, "Cannot copy NULL pointer to string");
    }

    /* we can't copy a NULL path */
    if (bayer_path_is_null(path)) {
        BAYER_ABORT(-1, "Cannot copy a NULL path to string");
    }

    /* get length of path */
    size_t len = bayer_path_strlen(path) + 1;

    /* if user buffer is too small, abort */
    if (n < len) {
        BAYER_ABORT(-1, "User buffer of %d bytes is too small to hold string of %d bytes",
                    n, len, __LINE__
                   );
    }

    /* copy contents into string buffer */
    bayer_path_strcpy_internal(buf, path);

    /* return number of bytes we copied to buffer */
    return len;
}

/* allocate memory and return path in string form */
char* bayer_path_strdup(const bayer_path* path)
{
    /* if we have no pointer to a path object return NULL */
    if (path == NULL) {
        return NULL;
    }

    /* if we have no components return NULL */
    if (path->components <= 0) {
        return NULL;
    }

    /* compute number of bytes we need to allocate and allocate string */
    size_t buflen = bayer_path_strlen(path) + 1;
    char* buf = (char*) malloc(buflen);
    if (buf == NULL) {
        BAYER_ABORT(-1, "Failed to allocate buffer for path");
    }

    /* copy contents into string buffer */
    bayer_path_strcpy_internal(buf, path);

    /* return new string to caller */
    return buf;
}

/*
=========================================
insert, append, prepend functions
=========================================
*/

/* integrates path2 so head element in path2 starts at specified offset
 * in path1 and deletes path2, e.g.,
 *   0   - before first element
 *   N-1 - before last element
 *   N   - after last element */
static int bayer_path_combine(bayer_path* path1, int offset, bayer_path** ptr_path2)
{
    if (path1 != NULL) {
        /* check that offset is in range */
        int components = path1->components;
        if (offset < 0 || offset > components) {
            BAYER_ABORT(-1, "Offset %d is out of range [0,%d]",
                        offset, components
                       );
        }

        if (ptr_path2 != NULL) {
            /* got an address to the pointer of a path object,
             * dereference to get pointer to path */
            bayer_path* path2 = *ptr_path2;
            if (path2 != NULL) {
                /* get pointer to head and tail of path2 */
                bayer_path_elem* head2 = path2->head;
                bayer_path_elem* tail2 = path2->tail;

                /* if offset equals number of components, insert after last element,
                 * otherwise, insert element before specified element */
                if (offset == components) {
                    /* get pointer to tail of path1 */
                    bayer_path_elem* tail1 = path1->tail;
                    if (tail1 != NULL) {
                        /* join tail of path1 to head of path2 */
                        tail1->next = head2;
                    }
                    else {
                        /* path1 has no items, set head to head of path2 */
                        path1->head = head2;
                    }

                    /* if path2 has a head element, tie it to tail of path1 */
                    if (head2 != NULL) {
                        head2->prev = tail1;
                    }

                    /* set new tail of path1 */
                    path1->tail = tail2;
                }
                else {
                    /* lookup element at specified offset */
                    bayer_path_elem* current = bayer_path_elem_index(path1, offset);

                    /* get pointer to element before current */
                    bayer_path_elem* prev = current->prev;

                    /* tie previous element to head of path2 */
                    if (prev != NULL) {
                        /* tie previous element to new element */
                        prev->next = head2;
                    }
                    else {
                        /* if prev is NULL, head of path2 will be new head of path1 */
                        path1->head = head2;
                    }

                    /* tie current to tail of path2 */
                    current->prev = tail2;

                    /* tie head of path2 to previous */
                    if (head2 != NULL) {
                        head2->prev = prev;
                    }

                    /* tie tail of path2 to current */
                    if (tail2 != NULL) {
                        tail2->next = current;
                    }
                }

                /* add component and character counts to first path */
                path1->components += path2->components;
                path1->chars      += path2->chars;
            }
        }

        /* free the path2 struct */
        bayer_free(ptr_path2);
    }
    else {
        BAYER_ABORT(-1, "Cannot attach a path to a NULL path");
    }

    return BAYER_SUCCESS;
}

/* inserts path2 so head element in path2 starts at specified offset
 * in path1, e.g.,
 *   0   - before first element of path1
 *   N-1 - before last element of path1
 *   N   - after last element of path1 */
int bayer_path_insert(bayer_path* path1, int offset, const bayer_path* path2)
{
    int rc = BAYER_SUCCESS;
    if (path1 != NULL) {
        /* make a copy of path2, and combint at specified offset in path1,
         * combine deletes copy of path2 */
        bayer_path* path2_copy = bayer_path_dup(path2);
        rc = bayer_path_combine(path1, offset, &path2_copy);
    }
    else {
        BAYER_ABORT(-1, "Cannot attach a path to a NULL path");
    }
    return rc;
}

/* prepends path2 to path1 */
int bayer_path_prepend(bayer_path* path1, const bayer_path* path2)
{
    int rc = bayer_path_insert(path1, 0, path2);
    return rc;
}

/* appends path2 to path1 */
int bayer_path_append(bayer_path* path1, const bayer_path* path2)
{
    int rc = BAYER_SUCCESS;
    if (path1 != NULL) {
        rc = bayer_path_insert(path1, path1->components, path2);
    }
    else {
        BAYER_ABORT(-1, "Cannot attach a path to a NULL path");
    }
    return rc;
}

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int bayer_path_insert_str(bayer_path* path, int offset, const char* str)
{
    /* verify that we got a path as input */
    if (path == NULL) {
        BAYER_ABORT(-1, "Cannot insert string to a NULL path");
    }

    /* create a path from this string */
    bayer_path* newpath = bayer_path_from_str(str);
    if (newpath == NULL) {
        BAYER_ABORT(-1, "Failed to allocate path for insertion");
    }

    /* attach newpath to original path */
    int rc = bayer_path_combine(path, offset, &newpath);
    return rc;
}

/* prepends components in string to path */
int bayer_path_prepend_str(bayer_path* path, const char* str)
{
    int rc = bayer_path_insert_str(path, 0, str);
    return rc;
}

/* appends components in string to path */
int bayer_path_append_str(bayer_path* path, const char* str)
{
    int rc = BAYER_SUCCESS;
    if (path != NULL) {
        rc = bayer_path_insert_str(path, path->components, str);
    }
    else {
        BAYER_ABORT(-1, "Cannot attach string to a NULL path");
    }
    return rc;
}

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int bayer_path_insert_strf(bayer_path* path, int offset, const char* format, ...)
{
    /* verify that we got a path as input */
    if (path == NULL) {
        BAYER_ABORT(-1, "Cannot append string to a NULL path");
    }

    /* allocate formatted string */
    va_list args1, args2;
    va_start(args1, format);
    va_start(args2, format);
    char* str = bayer_path_alloc_strf(format, args1, args2);
    va_end(args2);
    va_end(args1);
    if (str == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* attach str to path */
    int rc = bayer_path_insert_str(path, offset, str);

    /* free the string */
    bayer_free(&str);

    return rc;
}

/* prepends components in string to path */
int bayer_path_prepend_strf(bayer_path* path, const char* format, ...)
{
    /* verify that we got a path as input */
    if (path == NULL) {
        BAYER_ABORT(-1, "Cannot append string to a NULL path");
    }

    /* allocate formatted string */
    va_list args1, args2;
    va_start(args1, format);
    va_start(args2, format);
    char* str = bayer_path_alloc_strf(format, args1, args2);
    va_end(args2);
    va_end(args1);
    if (str == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* attach str to path */
    int rc = bayer_path_insert_str(path, 0, str);

    /* free the string */
    bayer_free(&str);

    return rc;
}

/* adds a new component to end of path using printf-like formatting */
int bayer_path_append_strf(bayer_path* path, const char* format, ...)
{
    /* verify that we got a path as input */
    if (path == NULL) {
        BAYER_ABORT(-1, "Cannot append string to a NULL path");
    }

    /* allocate formatted string */
    va_list args1, args2;
    va_start(args1, format);
    va_start(args2, format);
    char* str = bayer_path_alloc_strf(format, args1, args2);
    va_end(args2);
    va_end(args1);
    if (str == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* attach str to path */
    int rc = bayer_path_insert_str(path, path->components, str);

    /* free the string */
    bayer_free(&str);

    return rc;
}

/*
=========================================
cut, slice, and subpath functions
=========================================
*/

/* keeps upto length components of path starting at specified location
 * and discards the rest, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
int bayer_path_slice(bayer_path* path, int offset, int length)
{
    /* check that we have a path */
    if (path == NULL) {
        return BAYER_SUCCESS;
    }

    /* force offset into range */
    int components = path->components;
    if (components > 0) {
        while (offset < 0) {
            offset += components;
        }
        while (offset >= components) {
            offset -= components;
        }
    }
    else {
        /* nothing left to slice */
        return BAYER_SUCCESS;
    }

    /* lookup first element to be head of new path */
    bayer_path_elem* current = bayer_path_elem_index(path, offset);

    /* delete any items before this one */
    bayer_path_elem* elem = current->prev;
    while (elem != NULL) {
        bayer_path_elem* prev = elem->prev;
        bayer_path_elem_free(&elem);
        elem = prev;
    }

    /* remember our starting element and intialize tail to NULL */
    bayer_path_elem* head = current;
    bayer_path_elem* tail = NULL;

    /* step through length elements or to the end of the list,
     * a negative length means we step until end of list */
    components = 0;
    size_t chars = 0;
    while ((length < 0 || length > 0) && current != NULL) {
        /* count number of components and characters in list and
         * update tail */
        components++;
        chars += current->chars;
        tail = current;

        /* advance to next element */
        current = current->next;
        if (length > 0) {
            length--;
        }
    }

    /* current now points to first element to be cut,
     * delete it and all trailing items */
    while (current != NULL) {
        bayer_path_elem* next = current->next;
        bayer_path_elem_free(&current);
        current = next;
    }

    /* set new path members */
    path->components = components;
    path->chars      = chars;
    if (components > 0) {
        /* we have some components, update head and tail, terminate the list */
        path->head = head;
        path->tail = tail;
        head->prev = NULL;
        tail->next = NULL;
    }
    else {
        /* otherwise, we have no items in the path */
        path->head = NULL;
        path->tail = NULL;
    }

    return BAYER_SUCCESS;
}

/* drops last component from path */
int bayer_path_dirname(bayer_path* path)
{
    int components = bayer_path_components(path);
    if (components > 0) {
        int rc = bayer_path_slice(path, 0, components - 1);
        return rc;
    }
    return BAYER_SUCCESS;
}

/* only leaves last component of path */
int bayer_path_basename(bayer_path* path)
{
    int rc = bayer_path_slice(path, -1, 1);
    return rc;
}

/* copies upto length components of path starting at specified location
 * and returns subpath as new path, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
bayer_path* bayer_path_sub(bayer_path* path, int offset, int length)
{
    /* check that we have a path */
    if (path == NULL) {
        return NULL;
    }

    /* force offset into range */
    int components = path->components;
    if (components > 0) {
        while (offset < 0) {
            offset += components;
        }
        while (offset >= components) {
            offset -= components;
        }
    }
    else {
        /* in this case, unless length == 0, we'll fail check below,
         * and if length == 0, we'll return an empty path */
        offset = 0;
    }

    /* allocate and initialize an empty path object */
    bayer_path* newpath = bayer_path_alloc();
    if (newpath == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path object");
    }

    /* return the empty path if source path is empty */
    if (components == 0) {
        return newpath;
    }

    /* lookup first element to be head of new path */
    bayer_path_elem* current = bayer_path_elem_index(path, offset);

    /* copy elements from path and attach to newpath */
    while ((length < 0 || length > 0) && current != NULL) {
        /* duplicate element */
        bayer_path_elem* elem = bayer_path_elem_dup(current);
        if (elem == NULL) {
            BAYER_ABORT(-1, "Failed to duplicate element of path object");
        }

        /* insert element into newpath */
        bayer_path_elem_insert(newpath, newpath->components, elem);

        /* advance to next element */
        current = current->next;
        if (length > 0) {
            length--;
        }
    }

    /* return our newly constructed path */
    return newpath;
}

/* chops path at specified location and returns remainder as new path,
 * offset can be negative to count from back */
bayer_path* bayer_path_cut(bayer_path* path, int offset)
{
    /* check that we have a path */
    if (path == NULL) {
        return NULL;
    }

    /* allocate and initialize an empty path object */
    bayer_path* newpath = bayer_path_alloc();
    if (newpath == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for path object");
    }

    /* if path is empty, return an empty path */
    int components = path->components;
    if (components == 0) {
        return newpath;
    }

    /* force offset into range */
    while (offset < 0) {
        offset += components;
    }
    while (offset >= components) {
        offset -= components;
    }

    /* lookup first element to be head of new path */
    bayer_path_elem* current = bayer_path_elem_index(path, offset);

    /* set head and tail of newpath2 */
    newpath->head = current;
    newpath->tail = path->tail;

    /* set tail (and head) of path */
    if (current != NULL) {
        /* get element before current to be new tail */
        bayer_path_elem* prev = current->prev;

        /* cut current from previous element */
        current->prev = NULL;

        if (prev != NULL) {
            /* cut previous element from current */
            prev->next = NULL;
        }
        else {
            /* if there is no element before current,
             * we cut the first element, so update head */
            path->head = NULL;
        }

        /* set previous element as new tail for path */
        path->tail = prev;
    }
    else {
        /* current is NULL, meaning path is empty */
        path->head = NULL;
        path->tail = NULL;
    }

    /* finally, cycle through newpath, subtract counts from path
     * and add to newpath */
    while (current != NULL) {
        /* subtract counts from path */
        path->components--;
        path->chars -= current->chars;

        /* add counts to newpath */
        newpath->components++;
        newpath->chars += current->chars;

        /* advance to next element */
        current = current->next;
    }

    /* return our newly constructed path */
    return newpath;
}

/*
=========================================
simplify and resolve functions
=========================================
*/

/* removes consecutive '/', '.', '..', and trailing '/' */
int bayer_path_reduce(bayer_path* path)
{
    /* check that we got a path */
    if (path == NULL) {
        /* nothing to do in this case */
        return BAYER_SUCCESS;
    }


    /* now iterate through and remove any "." and empty strings,
     * we go from back to front to handle paths like "./" */
    bayer_path_elem* current = path->tail;
    while (current != NULL) {
        /* get pointer to previous element */
        bayer_path_elem* prev = current->prev;

        /* check whether component string matches "." or "" */
        char* component = current->component;
        if (strcmp(component, ".") == 0) {
            /* pull element out of path and delete it */
            bayer_path_elem_extract(path, current);
            bayer_path_elem_free(&current);
        }
        else if (strcmp(component, "") == 0 && current != path->head) {
            /* head is allowed to be empty string so that we don't chop leading '/' */
            /* pull element out of path and delete it */
            bayer_path_elem_extract(path, current);
            bayer_path_elem_free(&current);
        }

        /* advance to previous item */
        current = prev;
    }

    /* now remove any ".." and any preceding component */
    current = path->head;
    while (current != NULL) {
        /* get pointer to previous and next elements */
        bayer_path_elem* prev = current->prev;
        bayer_path_elem* next = current->next;

        /* check whether component string matches ".." */
        char* component = current->component;
        if (strcmp(component, "..") == 0) {
            /* pull current and previous elements out of path and delete them */
            if (prev != NULL) {
                /* check that previous is not "..", since we go front to back,
                 * previous ".." shouldn't exist unless it couldn't be popped */
                char* prev_component = prev->component;
                if (strcmp(prev_component, "..") != 0) {
                    /* check that item is not empty, only empty strings left
                     * should be one at very beginning of string */
                    if (strcmp(prev_component, "") != 0) {
                        /* delete previous element */
                        bayer_path_elem_extract(path, prev);
                        bayer_path_elem_free(&prev);

                        /* delete current element */
                        bayer_path_elem_extract(path, current);
                        bayer_path_elem_free(&current);
                    }
                    else {
                        /* trying to pop past root directory, just drop the ".." */
                        //BAYER_ABORT(-1, "Cannot pop past root directory");
                        bayer_path_elem_extract(path, current);
                        bayer_path_elem_free(&current);
                    }
                }
                else {
                    /* previous is also "..", so keep going */
                }
            }
            else {
                /* we got some path like "../foo", leave it in this form */
            }
        }

        /* advance to next item */
        current = next;
    }

    return BAYER_SUCCESS;
}

/* creates path from string, calls reduce, calls path_strdup,
 * and deletes path, caller must free returned string with bayer_free */
char* bayer_path_strdup_reduce_str(const char* str)
{
    bayer_path* path = bayer_path_from_str(str);
    bayer_path_reduce(path);
    char* newstr = bayer_path_strdup(path);
    bayer_path_delete(&path);
    return newstr;
}

/* same as above, but prepend curr working dir if path not absolute */
char* bayer_path_strdup_abs_reduce_str(const char* str)
{
    bayer_path* path = bayer_path_from_str(str);
    if (! bayer_path_is_absolute(path)) {
        char cwd[PATH_MAX];
        bayer_getcwd(cwd, PATH_MAX);
        bayer_path_prepend_str(path, cwd);
    }
    bayer_path_reduce(path);
    char* newstr = bayer_path_strdup(path);
    bayer_path_delete(&path);
    return newstr;
}

/* return 1 if path starts with an empty string, 0 otherwise */
int bayer_path_is_absolute(const bayer_path* path)
{
    if (path != NULL) {
        if (path->components > 0) {
            const bayer_path_elem* head = path->head;
            const char* component = head->component;
            if (strcmp(component, "") == 0) {
                return 1;
            }
        }
    }
    return 0;
}

/* given a path, create a copy, prepernd curr working dir if path
 * not absolute, and reduce it */
bayer_path* bayer_path_abs_reduce(const bayer_path* path)
{
    bayer_path* newpath = bayer_path_dup(path);
    if (! bayer_path_is_absolute(newpath)) {
        char cwd[PATH_MAX];
        bayer_getcwd(cwd, PATH_MAX);
        bayer_path_prepend_str(newpath, cwd);
    }
    bayer_path_reduce(newpath);
    return newpath;
}

bayer_path_result bayer_path_cmp(const bayer_path* src, const bayer_path* dst)
{
    /* check that we got pointers to both src and dst */
    if (src == NULL || dst == NULL) {
        /* if one is NULL but not both, consider them to be different */
        if (src != NULL || dst != NULL) {
            return BAYER_PATH_DIFF;
        }

        /* both values are NULL, so consider them to be equal */
        return BAYER_PATH_EQUAL;
    }

    /* check that src and dst aren't NULL paths */
    int src_null = bayer_path_is_null(src);
    int dst_null = bayer_path_is_null(dst);
    if (src_null || dst_null) {
        /* if one is NULL but not both, consider them to be different */
        if ((! src_null) || (! dst_null)) {
            return BAYER_PATH_DIFF;
        }

        /* both values are NULL, so consider them to be equal */
        return BAYER_PATH_EQUAL;
    }

    /* force source and destination to absolute form and reduce them */
    bayer_path* abs_src = bayer_path_abs_reduce(src);
    bayer_path* abs_dst = bayer_path_abs_reduce(dst);

    /* get pointers to start of parent and child */
    bayer_path_result result = BAYER_PATH_EQUAL;
    bayer_path_elem* src_elem = abs_src->head;
    bayer_path_elem* dst_elem = abs_dst->head;
    while (src_elem != NULL && dst_elem != NULL) {
        /* compare strings for this element */
        const char* src_component = src_elem->component;
        const char* dst_component = dst_elem->component;
        if (strcmp(src_component, dst_component) != 0) {
            /* found a component in src that's not in dst */
            result = BAYER_PATH_DIFF;
            break;
        }

        /* advance to compare next element */
        src_elem = src_elem->next;
        dst_elem = dst_elem->next;
    }

    /* if everything is equal so far, but we've run out of components,
     * check to see if one is contained within the other */
    if (result == BAYER_PATH_EQUAL) {
        if (src_elem == NULL && dst_elem != NULL) {
            /* dst is contained within source */
            result = BAYER_PATH_DEST_CHILD;
        } else if (src_elem != NULL && dst_elem == NULL) {
            /* src is contained within dst */
            result = BAYER_PATH_SRC_CHILD;
        }
    }

    bayer_path_delete(&abs_src);
    bayer_path_delete(&abs_dst);

    return result;
}

/* compute and return relative path from src to dst */
bayer_path* bayer_path_relative(const bayer_path* src, const bayer_path* dst)
{
    /* check that we don't have NULL pointers */
    if (src == NULL || dst == NULL) {
        BAYER_ABORT(-1, "Either src or dst pointer is NULL");
    }

    /* we can't get to a NULL path from a non-NULL path */
    int src_components = src->components;
    int dst_components = dst->components;
    if (src_components > 0 && dst_components == 0) {
        BAYER_ABORT(-1, "Cannot get from non-NULL path to NULL path");
    }

    /* allocate a new path to record relative path */
    bayer_path* rel = bayer_path_new();
    if (rel == NULL) {
        BAYER_ABORT(-1, "Failed to allocate memory for relative path");
    }

    /* walk down both paths until we find the first location where they
     * differ */
    const bayer_path_elem* src_elem = src->head;
    const bayer_path_elem* dst_elem = dst->head;
    while (1) {
        /* check that we have valid src and dst elements */
        if (src_elem == NULL) {
            break;
        }
        if (dst_elem == NULL) {
            break;
        }

        /* check that the current component is the same */
        const char* src_component = src_elem->component;
        const char* dst_component = dst_elem->component;
        if (strcmp(src_component, dst_component) != 0) {
            break;
        }

        /* go to next component */
        src_elem = src_elem->next;
        dst_elem = dst_elem->next;
    }

    /* if there is anything left in source, we need to pop back */
    while (src_elem != NULL) {
        /* pop back one level, and go to next element */
        bayer_path_append_str(rel, "..");
        src_elem = src_elem->next;
    }

    /* now tack on any items left from dst */
    while (dst_elem != NULL) {
        const char* dst_component = dst_elem->component;
        bayer_path_append_str(rel, dst_component);
        dst_elem = dst_elem->next;
    }

    return rel;
}

/*
=========================================
I/O routines with paths
=========================================
*/

#if 0
/* tests whether the file or directory is readable */
int bayer_path_is_readable(const bayer_path* file)
{
    /* convert to string and delegate to I/O routine */
    char* file_str = bayer_path_strdup(file);
    int rc = bayer_file_is_readable(file_str);
    bayer_free(&file_str);
    return rc;
}

/* tests whether the file or directory is writeable */
int bayer_path_is_writeable(const bayer_path* file)
{
    /* convert to string and delegate to I/O routine */
    char* file_str = bayer_path_strdup(file);
    int rc = bayer_file_is_writable(file_str);
    bayer_free(&file_str);
    return rc;
}
#endif

#if 0
#ifndef HIDE_TV
/*
=========================================
Pretty print for TotalView debug window
=========================================
*/

/* This enables a nicer display when diving on a path variable
 * under the TotalView debugger.  It requires TV 8.8 or later. */

#include "tv_data_display.h"

static int TV_ttf_display_type(const bayer_path* path)
{
    if (path == NULL) {
        /* empty path, nothing to display here */
        return TV_ttf_format_ok;
    }

    if (bayer_path_is_null(path)) {
        /* empty path, nothing to display here */
        return TV_ttf_format_ok;
    }

    /* print path in string form */
    char* str = bayer_path_strdup(path);
    TV_ttf_add_row("path", TV_ttf_type_ascii_string, str);
    bayer_free(&str);

    return TV_ttf_format_ok;
}
#endif /* HIDE_TV */
#endif
