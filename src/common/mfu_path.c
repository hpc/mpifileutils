/* Defines a double linked list representing a file path. */

#include "mfu.h"

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

static inline int mfu_path_elem_init(mfu_path_elem* elem)
{
    elem->component = NULL;
    elem->chars     = 0;
    elem->next      = NULL;
    elem->prev      = NULL;
    return MFU_SUCCESS;
}

static inline int mfu_path_init(mfu_path* path)
{
    path->components = 0;
    path->chars      = 0;
    path->head       = NULL;
    path->tail       = NULL;
    return MFU_SUCCESS;
}

/* allocate and initialize a new path element */
static mfu_path_elem* mfu_path_elem_alloc(void)
{
    mfu_path_elem* elem = (mfu_path_elem*) malloc(sizeof(mfu_path_elem));
    if (elem != NULL) {
        mfu_path_elem_init(elem);
    }
    else {
        MFU_ABORT(-1, "Failed to allocate memory for path element");
    }
    return elem;
}

/* free a path element */
static int mfu_path_elem_free(mfu_path_elem** ptr_elem)
{
    if (ptr_elem != NULL) {
        /* got an address to the pointer of an element,
         * dereference to get pointer to elem */
        mfu_path_elem* elem = *ptr_elem;
        if (elem != NULL) {
            /* free the component which was strdup'ed */
            mfu_free(&(elem->component));
        }
    }

    /* free the element structure itself */
    mfu_free(ptr_elem);

    return MFU_SUCCESS;
}

/* allocate a new path */
static mfu_path* mfu_path_alloc(void)
{
    mfu_path* path = (mfu_path*) malloc(sizeof(mfu_path));
    if (path != NULL) {
        mfu_path_init(path);
    }
    else {
        MFU_ABORT(-1, "Failed to allocate memory for path object");
    }
    return path;
}

/* allocate and return a duplicate of specified elememnt,
 * only copies value not next and previoud pointers */
static mfu_path_elem* mfu_path_elem_dup(const mfu_path_elem* elem)
{
    /* check that element is not NULL */
    if (elem == NULL) {
        return NULL;
    }

    /* allocate new element */
    mfu_path_elem* dup_elem = mfu_path_elem_alloc();
    if (dup_elem == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path element");
    }

    /* set component and chars fields (next and prev will be set later) */
    dup_elem->component = strdup(elem->component);
    dup_elem->chars     = elem->chars;

    return dup_elem;
}

/* return element at specified offset in path
 *   0   - points to first element
 *   N-1 - points to last element */
static mfu_path_elem* mfu_path_elem_index(const mfu_path* path, int idx)
{
    /* check that we got a path */
    if (path == NULL) {
        MFU_ABORT(-1, "Assert that path are not NULL");
    }

    /* check that index is in range */
    if (idx < 0 || idx >= path->components) {
        MFU_ABORT(-1, "Offset %d is out of range [0,%d)",
                    idx, path->components
                   );
    }

    /* scan until we find element at specified index */
    mfu_path_elem* current = NULL;
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
static int mfu_path_elem_insert(mfu_path* path, int offset, mfu_path_elem* elem)
{
    /* check that we got a path and element */
    if (path == NULL || elem == NULL) {
        MFU_ABORT(-1, "Assert that path and elem are not NULL");
    }

    /* check that offset is in range */
    if (offset < 0 || offset > path->components) {
        MFU_ABORT(-1, "Offset %d is out of range",
                    offset, path->components
                   );
    }

    /* if offset equals number of components, insert after last element */
    if (offset == path->components) {
        /* attach to path */
        path->components++;
        path->chars += elem->chars;

        /* get pointer to tail element and point to element as new tail */
        mfu_path_elem* tail = path->tail;
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

        return MFU_SUCCESS;
    }

    /* otherwise, insert element before current element */

    /* lookup element at specified offset */
    mfu_path_elem* current = mfu_path_elem_index(path, offset);

    /* attach to path */
    path->components++;
    path->chars += elem->chars;

    /* insert element before current */
    if (current != NULL) {
        /* get pointer to element before current */
        mfu_path_elem* prev = current->prev;
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

    return MFU_SUCCESS;
}

/* extract specified element from path */
static int mfu_path_elem_extract(mfu_path* path, mfu_path_elem* elem)
{
    /* check that we got a path and element */
    if (path == NULL || elem == NULL) {
        /* nothing to do in this case */
        MFU_ABORT(-1, "Assert that path and elem are not NULL");
    }

    /* TODO: would be nice to verify that elem is part of path */

    /* subtract component and number of chars from path */
    path->components--;
    path->chars -= elem->chars;

    /* lookup address of elements of next and previous items */
    mfu_path_elem* prev = elem->prev;
    mfu_path_elem* next = elem->next;

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

    return MFU_SUCCESS;
}

/* allocates and returns a string filled in with formatted text,
 * assumes that caller has called va_start before and will call va_end
 * after */
static char* mfu_path_alloc_strf(const char* format, va_list args1, va_list args2)
{
    /* get length of component string */
    size_t chars = (size_t) vsnprintf(NULL, 0, format, args1);

    /* allocate space to hold string, add one for the terminating NUL */
    size_t len = chars + 1;
    char* str = (char*) malloc(len);
    if (str == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path component string");
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
mfu_path* mfu_path_new()
{
    mfu_path* path = mfu_path_alloc();
    if (path == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path object");
    }
    return path;
}

/* allocates a path from string */
mfu_path* mfu_path_from_str(const char* str)
{
    /* allocate a path object */
    mfu_path* path = mfu_path_alloc();
    if (path == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path object");
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
                MFU_ABORT(-1, "Failed to allocate memory for component string");
            }

            /* copy characters into string buffer and add terminating NUL */
            size_t chars = buflen - 1;
            if (chars > 0) {
                strncpy(buf, start, chars);
            }
            buf[chars] = '\0';

            /* allocate new element */
            mfu_path_elem* elem = mfu_path_elem_alloc();
            if (elem == NULL) {
                MFU_ABORT(-1, "Failed to allocate memory for path component");
            }

            /* record string in element */
            elem->component = buf;
            elem->chars     = chars;

            /* add element to path */
            mfu_path_elem_insert(path, path->components, elem);

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
mfu_path* mfu_path_from_strf(const char* format, ...)
{
    /* allocate formatted string */
    va_list args1, args2;
    va_start(args1, format);
    va_start(args2, format);
    char* str = mfu_path_alloc_strf(format, args1, args2);
    va_end(args2);
    va_end(args1);
    if (str == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* create path from string */
    mfu_path* path = mfu_path_from_str(str);

    /* free the string */
    mfu_free(&str);

    return path;
}

/* duplicate a path */
mfu_path* mfu_path_dup(const mfu_path* path)
{
    /* easy if path is NULL */
    if (path == NULL) {
        return NULL;
    }

    /* allocate a new path */
    mfu_path* dup_path = mfu_path_new();
    if (dup_path == NULL) {
        MFU_ABORT(-1, "Failed to allocate path object");
    }

    /* get pointer to first element and delete elements in list */
    mfu_path_elem* current = path->head;
    while (current != NULL) {
        /* get pointer to element after current, delete current,
         * and set current to next */
        mfu_path_elem* dup_elem = mfu_path_elem_dup(current);
        if (dup_elem == NULL) {
            MFU_ABORT(-1, "Failed to allocate path element object");
        }

        /* insert new element at end of path */
        mfu_path_elem_insert(dup_path, dup_path->components, dup_elem);

        /* advance to next element */
        current = current->next;
    }

    return dup_path;
}

/* free a path */
int mfu_path_delete(mfu_path** ptr_path)
{
    if (ptr_path != NULL) {
        /* got an address to the pointer of a path object,
         * dereference to get pointer to path */
        mfu_path* path = *ptr_path;
        if (path != NULL) {
            /* get pointer to first element and delete elements in list */
            mfu_path_elem* current = path->head;
            while (current != NULL) {
                /* get pointer to element after current, delete current,
                 * and set current to next */
                mfu_path_elem* next = current->next;
                mfu_path_elem_free(&current);
                current = next;
            }
        }
    }

    /* free the path object itself */
    mfu_free(ptr_path);

    return MFU_SUCCESS;
}

/*
=========================================
get size and string functions
=========================================
*/

/* returns 1 if path has 0 components, 0 otherwise */
int mfu_path_is_null(const mfu_path* path)
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
int mfu_path_components(const mfu_path* path)
{
    if (path != NULL) {
        int components = path->components;
        return components;
    }
    return 0;
}

/* return number of characters needed to store path
 * (not including terminating NUL) */
size_t mfu_path_strlen(const mfu_path* path)
{
    if (path != NULL) {
        int components = path->components;
        if (components > 0) {
            /* special case for root directory, we want to print "/"
             * not the empty string */
            mfu_path_elem* head = path->head;
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
static int mfu_path_strcpy_internal(char* buf, const mfu_path* path)
{
    /* special case for root directory,
     * we want to print "/" not the empty string */
    int components = path->components;
    if (components == 1 && strcmp(path->head->component, "") == 0) {
        /* got the root directory, just print "/" */
        strcpy(buf, "/");
        return MFU_SUCCESS;
    }

    /* copy contents into string buffer */
    char* ptr = buf;
    mfu_path_elem* current = path->head;
    while (current != NULL) {
        /* copy component to buffer */
        char* component = current->component;
        size_t chars    = current->chars;
        memcpy((void*)ptr, (void*)component, chars);
        ptr += chars;

        /* if there is another component, add a slash */
        mfu_path_elem* next = current->next;
        if (next != NULL) {
            *ptr = '/';
            ptr++;
        }

        /* move to next component */
        current = next;
    }

    /* terminate the string */
    *ptr = '\0';

    return MFU_SUCCESS;
}

/* copy string into user buffer, abort if buffer is too small */
size_t mfu_path_strcpy(char* buf, size_t n, const mfu_path* path)
{
    /* check that we have a pointer to a path */
    if (path == NULL) {
        MFU_ABORT(-1, "Cannot copy NULL pointer to string");
    }

    /* we can't copy a NULL path */
    if (mfu_path_is_null(path)) {
        MFU_ABORT(-1, "Cannot copy a NULL path to string");
    }

    /* get length of path */
    size_t len = mfu_path_strlen(path) + 1;

    /* if user buffer is too small, abort */
    if (n < len) {
        MFU_ABORT(-1, "User buffer of %d bytes is too small to hold string of %d bytes",
                    n, len, __LINE__
                   );
    }

    /* copy contents into string buffer */
    mfu_path_strcpy_internal(buf, path);

    /* return number of bytes we copied to buffer */
    return len;
}

/* allocate memory and return path in string form */
char* mfu_path_strdup(const mfu_path* path)
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
    size_t buflen = mfu_path_strlen(path) + 1;
    char* buf = (char*) malloc(buflen);
    if (buf == NULL) {
        MFU_ABORT(-1, "Failed to allocate buffer for path");
    }

    /* copy contents into string buffer */
    mfu_path_strcpy_internal(buf, path);

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
static int mfu_path_combine(mfu_path* path1, int offset, mfu_path** ptr_path2)
{
    if (path1 != NULL) {
        /* check that offset is in range */
        int components = path1->components;
        if (offset < 0 || offset > components) {
            MFU_ABORT(-1, "Offset %d is out of range [0,%d]",
                        offset, components
                       );
        }

        if (ptr_path2 != NULL) {
            /* got an address to the pointer of a path object,
             * dereference to get pointer to path */
            mfu_path* path2 = *ptr_path2;
            if (path2 != NULL) {
                /* get pointer to head and tail of path2 */
                mfu_path_elem* head2 = path2->head;
                mfu_path_elem* tail2 = path2->tail;

                /* if offset equals number of components, insert after last element,
                 * otherwise, insert element before specified element */
                if (offset == components) {
                    /* get pointer to tail of path1 */
                    mfu_path_elem* tail1 = path1->tail;
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
                    mfu_path_elem* current = mfu_path_elem_index(path1, offset);

                    /* get pointer to element before current */
                    mfu_path_elem* prev = current->prev;

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
        mfu_free(ptr_path2);
    }
    else {
        MFU_ABORT(-1, "Cannot attach a path to a NULL path");
    }

    return MFU_SUCCESS;
}

/* inserts path2 so head element in path2 starts at specified offset
 * in path1, e.g.,
 *   0   - before first element of path1
 *   N-1 - before last element of path1
 *   N   - after last element of path1 */
int mfu_path_insert(mfu_path* path1, int offset, const mfu_path* path2)
{
    int rc = MFU_SUCCESS;
    if (path1 != NULL) {
        /* make a copy of path2, and combint at specified offset in path1,
         * combine deletes copy of path2 */
        mfu_path* path2_copy = mfu_path_dup(path2);
        rc = mfu_path_combine(path1, offset, &path2_copy);
    }
    else {
        MFU_ABORT(-1, "Cannot attach a path to a NULL path");
    }
    return rc;
}

/* prepends path2 to path1 */
int mfu_path_prepend(mfu_path* path1, const mfu_path* path2)
{
    int rc = mfu_path_insert(path1, 0, path2);
    return rc;
}

/* appends path2 to path1 */
int mfu_path_append(mfu_path* path1, const mfu_path* path2)
{
    int rc = MFU_SUCCESS;
    if (path1 != NULL) {
        rc = mfu_path_insert(path1, path1->components, path2);
    }
    else {
        MFU_ABORT(-1, "Cannot attach a path to a NULL path");
    }
    return rc;
}

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int mfu_path_insert_str(mfu_path* path, int offset, const char* str)
{
    /* verify that we got a path as input */
    if (path == NULL) {
        MFU_ABORT(-1, "Cannot insert string to a NULL path");
    }

    /* create a path from this string */
    mfu_path* newpath = mfu_path_from_str(str);
    if (newpath == NULL) {
        MFU_ABORT(-1, "Failed to allocate path for insertion");
    }

    /* attach newpath to original path */
    int rc = mfu_path_combine(path, offset, &newpath);
    return rc;
}

/* prepends components in string to path */
int mfu_path_prepend_str(mfu_path* path, const char* str)
{
    int rc = mfu_path_insert_str(path, 0, str);
    return rc;
}

/* appends components in string to path */
int mfu_path_append_str(mfu_path* path, const char* str)
{
    int rc = MFU_SUCCESS;
    if (path != NULL) {
        rc = mfu_path_insert_str(path, path->components, str);
    }
    else {
        MFU_ABORT(-1, "Cannot attach string to a NULL path");
    }
    return rc;
}

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int mfu_path_insert_strf(mfu_path* path, int offset, const char* format, ...)
{
    /* verify that we got a path as input */
    if (path == NULL) {
        MFU_ABORT(-1, "Cannot append string to a NULL path");
    }

    /* allocate formatted string */
    va_list args1, args2;
    va_start(args1, format);
    va_start(args2, format);
    char* str = mfu_path_alloc_strf(format, args1, args2);
    va_end(args2);
    va_end(args1);
    if (str == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* attach str to path */
    int rc = mfu_path_insert_str(path, offset, str);

    /* free the string */
    mfu_free(&str);

    return rc;
}

/* prepends components in string to path */
int mfu_path_prepend_strf(mfu_path* path, const char* format, ...)
{
    /* verify that we got a path as input */
    if (path == NULL) {
        MFU_ABORT(-1, "Cannot append string to a NULL path");
    }

    /* allocate formatted string */
    va_list args1, args2;
    va_start(args1, format);
    va_start(args2, format);
    char* str = mfu_path_alloc_strf(format, args1, args2);
    va_end(args2);
    va_end(args1);
    if (str == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* attach str to path */
    int rc = mfu_path_insert_str(path, 0, str);

    /* free the string */
    mfu_free(&str);

    return rc;
}

/* adds a new component to end of path using printf-like formatting */
int mfu_path_append_strf(mfu_path* path, const char* format, ...)
{
    /* verify that we got a path as input */
    if (path == NULL) {
        MFU_ABORT(-1, "Cannot append string to a NULL path");
    }

    /* allocate formatted string */
    va_list args1, args2;
    va_start(args1, format);
    va_start(args2, format);
    char* str = mfu_path_alloc_strf(format, args1, args2);
    va_end(args2);
    va_end(args1);
    if (str == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path component string");
    }

    /* attach str to path */
    int rc = mfu_path_insert_str(path, path->components, str);

    /* free the string */
    mfu_free(&str);

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
int mfu_path_slice(mfu_path* path, int offset, int length)
{
    /* check that we have a path */
    if (path == NULL) {
        return MFU_SUCCESS;
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
        return MFU_SUCCESS;
    }

    /* lookup first element to be head of new path */
    mfu_path_elem* current = mfu_path_elem_index(path, offset);

    /* delete any items before this one */
    mfu_path_elem* elem = current->prev;
    while (elem != NULL) {
        mfu_path_elem* prev = elem->prev;
        mfu_path_elem_free(&elem);
        elem = prev;
    }

    /* remember our starting element and intialize tail to NULL */
    mfu_path_elem* head = current;
    mfu_path_elem* tail = NULL;

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
        mfu_path_elem* next = current->next;
        mfu_path_elem_free(&current);
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

    return MFU_SUCCESS;
}

/* drops last component from path */
int mfu_path_dirname(mfu_path* path)
{
    int components = mfu_path_components(path);
    if (components > 0) {
        int rc = mfu_path_slice(path, 0, components - 1);
        return rc;
    }
    return MFU_SUCCESS;
}

/* only leaves last component of path */
int mfu_path_basename(mfu_path* path)
{
    int rc = mfu_path_slice(path, -1, 1);
    return rc;
}

/* copies upto length components of path starting at specified location
 * and returns subpath as new path, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
mfu_path* mfu_path_sub(mfu_path* path, int offset, int length)
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
    mfu_path* newpath = mfu_path_alloc();
    if (newpath == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path object");
    }

    /* return the empty path if source path is empty */
    if (components == 0) {
        return newpath;
    }

    /* lookup first element to be head of new path */
    mfu_path_elem* current = mfu_path_elem_index(path, offset);

    /* copy elements from path and attach to newpath */
    while ((length < 0 || length > 0) && current != NULL) {
        /* duplicate element */
        mfu_path_elem* elem = mfu_path_elem_dup(current);
        if (elem == NULL) {
            MFU_ABORT(-1, "Failed to duplicate element of path object");
        }

        /* insert element into newpath */
        mfu_path_elem_insert(newpath, newpath->components, elem);

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
mfu_path* mfu_path_cut(mfu_path* path, int offset)
{
    /* check that we have a path */
    if (path == NULL) {
        return NULL;
    }

    /* allocate and initialize an empty path object */
    mfu_path* newpath = mfu_path_alloc();
    if (newpath == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for path object");
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
    mfu_path_elem* current = mfu_path_elem_index(path, offset);

    /* set head and tail of newpath2 */
    newpath->head = current;
    newpath->tail = path->tail;

    /* set tail (and head) of path */
    if (current != NULL) {
        /* get element before current to be new tail */
        mfu_path_elem* prev = current->prev;

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
int mfu_path_reduce(mfu_path* path)
{
    /* check that we got a path */
    if (path == NULL) {
        /* nothing to do in this case */
        return MFU_SUCCESS;
    }


    /* now iterate through and remove any "." and empty strings,
     * we go from back to front to handle paths like "./" */
    mfu_path_elem* current = path->tail;
    while (current != NULL) {
        /* get pointer to previous element */
        mfu_path_elem* prev = current->prev;

        /* check whether component string matches "." or "" */
        char* component = current->component;
        if (strcmp(component, ".") == 0) {
            /* pull element out of path and delete it */
            mfu_path_elem_extract(path, current);
            mfu_path_elem_free(&current);
        }
        else if (strcmp(component, "") == 0 && current != path->head) {
            /* head is allowed to be empty string so that we don't chop leading '/' */
            /* pull element out of path and delete it */
            mfu_path_elem_extract(path, current);
            mfu_path_elem_free(&current);
        }

        /* advance to previous item */
        current = prev;
    }

    /* now remove any ".." and any preceding component */
    current = path->head;
    while (current != NULL) {
        /* get pointer to previous and next elements */
        mfu_path_elem* prev = current->prev;
        mfu_path_elem* next = current->next;

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
                        mfu_path_elem_extract(path, prev);
                        mfu_path_elem_free(&prev);

                        /* delete current element */
                        mfu_path_elem_extract(path, current);
                        mfu_path_elem_free(&current);
                    }
                    else {
                        /* trying to pop past root directory, just drop the ".." */
                        //MFU_ABORT(-1, "Cannot pop past root directory");
                        mfu_path_elem_extract(path, current);
                        mfu_path_elem_free(&current);
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

    return MFU_SUCCESS;
}

/* creates path from string, calls reduce, calls path_strdup,
 * and deletes path, caller must free returned string with mfu_free */
char* mfu_path_strdup_reduce_str(const char* str)
{
    mfu_path* path = mfu_path_from_str(str);
    mfu_path_reduce(path);
    char* newstr = mfu_path_strdup(path);
    mfu_path_delete(&path);
    return newstr;
}

/* same as above, but prepend curr working dir if path not absolute */
char* mfu_path_strdup_abs_reduce_str(const char* str)
{
    mfu_path* path = mfu_path_from_str(str);
    if (! mfu_path_is_absolute(path)) {
        char cwd[PATH_MAX];
        mfu_getcwd(cwd, PATH_MAX);
        mfu_path_prepend_str(path, cwd);
    }
    mfu_path_reduce(path);
    char* newstr = mfu_path_strdup(path);
    mfu_path_delete(&path);
    return newstr;
}

/* return 1 if path starts with an empty string, 0 otherwise */
int mfu_path_is_absolute(const mfu_path* path)
{
    if (path != NULL) {
        if (path->components > 0) {
            const mfu_path_elem* head = path->head;
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
mfu_path* mfu_path_abs_reduce(const mfu_path* path)
{
    mfu_path* newpath = mfu_path_dup(path);
    if (! mfu_path_is_absolute(newpath)) {
        char cwd[PATH_MAX];
        mfu_getcwd(cwd, PATH_MAX);
        mfu_path_prepend_str(newpath, cwd);
    }
    mfu_path_reduce(newpath);
    return newpath;
}

mfu_path_result mfu_path_cmp(const mfu_path* src, const mfu_path* dst)
{
    /* check that we got pointers to both src and dst */
    if (src == NULL || dst == NULL) {
        /* if one is NULL but not both, consider them to be different */
        if (src != NULL || dst != NULL) {
            return MFU_PATH_DIFF;
        }

        /* both values are NULL, so consider them to be equal */
        return MFU_PATH_EQUAL;
    }

    /* check that src and dst aren't NULL paths */
    int src_null = mfu_path_is_null(src);
    int dst_null = mfu_path_is_null(dst);
    if (src_null || dst_null) {
        /* if one is NULL but not both, consider them to be different */
        if ((! src_null) || (! dst_null)) {
            return MFU_PATH_DIFF;
        }

        /* both values are NULL, so consider them to be equal */
        return MFU_PATH_EQUAL;
    }

    /* force source and destination to absolute form and reduce them */
    mfu_path* abs_src = mfu_path_abs_reduce(src);
    mfu_path* abs_dst = mfu_path_abs_reduce(dst);

    /* get pointers to start of parent and child */
    mfu_path_result result = MFU_PATH_EQUAL;
    mfu_path_elem* src_elem = abs_src->head;
    mfu_path_elem* dst_elem = abs_dst->head;
    while (src_elem != NULL && dst_elem != NULL) {
        /* compare strings for this element */
        const char* src_component = src_elem->component;
        const char* dst_component = dst_elem->component;
        if (strcmp(src_component, dst_component) != 0) {
            /* found a component in src that's not in dst */
            result = MFU_PATH_DIFF;
            break;
        }

        /* advance to compare next element */
        src_elem = src_elem->next;
        dst_elem = dst_elem->next;
    }

    /* if everything is equal so far, but we've run out of components,
     * check to see if one is contained within the other */
    if (result == MFU_PATH_EQUAL) {
        if (src_elem == NULL && dst_elem != NULL) {
            /* dst is contained within source */
            result = MFU_PATH_DEST_CHILD;
        } else if (src_elem != NULL && dst_elem == NULL) {
            /* src is contained within dst */
            result = MFU_PATH_SRC_CHILD;
        }
    }

    mfu_path_delete(&abs_src);
    mfu_path_delete(&abs_dst);

    return result;
}

/* compute and return relative path from src to dst */
mfu_path* mfu_path_relative(const mfu_path* src, const mfu_path* dst)
{
    /* check that we don't have NULL pointers */
    if (src == NULL || dst == NULL) {
        MFU_ABORT(-1, "Either src or dst pointer is NULL");
    }

    /* we can't get to a NULL path from a non-NULL path */
    int src_components = src->components;
    int dst_components = dst->components;
    if (src_components > 0 && dst_components == 0) {
        MFU_ABORT(-1, "Cannot get from non-NULL path to NULL path");
    }

    /* allocate a new path to record relative path */
    mfu_path* rel = mfu_path_new();
    if (rel == NULL) {
        MFU_ABORT(-1, "Failed to allocate memory for relative path");
    }

    /* walk down both paths until we find the first location where they
     * differ */
    const mfu_path_elem* src_elem = src->head;
    const mfu_path_elem* dst_elem = dst->head;
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
        mfu_path_append_str(rel, "..");
        src_elem = src_elem->next;
    }

    /* now tack on any items left from dst */
    while (dst_elem != NULL) {
        const char* dst_component = dst_elem->component;
        mfu_path_append_str(rel, dst_component);
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
int mfu_path_is_readable(const mfu_path* file)
{
    /* convert to string and delegate to I/O routine */
    char* file_str = mfu_path_strdup(file);
    int rc = mfu_file_is_readable(file_str);
    mfu_free(&file_str);
    return rc;
}

/* tests whether the file or directory is writeable */
int mfu_path_is_writeable(const mfu_path* file)
{
    /* convert to string and delegate to I/O routine */
    char* file_str = mfu_path_strdup(file);
    int rc = mfu_file_is_writable(file_str);
    mfu_free(&file_str);
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

static int TV_ttf_display_type(const mfu_path* path)
{
    if (path == NULL) {
        /* empty path, nothing to display here */
        return TV_ttf_format_ok;
    }

    if (mfu_path_is_null(path)) {
        /* empty path, nothing to display here */
        return TV_ttf_format_ok;
    }

    /* print path in string form */
    char* str = mfu_path_strdup(path);
    TV_ttf_add_row("path", TV_ttf_type_ascii_string, str);
    mfu_free(&str);

    return TV_ttf_format_ok;
}
#endif /* HIDE_TV */
#endif
