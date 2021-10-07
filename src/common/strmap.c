#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include <stdint.h>

#include "strmap.h"
#include "mfu.h"

#define STRMAP_FAILURE (1)

#define AVL_CHILD_LEFT  (0)
#define AVL_CHILD_RIGHT (1)

/*
=========================================
Allocate and delete map objects
=========================================
*/

/* allocates a new tree and initializes it as a single element */
static strmap_node* strmap_node_new(const char* key, const char* value)
{
    strmap_node* node = (strmap_node*) MFU_MALLOC(sizeof(strmap_node));
    if (node != NULL) {
        node->key       = NULL;
        node->key_len   = 0;
        node->value     = NULL;
        node->value_len = 0;
        node->height    = 1;
        node->parent    = NULL;
        node->left      = NULL;
        node->right     = NULL;

        if (key != NULL) {
            node->key = MFU_STRDUP(key);
            node->key_len = strlen(key) + 1;
        }
        if (value != NULL) {
            node->value = MFU_STRDUP(value);
            node->value_len = strlen(value) + 1;
        }
        if (node->key == NULL || node->value == NULL) {
            /* error */
            //MFU_ERR("Failed to allocate key or value");
        }
    }
    else {
        /* error */
        //MFU_ERR("Failed to allocate AVL node");
    }
    return node;
}

/* frees a tree */
static int strmap_node_delete(strmap_node* node)
{
    if (node != NULL) {
        /* delete left child */
        strmap_node_delete(node->left);
        node->left = NULL;

        /* delete right child */
        strmap_node_delete(node->right);
        node->right = NULL;

        /* delete value */
        mfu_free(&node->value);

        /* delete key */
        mfu_free(&node->key);

        /* finally delete the node */
        mfu_free(&node);
    }
    return STRMAP_SUCCESS;
}

/* allocates a new tree and initializes it as a single element */
strmap* strmap_new()
{
    strmap* tree = (strmap*) MFU_MALLOC(sizeof(strmap));
    tree->root = NULL;
    tree->len = 0;
    tree->size = 0;
    return tree;
}

/* copies entries from src into dst */
void strmap_merge(strmap* dst, const strmap* src)
{
    const strmap_node* node;
    strmap_foreach(src, node) {
        const char* key = strmap_node_key(node);
        const char* val = strmap_node_value(node);
        strmap_set(dst, key, val);
    }
    return;
}

/* frees a tree */
void strmap_delete(strmap** ptree)
{
    if (ptree != NULL) {
        strmap* tree = *ptree;
        if (tree != NULL) {
            strmap_node_delete(tree->root);
            tree->root = NULL;
        }
        mfu_free(&tree);
        *ptree = NULL;
    }
    return;
}

/*
=========================================
set, get, unset functions
=========================================
*/

#if 0
/* return the overall root of the tree containing the specified node */
static strmap_node* strmap_node_root(const strmap_node* node)
{
    /* march up from the node to the root */
    if (node == NULL) {
        return NULL;
    }
    const strmap_node* target = node;
    while (target->parent != NULL) {
        target = target->parent;
    }
    return (strmap_node*) target;
}
#endif

/* return the leftmost node in the subtree
 * (disregards nodes available through parent) */
static const strmap_node* strmap_node_leftmost(const strmap_node* node)
{
    /* march down from the node to the leftmost child */
    if (node == NULL) {
        return NULL;
    }
    const strmap_node* target = node;
    while (target->left != NULL) {
        target = target->left;
    }
    return target;
}

/* return the largest node in the subtree
 * (disregards nodes available through parent) */
static const strmap_node* strmap_node_rightmost(const strmap_node* node)
{
    /* march down from the node to the rightmost child */
    if (node == NULL) {
        return NULL;
    }
    const strmap_node* target = node;
    while (target->right != NULL) {
        target = target->right;
    }
    return target;
}

/* sets the height field of the current element based on the
 * height fields of the children */
static int strmap_node_set_height(strmap_node* node)
{
    /* set the height of the current node to be (1 + max(left, right)) */
    if (node != NULL) {
        int height = 0;
        if (node->left != NULL) {
            height = node->left->height;
        }
        if (node->right != NULL) {
            int right_height = node->right->height;
            if (right_height > height) {
                height = right_height;
            }
        }
        node->height = height + 1;
    }

    return STRMAP_SUCCESS;
}

/* rotates the tree to the left about the specified node */
static strmap_node* strmap_node_rotate_left(strmap_node* node)
{
    if (node == NULL) {
        return NULL;
    }

    /* get the right child of the current node */
    strmap_node* target = node->right;
    if (target != NULL) {
        /* if we have a right child, then we need to rotate,
         * record our parent */
        strmap_node* parent = node->parent;

        /* move the current node to be the left child */
        node->right  = target->left;
        node->parent = target;
        if (target->left != NULL) {
            target->left->parent = node;
        }
        strmap_node_set_height(node);

        /* set the right child to be the new node */
        target->left   = node;
        target->parent = parent;
        strmap_node_set_height(target);

        /* update the parent node to point to the new node */
        if (parent != NULL) {
            if (parent->left == node) {
                parent->left = target;
            }
            else {
                parent->right = target;
            }
            strmap_node_set_height(parent);
        }

        /* return the address of the new node after rotation */
        return target;
    }

    return NULL;
}

/* rotates the tree to the right about the specified node */
static strmap_node* strmap_node_rotate_right(strmap_node* node)
{
    if (node == NULL) {
        return NULL;
    }

    strmap_node* target = node->left;
    if (target != NULL) {
        strmap_node* parent = node->parent;

        node->left   = target->right;
        node->parent = target;
        if (target->right != NULL) {
            target->right->parent = node;
        }
        strmap_node_set_height(node);

        target->right  = node;
        target->parent = parent;
        strmap_node_set_height(target);

        if (parent != NULL) {
            if (parent->left == node) {
                parent->left = target;
            }
            else {
                parent->right = target;
            }
            strmap_node_set_height(parent);
        }

        return target;
    }

    return NULL;
}

/* search for the node corresponding to the specified key,
 * returns the node if found, and returns the address of
 * the parent node for this node */
static strmap_node* strmap_node_search(
    const strmap_node* node,
    const void* key,
    strmap_node** parent,
    int* child)
{
    if (node != NULL) {
        int cmp = strcmp(key, node->key);
        if (cmp == 0) {
            /* the current element matches, we should only end up here
             * if the element that we're looking for happens to be the
             * root of the tree, in which case it has no parent */
            *parent = NULL;
            return (strmap_node*) node;
        }
        else if (cmp < 0) {
            /* key is smaller than key of current node, so look for
             * element in left subtree */
            if (node->left == NULL) {
                /* if our left child is NULL, then we would be the parent */
                *parent = (strmap_node*) node;
                *child  = AVL_CHILD_LEFT;
                return NULL;
            }
            int cmp_child = strcmp(key, node->left->key);
            if (cmp_child == 0) {
                /* if our left child matches, then we are the parent */
                *parent = (strmap_node*) node;
                *child  = AVL_CHILD_LEFT;
                return (strmap_node*) node->left;
            }
            /* otherwise, recursively search our left subtree for the
             * parent of the element */
            return strmap_node_search(node->left, key, parent, child);
        }
        else {
            /* key is larger than key of current node, so look for
             * element in right subtree */
            if (node->right == NULL) {
                /* if our right child is NULL, then we would be the parent */
                *parent = (strmap_node*) node;
                *child  = AVL_CHILD_RIGHT;
                return (strmap_node*) NULL;
            }
            int cmp_child = strcmp(key, node->right->key);
            if (cmp_child == 0) {
                /* if our right child matches, then we are the parent */
                *parent = (strmap_node*) node;
                *child  = AVL_CHILD_RIGHT;
                return (strmap_node*) node->right;
            }
            /* otherwise, recursively search our right subtree for the
             * parent of the element */
            return strmap_node_search(node->right, key, parent, child);
        }
    }

    /* we were not given a valid tree, so return NULL */
    *parent = NULL;
    return NULL;
}

/* compute the balance factor of the current node */
static int strmap_node_balance_factor(const strmap_node* node)
{
    if (node == NULL) {
        return 0;
    }

    /* get the left and right heights */
    int left  = 0;
    int right = 0;
    if (node->left != NULL) {
        left = node->left->height;
    }
    if (node->right != NULL) {
        right = node->right->height;
    }

    /* determine the balance of the node */
    int factor = left - right;
    return factor;
}

/* rebalance the tree starting at the specified node and working
 * upwards to the root, returns the address of the root node
 * after rebalancing */
static strmap_node* strmap_node_rebalance(strmap_node* node)
{
    if (node == NULL) {
        return NULL;
    }

    strmap_node* top = NULL;

    /* remember the parent since the rotations below may demote
     * the current node */
    strmap_node* parent = node->parent;

    /* determine the balance of the node */
    int factor = strmap_node_balance_factor(node);
    if (factor < -1) {
        /* right side of node is too heavy,
         * we need to rotate node to the left about the current node */
        strmap_node* target = node->right;

        /* determine whether the extra weight is to the left or right
         * side of our right child */
        int target_factor = strmap_node_balance_factor(target);
        if (target_factor > 0) {
            /* left side of our right child is too heavy,
             * rotate to the right about our right child first */
            strmap_node_rotate_right(target);
        }

        /* now rotate left about the current node */
        top = strmap_node_rotate_left(node);
    }
    else if (factor > 1) {
        /* left side of node is too heavy,
         * we need to rotate node to the right about the current node */
        strmap_node* target = node->left;

        /* determine whether the extra weight is to the left or
         * right side of our left child */
        int target_factor = strmap_node_balance_factor(target);
        if (target_factor < 0) {
            /* right side of our left child is too heavy,
             * rotate to the left about our left child first */
            strmap_node_rotate_left(target);
        }

        /* now rotate right about the current node */
        top = strmap_node_rotate_right(node);
    }
    else {
        /* no rotations needed, but we still need to set the
         * height of our node */
        strmap_node_set_height(node);
        top = node;
    }

    /* now rebalance at the next level up */
    if (parent != NULL) {
        top = strmap_node_rebalance(parent);
    }

    return top;
}

#if 0
/* returns the current height of the tree */
static int strmap_height(const strmap* tree)
{
    if (tree != NULL) {
        if (tree->root != NULL) {
            return tree->root->height;
        }
    }
    return 0;
}
#endif

/* This code extracts a node that is assumed to have at most one child,
 * it removes the node by updating the pointers in the parent and child
 * to point to each other.  It returns the child of the extracted node.
 * It does not update fields in the extracted node, nor does it update
 * the height of the parent. */
static strmap_node* strmap_node_extract_single(strmap_node* node)
{
    if (node != NULL) {
        /* we enter this if we have one child or less */
        strmap_node* child = NULL;
        if (node->left != NULL) {
            /* we only have a left child, just splice our parent
             * and child together */
            child = node->left;
        }
        else if (node->right != NULL) {
            /* we only have a right child, just splice our parent
             * and child together */
            child = node->right;
        }

        /* get a pointer to our parent */
        strmap_node* parent = node->parent;

        /* set the parent of the replacement node to our parent */
        if (child != NULL) {
            child->parent = parent;
        }

        if (parent != NULL) {
            /* if we have a parent, set the replacement as its new child */
            if (parent->left == node) {
                parent->left = child;
            }
            else {
                parent->right = child;
            }
        }

        /* return the address of the child node that replaces
         * the extracted node */
        return child;
    }

    return NULL;
}

/* return the smallest node in the subtree
 * (disregards nodes available through parent) */
const strmap_node* strmap_node_first(const strmap* tree)
{
    /* first march up the tree to the root, then find leftmost child */
    if (tree == NULL) {
        return NULL;
    }
    const strmap_node* target = strmap_node_leftmost(tree->root);
    return target;
}

/* return the largest node in the subtree
 * (disregards nodes available through parent) */
const strmap_node* strmap_node_last(const strmap* tree)
{
    /* first march up the tree to the root, then find leftmost child */
    if (tree == NULL) {
        return NULL;
    }
    const strmap_node* target = strmap_node_rightmost(tree->root);
    return target;
}

/* the previous element is the rightmost element of the left subtree */
const strmap_node* strmap_node_previous(const strmap_node* node)
{
    if (node == NULL) {
        return NULL;
    }

    /* return the rightmost node of our left subtree if we have one */
    const strmap_node* target = strmap_node_rightmost(node->left);
    if (target != NULL) {
        return target;
    }

    /* if we don't have a left subtree, return our parent
     * if we have one, and if we are its right child */
    const strmap_node* current = node;
    const strmap_node* parent  = current->parent;
    while (parent != NULL) {
        if (parent->right == current) {
            return parent;
        }
        current = parent;
        parent = current->parent;
    }

    /* otherwise, there is no smaller value */
    return NULL;
}

/* the next element is the leftmost element of the right subtree */
const strmap_node* strmap_node_next(const strmap_node* node)
{
    if (node == NULL) {
        return NULL;
    }

    /* return the leftmost node of our right subtree if we have one */
    const strmap_node* target = strmap_node_leftmost(node->right);
    if (target != NULL) {
        return target;
    }

    /* if we don't have a right subtree, return our parent if we
     * have one, and if we are its left child */
    const strmap_node* current = node;
    const strmap_node* parent  = current->parent;
    while (parent != NULL) {
        if (parent->left == current) {
            return parent;
        }
        current = parent;
        parent = current->parent;
    }

    /* otherwise, there is no larger value */
    return NULL;
}

const char* strmap_node_key(const strmap_node* node)
{
    if (node != NULL) {
        return node->key;
    }
    return NULL;
}

const char* strmap_node_value(const strmap_node* node)
{
    if (node != NULL) {
        return node->value;
    }
    return NULL;
}

/* return number of key/value pairs in tree */
uint64_t strmap_size(const strmap* tree)
{
    if (tree != NULL) {
        return tree->size;
    }
    return 0;
}

/* insert the specified key and value into the tree */
int strmap_set(strmap* tree, const char* key, const char* value)
{
    if (tree != NULL) {
        /* search the tree for the parent to our item */
        int child;
        strmap_node* parent;
        strmap_node* node = strmap_node_search(tree->root, key, &parent, &child);

        if (node == NULL) {
            /* key does not exist, create a new item for this element */
            node = strmap_node_new(key, value);

            /* increase pack size of tree */
            size_t node_len = node->key_len + node->value_len;
            tree->len += node_len;
            tree->size++;

            /* insert the new item as a child to the parent, if we found one,
             * otherwise if the item has no parent, the tree must be
             * empty which means it is the new root */
            if (parent != NULL) {
                /* add item as child to parent */
                node->parent = parent;
                if (child == AVL_CHILD_LEFT) {
                    parent->left = node;
                }
                else {
                    parent->right = node;
                }

                /* rebalance tree */
                tree->root = strmap_node_rebalance(parent);
            }
            else {
                /* the tree is empty, set new node as the root,
                 * no need to rebalance since this is the only item in the tree */
                tree->root = node;
            }
        }
        else {
            /* key already exists, free the current value and reset it */
            tree->len -= node->value_len;
            mfu_free(&node->value);
            node->value_len = 0;

            /* copy in the new value */
            if (value != NULL) {
                node->value = MFU_STRDUP(value);
                node->value_len = strlen(value) + 1;
            }
            tree->len += node->value_len;
        }
    }

    return STRMAP_SUCCESS;
}

/* insert key/value into map as "key=value" with printf formatting */
int strmap_setf(strmap* map, const char* format, ...)
{
    va_list args;
    char* str = NULL;

    /* check that we have a format string */
    if (format == NULL) {
        return STRMAP_FAILURE;
    }

    /* compute the size of the string we need to allocate */
    va_start(args, format);
    int size = vsnprintf(NULL, 0, format, args) + 1;
    va_end(args);

    /* allocate and print the string */
    if (size > 0) {
        str = (char*) MFU_MALLOC((size_t)size);

        va_start(args, format);
        vsnprintf(str, (size_t)size, format, args);
        va_end(args);

        /* break into key/value strings at '=' sign */
        char* key = str;
        char* val = str;
        char delim[] = "=";
        strsep(&val, delim);

        /* if we have a key and value, insert into map */
        int rc;
        if (val != NULL) {
            rc = strmap_set(map, key, val);
        }

        mfu_free(&str);
        return rc;
    }

    return STRMAP_FAILURE;
}

/* search the tree and return the address of the node that
 * matches the specified key */
const char* strmap_get(const strmap* tree, const char* key)
{
    if (tree != NULL) {
        /* search the tree for the parent to our item */
        int child;
        strmap_node* parent;
        strmap_node* node = strmap_node_search(tree->root, key, &parent, &child);
        if (node != NULL) {
            return node->value;
        }
    }
    return NULL;
}

/* returns pointer to value string if found, NULL otherwise,
 * key can use printf formatting */
const char* strmap_getf(strmap* map, const char* format, ...)
{
    va_list args;
    char* str = NULL;

    /* check that we have a format string */
    if (format == NULL) {
        return strmap_get(map, NULL);
    }

    /* compute the size of the string we need to allocate */
    va_start(args, format);
    int size = vsnprintf(NULL, 0, format, args) + 1;
    va_end(args);

    /* allocate and print the string */
    if (size > 0) {
        str = (char*) MFU_MALLOC((size_t)size);

        va_start(args, format);
        vsnprintf(str, (size_t)size, format, args);
        va_end(args);

        /* if we have a key and value, insert into map */
        const char* val;
        val = strmap_get(map, str);

        mfu_free(&str);
        return val;
    }

    return strmap_get(map, NULL);
}

/* remove the node corresponding to the specified key */
int strmap_unset(strmap* tree, const char* key)
{
    if (tree != NULL) {
        /* search for the element by key */
        int child;
        strmap_node* parent;
        strmap_node* node = strmap_node_search(tree->root, key, &parent, &child);

        /* we only need to do anything if we actually find it */
        if (node != NULL) {
            /* decrease the tree pack size */
            size_t node_len = node->key_len + node->value_len;
            tree->len -= node_len;
            tree->size--;

            /* found it, identify the node to replace it */
            if (node->left != NULL && node->right != NULL) {
                /* we have two children, identify rightmost node of left subtree */
                strmap_node* replacement = (strmap_node*) strmap_node_rightmost(node->left);

                /* if the rightmost node of our left subtree is not our left child,
                 * extract it and promote it to be our replacement */ 
                if (replacement != node->left) {
                    /* since this is a rightmost node, it has at most one child,
                     * so safe to use extract_single  */
                    strmap_node_extract_single(replacement);

                    /* update the left child of this node to point to our replacement */
                    replacement->left = node->left;
                    node->left->parent = replacement;
                }

                /* update the right child of this node to point to our right child,
                 * (we're guaranteed that the rightmost node from our left subtree
                 * does not have a right child of its own) */
                replacement->right = node->right;
                node->right->parent = replacement;

                /* now tie the replacement node to our parent, (record the current
                 * parent of the replacement since we need it for rebalancing
                 * the tree) */
                strmap_node* replacement_parent = replacement->parent;
                replacement->parent = parent;
                if (parent != NULL) {
                    if (child == AVL_CHILD_LEFT) {
                        parent->left = replacement;
                    }
                    else {
                        parent->right = replacement;
                    }
                }

                /* finally, rebalance the tree */
                if (replacement_parent != node) {
                    /* if the replacement node is not our left child,
                     * then we need to rebalance starting at the parent of
                     * the replacement node */
                    tree->root = strmap_node_rebalance(replacement_parent);
                }
                else {
                    /* if the replacement node is our left child,
                     * then we need to rebalance starting at the
                     * replacement node itself */
                    tree->root = strmap_node_rebalance(replacement);
                }
            }
            else {
                /* we have one child or less, just extract the node */
                strmap_node* newnode = strmap_node_extract_single(node);
                if (parent != NULL) {
                    /* if we have a parent, we need to rebalance the tree
                     * starting at the parent */
                    tree->root = strmap_node_rebalance(parent);
                }
                else {
                    /* the node we're extracting is the root, so set child
                     * node as the new root */
                    tree->root = newnode;
                }
            }

            /* finally, delete the current node */
            node->height = 1;
            node->parent = NULL;
            node->left   = NULL;
            node->right  = NULL;
            strmap_node_delete(node);
        }
    }

    return STRMAP_SUCCESS;
}

/* deletes specified key using printf formatting */
int strmap_unsetf(strmap* map, const char* format, ...)
{
    va_list args;
    char* str = NULL;

    /* check that we have a format string */
    if (format == NULL) {
        return strmap_unset(map, NULL);
    }

    /* compute the size of the string we need to allocate */
    va_start(args, format);
    int size = vsnprintf(NULL, 0, format, args) + 1;
    va_end(args);

    /* allocate and print the string */
    if (size > 0) {
        str = (char*) MFU_MALLOC((size_t)size);

        va_start(args, format);
        vsnprintf(str, (size_t)size, format, args);
        va_end(args);

        /* if we have a key and value, insert into map */
        int rc;
        rc = strmap_unset(map, str);

        mfu_free(&str);
        return rc;
    }

    return strmap_unset(map, NULL);
}

#if 0
size_t strmap_pack_size(const strmap* tree)
{
    size_t size = 8; /* uint64_t */
    if (tree != NULL) {
        size += tree->len;
    }
    return size;
}

/* pack tree into buffer specified at given memory location */
size_t strmap_pack(void* buf, const strmap* tree)
{
    char* ptr = (char*) buf;

    /* TODO: convert size to network order */
    uint64_t size = (uint64_t) strmap_pack_size(tree);
    ptr += mfu_pack_uint64(ptr, size);

    /* TODO: be sure we don't write past end of buffer */
    /* copy key/value pairs */
    const strmap_node* current = strmap_node_first(tree);
    while (current != NULL) {
        size_t key_len = current->key_len;
        memcpy(ptr, current->key, key_len);
        ptr += key_len;

        size_t value_len = current->value_len;
        memcpy(ptr, current->value, value_len);
        ptr += value_len;

        current = strmap_node_next(current);
    }

    return size;
}

/* unpack tree stored at buf into tree */
size_t strmap_unpack(const void* buf, strmap* tree)
{
    char* ptr = (char*) buf;

    /* TODO: convert size to network order */
    uint64_t size;
    ptr += mfu_unpack_uint64(ptr, &size);

    /* TODO: be sure we don't try to read past size bytes */
    char* end = (char*)buf + size;
    while (ptr < end) {
        char* key = ptr;
        ptr += strlen(key) + 1;

        char* value = ptr;
        ptr += strlen(value) + 1;

        strmap_set(tree, key, value);
    }

    return size;
}

void strmap_print(const strmap* map)
{
    int i = 0;
    const strmap_node* node;
    for (node = strmap_node_first(map);
            node != NULL;
            node = strmap_node_next(node)) {
        const char* key = strmap_node_key(node);
        const char* val = strmap_node_value(node);
        printf("%d: %s --> %s\n", i, key, val);
        i++;
    }

    size_t bytes = strmap_pack_size(map);
    printf("%d entries, %llu bytes packed\n", i, (unsigned long long) bytes);

    return;
}
#endif
