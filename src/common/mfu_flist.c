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

#define _GNU_SOURCE
#ifndef __G_MACROS_H__
#define __G_MACROS_H__
#endif
#include <dirent.h>
#include <fcntl.h>
#include <sys/syscall.h>

#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h> /* asctime / localtime */
#include <regex.h>

/* These headers are needed to query the Lustre MDS for stat
 * information.  This information may be incomplete, but it
 * is faster than a normal stat, which requires communication
 * with the MDS plus every OST a file is striped across. */
//#define LUSTRE_STAT
#ifdef LUSTRE_STAT
#include <lustre/lustre_user.h>
#endif /* LUSTRE_STAT */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <sys/ioctl.h>
#include <sys/param.h>

#include <linux/fs.h>
#include <linux/fiemap.h>

#include <libgen.h> /* dirname */
#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"
#include "mfu_flist_internal.h"
#include "strmap.h"

#ifdef LUSTRE_SUPPORT
#include <lustre/lustre_user.h>
#include <sys/ioctl.h>
#endif

/****************************************
 * Functions on types
 ***************************************/

/* given a mode_t from stat, return the corresponding MFU filetype */
mfu_filetype mfu_flist_mode_to_filetype(mode_t mode)
{
    /* set file type */
    mfu_filetype type;
    if (S_ISDIR(mode)) {
        type = MFU_TYPE_DIR;
    }
    else if (S_ISREG(mode)) {
        type = MFU_TYPE_FILE;
    }
    else if (S_ISLNK(mode)) {
        type = MFU_TYPE_LINK;
    }
    else {
        /* unknown file type */
        type = MFU_TYPE_UNKNOWN;
    }
    return type;
}

/* given path, return level within directory tree,
 * counts '/' characters assuming path is standardized
 * and absolute */
int mfu_flist_compute_depth(const char* path)
{
    const char* c;
    int depth = 0;
    for (c = path; *c != '\0'; c++) {
        if (*c == '/') {
            depth++;
        }
    }
    return depth;
}

/* return number of bytes needed to pack element */
static size_t list_elem_pack2_size(int detail, uint64_t chars, const elem_t* elem)
{
    size_t size;
    if (detail) {
        size = 2 * 4 + chars + 0 * 4 + 10 * 8;
    }
    else {
        size = 2 * 4 + chars + 1 * 4;
    }
    return size;
}

/* pack element into buffer and return number of bytes written */
static size_t list_elem_pack2(void* buf, int detail, uint64_t chars, const elem_t* elem)
{
    /* set pointer to start of buffer */
    char* start = (char*) buf;
    char* ptr = start;

    /* copy in detail flag */
    mfu_pack_uint32(&ptr, (uint32_t) detail);

    /* copy in length of file name field */
    mfu_pack_uint32(&ptr, (uint32_t) chars);

    /* copy in file name */
    char* file = elem->file;
    strcpy(ptr, file);
    ptr += chars;

    if (detail) {
        /* copy in fields */
        mfu_pack_uint64(&ptr, elem->mode);
        mfu_pack_uint64(&ptr, elem->uid);
        mfu_pack_uint64(&ptr, elem->gid);
        mfu_pack_uint64(&ptr, elem->atime);
        mfu_pack_uint64(&ptr, elem->atime_nsec);
        mfu_pack_uint64(&ptr, elem->mtime);
        mfu_pack_uint64(&ptr, elem->mtime_nsec);
        mfu_pack_uint64(&ptr, elem->ctime);
        mfu_pack_uint64(&ptr, elem->ctime_nsec);
        mfu_pack_uint64(&ptr, elem->size);
    }
    else {
        /* just have the file type */
        mfu_pack_uint32(&ptr, elem->type);
    }

    size_t bytes = (size_t)(ptr - start);
    return bytes;
}

/* unpack element from buffer and return number of bytes read */
static size_t list_elem_unpack2(const void* buf, elem_t* elem)
{
    /* set pointer to start of buffer */
    const char* start = (const char*) buf;
    const char* ptr = start;

    /* extract detail field */
    uint32_t detail;
    mfu_unpack_uint32(&ptr, &detail);

    /* extract length of file name field */
    uint32_t chars;
    mfu_unpack_uint32(&ptr, &chars);

    /* get name and advance pointer */
    const char* file = ptr;
    ptr += chars;

    /* copy path */
    elem->file = MFU_STRDUP(file);

    /* set depth */
    elem->depth = mfu_flist_compute_depth(file);

    elem->detail = (int) detail;

    if (detail) {
        /* extract fields */
        mfu_unpack_uint64(&ptr, &elem->mode);
        mfu_unpack_uint64(&ptr, &elem->uid);
        mfu_unpack_uint64(&ptr, &elem->gid);
        mfu_unpack_uint64(&ptr, &elem->atime);
        mfu_unpack_uint64(&ptr, &elem->atime_nsec);
        mfu_unpack_uint64(&ptr, &elem->mtime);
        mfu_unpack_uint64(&ptr, &elem->mtime_nsec);
        mfu_unpack_uint64(&ptr, &elem->ctime);
        mfu_unpack_uint64(&ptr, &elem->ctime_nsec);
        mfu_unpack_uint64(&ptr, &elem->size);

        /* use mode to set file type */
        elem->type = mfu_flist_mode_to_filetype((mode_t)elem->mode);
    }
    else {
        /* only have type */
        uint32_t type;
        mfu_unpack_uint32(&ptr, &type);
        elem->type = (mfu_filetype) type;
    }

    size_t bytes = (size_t)(ptr - start);
    return bytes;
}

/* append element to tail of linked list */
void mfu_flist_insert_elem(flist_t* flist, elem_t* elem)
{
    /* set head if this is the first item */
    if (flist->list_head == NULL) {
        flist->list_head = elem;
    }

    /* update last element to point to this new element */
    elem_t* tail = flist->list_tail;
    if (tail != NULL) {
        tail->next = elem;
    }

    /* make this element the new tail */
    flist->list_tail = elem;
    elem->next = NULL;

    /* increase list count by one */
    flist->list_count++;

    /* delete the index if we have one, it's out of date */
    mfu_free(&flist->list_index);

    return;
}

/* insert copy of specified element into list */
static void list_insert_copy(flist_t* flist, elem_t* src)
{
    /* create new element */
    elem_t* elem = (elem_t*) MFU_MALLOC(sizeof(elem_t));

    /* copy values from source */
    elem->file       = MFU_STRDUP(src->file);
    elem->depth      = src->depth;
    elem->type       = src->type;
    elem->detail     = src->detail;
    elem->mode       = src->mode;
    elem->uid        = src->uid;
    elem->gid        = src->gid;
    elem->atime      = src->atime;
    elem->atime_nsec = src->atime_nsec;
    elem->mtime      = src->mtime;
    elem->mtime_nsec = src->mtime_nsec;
    elem->ctime      = src->ctime;
    elem->ctime_nsec = src->ctime_nsec;
    elem->size       = src->size;

    /* append element to tail of linked list */
    mfu_flist_insert_elem(flist, elem);

    return;
}

/* delete linked list of stat items */
static void list_delete(flist_t* flist)
{
    elem_t* current = flist->list_head;
    while (current != NULL) {
        elem_t* next = current->next;
        mfu_free(&current->file);
        mfu_free(&current);
        current = next;
    }
    flist->list_count = 0;
    flist->list_head  = NULL;
    flist->list_tail  = NULL;

    /* delete the cached index */
    mfu_free(&flist->list_index);

    return;
}

/* given an index, return pointer to that file element,
 * NULL if index is not in range */
static elem_t* list_get_elem(flist_t* flist, uint64_t idx)
{
    uint64_t max = flist->list_count;
    
    /* build index of list elements if we don't already have one */
    if (flist->list_index == NULL) {
        /* allocate array to record pointer to each element */
        size_t index_size = max * sizeof(elem_t*);
        
        flist->list_index = (elem_t**) MFU_MALLOC(index_size);

        /* get pointer to each element */
        uint64_t i = 0;
        elem_t* current = flist->list_head;
        while (i < max && current != NULL) {
            flist->list_index[i] = current;
            current = current->next;
            i++;
        }
    }

    /* return pointer to element if index is within range */
    if (idx < max) {
        elem_t* elem = flist->list_index[idx];
        return elem;
    }
    return NULL;
}

static void list_compute_summary(flist_t* flist)
{
    /* initialize summary values */
    flist->max_file_name  = 0;
    flist->max_user_name  = 0;
    flist->max_group_name = 0;
    flist->min_depth      = 0;
    flist->max_depth      = 0;
    flist->total_files    = 0;
    flist->offset         = 0;

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* get total number of files in list */
    uint64_t total;
    uint64_t count = flist->list_count;
    MPI_Allreduce(&count, &total, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    flist->total_files = total;

    /* bail out early if no one has anything */
    if (total <= 0) {
        return;
    }

    /* compute the global offset of our first item */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }
    flist->offset = offset;

    /* compute local min/max values */
    int min_depth = -1;
    int max_depth = -1;
    uint64_t max_name = 0;
    elem_t* current = flist->list_head;
    while (current != NULL) {
        uint64_t len = (uint64_t)(strlen(current->file) + 1);
        if (len > max_name) {
            max_name = len;
        }

        int depth = current->depth;
        if (depth < min_depth || min_depth == -1) {
            min_depth = depth;
        }
        if (depth > max_depth || max_depth == -1) {
            max_depth = depth;
        }

        /* go to next item */
        current = current->next;
    }

    /* get global maximums */
    int global_max_depth;
    MPI_Allreduce(&max_depth, &global_max_depth, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    uint64_t global_max_name;
    MPI_Allreduce(&max_name, &global_max_name, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* since at least one rank has an item and max will be -1 on ranks
     * without an item, set our min to global max if we have no items,
     * this will ensure that our contribution is >= true global min */
    int global_min_depth;
    if (count == 0) {
        min_depth = global_max_depth;
    }
    MPI_Allreduce(&min_depth, &global_min_depth, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

    /* set summary values */
    flist->max_file_name = global_max_name;
    flist->min_depth = global_min_depth;
    flist->max_depth = global_max_depth;

    /* set summary on users and groups */
    if (flist->detail) {
        flist->total_users    = flist->users.count;
        flist->total_groups   = flist->groups.count;
        flist->max_user_name  = flist->users.chars;
        flist->max_group_name = flist->groups.chars;
    }

    return;
}

/****************************************
 * File list user API
 ***************************************/

/* create object that MFU_FLIST_NULL points to */
static flist_t flist_null;
mfu_flist MFU_FLIST_NULL = &flist_null;

/* allocate and initialize a new file list object */
mfu_flist mfu_flist_new()
{
    /* allocate memory for file list, cast it to handle, initialize and return */
    flist_t* flist = (flist_t*) MFU_MALLOC(sizeof(flist_t));

    flist->detail = 0;
    flist->total_files = 0;

    /* initialize linked list */
    flist->list_count = 0;
    flist->list_head  = NULL;
    flist->list_tail  = NULL;
    flist->list_index = NULL;

    /* initialize user and group structures */
    mfu_flist_usrgrp_init(flist);

    mfu_flist bflist = (mfu_flist) flist;
    return bflist;
}

/* free resouces in file list */
void mfu_flist_free(mfu_flist* pbflist)
{
    /* convert handle to flist_t */
    flist_t* flist = *(flist_t**)pbflist;

    /* delete linked list */
    list_delete(flist);

    /* free user and group structures */
    mfu_flist_usrgrp_free(flist);

    mfu_free(&flist);

    /* set caller's pointer to NULL */
    *pbflist = MFU_FLIST_NULL;

    return;
}

/* given an input list, split items into separate lists depending
 * on their depth, returns number of levels, minimum depth, and
 * array of lists as output */
void mfu_flist_array_by_depth(
    mfu_flist srclist,
    int* outlevels,
    int* outmin,
    mfu_flist** outlists)
{
    /* check that our pointers are valid */
    if (outlevels == NULL || outmin == NULL || outlists == NULL) {
        return;
    }

    /* initialize return values */
    *outlevels = 0;
    *outmin    = -1;
    *outlists  = NULL;

    /* get total file count */
    uint64_t total = mfu_flist_global_size(srclist);
    if (total == 0) {
        return;
    }

    /* get min and max depths, determine number of levels,
     * allocate array of lists */
    int min = mfu_flist_min_depth(srclist);
    int max = mfu_flist_max_depth(srclist);
    int levels = max - min + 1;
    mfu_flist* lists = (mfu_flist*) MFU_MALLOC((size_t)levels * sizeof(mfu_flist));

    /* create a list for each level */
    int i;
    for (i = 0; i < levels; i++) {
        lists[i] = mfu_flist_subset(srclist);
    }

    /* copy each item from source list to its corresponding level */
    uint64_t idx = 0;
    uint64_t size = mfu_flist_size(srclist);
    while (idx < size) {
        int depth = mfu_flist_file_get_depth(srclist, idx);
        int depth_index = depth - min;
        mfu_flist dstlist = lists[depth_index];
        mfu_flist_file_copy(srclist, idx, dstlist);
        idx++;
    }

    /* summarize each list */
    for (i = 0; i < levels; i++) {
        mfu_flist_summarize(lists[i]);
    }

    /* set return parameters */
    *outlevels = levels;
    *outmin    = min;
    *outlists  = lists;

    return;
}

/* frees array of lists created in call to
 * mfu_flist_split_by_depth */
void mfu_flist_array_free(int levels, mfu_flist** outlists)
{
    /* check that our pointer is valid */
    if (outlists == NULL) {
        return;
    }

    /* free each list */
    int i;
    mfu_flist* lists = *outlists;
    for (i = 0; i < levels; i++) {
        mfu_flist_free(&lists[i]);
    }

    /* free the array of lists and set caller's pointer to NULL */
    mfu_free(outlists);
    return;
}

/* return number of files across all procs */
uint64_t mfu_flist_global_size(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->total_files;
    return val;
}

/* returns the global index of first item on this rank,
 * when placing items in rank order */
uint64_t mfu_flist_global_offset(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->offset;
    return val;
}

/* return number of files in local list */
uint64_t mfu_flist_size(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->list_count;
    return val;
}

/* return number of users */
uint64_t mfu_flist_user_count(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->total_users;
    return val;
}

/* return number of groups */
uint64_t mfu_flist_group_count(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->total_groups;
    return val;
}

/* return maximum length of file names */
uint64_t mfu_flist_file_max_name(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->max_file_name;
    return val;
}

/* return maximum length of user names */
uint64_t mfu_flist_user_max_name(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->max_user_name;
    return val;
}

/* return maximum length of group names */
uint64_t mfu_flist_group_max_name(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    uint64_t val = flist->max_group_name;
    return val;
}

/* return min depth */
int mfu_flist_min_depth(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    int val = flist->min_depth;
    return val;
}

/* return max depth */
int mfu_flist_max_depth(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    int val = flist->max_depth;
    return val;
}

int mfu_flist_have_detail(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    int val = flist->detail;
    return val;
}

const char* mfu_flist_file_get_name(mfu_flist bflist, uint64_t idx)
{
    const char* name = NULL;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        name = elem->file;
    }
    return name;
}

int mfu_flist_file_get_depth(mfu_flist bflist, uint64_t idx)
{
    int depth = -1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        depth = elem->depth;
    }
    return depth;
}

mfu_filetype mfu_flist_file_get_type(mfu_flist bflist, uint64_t idx)
{
    mfu_filetype type = MFU_TYPE_NULL;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        type = elem->type;
    }
    return type;
}

uint64_t mfu_flist_file_get_mode(mfu_flist bflist, uint64_t idx)
{
    uint64_t mode = 0;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail > 0) {
        mode = elem->mode;
    }
    return mode;
}

uint64_t mfu_flist_file_get_perm(mfu_flist bflist, uint64_t idx) {
    uint64_t mode = mfu_flist_file_get_mode(bflist, idx);

    return mode & (S_IRWXU | S_IRWXG | S_IRWXO);
}

uint64_t mfu_flist_file_get_uid(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->uid;
    }
    return ret;
}

uint64_t mfu_flist_file_get_gid(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->gid;
    }
    return ret;
}

uint64_t mfu_flist_file_get_atime(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->atime;
    }
    return ret;
}

uint64_t mfu_flist_file_get_atime_nsec(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->atime_nsec;
    }
    return ret;
}

uint64_t mfu_flist_file_get_mtime(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->mtime;
    }
    return ret;
}

uint64_t mfu_flist_file_get_mtime_nsec(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->mtime_nsec;
    }
    return ret;
}

uint64_t mfu_flist_file_get_ctime(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->ctime;
    }
    return ret;
}

uint64_t mfu_flist_file_get_ctime_nsec(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->ctime_nsec;
    }
    return ret;
}

uint64_t mfu_flist_file_get_size(mfu_flist bflist, uint64_t idx)
{
    uint64_t ret = (uint64_t) - 1;
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL && flist->detail) {
        ret = elem->size;
    }
    return ret;
}

const char* mfu_flist_file_get_username(mfu_flist bflist, uint64_t idx)
{
    const char* ret = NULL;
    flist_t* flist = (flist_t*) bflist;
    if (flist->detail) {
        uint64_t id = mfu_flist_file_get_uid(bflist, idx);
        ret = mfu_flist_usrgrp_get_name_from_id(flist->user_id2name, id);
    }
    return ret;
}

const char* mfu_flist_file_get_groupname(mfu_flist bflist, uint64_t idx)
{
    const char* ret = NULL;
    flist_t* flist = (flist_t*) bflist;
    if (flist->detail) {
        uint64_t id = mfu_flist_file_get_gid(bflist, idx);
        ret = mfu_flist_usrgrp_get_name_from_id(flist->group_id2name, id);
    }
    return ret;
}

mfu_flist mfu_flist_subset(mfu_flist src)
{
    /* allocate a new file list */
    mfu_flist bflist = mfu_flist_new();

    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;
    flist_t* srclist = (flist_t*)src;

    /* copy user and groups if we have them */
    flist->detail = srclist->detail;
    if (srclist->detail) {
        mfu_flist_usrgrp_copy(srclist, flist);
    }

    return bflist;
}

/* given an input flist, return a newly allocated flist consisting of 
 * a filtered set by finding all items that match/don't match a given
 * regular expression */
mfu_flist mfu_flist_filter_regex(mfu_flist flist, const char* regex_exp, int exclude, int name)
{
    /* create our list to return */
    mfu_flist dest = mfu_flist_subset(flist);

    /* check if user passed in an expression, if so then filter the list */
    if (regex_exp != NULL) {
        /* compile regular expression, if it fails print error */
        regex_t regex;
        int regex_return = regcomp(&regex, regex_exp, 0);
        if (regex_return) {
            MFU_ABORT(-1, "Could not compile regex: `%s' rc=%d\n", regex_exp, regex_return);
        }

        /* copy the things that don't or do (based on input) match the regex into a
         * filtered list */
        uint64_t idx = 0;
        uint64_t size = mfu_flist_size(flist);
        while (idx < size) {
            /* get full path of item */
            const char* file_name = mfu_flist_file_get_name(flist, idx);

            /* get basename of item (exclude the path) */
            mfu_path* pathname = mfu_path_from_str(file_name);
            mfu_path_basename(pathname);
            char* base = mfu_path_strdup(pathname);

            /* execute regex on item, either against the basename or
             * the full path depending on name flag */
            if (name) {
                /* run regex on basename */
                regex_return = regexec(&regex, base, 0, NULL, 0);
            }
            else {
                /* run regex on full path */
                regex_return = regexec(&regex, file_name, 0, NULL, 0);
            }

            /* copy item to the filtered list */
            if (exclude) {
                /* user wants to exclude items that match, so copy everything that
                 * does not match */
                if (regex_return == REG_NOMATCH) {
                    mfu_flist_file_copy(flist, idx, dest);
                }
            }
            else {
                /* user wants to copy over any matching items */
                if (regex_return == 0) {
                    mfu_flist_file_copy(flist, idx, dest);
                }
            }

            /* free the basename */
            mfu_free(&base);
            mfu_path_delete(&pathname);

            /* get next item in our list */
            idx++;
        }

        /* summarize the filtered list */
        mfu_flist_summarize(dest);
    }

    /* return the filtered list */
    return dest;
}

void mfu_flist_file_copy(mfu_flist bsrc, uint64_t idx, mfu_flist bdst)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bsrc;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        flist_t* dstlist = (flist_t*) bdst;
        list_insert_copy(dstlist, elem);
    }
    return;
}

size_t mfu_flist_file_pack_size(mfu_flist bflist)
{
    flist_t* flist = (flist_t*) bflist;
    size_t size = list_elem_pack2_size(flist->detail, flist->max_file_name, NULL);
    return size;
}

size_t mfu_flist_file_pack(void* buf, mfu_flist bflist, uint64_t idx)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = list_get_elem(flist, idx);
    if (elem != NULL) {
        size_t size = list_elem_pack2(buf, flist->detail, flist->max_file_name, elem);
        return size;
    }
    return 0;
}

size_t mfu_flist_file_unpack(const void* buf, mfu_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;
    elem_t* elem = (elem_t*) MFU_MALLOC(sizeof(elem_t));
    size_t size = list_elem_unpack2(buf, elem);
    mfu_flist_insert_elem(flist, elem);
    return size;
}

int mfu_flist_summarize(mfu_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;
    list_compute_summary(flist);
    return MFU_SUCCESS;
}

/*****************************
 * Randomly hash items to processes by filename, then remove
 ****************************/

/* for given depth, hash directory name and map to processes to
 * test whether having all files in same directory on one process
 * matters */
size_t mfu_flist_distribute_map(mfu_flist list, char** buffer,
                                  mfu_flist_name_encode_fn encode,
                                  mfu_flist_map_fn map, void* args)
{
    uint64_t idx;

    /* get our rank and number of ranks in job */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* allocate arrays for alltoall */
    size_t bufsize = (size_t)ranks * sizeof(int);
    int* sendsizes  = (int*) MFU_MALLOC(bufsize);
    int* senddisps  = (int*) MFU_MALLOC(bufsize);
    int* sendoffset = (int*) MFU_MALLOC(bufsize);
    int* recvsizes  = (int*) MFU_MALLOC(bufsize);
    int* recvdisps  = (int*) MFU_MALLOC(bufsize);

    /* initialize sendsizes and offsets */
    int i;
    for (i = 0; i < ranks; i++) {
        sendsizes[i]  = 0;
        sendoffset[i] = 0;
    }

    /* compute number of bytes we'll send to each rank */
    size_t sendbytes = 0;
    uint64_t size = mfu_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        int dest = map(list, idx, ranks, args);

        /* TODO: check that pack size doesn't overflow int */
        /* total number of bytes we'll send to each rank and the total overall */
        size_t count = encode(NULL, list, idx, args);
        sendsizes[dest] += (int) count;
        sendbytes += count;
    }

    /* compute displacements */
    senddisps[0] = 0;
    for (i = 1; i < ranks; i++) {
        senddisps[i] = senddisps[i - 1] + sendsizes[i - 1];
    }

    /* allocate space */
    char* sendbuf = (char*) MFU_MALLOC(sendbytes);

    /* copy data into buffer */
    for (idx = 0; idx < size; idx++) {
        int dest = map(list, idx, ranks, args);

        /* identify region to be sent to rank */
        char* path = sendbuf + senddisps[dest] + sendoffset[dest];
        size_t count = encode(path, list, idx, args);

        /* TODO: check that pack size doesn't overflow int */
        /* bump up the offset for this rank */
        sendoffset[dest] += (int) count;
    }

    /* alltoall to specify incoming counts */
    MPI_Alltoall(sendsizes, 1, MPI_INT, recvsizes, 1, MPI_INT, MPI_COMM_WORLD);

    /* compute size of recvbuf and displacements */
    size_t recvbytes = 0;
    recvdisps[0] = 0;
    for (i = 0; i < ranks; i++) {
        recvbytes += (size_t) recvsizes[i];
        if (i > 0) {
            recvdisps[i] = recvdisps[i - 1] + recvsizes[i - 1];
        }
    }

    /* allocate recvbuf */
    char* recvbuf = (char*) MFU_MALLOC(recvbytes);

    /* alltoallv to send data */
    MPI_Alltoallv(
        sendbuf, sendsizes, senddisps, MPI_CHAR,
        recvbuf, recvsizes, recvdisps, MPI_CHAR, MPI_COMM_WORLD
    );

    /* free memory */
    mfu_free(&recvdisps);
    mfu_free(&recvsizes);
    mfu_free(&sendbuf);
    mfu_free(&sendoffset);
    mfu_free(&senddisps);
    mfu_free(&sendsizes);

    *buffer = recvbuf;
    return recvbytes;
}

/* given an input list and a map function pointer, call map function
 * for each item in list, identify new rank to send item to and then
 * exchange items among ranks and return new output list */
mfu_flist mfu_flist_remap(mfu_flist list, mfu_flist_map_fn map, const void* args)
{
    uint64_t idx;

    /* create new list as subset (actually will be a remapping of
     * input list */
    mfu_flist newlist = mfu_flist_subset(list);

    /* get our rank and number of ranks in job */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* allocate arrays for alltoall */
    size_t bufsize = (size_t)ranks * sizeof(int);
    int* sendsizes  = (int*) MFU_MALLOC(bufsize);
    int* senddisps  = (int*) MFU_MALLOC(bufsize);
    int* sendoffset = (int*) MFU_MALLOC(bufsize);
    int* recvsizes  = (int*) MFU_MALLOC(bufsize);
    int* recvdisps  = (int*) MFU_MALLOC(bufsize);

    /* initialize sendsizes and offsets */
    int i;
    for (i = 0; i < ranks; i++) {
        sendsizes[i]  = 0;
        sendoffset[i] = 0;
    }

    /* get number of elements in our local list */
    uint64_t size = mfu_flist_size(list);

    /* allocate space to record file-to-rank mapping */
    int* file2rank = (int*) MFU_MALLOC(size * sizeof(int));

    /* call map function for each item to identify its new rank,
     * and compute number of bytes we'll send to each rank */
    size_t sendbytes = 0;
    for (idx = 0; idx < size; idx++) {
        /* determine which rank we'll map this file to */
        int dest = map(list, idx, ranks, args);

        /* cache mapping so we don't have to compute it again
         * below while packing items for send */
        file2rank[idx] = dest;

        /* TODO: check that pack size doesn't overflow int */
        /* total number of bytes we'll send to each rank and the total overall */
        size_t count = mfu_flist_file_pack_size(list);
        sendsizes[dest] += (int) count;
        sendbytes += count;
    }

    /* compute send buffer displacements */
    senddisps[0] = 0;
    for (i = 1; i < ranks; i++) {
        senddisps[i] = senddisps[i - 1] + sendsizes[i - 1];
    }

    /* allocate space for send buffer */
    char* sendbuf = (char*) MFU_MALLOC(sendbytes);

    /* copy data into send buffer */
    for (idx = 0; idx < size; idx++) {
        /* determine which rank we mapped this file to */
        int dest = file2rank[idx];

        /* get pointer into send buffer and pack item */
        char* ptr = sendbuf + senddisps[dest] + sendoffset[dest];
        size_t count = mfu_flist_file_pack(ptr, list, idx);

        /* TODO: check that pack size doesn't overflow int */
        /* bump up the offset for this rank */
        sendoffset[dest] += (int) count;
    }

    /* alltoall to get our incoming counts */
    MPI_Alltoall(sendsizes, 1, MPI_INT, recvsizes, 1, MPI_INT, MPI_COMM_WORLD);

    /* compute size of recvbuf and displacements */
    size_t recvbytes = 0;
    recvdisps[0] = 0;
    for (i = 0; i < ranks; i++) {
        recvbytes += (size_t) recvsizes[i];
        if (i > 0) {
            recvdisps[i] = recvdisps[i - 1] + recvsizes[i - 1];
        }
    }

    /* allocate recvbuf */
    char* recvbuf = (char*) MFU_MALLOC(recvbytes);

    /* alltoallv to send data */
    MPI_Alltoallv(
        sendbuf, sendsizes, senddisps, MPI_CHAR,
        recvbuf, recvsizes, recvdisps, MPI_CHAR, MPI_COMM_WORLD
    );

    /* unpack items into new list */
    char* ptr = recvbuf;
    char* recvend = recvbuf + recvbytes;
    while (ptr < recvend) {
        size_t count = mfu_flist_file_unpack(ptr, newlist);
        ptr += count;
    }
    mfu_flist_summarize(newlist);

    /* free memory */
    mfu_free(&file2rank);
    mfu_free(&recvbuf);
    mfu_free(&recvdisps);
    mfu_free(&recvsizes);
    mfu_free(&sendbuf);
    mfu_free(&sendoffset);
    mfu_free(&senddisps);
    mfu_free(&sendsizes);

    /* return list to caller */
    return newlist;
}

/* map function to evenly spread list among ranks, using block allocation */
static int map_spread(mfu_flist flist, uint64_t idx, int ranks, void* args)
{
    /* compute global index of this item */
    uint64_t offset = mfu_flist_global_offset(flist);
    uint64_t global_idx = offset + idx;

    /* get global size of the list */
    uint64_t global_size = mfu_flist_global_size(flist);

    /* get whole number of items on each rank */
    uint64_t items_per_rank = global_size / (uint64_t)ranks;

    /* if global list size is not divisible by the number of ranks
     * then we need to use the remainder */
    uint64_t remainder = (global_size) - (items_per_rank * (uint64_t)ranks);

    /* If have a remainder, then we give one extra item to
     * to each rank starting from rank 0.  Compute the number
     * of items contained in this set of ranks.  There are
     * remainder such ranks. */
    uint64_t extra = remainder * (items_per_rank + 1);

    /* break up into two cases: if you are adding +1 or not 
     * calculate target rank */
    int target_rank;
    if (global_idx < extra) {
       /* the item falls in the set of ranks that all have an extra item */
       target_rank = (int) (global_idx / (items_per_rank + 1));
    } else {
       /* the item falls into the set of ranks beyond the set holding an extra item */
       target_rank = (int) (remainder + (global_idx - extra) / items_per_rank);
    }

    return target_rank;
}

/* This takes in a list, spreads it out evenly, and then returns the newly created 
 * list to the caller */
mfu_flist mfu_flist_spread(mfu_flist flist)
{
    /* remap files to evenly distribute items to processes */
    mfu_flist newlist = mfu_flist_remap(flist, map_spread, NULL);
    return newlist;
}

/* print information about a file given the index and rank (used in print_files) */
static void print_file(mfu_flist flist, uint64_t idx)
{
    /* store types as strings for print_file */
    char type_str_unknown[] = "UNK";
    char type_str_dir[]     = "DIR";
    char type_str_file[]    = "REG";
    char type_str_link[]    = "LNK";

    /* get filename */
    const char* file = mfu_flist_file_get_name(flist, idx);

    if (mfu_flist_have_detail(flist)) {
        /* get mode */
        mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

        //uint32_t uid = (uint32_t) mfu_flist_file_get_uid(flist, idx);
        //uint32_t gid = (uint32_t) mfu_flist_file_get_gid(flist, idx);
        uint64_t acc = mfu_flist_file_get_atime(flist, idx);
        uint64_t mod = mfu_flist_file_get_mtime(flist, idx);
        uint64_t cre = mfu_flist_file_get_ctime(flist, idx);
        uint64_t size = mfu_flist_file_get_size(flist, idx);
        const char* username  = mfu_flist_file_get_username(flist, idx);
        const char* groupname = mfu_flist_file_get_groupname(flist, idx);

        char access_s[30];
        char modify_s[30];
        char create_s[30];
        time_t access_t = (time_t) acc;
        time_t modify_t = (time_t) mod;
        time_t create_t = (time_t) cre;
        size_t access_rc = strftime(access_s, sizeof(access_s) - 1, "%FT%T", localtime(&access_t));
        //size_t modify_rc = strftime(modify_s, sizeof(modify_s) - 1, "%FT%T", localtime(&modify_t));
        size_t modify_rc = strftime(modify_s, sizeof(modify_s) - 1, "%b %e %Y %H:%M", localtime(&modify_t));
        size_t create_rc = strftime(create_s, sizeof(create_s) - 1, "%FT%T", localtime(&create_t));
        if (access_rc == 0 || modify_rc == 0 || create_rc == 0) {
            /* error */
            access_s[0] = '\0';
            modify_s[0] = '\0';
            create_s[0] = '\0';
        }

        char mode_format[11];
        mfu_format_mode(mode, mode_format);

        double size_tmp;
        const char* size_units;
        mfu_format_bytes(size, &size_tmp, &size_units);

        printf("%s %s %s %7.3f %2s %s %s\n",
               mode_format, username, groupname,
               size_tmp, size_units, modify_s, file
              );
#if 0
        printf("%s %s %s A%s M%s C%s %lu %s\n",
               mode_format, username, groupname,
               access_s, modify_s, create_s, (unsigned long)size, file
              );
        printf("Mode=%lx(%s) UID=%d(%s) GUI=%d(%s) Access=%s Modify=%s Create=%s Size=%lu File=%s\n",
               (unsigned long)mode, mode_format, uid, username, gid, groupname,
               access_s, modify_s, create_s, (unsigned long)size, file
              );
#endif
    }
    else {
        /* get type */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        char* type_str = type_str_unknown;
        if (type == MFU_TYPE_DIR) {
            type_str = type_str_dir;
        }
        else if (type == MFU_TYPE_FILE) {
            type_str = type_str_file;
        }
        else if (type == MFU_TYPE_LINK) {
            type_str = type_str_link;
        }

        printf("Type=%s File=%s\n",
               type_str, file
              );
    }
}

/* given a list of files print from the start to end of the list */
void mfu_flist_print(mfu_flist flist)
{
    /* number of items to print from start and end of list */
    uint64_t range = 10;

    /* allocate send and receive buffers */
    size_t pack_size = mfu_flist_file_pack_size(flist);
    size_t bufsize = 2 * range * pack_size;
    void* sendbuf = MFU_MALLOC(bufsize);
    void* recvbuf = MFU_MALLOC(bufsize);

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* identify the number of items we have, the total number,
     * and our offset in the global list */
    uint64_t count  = mfu_flist_size(flist);
    uint64_t total  = mfu_flist_global_size(flist);
    uint64_t offset = mfu_flist_global_offset(flist);

    /* count the number of items we'll send */
    int num = 0;
    uint64_t idx = 0;
    while (idx < count) {
        uint64_t global = offset + idx;
        if (global < range || (total - global) <= range) {
            num++;
        }
        idx++;
    }

    /* allocate arrays to store counts and displacements */
    int* counts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* disps  = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));

    /* tell rank 0 where the data is coming from */
    int bytes = num * (int)pack_size;
    MPI_Gather(&bytes, 1, MPI_INT, counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* pack items into sendbuf */
    idx = 0;
    char* ptr = (char*) sendbuf;
    while (idx < count) {
        uint64_t global = offset + idx;
        if (global < range || (total - global) <= range) {
            ptr += mfu_flist_file_pack(ptr, flist, idx);
        }
        idx++;
    }

    /* compute displacements and total bytes */
    int recvbytes = 0;
    if (rank == 0) {
        int i;
        disps[0] = 0;
        recvbytes += counts[0];
        for (i = 1; i < ranks; i++) {
            disps[i] = disps[i - 1] + counts[i - 1];
            recvbytes += counts[i];
        }
    }

    /* gather data to rank 0 */
    MPI_Gatherv(sendbuf, bytes, MPI_BYTE, recvbuf, counts, disps, MPI_BYTE, 0, MPI_COMM_WORLD);

    /* create temporary list to unpack items into */
    mfu_flist tmplist = mfu_flist_subset(flist);

    /* unpack items into new list */
    if (rank == 0) {
        ptr = (char*) recvbuf;
        char* end = ptr + recvbytes;
        while (ptr < end) {
            mfu_flist_file_unpack(ptr, tmplist);
            ptr += pack_size;
        }
    }

    /* summarize list */
    mfu_flist_summarize(tmplist);

    /* print files */
    if (rank == 0) {
        printf("\n");
        uint64_t tmpidx = 0;
        uint64_t tmpsize = mfu_flist_size(tmplist);
        while (tmpidx < tmpsize) {
            print_file(tmplist, tmpidx);
            tmpidx++;
            if (tmpidx == range && total > 2 * range) {
                /* going to have to leave some out */
                printf("\n<snip>\n\n");
            }
        }
        printf("\n");
    }

    /* free our temporary list */
    mfu_flist_free(&tmplist);

    /* free memory */
    mfu_free(&disps);
    mfu_free(&counts);
    mfu_free(&sendbuf);
    mfu_free(&recvbuf);

    return;
}
