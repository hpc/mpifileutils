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

/* Implements logic to walk directories to build an flist */

#define _GNU_SOURCE
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
#ifdef LUSTRE_SUPPORT
#include <sys/ioctl.h>
#include <lustre/lustre_user.h>
#endif /* LUSTRE_SUPPORT */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"
#include "bayer_flist_internal.h"
#include "strmap.h"

/****************************************
 * Globals
 ***************************************/

/* Need global variables during walk to record top directory
 * and file list */
static uint64_t CURRENT_NUM_DIRS;
static const char** CURRENT_DIRS;
static flist_t* CURRENT_LIST;
static int SET_DIR_PERMS;

/****************************************
 * Functions on types
 ***************************************/

/* given a mode_t from stat, return the corresponding BAYER filetype */
static bayer_filetype get_bayer_filetype(mode_t mode)
{
    /* set file type */
    bayer_filetype type;
    if (S_ISDIR(mode)) {
        type = BAYER_TYPE_DIR;
    }
    else if (S_ISREG(mode)) {
        type = BAYER_TYPE_FILE;
    }
    else if (S_ISLNK(mode)) {
        type = BAYER_TYPE_LINK;
    }
    else {
        /* unknown file type */
        type = BAYER_TYPE_UNKNOWN;
    }
    return type;
}

/* given path, return level within directory tree,
 * counts '/' characters assuming path is standardized
 * and absolute */
static int get_depth(const char* path)
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

/* append element to tail of linked list */
static void list_insert_elem(flist_t* flist, elem_t* elem)
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
    bayer_free(&flist->list_index);

    return;
}

/* insert a file given its mode and optional stat data */
static void list_insert_stat(flist_t* flist, const char* fpath, mode_t mode, const struct stat* sb)
{
    /* create new element to record file path, file type, and stat info */
    elem_t* elem = (elem_t*) BAYER_MALLOC(sizeof(elem_t));

    /* copy path */
    elem->file = BAYER_STRDUP(fpath);

    /* set depth */
    elem->depth = get_depth(fpath);

    /* set file type */
    elem->type = get_bayer_filetype(mode);

    /* copy stat info */
    if (sb != NULL) {
        elem->detail = 1;
        elem->mode  = (uint64_t) sb->st_mode;
        elem->uid   = (uint64_t) sb->st_uid;
        elem->gid   = (uint64_t) sb->st_gid;
        elem->atime = (uint64_t) sb->st_atime;
        elem->mtime = (uint64_t) sb->st_mtime;
        elem->ctime = (uint64_t) sb->st_ctime;
        elem->size  = (uint64_t) sb->st_size;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
        elem->atime_nsec = (uint64_t) sb->st_atimespec.tv_nsec;
        elem->ctime_nsec = (uint64_t) sb->st_ctimespec.tv_nsec;
        elem->mtime_nsec = (uint64_t) sb->st_mtimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
        elem->atime_nsec = (uint64_t) sb->st_atim.tv_nsec;
        elem->ctime_nsec = (uint64_t) sb->st_ctim.tv_nsec;
        elem->mtime_nsec = (uint64_t) sb->st_mtim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
        elem->atime_nsec = (uint64_t) sb->st_atime_n;
        elem->ctime_nsec = (uint64_t) sb->st_ctime_n;
        elem->mtime_nsec = (uint64_t) sb->st_mtime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
        elem->atime_nsec = (uint64_t) sb->st_uatime * 1000;
        elem->ctime_nsec = (uint64_t) sb->st_uctime * 1000;
        elem->mtime_nsec = (uint64_t) sb->st_umtime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
        elem->atime_nsec = (uint64_t) sb->st_atime_usec * 1000;
        elem->ctime_nsec = (uint64_t) sb->st_ctime_usec * 1000;
        elem->mtime_nsec = (uint64_t) sb->st_mtime_usec * 1000;
#else
        elem->atime_nsec = 0;
        elem->ctime_nsec = 0;
        elem->mtime_nsec = 0;
#endif

        /* TODO: link to user and group names? */
    }
    else {
        elem->detail = 0;
    }

    /* append element to tail of linked list */
    list_insert_elem(flist, elem);

    return;
}

/****************************************
 * Global counter and callbacks for LIBCIRCLE reductions
 ***************************************/

static uint64_t reduce_items;

static void reduce_init(void)
{
    CIRCLE_reduce(&reduce_items, sizeof(uint64_t));
}

static void reduce_exec(const void* buf1, size_t size1, const void* buf2, size_t size2)
{
    const uint64_t* a = (const uint64_t*) buf1;
    const uint64_t* b = (const uint64_t*) buf2;
    uint64_t val = a[0] + b[0];
    CIRCLE_reduce(&val, sizeof(uint64_t));
}

static void reduce_fini(const void* buf, size_t size)
{
    /* get current time */
    time_t walk_start_t = time(NULL);
    if (walk_start_t == (time_t) - 1) {
        /* TODO: ERROR! */
    }

    /* format timestamp string */
    char walk_s[30];
    size_t rc = strftime(walk_s, sizeof(walk_s) - 1, "%FT%T", localtime(&walk_start_t));
    if (rc == 0) {
        walk_s[0] = '\0';
    }

    /* get result of reduction */
    const uint64_t* a = (const uint64_t*) buf;
    unsigned long long val = (unsigned long long) a[0];

    /* print status to stdout */
    printf("%s: Items walked %llu ...\n", walk_s, val);
    fflush(stdout);
}

#ifdef LUSTRE_SUPPORT
/****************************************
 * Walk directory tree using Lustre's MDS stat
 ***************************************/

static void lustre_stripe_info(void* buf)
{
    struct lov_user_md* md = &((struct lov_user_mds_data*) buf)->lmd_lmm;

    uint32_t pattern = (uint32_t) md->lmm_pattern;
    if (pattern != LOV_PATTERN_RAID0) {
        /* we don't know how to interpret this pattern */
        return;
    }

    /* get stripe info for file */
    uint32_t size   = (uint32_t) md->lmm_stripe_size;
    uint16_t count  = (uint16_t) md->lmm_stripe_count;
    uint16_t offset = (uint16_t) md->lmm_stripe_offset;

    uint16_t i;
    if (md->lmm_magic == LOV_USER_MAGIC_V1) {
        struct lov_user_md_v1* md1 = (struct lov_user_md_v1*) md;
        for (i = 0; i < count; i++) {
            uint32_t idx = md1->lmm_objects[i].l_ost_idx;
        }
    }
    else if (md->lmm_magic == LOV_USER_MAGIC_V3) {
        struct lov_user_md_v3* md3 = (struct lov_user_md_v3*) md;
        for (i = 0; i < count; i++) {
            uint32_t idx = md3->lmm_objects[i].l_ost_idx;
        }
    }
    else {
        /* unknown magic number */
    }

    return;
}

static int lustre_mds_stat(int fd, char* fname, struct stat* sb)
{
    /* allocate a buffer */
    size_t pathlen = strlen(fname) + 1;
    size_t bufsize = pathlen;
    //size_t datasize = sizeof(lstat_t) + lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
    size_t datasize = sizeof(struct lov_user_mds_data) + LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
    if (datasize > bufsize) {
        bufsize = datasize;
    }
    char* buf = (char*) BAYER_MALLOC(bufsize);

    /* Usage: ioctl(fd, IOC_MDC_GETFILEINFO, buf)
     * IN: fd open file descriptor of file's parent directory
     * IN: buf file name (no path)
     * OUT: buf lstat_t */
    strcpy(buf, fname);
    //  strncpy(buf, fname, bufsize);

    int ret = ioctl(fd, IOC_MDC_GETFILEINFO, buf);

    /* Copy lstat_t to struct stat */
    if (ret != -1) {
        lstat_t* ls = (lstat_t*) & ((struct lov_user_mds_data*) buf)->lmd_st;
        sb->st_dev     = ls->st_dev;
        sb->st_ino     = ls->st_ino;
        sb->st_mode    = ls->st_mode;
        sb->st_nlink   = ls->st_nlink;
        sb->st_uid     = ls->st_uid;
        sb->st_gid     = ls->st_gid;
        sb->st_rdev    = ls->st_rdev;
        sb->st_size    = ls->st_size;
        sb->st_blksize = ls->st_blksize;
        sb->st_blocks  = ls->st_blocks;
        sb->st_atime   = ls->st_atime;
        sb->st_mtime   = ls->st_mtime;
        sb->st_ctime   = ls->st_ctime;

        lustre_stripe_info(buf);
    }
    else {
        printf("ioctl errno=%d %s\n", errno, strerror(errno));
    }

    /* free the buffer */
    bayer_free(&buf);

    return ret;
}

static void walk_lustrestat_process_dir(char* dir, CIRCLE_handle* handle)
{
    /* TODO: may need to try these functions multiple times */
    DIR* dirp = bayer_opendir(dir);

    if (! dirp) {
        /* TODO: print error */
    }
    else {
        /* get file descriptor for open directory */
        int fd = dirfd(dirp);
        if (fd < 0) {
            /* TODO: print error */
            goto done;
        }

        /* Read all directory entries */
        while (1) {
            /* read next directory entry */
            struct dirent* entry = bayer_readdir(dirp);
            if (entry == NULL) {
                break;
            }

            /* process component, unless it's "." or ".." */
            char* name = entry->d_name;
            if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                size_t len = strlen(dir) + 1 + strlen(name) + 1;
                if (len < sizeof(newpath)) {
                    /* build full path to item */
                    strcpy(newpath, dir);
                    strcat(newpath, "/");
                    strcat(newpath, name);

                    /* stat item */
                    mode_t mode;
                    int have_mode = 0;
                    struct stat st;
                    int status = lustre_mds_stat(fd, name, &st);
                    if (status != -1) {
                        have_mode = 1;
                        mode = st.st_mode;
                        list_insert_stat(CURRENT_LIST, newpath, mode, &st);
                    }
                    else {
                        /* error */
                    }

                    /* increment our item count */
                    reduce_items++;

                    /* recurse into directories */
                    if (have_mode && S_ISDIR(mode)) {
                        handle->enqueue(newpath);
                    }
                }
                else {
                    /* TODO: print error in correct format */
                    /* name is too long */
                    printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
                    fflush(stdout);
                }
            }
        }
    }

done:
    bayer_closedir(dirp);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_lustrestat_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        const char* path = CURRENT_DIRS[i];

        /* stat top level item */
        struct stat st;
        int status = bayer_lstat(path, &st);
        if (status != 0) {
            /* TODO: print error */
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        list_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

        /* recurse into directory */
        if (S_ISDIR(st.st_mode)) {
            walk_lustrestat_process_dir(path, handle);
        }
    }

    return;
}

/** Callback given to process the dataset. */
static void walk_lustrestat_process(CIRCLE_handle* handle)
{
    /* in this case, only items on queue are directories */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);
    walk_lustrestat_process_dir(path, handle);
    return;
}

#endif /* LUSTRE_SUPPORT */

/****************************************
 * Walk directory tree using stat at top level and getdents system call
 ***************************************/

struct linux_dirent {
    long           d_ino;
    off_t          d_off;
    unsigned short d_reclen;
    char           d_name[];
};

//#define BUF_SIZE 10*1024*1024
#define BUF_SIZE 128*1024U

static void walk_getdents_process_dir(const char* dir, CIRCLE_handle* handle)
{
    char buf[BUF_SIZE];

    /* TODO: may need to try these functions multiple times */
    int fd = bayer_open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        /* print error */
        BAYER_LOG(BAYER_LOG_ERR, "Failed to open directory for reading: %s", dir);
        return;
    }

    /* Read all directory entries */
    while (1) {
        /* execute system call to get block of directory entries */
        int nread = syscall(SYS_getdents, fd, buf, (int) BUF_SIZE);
        if (nread == -1) {
            BAYER_LOG(BAYER_LOG_ERR, "syscall to getdents failed when reading %s (errno=%d %s)", dir, errno, strerror(errno));
            break;
        }

        /* bail out if we're done */
        if (nread == 0) {
            break;
        }

        /* otherwise, we read some bytes, so process each record */
        int bpos = 0;
        while (bpos < nread) {
            /* get pointer to current record */
            struct linux_dirent* d = (struct linux_dirent*)(buf + bpos);

            /* get name of directory item, skip d_ino== 0, ".", and ".." entries */
            char* name = d->d_name;
            if (d->d_ino != 0 && (strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* check whether we can define path to item:
                 * <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                size_t len = strlen(dir) + 1 + strlen(name) + 1;
                if (len < sizeof(newpath)) {
                    /* build full path to item */
                    strcpy(newpath, dir);
                    strcat(newpath, "/");
                    strcat(newpath, name);

                    /* get type of item */
                    char d_type = *(buf + bpos + d->d_reclen - 1);

#if 0
                    printf("%-10s ", (d_type == DT_REG) ?  "regular" :
                           (d_type == DT_DIR) ?  "directory" :
                           (d_type == DT_FIFO) ? "FIFO" :
                           (d_type == DT_SOCK) ? "socket" :
                           (d_type == DT_LNK) ?  "symlink" :
                           (d_type == DT_BLK) ?  "block dev" :
                           (d_type == DT_CHR) ?  "char dev" : "???");

                    printf("%4d %10lld  %s\n", d->d_reclen,
                           (long long) d->d_off, (char*) d->d_name);
#endif

                    /* TODO: this is hacky, would be better to create list elem directly */
                    /* determine type of item (just need to set bits in mode
                     * that get_bayer_filetype checks for) */
                    mode_t mode = 0;
                    if (d_type == DT_REG) {
                        mode |= S_IFREG;
                    }
                    else if (d_type == DT_DIR) {
                        mode |= S_IFDIR;
                    }
                    else if (d_type == DT_LNK) {
                        mode |= S_IFLNK;
                    }

                    /* insert a record for this item into our list */
                    list_insert_stat(CURRENT_LIST, newpath, mode, NULL);

                    /* increment our item count */
                    reduce_items++;

                    /* recurse on directory if we have one */
                    if (d_type == DT_DIR) {
                        handle->enqueue(newpath);
                    }
                }
                else {
                    BAYER_LOG(BAYER_LOG_ERR, "Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
                }
            }

            /* advance to next record */
            bpos += d->d_reclen;
        }
    }

    bayer_close(dir, fd);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_getdents_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        const char* path = CURRENT_DIRS[i];

        /* stat top level item */
        struct stat st;
        int status = bayer_lstat(path, &st);
        if (status != 0) {
            /* TODO: print error */
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        list_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

        /* recurse into directory */
        if (S_ISDIR(st.st_mode)) {
            walk_getdents_process_dir(path, handle);
        }
    }

    return;
}

/** Callback given to process the dataset. */
static void walk_getdents_process(CIRCLE_handle* handle)
{
    /* in this case, only items on queue are directories */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);
    walk_getdents_process_dir(path, handle);
    return;
}

/****************************************
 * Walk directory tree using stat at top level and readdir
 ***************************************/

static void walk_readdir_process_dir(char* dir, CIRCLE_handle* handle)
{
    /* TODO: may need to try these functions multiple times */
    DIR* dirp = bayer_opendir(dir);

    /* if there is a permissions error and the usr read & execute are being turned
     * on when walk_stat=0 then catch the permissions error and turn the bits on */
    if (dirp == NULL) {
        if (errno == EACCES && SET_DIR_PERMS) {
            struct stat st;
            bayer_lstat(dir, &st);
            // turn on the usr read & execute bits
            st.st_mode |= S_IRUSR;
            st.st_mode |= S_IXUSR;
            bayer_chmod(dir, st.st_mode);
            dirp = bayer_opendir(dir);
            if (dirp == NULL) {
                if (errno == EACCES) {
                    printf("can't open directory at this time\n");
                }
            }
        }
    }

    if (! dirp) {
        /* TODO: print error */
    }
    else {
        /* Read all directory entries */
        while (1) {
            /* read next directory entry */
            struct dirent* entry = bayer_readdir(dirp);
            if (entry == NULL) {
                break;
            }

            /* process component, unless it's "." or ".." */
            char* name = entry->d_name;
            if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                size_t len = strlen(dir) + 1 + strlen(name) + 1;
                if (len < sizeof(newpath)) {
                    /* build full path to item */
                    strcpy(newpath, dir);
                    strcat(newpath, "/");
                    strcat(newpath, name);

#ifdef _DIRENT_HAVE_D_TYPE
                    /* record info for item */
                    mode_t mode;
                    int have_mode = 0;
                    if (entry->d_type != DT_UNKNOWN) {
                        /* we can read object type from directory entry */
                        have_mode = 1;
                        mode = DTTOIF(entry->d_type);
                        list_insert_stat(CURRENT_LIST, newpath, mode, NULL);
                    }
                    else {
                        /* type is unknown, we need to stat it */
                        struct stat st;
                        int status = bayer_lstat(newpath, &st);
                        if (status == 0) {
                            have_mode = 1;
                            mode = st.st_mode;
                            list_insert_stat(CURRENT_LIST, newpath, mode, &st);
                        }
                        else {
                            /* error */
                        }
                    }

                    /* increment our item count */
                    reduce_items++;

                    /* recurse into directories */
                    if (have_mode && S_ISDIR(mode)) {
                        handle->enqueue(newpath);
                    }
#endif
                }
                else {
                    /* TODO: print error in correct format */
                    /* name is too long */
                    printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
                    fflush(stdout);
                }
            }
        }
    }

    bayer_closedir(dirp);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_readdir_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        char* path = CURRENT_DIRS[i];

        /* stat top level item */
        struct stat st;
        int status = bayer_lstat(path, &st);
        if (status != 0) {
            /* TODO: print error */
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        list_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

        /* recurse into directory */
        if (S_ISDIR(st.st_mode)) {
            walk_readdir_process_dir(path, handle);
        }
    }

    return;
}

/** Callback given to process the dataset. */
static void walk_readdir_process(CIRCLE_handle* handle)
{
    /* in this case, only items on queue are directories */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);
    walk_readdir_process_dir(path, handle);
    return;
}

/****************************************
 * Walk directory tree using stat on every object
 ***************************************/

static void walk_stat_process_dir(char* dir, CIRCLE_handle* handle)
{
    /* TODO: may need to try these functions multiple times */
    DIR* dirp = bayer_opendir(dir);

    if (! dirp) {
        /* TODO: print error */
    }
    else {
        while (1) {
            /* read next directory entry */
            struct dirent* entry = bayer_readdir(dirp);
            if (entry == NULL) {
                break;
            }

            /* We don't care about . or .. */
            char* name = entry->d_name;
            if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                size_t len = strlen(dir) + 1 + strlen(name) + 1;
                if (len < sizeof(newpath)) {
                    /* build full path to item */
                    strcpy(newpath, dir);
                    strcat(newpath, "/");
                    strcat(newpath, name);

                    /* add item to queue */
                    handle->enqueue(newpath);
                }
                else {
                    /* TODO: print error in correct format */
                    /* name is too long */
                    printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
                    fflush(stdout);
                }
            }
        }
    }

    bayer_closedir(dirp);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_stat_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        /* we'll call stat on every item */
        const char* path = CURRENT_DIRS[i];
        handle->enqueue(path);
    }
}

/** Callback given to process the dataset. */
static void walk_stat_process(CIRCLE_handle* handle)
{
    /* get path from queue */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);

    /* stat item */
    struct stat st;
    int status = bayer_lstat(path, &st);
    if (status != 0) {
        /* print error */
        return;
    }

    /* increment our item count */
    reduce_items++;

    /* TODO: filter items by stat info */

    /* record info for item in list */
    list_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

    /* recurse into directory */
    if (S_ISDIR(st.st_mode)) {
        /* before more processing check if SET_DIR_PERMS is set,
         * and set usr read and execute bits if need be */
        if (SET_DIR_PERMS) {
            /* use masks to check if usr_r and usr_x are already on */
            long usr_r_mask = 1 << 8;
            long usr_x_mask = 1 << 6;
            /* turn on the usr read & execute bits if they are not already on*/
            if (!((usr_r_mask & st.st_mode) && (usr_x_mask & st.st_mode))) {
                st.st_mode |= S_IRUSR;
                st.st_mode |= S_IXUSR;
                bayer_chmod(path, st.st_mode);
            }
        }
        /* TODO: check that we can recurse into directory */
        walk_stat_process_dir(path, handle);
    }

    return;
}

/* Set up and execute directory walk */
void bayer_flist_walk_path(const char* dirpath, int use_stat, int dir_permissions, bayer_flist bflist)
{
    bayer_flist_walk_paths(1, &dirpath, use_stat, dir_permissions, bflist);
    return;
}

/* Set up and execute directory walk */
void bayer_flist_walk_paths(uint64_t num_paths, const char** paths, int use_stat, int dir_permissions, bayer_flist bflist)
{
    /* report walk count, time, and rate */
    double start_walk = MPI_Wtime();

    /* if dir_permission is set to 1 then set global variable */
    if (dir_permissions) {
        SET_DIR_PERMS = 1;
    }
    else {
        SET_DIR_PERMS = 0;
    }

    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* print message to user that we're starting */
    if (bayer_debug_level >= BAYER_LOG_VERBOSE && bayer_rank == 0) {
        uint64_t i;
        for (i = 0; i < num_paths; i++) {
            time_t walk_start_t = time(NULL);
            if (walk_start_t == (time_t) - 1) {
                /* TODO: ERROR! */
            }
            char walk_s[30];
            size_t rc = strftime(walk_s, sizeof(walk_s) - 1, "%FT%T", localtime(&walk_start_t));
            if (rc == 0) {
                walk_s[0] = '\0';
            }
            printf("%s: Walking %s\n", walk_s, paths[i]);
        }
        fflush(stdout);
    }

    /* initialize libcircle */
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL);

    /* set libcircle verbosity level */
    enum CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    CIRCLE_enable_logging(loglevel);

    /* TODO: check that paths is not NULL */
    /* TODO: check that each path is within limits */

    /* set some global variables to do the file walk */
    CURRENT_NUM_DIRS = num_paths;
    CURRENT_DIRS     = paths;
    CURRENT_LIST     = flist;

    /* we lookup users and groups first in case we can use
     * them to filter the walk */
    flist->detail = 0;
    if (use_stat) {
        flist->detail = 1;
        if (flist->have_users == 0) {
            bayer_flist_usrgrp_get_users(flist);
        }
        if (flist->have_groups == 0) {
            bayer_flist_usrgrp_get_groups(flist);
        }
    }

    /* register callbacks */
    if (use_stat) {
        /* walk directories by calling stat on every item */
        CIRCLE_cb_create(&walk_stat_create);
        CIRCLE_cb_process(&walk_stat_process);
        //        CIRCLE_cb_create(&walk_lustrestat_create);
        //        CIRCLE_cb_process(&walk_lustrestat_process);
    }
    else {
        /* walk directories using file types in readdir */
        CIRCLE_cb_create(&walk_readdir_create);
        CIRCLE_cb_process(&walk_readdir_process);
        //        CIRCLE_cb_create(&walk_getdents_create);
        //        CIRCLE_cb_process(&walk_getdents_process);
    }

    /* prepare callbacks and initialize variables for reductions */
    reduce_items = 0;
    CIRCLE_cb_reduce_init(&reduce_init);
    CIRCLE_cb_reduce_op(&reduce_exec);
    CIRCLE_cb_reduce_fini(&reduce_fini);

    /* run the libcircle job */
    CIRCLE_begin();
    CIRCLE_finalize();

    /* compute global summary */
    bayer_flist_summarize(bflist);

    double end_walk = MPI_Wtime();

    /* report walk count, time, and rate */
    if (bayer_debug_level >= BAYER_LOG_VERBOSE && bayer_rank == 0) {
        uint64_t all_count = bayer_flist_global_size(bflist);
        double time_diff = end_walk - start_walk;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        printf("Walked %lu files in %f seconds (%f files/sec)\n",
               all_count, time_diff, rate
              );
    }

    return;
}

/* Given an input file list, stat each file and enqueue details
 * in output file list, skip entries excluded by skip function
 * and skip args */
void bayer_flist_stat(
  bayer_flist input_flist,
  bayer_flist flist,
  bayer_flist_skip_fn skip_fn,
  void *skip_args)
{
    uint64_t idx;
    struct stat st;
    int status;
    const char* name;
    flist_t* file_list = (flist_t*)flist;

    /* we will stat all items in output list */
    file_list->detail = 1;

    /* get user data if needed */
    if (file_list->have_users == 0) {
        bayer_flist_usrgrp_get_users(flist);
    }

    /* get groups data if needed */
    if (file_list->have_groups == 0) {
        bayer_flist_usrgrp_get_groups(flist);
    }

    /* step through each item in input list and stat it */
    for (idx = 0; idx < bayer_flist_size(input_flist); idx++) {
        /* get name of item */
        name = bayer_flist_file_get_name(input_flist, idx);

        /* check whether we should skip this item */
        if (skip_fn != NULL && skip_fn(name, skip_args)) {
            /* skip this file, don't include it in new list */
            BAYER_LOG(BAYER_LOG_INFO, "skip %s");
            continue;
        }

        /* stat the item */
        status = bayer_lstat(name, &st);
        if (status != 0) {
            BAYER_LOG(BAYER_LOG_ERR, "bayer_lstat(): %d", status);
            continue;
        }

        /* insert item into output list */
        list_insert_stat(flist, name, st.st_mode, &st);
    }

    /* compute global summary */
    bayer_flist_summarize(flist);
}
