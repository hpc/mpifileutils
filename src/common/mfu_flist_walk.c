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
#include "mfu.h"
#include "mfu_flist_internal.h"
#include "strmap.h"

/****************************************
 * Globals
 ***************************************/

/* Need global variables during walk to record top directory
 * and file list */
static uint64_t CURRENT_NUM_DIRS;
static const char** CURRENT_DIRS;
static flist_t* CURRENT_LIST;
static flist_t* HARDLINKS_TMP_LIST;
static inodes_hardlink_map_t* HARDLINKS_INODES_MAP;
static int SET_DIR_PERMS;
static int REMOVE_FILES;
static int DEREFERENCE;
static int WALK_RESULT = 0;
static int NO_ATIME;
static mfu_file_t** CURRENT_PFILE;

/****************************************
 * Global counter and callbacks for LIBCIRCLE reductions
 ***************************************/

static double   reduce_start;
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
    /* get result of reduction */
    const uint64_t* a = (const uint64_t*) buf;
    unsigned long long val = (unsigned long long) a[0];

    /* get current time */
    double now = MPI_Wtime();

    /* compute walk rate */
    double rate = 0.0;
    double secs = now - reduce_start;
    if (secs > 0.0) {
        rate = (double)val / secs;
    }

    /* print status to stdout */
    MFU_LOG(MFU_LOG_INFO, "Walked %llu items in %.3lf secs (%.3lf items/sec) ...", val, secs, rate);
}

/****************************************
 * Global helper functions
 ***************************************/

/** Build a full path from a dirname and basename in the form:
 * <dir> + '/' + <name> + '/0'
 * up to path_len long.
 * Returns 0 on success and -1 if the new path is too long. */
static int build_path(char* path, size_t path_len, const char* dir, const char* name)
{
    size_t dir_len = strlen(dir);

    /* Only separate with a '/' if the dir does not have a trailing slash.
     * Builds a path to at most path_len long. */
    int new_len;
    if ((dir_len > 0) && (dir[dir_len - 1] == '/')) {
        new_len = snprintf(path, path_len, "%s%s", dir, name);
    } else {
        new_len = snprintf(path, path_len, "%s/%s", dir, name);
    }
    if (new_len > path_len) {
        MFU_LOG(MFU_LOG_ERR, "Path name is too long, %lu chars exceeds limit %lu: '%s/%s'",
                new_len, path_len, dir, name);
        WALK_RESULT = -1;
        return -1;
    }

    return 0;
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
    int flags = O_RDONLY | O_DIRECTORY;
    char buf[BUF_SIZE];

    if (NO_ATIME)
        flags |= O_NOATIME;

    /* TODO: may need to try these functions multiple times */
    mfu_file_t* mfu_file = *CURRENT_PFILE;
    mfu_file_open(dir, flags, mfu_file);
    if (mfu_file->fd == -1) {
        /* print error */
        MFU_LOG(MFU_LOG_ERR, "Failed to open directory for reading: `%s' (errno=%d %s)", dir, errno, strerror(errno));
        WALK_RESULT = -1;
        return;
    }

#if !defined(SYS_getdents) && defined(SYS_getdents64)
#define SYS_getdents SYS_getdents64
#endif
    /* Read all directory entries */
    while (1) {
        /* execute system call to get block of directory entries */
        int nread = syscall(SYS_getdents, mfu_file->fd, buf, (int) BUF_SIZE);
        if (nread == -1) {
            MFU_LOG(MFU_LOG_ERR, "syscall to getdents failed when reading `%s' (errno=%d %s)", dir, errno, strerror(errno));
            WALK_RESULT = -1;
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
                int rc = build_path(newpath, CIRCLE_MAX_STRING_LEN, dir, name);
                if (rc == 0) {
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
                     * that get_mfu_filetype checks for) */
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
                    mfu_flist_insert_stat(CURRENT_LIST, newpath, mode, NULL);

                    /* recurse on directory if we have one */
                    if (d_type == DT_DIR) {
                        handle->enqueue(newpath);
                    } else {
                        /* increment our item count */
                        reduce_items++;
                    }
                }
            }

            /* advance to next record */
            bpos += d->d_reclen;
        }
    }

    mfu_file_close(dir, mfu_file);

    return;
}

/****************************************
 * Walk directory tree using stat at top level and readdir
 ***************************************/

static void walk_readdir_process_dir(const char* dir, CIRCLE_handle* handle)
{
    /* TODO: may need to try these functions multiple times */
    mfu_file_t* mfu_file = *CURRENT_PFILE;
    DIR* dirp = mfu_file_opendir(dir, mfu_file);

    /* if there is a permissions error and the usr read & execute are being turned
     * on when walk_stat=0 then catch the permissions error and turn the bits on */
    if (dirp == NULL) {
        if (errno == EACCES && SET_DIR_PERMS) {
            struct stat st;
            mfu_file_t* mfu_file = *CURRENT_PFILE;
            int status = mfu_file_lstat(dir, &st, mfu_file);
            // TODO handle status != 0
            // turn on the usr read & execute bits
            st.st_mode |= S_IRUSR;
            st.st_mode |= S_IXUSR;
            mfu_file_chmod(dir, st.st_mode, mfu_file);
            dirp = mfu_file_opendir(dir, mfu_file);
        }
    }

    if (! dirp) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open directory with opendir: '%s' (errno=%d %s)",
                dir, errno, strerror(errno));
        WALK_RESULT = -1;
    }
    else {
        /* Read all directory entries */
        while (1) {
            /* read next directory entry */
            struct dirent* entry = mfu_file_readdir(dirp, mfu_file);
            if (entry == NULL) {
                break;
            }

            /* process component, unless it's "." or ".." */
            char* name = entry->d_name;
            if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                int rc = build_path(newpath, CIRCLE_MAX_STRING_LEN, dir, name);
                if (rc == 0) {
#ifdef _DIRENT_HAVE_D_TYPE
                    /* record info for item */
                    mode_t mode;
                    int have_mode = 0;
                    if (entry->d_type != DT_UNKNOWN) {
                        /* unlink files here if remove option is on,
                         * and dtype is known without a stat */
                        if (REMOVE_FILES && (entry->d_type != DT_DIR)) {
                            mfu_file_unlink(newpath, mfu_file);
                        } else {
                            /* we can read object type from directory entry */
                            have_mode = 1;
                            mode = DTTOIF(entry->d_type);
                            mfu_flist_insert_stat(CURRENT_LIST, newpath, mode, NULL);
                        }
                    }
                    else {
                        /* type is unknown, we need to stat it */
                        struct stat st;
                        int status = mfu_file_lstat(newpath, &st, mfu_file);
                        if (status == 0) {
                            have_mode = 1;
                            mode = st.st_mode;
                            /* unlink files here if remove option is on,
                             * and stat was necessary to get type */
                            if (REMOVE_FILES && !S_ISDIR(st.st_mode)) {
                                mfu_file_unlink(newpath, mfu_file);
                            } else {
                                mfu_flist_insert_stat(CURRENT_LIST, newpath, mode, &st);
                            }
                        }
                        else {
                            MFU_LOG(MFU_LOG_ERR, "Failed to stat: '%s' (errno=%d %s)",
                                    newpath, errno, strerror(errno));
                            WALK_RESULT = -1;
                        }
                    }

                    /* recurse into directories */
                    if (have_mode && S_ISDIR(mode)) {
                        handle->enqueue(newpath);
                    } else {
                        /* increment our item count */
                        reduce_items++;
                    }
#endif
                }
            }
        }
    }

    mfu_file_closedir(dirp, mfu_file);

    return;
}

/** Call back given to initialize the dataset. */
static void walk_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        const char* path = CURRENT_DIRS[i];

        /* stat top level item */
        struct stat st;
        mfu_file_t* mfu_file = *CURRENT_PFILE;
        int status = mfu_file_lstat(path, &st, mfu_file);
        if (status != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to stat: '%s' (errno=%d %s)",
                    path, errno, strerror(errno));
            WALK_RESULT = -1;
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        mfu_flist_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

        /* recurse into directory */
        if (S_ISDIR(st.st_mode)) {
            if (NO_ATIME) {
                // walk directories without updating the file last access time
                walk_getdents_process_dir(path, handle);
            } else {
                //walk directories using file types in readdir
                walk_readdir_process_dir(path, handle);
            }
        }
    }

    return;
}

/** Callback given to process the dataset. */
static void walk_process(CIRCLE_handle* handle)
{
    /* in this case, only items on queue are directories */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);
    if (NO_ATIME) {
        // walk directories without updating the file last access time
        walk_getdents_process_dir(path, handle);
    } else {
        //walk directories using file types in readdir
        walk_readdir_process_dir(path, handle);
    }
    reduce_items++;
    return;
}

/****************************************
 * Walk directory tree using stat on every object
 ***************************************/

static void walk_stat_process_dir(char* dir, CIRCLE_handle* handle)
{
    /* TODO: may need to try these functions multiple times */
    mfu_file_t* mfu_file = *CURRENT_PFILE;
    DIR* dirp = mfu_file_opendir(dir, mfu_file);

    if (! dirp) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open directory with opendir: '%s' (errno=%d %s)",
                dir, errno, strerror(errno));
        WALK_RESULT = -1;
    }
    else {
        while (1) {
            /* read next directory entry */
            struct dirent* entry = mfu_file_readdir(dirp, mfu_file);
            if (entry == NULL) {
                break;
            }

            /* We don't care about . or .. */
            char* name = entry->d_name;
            if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
                /* <dir> + '/' + <name> + '/0' */
                char newpath[CIRCLE_MAX_STRING_LEN];
                int rc = build_path(newpath, CIRCLE_MAX_STRING_LEN, dir, name);
                if (rc == 0) {
                    /* add item to queue */
                    handle->enqueue(newpath);
                }
            }
        }
    }
    mfu_file_closedir(dirp, mfu_file);
    return;
}

/** Call back given to initialize the dataset. */
static void walk_stat_create(CIRCLE_handle* handle)
{
    uint64_t i;
    for (i = 0; i < CURRENT_NUM_DIRS; i++) {
        /* we'll call stat on every item */
        const char* path = CURRENT_DIRS[i];
        handle->enqueue((char*)path);
    }
}

/* allocate and initialize a new inodes map */
inodes_hardlink_map_t* inodes_map_new()
{
    /* allocate memory for map, cast it to handle, initialize and return */
    inodes_hardlink_map_t* map = (inodes_hardlink_map_t*) MFU_MALLOC(sizeof(inodes_hardlink_map_t));

    map->inodes = NULL;
    map->count  = 0;
    map->cap    = 0;

    return map;
}

/* free memory of inodes map */
inodes_hardlink_map_t* inodes_map_free(inodes_hardlink_map_t** map)
{
    mfu_free(&(*map)->inodes);
    mfu_free(map);
}

/* add new element to running list index, allocates additional
 * capactiy for index if needed */
static void inodes_map_insert(inodes_hardlink_map_t* map, uint64_t inode)
{
    /* if we have no capacity for the index,
     * initialize with a small array */
    uint64_t cap = map->cap;
    if (cap == 0) {
        /* have no index at all, initialize it */
        uint64_t new_capacity = 32;
        size_t index_size = new_capacity * sizeof(uint64_t);
        map->inodes = (uint64_t*) MFU_MALLOC(index_size);
        map->cap = new_capacity;
    }

    map->count++;

    /* check that our index has space before we add it */
    uint64_t count = map->count;
    if (count == cap) {
        /* we have exhausted the current capacity of the index array,
         * allocate a new memory region that is double the size */
        uint64_t new_capacity = cap * 2;
        size_t index_size = new_capacity * sizeof(uint64_t);
        uint64_t* new_inodes = (uint64_t*) MFU_MALLOC(index_size);

        /* copy over existing list */
        memcpy(new_inodes, map->inodes, count * sizeof(uint64_t));

        /* free the old index memory and assign the new one */
        mfu_free(&map->inodes);
        map->inodes = new_inodes;
        map->cap   = new_capacity;
    }

    /* append the item to the index */
    map->inodes[count - 1] = inode;

    return;
}

/** Callback given to process the dataset. */
static void walk_stat_process(CIRCLE_handle* handle)
{
    /* get path from queue */
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);
    mfu_file_t* mfu_file = *CURRENT_PFILE;

    /* stat item */
    struct stat st;
    int status;
    if (DEREFERENCE) {
        /* if symlink, stat the symlink value */
        status = mfu_file_stat(path, &st, mfu_file);
    } else {
        /* if symlink, stat the symlink itself */
        status = mfu_file_lstat(path, &st, mfu_file);
    }
    if (status != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to stat: '%s' (errno=%d %s)",
                path, errno, strerror(errno));
        WALK_RESULT = -1;
        return;
    }

    /* increment our item count */
    reduce_items++;

    /* TODO: filter items by stat info */

    if (REMOVE_FILES && !S_ISDIR(st.st_mode)) {
        mfu_file_unlink(path, mfu_file);
    } else {
        if (S_ISREG(st.st_mode) && st.st_nlink > 1) {
            /* record info for item in temporary hardlinks list and inodes map */
            mfu_flist_insert_stat(HARDLINKS_TMP_LIST, path, st.st_mode, &st);
            inodes_map_insert(HARDLINKS_INODES_MAP, (uint64_t)st.st_ino);
        } else
            /* record info for item in list */
            mfu_flist_insert_stat(CURRENT_LIST, path, st.st_mode, &st);
    }

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
                mfu_file_chmod(path, st.st_mode, mfu_file);
            }
        }
        /* TODO: check that we can recurse into directory */
        walk_stat_process_dir(path, handle);
    }
    return;
}

/* sort elements in flist and inodes by name and place them in sorted_list and
 * sorted_inodes respectively. */
static void walk_hardlinks_sort_names(flist_t* flist, inodes_hardlink_map_t* inodes, flist_t** sorted_flist, inodes_hardlink_map_t** sorted_inodes) {

    uint64_t incount = mfu_flist_size(flist);
    uint64_t chars = mfu_flist_file_max_name(flist);

    /* create datatype for packed file list element */
    MPI_Datatype dt_elem;
    size_t bytes = mfu_flist_file_pack_size(flist);
    MPI_Type_contiguous((int)bytes, MPI_BYTE, &dt_elem);

    MPI_Datatype dt_key;
    DTCMP_Op op_str;
    DTCMP_Str_create_ascend(chars, &dt_key, &op_str);

    /* build keysat type */
    MPI_Datatype dt_keysat, keysat_types[3] = { dt_key, MPI_UINT64_T, dt_elem };
    if (DTCMP_Type_create_series(3, keysat_types, &dt_keysat) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create keysat type");
    }

    /* get extent of key type */
    MPI_Aint key_lb, key_extent;
    MPI_Type_get_extent(dt_key, &key_lb, &key_extent);

    /* get extent of keysat type */
    MPI_Aint inode_lb, inode_extent;
    MPI_Type_get_extent(MPI_UINT64_T, &inode_lb, &inode_extent);

    /* get extent of keysat type */
    MPI_Aint keysat_lb, keysat_extent;
    MPI_Type_get_extent(dt_keysat, &keysat_lb, &keysat_extent);

    /* compute size of sort element and allocate buffer */
    size_t sortbufsize = (size_t)keysat_extent * incount;
    void* sortbuf = MFU_MALLOC(sortbufsize);

    /* copy data into sort elements */
    char* sortptr = (char*) sortbuf;
    for (uint64_t idx=0; idx<incount; idx++) {
        const char* name = mfu_flist_file_get_name(flist, idx);
        strcpy(sortptr, name);
        sortptr += key_extent;
        *(uint64_t *)sortptr = inodes->inodes[idx];

        sortptr += inode_extent;
        /* pack file element */
        sortptr += mfu_flist_file_pack(sortptr, flist, idx);
    }

    /* sort data */
    void* outsortbuf;
    int outsortcount;
    DTCMP_Handle handle;
    int sort_rc = DTCMP_Sortz(
                      sortbuf, (int)incount, &outsortbuf, &outsortcount,
                      dt_key, dt_keysat, op_str, DTCMP_FLAG_NONE,
                      MPI_COMM_WORLD, &handle
                  );
    if (sort_rc != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to sort data");
    }

    /* free input buffer holding sort elements */
    mfu_free(&sortbuf);

    /* create a new list as subset of original list */
    *sorted_flist = mfu_flist_subset(flist);
    *sorted_inodes = inodes_map_new();

    /* step through sorted data filenames */
    sortptr = (char*) outsortbuf;
    for (uint64_t idx=0; idx<(uint64_t)outsortcount; idx++) {
        sortptr += key_extent;
        inodes_map_insert(*sorted_inodes, *(uint64_t*)sortptr);
        sortptr += inode_extent;
        sortptr += mfu_flist_file_unpack(sortptr, *sorted_flist);
    }

    /* compute summary of new list */
    mfu_flist_summarize(*sorted_flist);

    /* free memory */
    DTCMP_Free(&handle);

    DTCMP_Op_free(&op_str);
    MPI_Type_free(&dt_keysat);
    MPI_Type_free(&dt_key);
    MPI_Type_free(&dt_elem);

}

/* rank elements in flist by inodes in order to determine reference and secondary
 * links (aka. hardlinks). */
static void walk_hardlinks_rank(flist_t* flist, inodes_hardlink_map_t* inodes) {

    uint64_t incount = mfu_flist_size(flist);
    uint64_t chars = mfu_flist_file_max_name(flist);

    uint64_t* rankbuf = NULL;
    if(incount)
        rankbuf = (uint64_t*) MFU_MALLOC(sizeof(uint64_t)*incount);

    for(int idx=0; idx<incount; idx++)
        rankbuf[idx] = inodes->inodes[idx];

    uint64_t groups = 0;
    uint64_t output_bytes = incount * sizeof(uint64_t);
    uint64_t* group_id    = (uint64_t*) MFU_MALLOC(output_bytes);
    uint64_t* group_ranks = (uint64_t*) MFU_MALLOC(output_bytes);
    uint64_t* group_rank  = (uint64_t*) MFU_MALLOC(output_bytes);
    int rank_rc = DTCMP_Rankv(
                      (int)incount, rankbuf, &groups, group_id, group_ranks,
                      group_rank, MPI_UINT64_T, MPI_UINT64_T, DTCMP_OP_UINT64T_ASCEND, DTCMP_FLAG_NONE,
                      MPI_COMM_WORLD);

    if (rank_rc != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to rank hardlinks inodes");
    }

    /* The rank 0 is considered the reference link to the inode (ie. the regular
     * file). Set file type MFU_TYPE_HARDLINK on all other elements. */
    for(int idx=0; idx<incount; idx++) {
        if(group_rank[idx] != 0)
            mfu_flist_file_set_type(flist, idx, MFU_TYPE_HARDLINK);
    }
    /* free off temporary memory */
    mfu_free(&group_rank);
    mfu_free(&group_ranks);
    mfu_free(&group_id);

    /* free input buffer holding rank elements */
    mfu_free(&rankbuf);

}

/* propagate the determined references names of all inodes with multiple links
 * for all hardlinks on these inodes.*/
static void walk_hardlinks_propagate_refs(flist_t* flist, inodes_hardlink_map_t* inodes) {

    uint64_t incount = mfu_flist_size(flist);
    uint64_t chars = mfu_flist_file_max_name(flist);

    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* Count local ref */
    int nb_local_refs = 0;
    for(int idx=0; idx<incount; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_FILE)
            nb_local_refs += 1;
    }

    /* allgather to get bytes on each process */
    int* recvcounts = (int*) MFU_MALLOC(ranks * sizeof(int));
    int* recvdispls = (int*) MFU_MALLOC(ranks * sizeof(int));
    MPI_Allgather(&nb_local_refs, 1, MPI_INT, recvcounts, 1, MPI_INT, MPI_COMM_WORLD);

    MPI_Aint inode_extent, inode_lb;
    MPI_Type_get_extent(MPI_UINT64_T, &inode_lb, &inode_extent);

    MPI_Datatype types[2] = { MPI_UINT64_T, MPI_CHAR };
    MPI_Aint offsets[2] = {0, inode_extent};
    int blockcounts[2] = { 1, (int)chars};

    MPI_Datatype dt_struct;
    MPI_Aint struct_extent, struct_lb;
    MPI_Type_create_struct(2, blockcounts, offsets, types, &dt_struct);
    MPI_Type_commit(&dt_struct);
    MPI_Type_get_extent(dt_struct, &struct_lb, &struct_extent);

    /* compute displacements and total number of bytes that we'll receive */
    size_t allbytes = 0;
    int disp = 0;
    for (int i = 0; i < (int) ranks; i++) {
        recvdispls[i] = disp;
        disp += (int) recvcounts[i];
        allbytes += (size_t) recvcounts[i] * struct_extent;
    }

    /* allocate memory for recv buffers */
    char* recvbuf = MFU_MALLOC(allbytes);
    void* sendbuf = NULL;

    /* fill sendbuf with local references */
    if (nb_local_refs) {
        sendbuf = MFU_MALLOC((size_t)struct_extent * nb_local_refs);
        char* sendptr = (char*) sendbuf;
        for(int idx=0; idx<incount; idx++) {
            mfu_filetype type = mfu_flist_file_get_type(flist, idx);
            if (type == MFU_TYPE_FILE) {
                const char* name = mfu_flist_file_get_name(flist, idx);
                *(uint64_t *)sendptr = inodes->inodes[idx];
                sendptr += inode_extent;
                strncpy(sendptr, name, chars);
                sendptr += (struct_extent - inode_extent);
            }
        }
    }

    MPI_Allgatherv(sendbuf, nb_local_refs, dt_struct, recvbuf, recvcounts, recvdispls, dt_struct, MPI_COMM_WORLD);

    /* set reference on all local hardlinks */
    char* recvptr = (char*) recvbuf;
    for (int i = 0; i < (int) ranks; i++) {
        for (int j = 0; j < recvcounts[i]; j++) {
            uint64_t inode = *(uint64_t *)recvptr;
            const char* ref = recvptr + inode_extent;
            /* look for indexes with the name inode and set the refs accordingly */
            for (int idx = 0; idx < incount; idx++) {
                mfu_filetype type = mfu_flist_file_get_type(flist, idx);
                if(inodes->inodes[idx] == inode && type == MFU_TYPE_HARDLINK) {
                    mfu_flist_file_set_ref(flist, idx, ref);
                }
            }
            recvptr += struct_extent;
        }
    }

    mfu_free(&recvcounts);
    mfu_free(&recvdispls);
    mfu_free(&recvbuf);
    mfu_free(&sendbuf);
    MPI_Type_free(&dt_struct);

}

/* extend flist with add all items from sorted_hardlinks_flist */
static void walk_hardlinks_merge(flist_t* flist, flist_t* sorted_hardlinks_flist) {

    uint64_t incount = mfu_flist_size(sorted_hardlinks_flist);
    for(uint64_t idx=0; idx<incount; idx++)
        mfu_flist_file_copy(sorted_hardlinks_flist, idx, flist);

}

/* resolve hardlinks (identify references and secondary links) in hardlinks_flist and merge them in flist */
static void walk_resolve_hardlinks(flist_t* flist, flist_t *hardlinks_flist, inodes_hardlink_map_t* inodes)
{
    flist_t *sorted_hardlinks_flist;
    inodes_hardlink_map_t *sorted_inodes;

    /* bail out when no hardlinks in global list */
    if (!mfu_flist_global_size(hardlinks_flist)) {
        inodes_map_free(&inodes);
        mfu_flist_free(&hardlinks_flist);
        return;
    }

    /* sort hardlinks list by name in order to get deterministic lists and
     * ranking, and minimize differences in dcmp/dsync eventually. */
    walk_hardlinks_sort_names(hardlinks_flist, inodes, &sorted_hardlinks_flist, &sorted_inodes);

    /* free inodes map and unsorted hardlinks list */
    inodes_map_free(&inodes);
    mfu_flist_free(&hardlinks_flist);

    /* rank links to inodes to determine reference and secondary links */
    walk_hardlinks_rank(sorted_hardlinks_flist, sorted_inodes);

    /* propagate reference paths to all ranks */
    walk_hardlinks_propagate_refs(sorted_hardlinks_flist, sorted_inodes);

    /* extend flist with add all items from sorted_hardlinks_flist */
    walk_hardlinks_merge(flist, sorted_hardlinks_flist);

    /* free sorted inodes map and sorted hardlinks list */
    mfu_flist_free(&sorted_hardlinks_flist);
    inodes_map_free(&sorted_inodes);

    return;
}

/* Set up and execute directory walk */
int mfu_flist_walk_path(const char* dirpath,
                         mfu_walk_opts_t* walk_opts,
                         mfu_flist bflist,
                         mfu_file_t* mfu_file)
{
    return mfu_flist_walk_paths(1, &dirpath, walk_opts, bflist, mfu_file);
}

/* Set up and execute directory walk */
int mfu_flist_walk_paths(uint64_t num_paths, const char** paths,
                          mfu_walk_opts_t* walk_opts, mfu_flist bflist,
                          mfu_file_t* mfu_file)
{
    /* report walk count, time, and rate */
    double start_walk = MPI_Wtime();

    /* if dir_permission is set to 1 then set global variable */
    SET_DIR_PERMS = 0;
    if (walk_opts->dir_perms) {
        SET_DIR_PERMS = 1;
    }

    /* if remove is set to 1 then set global variable */
    REMOVE_FILES  = 0;
    if (walk_opts->remove) {
        REMOVE_FILES = 1;
    }

    /* if dereference is set to 1 then set global variable */
    DEREFERENCE = 0;
    if (walk_opts->dereference) {
        DEREFERENCE = 1;
    }

    /* if no_atime is set to 1 then set global variable */
    NO_ATIME = 0;
    if (walk_opts->no_atime) {
        NO_ATIME = 1;
    }

    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* print message to user that we're starting */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        uint64_t i;
        for (i = 0; i < num_paths; i++) {
            MFU_LOG(MFU_LOG_INFO, "Walking %s", paths[i]);
        }
    }

    /* initialize libcircle */
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_TERM_TREE);

    /* set libcircle verbosity level */
    enum CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    CIRCLE_enable_logging(loglevel);

    /* TODO: check that paths is not NULL */
    /* TODO: check that each path is within limits */

    /* set some global variables to do the file walk */
    CURRENT_NUM_DIRS = num_paths;
    CURRENT_DIRS     = paths;
    CURRENT_LIST     = flist;
    HARDLINKS_TMP_LIST = mfu_flist_new();
    HARDLINKS_INODES_MAP = inodes_map_new();

    /* we lookup users and groups first in case we can use
     * them to filter the walk */
    flist->detail = 0;
    if (walk_opts->use_stat) {
        flist->detail = 1;
        HARDLINKS_TMP_LIST->detail = 1;
        if (flist->have_users == 0) {
            mfu_flist_usrgrp_get_users(flist);
        }
        if (flist->have_groups == 0) {
            mfu_flist_usrgrp_get_groups(flist);
        }
    }

    /* register callbacks */
    CURRENT_PFILE = &mfu_file;
    if (walk_opts->use_stat) {
        /* walk directories by calling stat on every item */
        CIRCLE_cb_create(&walk_stat_create);
        CIRCLE_cb_process(&walk_stat_process);
    }
    else {
        CIRCLE_cb_create(&walk_create);
        CIRCLE_cb_process(&walk_process);
    }

    /* prepare callbacks and initialize variables for reductions */
    reduce_start = start_walk;
    reduce_items = 0;
    CIRCLE_cb_reduce_init(&reduce_init);
    CIRCLE_cb_reduce_op(&reduce_exec);
    CIRCLE_cb_reduce_fini(&reduce_fini);

    /* set libcircle reduction period */
    int reduce_secs = 0;
    if (mfu_progress_timeout > 0) {
        reduce_secs = mfu_progress_timeout;
    }
    CIRCLE_set_reduce_period(reduce_secs);

    /* run the libcircle job */
    CIRCLE_begin();
    CIRCLE_finalize();

    /* compute hardlinks temporary list global summary */
    mfu_flist_summarize(HARDLINKS_TMP_LIST);

    /* resolve hardlinks and merge them in flist */
    walk_resolve_hardlinks(flist, HARDLINKS_TMP_LIST, HARDLINKS_INODES_MAP);

    /* compute global summary */
    mfu_flist_summarize(bflist);

    double end_walk = MPI_Wtime();

    /* report walk count, time, and rate */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        uint64_t all_count = mfu_flist_global_size(bflist);
        double time_diff = end_walk - start_walk;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        MFU_LOG(MFU_LOG_INFO, "Walked %lu items in %.3lf seconds (%.3lf items/sec)",
               all_count, time_diff, rate
              );
    }

    /* hold procs here until summary is printed */
    MPI_Barrier(MPI_COMM_WORLD);

    int all_rc;
    MPI_Allreduce(&WALK_RESULT, &all_rc, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

    return all_rc;
}

/* given a list of param_paths, walk each one and add to flist */
int mfu_flist_walk_param_paths(uint64_t num,
                                const mfu_param_path* params,
                                mfu_walk_opts_t* walk_opts,
                                mfu_flist flist,
                                mfu_file_t* mfu_file)
{
    /* allocate memory to hold a list of paths */
    const char** path_list = (const char**) MFU_MALLOC(num * sizeof(char*));
    int walk_result;

#ifdef DAOS_SUPPORT
    /* DAOS only supports using one source path */
    if (mfu_file->type == DFS) {
        if (num != 1) {
            MFU_LOG(MFU_LOG_ERR, "Only one source can be specified when using DAOS");
            return EINVAL;
        }
    }
#endif

    /* fill list of paths and print each one */
    uint64_t i;
    for (i = 0; i < num; i++) {
        /* get path for this step */
        path_list[i] = params[i].path;
    }

    /* walk file tree and record stat data for each file */
    walk_result = mfu_flist_walk_paths((uint64_t) num, path_list, walk_opts, flist, mfu_file);

    /* free the list */
    mfu_free(&path_list);

    return walk_result;
}

/* Given an input file list, stat each file and enqueue details
 * in output file list, skip entries excluded by skip function
 * and skip args */
void mfu_flist_stat(
  mfu_flist input_flist,
  mfu_flist flist,
  mfu_flist_skip_fn skip_fn,
  void *skip_args,
  int dereference,
  mfu_file_t* mfu_file)
{
    flist_t* file_list = (flist_t*)flist;
    /* lists to track and resolve hardlinks */
    flist_t* hardlinks_tmp_list = mfu_flist_new();
    inodes_hardlink_map_t* hardlinks_inodes_map = inodes_map_new();

    /* we will stat all items in output list, so set detail to 1 */
    file_list->detail = 1;
    hardlinks_tmp_list->detail = 1;

    /* get user data if needed */
    if (file_list->have_users == 0) {
        mfu_flist_usrgrp_get_users(flist);
    }

    /* get groups data if needed */
    if (file_list->have_groups == 0) {
        mfu_flist_usrgrp_get_groups(flist);
    }

    /* step through each item in input list and stat it */
    uint64_t idx;
    uint64_t size = mfu_flist_size(input_flist);
    for (idx = 0; idx < size; idx++) {
        /* get name of item */
        const char* name = mfu_flist_file_get_name(input_flist, idx);

        /* check whether we should skip this item */
        if (skip_fn != NULL && skip_fn(name, skip_args)) {
            /* skip this file, don't include it in new list */
            MFU_LOG(MFU_LOG_INFO, "skip %s");
            continue;
        }

        /* If the dereference flag is passed in, try to dereference all paths.
         * Otherwise, if we have stat info for the mode, and the path is
         * not a link, then try to dereference it.
         * This accounts for dwalk --dereference, because a link might be
         * stored as a file, meaning that it should be dereferenced */
        bool do_dereference = false;
        if (dereference) {
            do_dereference = true;
        } else {
            mode_t mode = mfu_flist_file_get_mode(input_flist, idx);
            if (mode && !S_ISLNK(mode)) {
                do_dereference = true;
            }
        }
        
        /* stat the item */
        struct stat st;
        int status;
        if (do_dereference) {
            /* dereference symbolic link */
            status = mfu_file_stat(name, &st, mfu_file);
            if (status != 0) {
                MFU_LOG(MFU_LOG_ERR, "mfu_file_stat() failed: '%s' rc=%d (errno=%d %s)",
                        name, status, errno, strerror(errno));
                WALK_RESULT = -1;
                continue;
            }
        } else {
            /* don't dereference symbolic links */
            status = mfu_file_lstat(name, &st, mfu_file);
            if (status != 0) {
                MFU_LOG(MFU_LOG_ERR, "mfu_file_lstat() failed: '%s' rc=%d (errno=%d %s)",
                        name, status, errno, strerror(errno));
                WALK_RESULT = -1;
                continue;
            }
        }

        if (S_ISREG(st.st_mode) && st.st_nlink > 1) {
            /* record info for item in temporary hardlinks list and inodes map */
            mfu_flist_insert_stat(hardlinks_tmp_list, name, st.st_mode, &st);
            inodes_map_insert(hardlinks_inodes_map, (uint64_t)st.st_ino);
        } else
            /* record info for item in list */
            mfu_flist_insert_stat(flist, name, st.st_mode, &st);

    }

    /* compute hardlinks temporary list global summary */
    mfu_flist_summarize(hardlinks_tmp_list);
    /* resolve hardlinks and merge them in flist */
    walk_resolve_hardlinks(flist, hardlinks_tmp_list, hardlinks_inodes_map);

    /* compute global summary */
    mfu_flist_summarize(flist);
}
