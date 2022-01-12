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
static int SET_DIR_PERMS;
static int REMOVE_FILES;
static int DEREFERENCE;
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
    char buf[BUF_SIZE];

    /* TODO: may need to try these functions multiple times */
    mfu_file_t* mfu_file = *CURRENT_PFILE;
    mfu_file_open(dir, O_RDONLY | O_DIRECTORY, mfu_file);
    if (mfu_file->fd == -1) {
        /* print error */
        MFU_LOG(MFU_LOG_ERR, "Failed to open directory for reading: `%s' (errno=%d %s)", dir, errno, strerror(errno));
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

/** Call back given to initialize the dataset. */
static void walk_getdents_create(CIRCLE_handle* handle)
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
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        mfu_flist_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

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
    reduce_items++;
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
static void walk_readdir_create(CIRCLE_handle* handle)
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
            return;
        }

        /* increment our item count */
        reduce_items++;

        /* record item info */
        mfu_flist_insert_stat(CURRENT_LIST, path, st.st_mode, &st);

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
        return;
    }

    /* increment our item count */
    reduce_items++;

    /* TODO: filter items by stat info */

    if (REMOVE_FILES && !S_ISDIR(st.st_mode)) {
        mfu_file_unlink(path, mfu_file);
    } else {
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

/* Set up and execute directory walk */
void mfu_flist_walk_path(const char* dirpath,
                         mfu_walk_opts_t* walk_opts,
                         mfu_flist bflist,
                         mfu_file_t* mfu_file)
{
    mfu_flist_walk_paths(1, &dirpath, walk_opts, bflist, mfu_file);
    return;
}

/* Set up and execute directory walk */
void mfu_flist_walk_paths(uint64_t num_paths, const char** paths,
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

    /* we lookup users and groups first in case we can use
     * them to filter the walk */
    flist->detail = 0;
    if (walk_opts->use_stat) {
        flist->detail = 1;
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
        /* walk directories using file types in readdir */
        CIRCLE_cb_create(&walk_readdir_create);
        CIRCLE_cb_process(&walk_readdir_process);
        //        CIRCLE_cb_create(&walk_getdents_create);
        //        CIRCLE_cb_process(&walk_getdents_process);
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

    return;
}

/* given a list of param_paths, walk each one and add to flist */
void mfu_flist_walk_param_paths(uint64_t num,
                                const mfu_param_path* params,
                                mfu_walk_opts_t* walk_opts,
                                mfu_flist flist,
                                mfu_file_t* mfu_file)
{
    /* allocate memory to hold a list of paths */
    const char** path_list = (const char**) MFU_MALLOC(num * sizeof(char*));

#ifdef DAOS_SUPPORT
    /* DAOS only supports using one source path */
    if (mfu_file->type == DFS) {
        if (num != 1) {
            MFU_LOG(MFU_LOG_ERR, "Only one source can be specified when using DAOS");
            return;
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
    mfu_flist_walk_paths((uint64_t) num, path_list, walk_opts, flist, mfu_file);

    /* free the list */
    mfu_free(&path_list);

    return;
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

    /* we will stat all items in output list, so set detail to 1 */
    file_list->detail = 1;

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
                continue;
            }
        } else {
            /* don't dereference symbolic links */
            status = mfu_file_lstat(name, &st, mfu_file);
            if (status != 0) {
                MFU_LOG(MFU_LOG_ERR, "mfu_file_lstat() failed: '%s' rc=%d (errno=%d %s)",
                        name, status, errno, strerror(errno));
                continue;
            }
        }

        /* insert item into output list */
        mfu_flist_insert_stat(flist, name, st.st_mode, &st);
    }

    /* compute global summary */
    mfu_flist_summarize(flist);
}
