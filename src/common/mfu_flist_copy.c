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

/* define PRI64 */
#include <inttypes.h>

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
 * Define types
 ***************************************/

typedef struct {
    int64_t  total_dirs;         /* sum of all directories */
    int64_t  total_files;        /* sum of all files */
    int64_t  total_links;        /* sum of all symlinks */
    int64_t  total_size;         /* sum of all file sizes */
    int64_t  total_bytes_copied; /* total bytes written */
    time_t   time_started;       /* time when dcp command started */
    time_t   time_ended;         /* time when dcp command ended */
    double   wtime_started;      /* time when dcp command started */
    double   wtime_ended;        /* time when dcp command ended */
} mfu_copy_stats_t;

/* cache open file descriptor to avoid
 * opening / closing the same file */
typedef struct {
    char* name; /* name of open file (NULL if none) */
    int   read; /* whether file is open for read-only (1) or write (0) */
    int   fd;   /* file descriptor */
} mfu_copy_file_cache_t;

/****************************************
 * Define globals
 ***************************************/

/** Where we should keep statistics related to this file copy. */
static mfu_copy_stats_t mfu_copy_stats;

/** Cache most recent open file descriptor to avoid opening / closing the same file */
static mfu_copy_file_cache_t mfu_copy_src_cache;
static mfu_copy_file_cache_t mfu_copy_dst_cache;

static int mfu_copy_open_file(const char* file, int read_flag, 
        mfu_copy_file_cache_t* cache, mfu_copy_opts_t* mfu_copy_opts)
{
    int newfd = -1;

    /* see if we have a cached file descriptor */
    char* name = cache->name;
    if (name != NULL) {
        /* we have a cached file descriptor */
        int fd = cache->fd;
        if (strcmp(name, file) == 0 && cache->read == read_flag) {
            /* the file we're trying to open matches name and read/write mode,
             * so just return the cached descriptor */
            return fd;
        } else {
            /* the file we're trying to open is different,
             * close the old file and delete the name */
            mfu_close(name, fd);
            mfu_free(&cache->name);
        }
    }

    /* open the new file */
    if (read_flag) {
        int flags = O_RDONLY;
        if (mfu_copy_opts->synchronous) {
            flags |= O_DIRECT;
        }
        newfd = mfu_open(file, flags);
    } else {
        int flags = O_WRONLY | O_CREAT;
        if (mfu_copy_opts->synchronous) {
            flags |= O_DIRECT;
        }
        newfd = mfu_open(file, flags, DCOPY_DEF_PERMS_FILE);
    }

    /* cache the file descriptor */
    if (newfd != -1) {
        cache->name = MFU_STRDUP(file);
        cache->fd   = newfd;
        cache->read = read_flag;
#ifdef LUSTRE_SUPPORT
        /* Zero is an invalid ID for grouplock. */
        if (mfu_copy_opts->grouplock_id != 0) {
            errno = 0;
            int rc = ioctl(newfd, LL_IOC_GROUP_LOCK, mfu_copy_opts->grouplock_id);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "Failed to obtain grouplock with ID %d "
                    "on file `%s', ignoring this error (errno=%d %s)",
                    mfu_copy_opts->grouplock_id, file, errno, strerror(errno));
            } else {
                MFU_LOG(MFU_LOG_INFO, "Obtained grouplock with ID %d "
                    "on file `%s', fd %d", mfu_copy_opts->grouplock_id,
                    file, newfd);
            }
        }
#endif
    }

    return newfd;
}

/* copy all extended attributes from op->operand to dest_path,
 * returns 0 on success and -1 on failure */
static int mfu_copy_xattrs(
    mfu_flist flist,
    uint64_t idx,
    const char* dest_path)
{
    /* assume that we'll succeed */
    int rc = 0;

#if DCOPY_USE_XATTRS
    /* get source file name */
    const char* src_path = mfu_flist_file_get_name(flist, idx);

    /* start with a reasonable buffer, we'll allocate more as needed */
    size_t list_bufsize = 1204;
    char* list = (char*) MFU_MALLOC(list_bufsize);

    /* get list, if list_size == ERANGE, try again */
    ssize_t list_size;
    int got_list = 0;

    /* get current estimate for list size */
    while(! got_list) {
        errno = 0;
        list_size = llistxattr(src_path, list, list_bufsize);

        if(list_size < 0) {
            if(errno == ERANGE) {
                /* buffer is too small, free our current buffer
                 * and call it again with size==0 to get new size */
                mfu_free(&list);
                list_bufsize = 0;
            }
            else if(errno == ENOTSUP) {
                /* this is common enough that we silently ignore it */
                break;
            }
            else {
                /* this is a real error */
                MFU_LOG(MFU_LOG_ERR, "Failed to get list of extended attributes on `%s' llistxattr() (errno=%d %s)",
                    src_path, errno, strerror(errno)
                   );
                rc = -1;
                break;
            }
        }
        else {
            if(list_size > 0 && list_bufsize == 0) {
                /* called llistxattr with size==0 and got back positive
                 * number indicating size of buffer we need to allocate */
                list_bufsize = (size_t) list_size;
                list = (char*) MFU_MALLOC(list_bufsize);
            }
            else {
                /* got our list, it's size is in list_size, which may be 0 */
                got_list = 1;
            }
        }
    }

    /* iterate over list and copy values to new object lgetxattr/lsetxattr */
    if(got_list) {
        char* name = list;

        while(name < list + list_size) {
            /* start with a reasonable buffer,
             * allocate something bigger as needed */
            size_t val_bufsize = 1024;
            void* val = (void*) MFU_MALLOC(val_bufsize);

            /* lookup value for name */
            ssize_t val_size;
            int got_val = 0;

            while(! got_val) {
                errno = 0;
                val_size = lgetxattr(src_path, name, val, val_bufsize);

                if(val_size < 0) {
                    if(errno == ERANGE) {
                        /* buffer is too small, free our current buffer
                         * and call it again with size==0 to get new size */
                        mfu_free(&val);
                        val_bufsize = 0;
                    }
                    else if(errno == ENOATTR) {
                        /* source object no longer has this attribute,
                         * maybe deleted out from under us, ignore but print warning */
                        MFU_LOG(MFU_LOG_WARN, "Attribute does not exist for name=%s on `%s' llistxattr() (errno=%d %s)",
                            name, src_path, errno, strerror(errno)
                           );
                        break;
                    }
                    else {
                        /* this is a real error */
                        MFU_LOG(MFU_LOG_ERR, "Failed to get value for name=%s on `%s' llistxattr() (errno=%d %s)",
                            name, src_path, errno, strerror(errno)
                           );
                        rc = -1;
                        break;
                    }
                }
                else {
                    if(val_size > 0 && val_bufsize == 0) {
                        /* called lgetxattr with size==0 and got back positive
                         * number indicating size of buffer we need to allocate */
                        val_bufsize = (size_t) val_size;
                        val = (void*) MFU_MALLOC(val_bufsize);
                    }
                    else {
                        /* got our value, it's size is in val_size, which may be 0 */
                        got_val = 1;
                    }
                }
            }

            /* set attribute on destination object */
            if(got_val) {
                errno = 0;
                int setrc = lsetxattr(dest_path, name, val, (size_t) val_size, 0);
                if(setrc != 0) {
                    /* failed to set attribute */
                    MFU_LOG(MFU_LOG_ERR, "Failed to set value for name=%s on `%s' llistxattr() (errno=%d %s)",
                        name, dest_path, errno, strerror(errno)
                       );
                    rc = -1;
                }
            }

            /* free value string */
            mfu_free(&val);
            val_bufsize = 0;

            /* jump to next name */
            size_t namelen = strlen(name) + 1;
            name += namelen;
        }
    }

    /* free space allocated for list */
    mfu_free(&list);
    list_bufsize = 0;
#endif /* DCOPY_USE_XATTR */

    return rc;
}

static int mfu_copy_ownership(
    mfu_flist flist,
    uint64_t idx,
    const char* dest_path)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get user id and group id of file */
    uid_t uid = (uid_t) mfu_flist_file_get_uid(flist, idx);
    gid_t gid = (gid_t) mfu_flist_file_get_gid(flist, idx);

    /* note that we use lchown to change ownership of link itself, it path happens to be a link */
    if(mfu_lchown(dest_path, uid, gid) != 0) {
        /* TODO: are there other EPERM conditions we do want to report? */

        /* since the user running dcp may not be the owner of the
         * file, we could hit an EPERM error here, and the file
         * will be left with the effective uid and gid of the dcp
         * process, don't bother reporting an error for that case */
        if (errno != EPERM) {
            MFU_LOG(MFU_LOG_ERR, "Failed to change ownership on `%s' lchown() (errno=%d %s)",
                dest_path, errno, strerror(errno)
               );
        }
        rc = -1;
    }

    return rc;
}

/* TODO: condionally set setuid and setgid bits? */
static int mfu_copy_permissions(
    mfu_flist flist,
    uint64_t idx,
    const char* dest_path)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get mode and type */
    mfu_filetype type = mfu_flist_file_get_type(flist, idx);
    mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

    /* change mode */
    if(type != MFU_TYPE_LINK) {
        if(mfu_chmod(dest_path, mode) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to change permissions on `%s' chmod() (errno=%d %s)",
                dest_path, errno, strerror(errno)
               );
            rc = -1;
        }
    }

    return rc;
}

static int mfu_copy_timestamps(
    mfu_flist flist,
    uint64_t idx,
    const char* dest_path)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get atime seconds and nsecs */
    uint64_t atime      = mfu_flist_file_get_atime(flist, idx);
    uint64_t atime_nsec = mfu_flist_file_get_atime_nsec(flist, idx);

    /* get mtime seconds and nsecs */
    uint64_t mtime      = mfu_flist_file_get_mtime(flist, idx);
    uint64_t mtime_nsec = mfu_flist_file_get_mtime_nsec(flist, idx);

    /* fill in time structures */
    struct timespec times[2];
    times[0].tv_sec  = (time_t) atime;
    times[0].tv_nsec = (long)   atime_nsec;
    times[1].tv_sec  = (time_t) mtime;
    times[1].tv_nsec = (long)   mtime_nsec;

    /* set times with nanosecond precision using utimensat,
     * assume path is relative to current working directory,
     * if it's not absolute, and set times on link (not target file)
     * if dest_path refers to a link */
    if(mfu_utimensat(AT_FDCWD, dest_path, times, AT_SYMLINK_NOFOLLOW) != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to change timestamps on `%s' utime() (errno=%d %s)",
            dest_path, errno, strerror(errno)
           );
        rc = -1;
    }

    return rc;
}

static int mfu_copy_close_file(mfu_copy_file_cache_t* cache)
{
    int rc = 0;

    /* close file if we have one */
    char* name = cache->name;
    if (name != NULL) {
        int fd = cache->fd;

        /* if open for write, fsync */
        int read_flag = cache->read;
        if (! read_flag) {
            rc = mfu_fsync(name, fd);
        }

        /* close the file and delete the name string */
        rc = mfu_close(name, fd);
        mfu_free(&cache->name);
    }

    return rc;
}

/* iterate through list of files and set ownership, timestamps,
 * and permissions starting from deepest level and working upwards,
 * we go in this direction in case updating a file updates its
 * parent directory */
static int mfu_copy_set_metadata(int levels, int minlevel, mfu_flist* lists,
        int numpaths, const mfu_param_path* paths, 
        const mfu_param_path* destpath, mfu_copy_opts_t* mfu_copy_opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* determine whether we should print status messages */
    int verbose = (mfu_debug_level >= MFU_LOG_VERBOSE);

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        if(mfu_copy_opts->preserve) {
            MFU_LOG(MFU_LOG_INFO, "Setting ownership, permissions, and timestamps.");
        }
        else {
            MFU_LOG(MFU_LOG_INFO, "Fixing permissions.");
        }
    }

    /* start timer for entie operation */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_start = MPI_Wtime();
    uint64_t total_count = 0;

    /* now set timestamps on files starting from deepest level */
    int tmp_rc;
    int level;
    for (level = levels-1; level >= 0; level--) {
        /* get list at this level */
        mfu_flist list = lists[level];

        /* cycle through our list of items and set timestamps
         * for each one at this level */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        for (idx = 0; idx < size; idx++) {
            /* TODO: skip file if it's not readable */

            /* get source name of item */
            const char* name = mfu_flist_file_get_name(list, idx);

            /* get destination name of item */
            char* dest = mfu_param_path_copy_dest(name, numpaths, 
                    paths, destpath, mfu_copy_opts);

            /* No need to copy it */
            if (dest == NULL) {
                continue;
            }

            /* update our running total */
            total_count++;

            if(mfu_copy_opts->preserve) {
                tmp_rc = mfu_copy_ownership(list, idx, dest);
                if (tmp_rc < 0) {
                    rc = -1;
                }
                tmp_rc = mfu_copy_permissions(list, idx, dest);
                if (tmp_rc < 0) {
                    rc = -1;
                }
                tmp_rc = mfu_copy_timestamps(list, idx, dest);
                if (tmp_rc < 0) {
                    rc = -1;
                }
            }
            else {
                /* TODO: set permissions based on source permissons
                 * masked by umask */
                tmp_rc = mfu_copy_permissions(list, idx, dest);
                if (tmp_rc < 0) {
                    rc = -1;
                }
            }

            /* free destination item */
            mfu_free(&dest);
        }
        
        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /* stop timer and report total count */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_end = MPI_Wtime();

    /* print timing statistics */
    if (verbose) {
        uint64_t sum;
        MPI_Allreduce(&total_count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        double rate = 0.0;
        double secs = total_end - total_start;
        if (secs > 0.0) {
          rate = (double)sum / secs;
        }
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Updated %lu items in %f seconds (%f items/sec)",
              (unsigned long)sum, secs, rate
            );
        }
    }

    return rc;
}

/* iterate through list of files and set ownership, timestamps,
 * and permissions starting from deepest level and working upwards,
 * we go in this direction in case updating a file updates its
 * parent directory */
static int mfu_copy_set_metadata_dirs(int levels, int minlevel, mfu_flist* lists,
        int numpaths, const mfu_param_path* paths, 
        const mfu_param_path* destpath, mfu_copy_opts_t* mfu_copy_opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* determine whether we should print status messages */
    int verbose = (mfu_debug_level >= MFU_LOG_VERBOSE);

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        if(mfu_copy_opts->preserve) {
            MFU_LOG(MFU_LOG_INFO, "Setting ownership, permissions, and timestamps on directories.");
        }
        else {
            MFU_LOG(MFU_LOG_INFO, "Fixing permissions on directories.");
        }
    }

    /* start timer for entie operation */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_start = MPI_Wtime();
    uint64_t total_count = 0;

    /* now set timestamps on files starting from deepest level */
    int tmp_rc;
    int level;
    for (level = levels-1; level >= 0; level--) {
        /* get list at this level */
        mfu_flist list = lists[level];

        /* cycle through our list of items and set timestamps
         * for each one at this level */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        for (idx = 0; idx < size; idx++) {
            /* TODO: skip file if it's not readable */

            /* get source name of item */
            const char* name = mfu_flist_file_get_name(list, idx);

            /* get destination name of item */
            char* dest = mfu_param_path_copy_dest(name, numpaths, 
                    paths, destpath, mfu_copy_opts);

            /* No need to copy it */
            if (dest == NULL) {
                continue;
            }

            /* only need to set metadata on directories */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type != MFU_TYPE_DIR) {
                continue;
            }

            /* update our running total */
            total_count++;

            if(mfu_copy_opts->preserve) {
                tmp_rc = mfu_copy_ownership(list, idx, dest);
                if (tmp_rc < 0) {
                    rc = -1;
                }
                tmp_rc = mfu_copy_permissions(list, idx, dest);
                if (tmp_rc < 0) {
                    rc = -1;
                }
                tmp_rc = mfu_copy_timestamps(list, idx, dest);
                if (tmp_rc < 0) {
                    rc = -1;
                }
            }
            else {
                /* TODO: set permissions based on source permissons
                 * masked by umask */
                tmp_rc = mfu_copy_permissions(list, idx, dest);
                if (tmp_rc < 0) {
                    rc = -1;
                }
            }

            /* free destination item */
            mfu_free(&dest);
        }
        
        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /* stop timer and report total count */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_end = MPI_Wtime();

    /* print timing statistics */
    if (verbose) {
        uint64_t sum;
        MPI_Allreduce(&total_count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        double rate = 0.0;
        double secs = total_end - total_start;
        if (secs > 0.0) {
          rate = (double)sum / secs;
        }
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Updated %lu items in %f seconds (%f items/sec)",
              (unsigned long)sum, secs, rate
            );
        }
    }

    return rc;
}

/* creates dir in destpath for specified item, identifies source path
 * that contains source dir, computes relative path to dir under source path,
 * and creates dir at same relative path under destpath, copies xattrs
 * when preserving permissions, which contains file striping info on Lustre,
 * returns 0 on success and -1 on error */
static int mfu_create_directory(mfu_flist list, uint64_t idx,
        int numpaths, const mfu_param_path* paths,
        const mfu_param_path* destpath, mfu_copy_opts_t* mfu_copy_opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get name of directory */
    const char* name = mfu_flist_file_get_name(list, idx);

    /* get destination name */
    char* dest_path = mfu_param_path_copy_dest(name, numpaths, paths,
            destpath, mfu_copy_opts);

    /* No need to copy it */
    if (dest_path == NULL) {
        return 0;
    }

    /* Skipping the destination directory ONLY if it already exists.
     * If we are doing a sync operation and if the dest dir does not
     * exist, we need to create it. The reason that
     * the dest_path and the destpath->path are compared is because
     * if we are syncing two directories we want the tree to have the
     * same number of levels. If dsync is on then only the contents of
     * the top level source directory will be copied (if necessary) into
     * the target directory. So, the top level src directory is removed
     * from the destination path. This path slicing based on whether or
     * not dsync is on happens prior to this in
     * mfu_param_path_copy_dest. */

    if (mfu_copy_opts->do_sync &&
        (strncmp(dest_path, destpath->path, strlen(dest_path)) == 0) &&
        destpath->target_stat_valid)
    {
        mfu_free(&dest_path);
        return 0;
    }

    /* create the destination directory */
    MFU_LOG(MFU_LOG_DBG, "Creating directory `%s'", dest_path);
    int mkdir_rc = mfu_mkdir(dest_path, DCOPY_DEF_PERMS_DIR);
    if(mkdir_rc < 0) {
        if(errno == EEXIST) {
            MFU_LOG(MFU_LOG_WARN,
                    "Original directory exists, skip the creation: `%s' (errno=%d %s)",
                    dest_path, errno, strerror(errno));

        } else {
            MFU_LOG(MFU_LOG_ERR, "Create `%s' mkdir() failed (errno=%d %s)",
                    dest_path, errno, strerror(errno)
            );
            mfu_free(&dest_path);
            return -1;
        }
    }

    /* we do this now in case there are Lustre attributes for
     * creating / striping files in the directory */

    /* copy extended attributes on directory */
    if (mfu_copy_opts->preserve) {
        int tmp_rc = mfu_copy_xattrs(list, idx, dest_path);
        if (tmp_rc < 0) {
            rc = -1;
        }
    }

    /* increment our directory count by one */
    mfu_copy_stats.total_dirs++;

    /* free the directory name */
    mfu_free(&dest_path);

    return rc;
}

/* create directories, we work from shallowest level to the deepest
 * with a barrier in between levels, so that we don't try to create
 * a child directory until the parent exists,
 * returns 0 on success and -1 on failure */
static int mfu_create_directories(int levels, int minlevel, mfu_flist* lists,
        int numpaths, const mfu_param_path* paths, 
        const mfu_param_path* destpath, mfu_copy_opts_t* mfu_copy_opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* determine whether we should print status messages */
    int verbose = (mfu_debug_level >= MFU_LOG_VERBOSE);

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* indicate to user what phase we're in */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating directories.");
    }

    /* start timer for entie operation */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_start = MPI_Wtime();
    uint64_t total_count = 0;

    /* work from shallowest level to deepest level */
    int level;
    for (level = 0; level < levels; level++) {
        /* time how long this takes */
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* create each directory we have at this level */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* check whether we have a directory */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_DIR) {
                /* create the directory */
                int tmp_rc = mfu_create_directory(list, idx, numpaths, 
                        paths, destpath, mfu_copy_opts);
                if (tmp_rc < 0) {
                    rc = -1;
                }

                count++;
            }
        }

        /* add items to our running total */
        total_count += count;

        /* wait for all procs to finish before we start
         * creating directories at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        /* stop our timer */
        double end = MPI_Wtime();

         /* print statistics */
        if (verbose) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            double secs = end - start;
            if (secs > 0.0) {
              rate = (double)sum / secs;
            }
            if (rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "  level=%d min=%lu max=%lu sum=%lu rate=%f/sec secs=%f",
                  (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
                );
            }
        }
    }

    /* stop timer and report total count */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_end = MPI_Wtime();

    /* print timing statistics */
    if (verbose) {
        uint64_t sum;
        MPI_Allreduce(&total_count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        double rate = 0.0;
        double secs = total_end - total_start;
        if (secs > 0.0) {
          rate = (double)sum / secs;
        }
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Created %lu directories in %f seconds (%f items/sec)",
              (unsigned long)sum, secs, rate
            );
        }
    }

    return rc;
}

/* creates symlink in destpath for specified file, identifies source path
 * that contains source link, computes relative path to link under source path,
 * and creates link at same relative path under destpath,
 * returns 0 on success and -1 on error */
static int mfu_create_link(mfu_flist list, uint64_t idx,
        int numpaths, const mfu_param_path* paths,
        const mfu_param_path* destpath, mfu_copy_opts_t* mfu_copy_opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get source name */
    const char* src_path = mfu_flist_file_get_name(list, idx);

    /* get destination name */
    const char* dest_path = mfu_param_path_copy_dest(src_path, numpaths, 
           paths, destpath, mfu_copy_opts);

    /* No need to copy it */
    if (dest_path == NULL) {
        return 0;
    }

    /* read link target */
    char path[PATH_MAX + 1];
    ssize_t readlink_rc = mfu_readlink(src_path, path, sizeof(path) - 1);
    if(readlink_rc < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to read link `%s' readlink() (errno=%d %s)",
            src_path, errno, strerror(errno)
        );
        mfu_free(&dest_path);
        return -1;
    }

    /* ensure that string ends with NUL */
    path[rc] = '\0';

    /* create new link */
    int symlink_rc = mfu_symlink(path, dest_path);
    if(symlink_rc < 0) {
        if(errno == EEXIST) {
            MFU_LOG(MFU_LOG_WARN,
                    "Original link exists, skip the creation: `%s' (errno=%d %s)",
                    dest_path, errno, strerror(errno));
        } else {
            MFU_LOG(MFU_LOG_ERR, "Create `%s' symlink() failed, (errno=%d %s)",
                    dest_path, errno, strerror(errno)
            );
            mfu_free(&dest_path);
            return -1;
        }
    }

    /* set permissions on link */
    if (mfu_copy_opts->preserve) {
        int xattr_rc = mfu_copy_xattrs(list, idx, dest_path);
        if (xattr_rc < 0) {
            rc = -1;
        }
    }

    /* free destination path */
    mfu_free(&dest_path);

    /* increment our directory count by one */
    mfu_copy_stats.total_links++;

    return rc;
}

/* creates inode in destpath for specified file, identifies source path
 * that contains source file, computes relative path to file under source path,
 * and creates file at same relative path under destpath, copies xattrs
 * when preserving permissions, which contains file striping info on Lustre,
 * returns 0 on success and -1 on error */
static int mfu_create_file(mfu_flist list, uint64_t idx,
        int numpaths, const mfu_param_path* paths, 
        const mfu_param_path* destpath, mfu_copy_opts_t* mfu_copy_opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get source name */
    const char* src_path = mfu_flist_file_get_name(list, idx);

    /* get destination name */
    const char* dest_path = mfu_param_path_copy_dest(src_path, numpaths,
            paths, destpath, mfu_copy_opts);

    /* No need to copy it */
    if (dest_path == NULL) {
        return 0;
    }

    /* since file systems like Lustre require xattrs to be set before file is opened,
     * we first create it with mknod and then set xattrs */

    /* create file with mknod
     * for regular files, dev argument is supposed to be ignored,
     * see makedev() to create valid dev */
    dev_t dev;
    memset(&dev, 0, sizeof(dev_t));
    int mknod_rc = mfu_mknod(dest_path, DCOPY_DEF_PERMS_FILE | S_IFREG, dev);
    if(mknod_rc < 0) {
        if(errno == EEXIST) {
            /* destination already exists, no big deal, but print warning */
            MFU_LOG(MFU_LOG_WARN,
                    "Original file exists, skip the creation: `%s' (errno=%d %s)",
                    dest_path, errno, strerror(errno));
        } else {
            /* failed to create inode, that's a problem */
            MFU_LOG(MFU_LOG_ERR, "File `%s' mknod() failed (errno=%d %s)",
                    dest_path, errno, strerror(errno)
            );
            mfu_free(&dest_path);
            return -1;
        }
    }

    /* copy extended attributes, important to do this first before
     * writing data because some attributes tell file system how to
     * stripe data, e.g., Lustre */
    if (mfu_copy_opts->preserve) {
        int tmp_rc = mfu_copy_xattrs(list, idx, dest_path);
        if (tmp_rc < 0) {
            rc = -1;
        }
    }

    /* Truncate destination files to 0 bytes when sparse file is enabled,
     * this is because we will not overwrite sections corresponding to holes
     * and we need those to be set to 0 */
    if (mfu_copy_opts->sparse) {
        /* truncate destination file to 0 bytes */
        struct stat st;
        int status = mfu_lstat(dest_path, &st);
        if (status == 0) {
            /* destination exists, truncate it to 0 bytes */
            status = mfu_truncate(dest_path, 0);
            if (status) {
                /* when using sparse file optimization, consider this to be an error,
                 * since we will not be overwriting the holes */
                MFU_LOG(MFU_LOG_ERR, "Failed to truncate destination file: `%s' (errno=%d %s)",
                          dest_path, errno, strerror(errno));
                rc = -1;
            }
        } else if (errno == -ENOENT) {
            /* destination does not exist, which is fine */
            status = 0;
        } else {
            /* had an error stating destination file */
            MFU_LOG(MFU_LOG_ERR, "mfu_lstat() file: `%s' (errno=%d %s)",
                      dest_path, errno, strerror(errno));
        }
    }

    /* free destination path */
    mfu_free(&dest_path);

    /* increment our file count by one */
    mfu_copy_stats.total_files++;

    return rc;
}

/* creates file inodes and symlinks,
 * returns 0 on success and -1 on error */
static int mfu_create_files(int levels, int minlevel, mfu_flist* lists,
        int numpaths, const mfu_param_path* paths,
        const mfu_param_path* destpath, mfu_copy_opts_t* mfu_copy_opts)
{
    int rc = 0;

    /* determine whether we should print status messages */
    int verbose = (mfu_debug_level >= MFU_LOG_VERBOSE);

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* indicate to user what phase we're in */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating files.");
    }

    /* start timer for entie operation */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_start = MPI_Wtime();
    uint64_t total_count = 0;

    int level;
    for (level = 0; level < levels; level++) {
        /* time how long this takes */
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* iterate over items and set write bit on directories if needed */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* get type of item */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);

            /* process files and links */
            if (type == MFU_TYPE_FILE) {
                /* create inode and copy xattr for regular file */
                int tmp_rc = mfu_create_file(list, idx, numpaths,
                        paths, destpath, mfu_copy_opts);
                if (tmp_rc < 0) {
                    rc = -1;
                }
                count++;
            } else if (type == MFU_TYPE_LINK) {
                /* create symlink */
                int tmp_rc = mfu_create_link(list, idx, numpaths,
                        paths, destpath, mfu_copy_opts);
                if (tmp_rc < 0) {
                    rc = -1;
                }
                count++;
            }
        }

        /* add items to our running total */
        total_count += count;

        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        /* stop our timer */
        double end = MPI_Wtime();

        /* print timing statistics */
        if (verbose) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            double secs = end - start;
            if (secs > 0.0) {
              rate = (double)sum / secs;
            }
            if (rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "  level=%d min=%lu max=%lu sum=%lu rate=%f secs=%f",
                  (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
                );
            }
        }
    }

    /* stop timer and report total count */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_end = MPI_Wtime();

    /* print timing statistics */
    if (verbose) {
        uint64_t sum;
        MPI_Allreduce(&total_count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        double rate = 0.0;
        double secs = total_end - total_start;
        if (secs > 0.0) {
          rate = (double)sum / secs;
        }
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Created %lu items in %f seconds (%f items/sec)",
              (unsigned long)sum, secs, rate
            );
        }
    }

    return rc;
}

/* return 1 if entire buffer is 0, return 0 if any byte is not 0,
 * we avoid writing NULL blocks when supporting sparse files */
static int mfu_is_all_null(const char* buf, uint64_t buf_size)
{
    uint64_t i;
    for (i = 0; i < buf_size; i++) {
        if (buf[i] != 0) {
            return 0;
        }
    }
    return 1;
}

/* when using sparse files, we need to write the last byte if the
 * hole is adjacent to EOF, so we need to detect whether we're at
 * the end of the file */
static int mfu_is_eof(const char* file, int fd)
{
    /* read one byte from fd to determine whether this is EOF.
     * This is not efficient, but it is the only reliable way */
    char buf[1];
    ssize_t num_of_bytes_read = mfu_read(file, fd, buf, 1);

    /* return if we detect EOF */
    if(! num_of_bytes_read) {
        return 1;
    }

    /* otherwise, we're not at EOF yet, seek back one byte */
    if(mfu_lseek(file, fd, -1, SEEK_CUR) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in path `%s' (errno=%d %s)",
                  file, errno, strerror(errno));
        return -1;
    }
    return 0;
}

static int mfu_copy_file_normal(
    const char* src,
    const char* dest,
    const int in_fd,
    const int out_fd,
    uint64_t offset,
    uint64_t length,
    uint64_t file_size,
    mfu_copy_opts_t* mfu_copy_opts)
{
    /* hint that we'll read from file sequentially */
//    posix_fadvise(in_fd, offset, chunk_size, POSIX_FADV_SEQUENTIAL);

    /* seek to offset in source file */
    if(mfu_lseek(src, in_fd, offset, SEEK_SET) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in source path `%s' (errno=%d %s)",
            src, errno, strerror(errno));
        return -1;
    }

    /* seek to offset in destination file */
    if(mfu_lseek(dest, out_fd, offset, SEEK_SET) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' (errno=%d %s)",
            dest, errno, strerror(errno));
        return -1;
    }

    /* get buffer */
    size_t buf_size = mfu_copy_opts->block_size;
    void* buf = mfu_copy_opts->block_buf1;

    /* write data */
    size_t total_bytes = 0;
    while(total_bytes <= (size_t)length) {
        /* determine number of bytes that we
         * can read = max(buf size, remaining chunk) */
        size_t left_to_read = (size_t)length - total_bytes;
        if(left_to_read > buf_size) {
            left_to_read = buf_size;
        }

        /* read data from source file */
        ssize_t num_of_bytes_read = mfu_read(src, in_fd, buf, left_to_read);

        /* check for EOF */
        if(! num_of_bytes_read) {
            break;
        }

        /* compute number of bytes to write */
        size_t bytes_to_write = (size_t) num_of_bytes_read;
        if(mfu_copy_opts->synchronous) {
            /* O_DIRECT requires particular write sizes,
             * ok to write beyond end of file so long as
             * we truncate in cleanup step */
            size_t remainder = buf_size - (size_t) num_of_bytes_read;
            if(remainder > 0) {
                /* zero out the end of the buffer for security,
                 * don't want to leave data from another file at end of
                 * current file if we fail before truncating */
                char* bufzero = ((char*)buf + num_of_bytes_read);
                memset(bufzero, 0, remainder);
            }

            /* assumes buf_size is magic size for O_DIRECT */
            bytes_to_write = buf_size;
        }

        /* Write data to destination file.
         * Do nothing for a hole in the middle of a file,
         * because write of next chunk will create one for us.
         * Write only the last byte to create the hole,
         * if the hole is next to EOF. */
        ssize_t num_of_bytes_written = (ssize_t)bytes_to_write;
        if (mfu_copy_opts->sparse && mfu_is_all_null(buf, bytes_to_write)) {
            /* TODO: isn't there a better way to know if we're at EOF,
             * e.g., by using file size? */
            /* determine whether we're at the end of the file */
            int end_of_file = mfu_is_eof(src, in_fd);
            if (end_of_file < 0) {
                /* hit an error while looking for EOF */
                return -1;
            }

            /* if we're at the end of the file, write out a byte,
             * otherwise just seek out destination file pointer
             * ahead without writing anything */
            if (end_of_file) {
                /* seek to last byte position in file */
                if(mfu_lseek(dest, out_fd, (off_t)bytes_to_write - 1, SEEK_CUR) == (off_t)-1) {
                    MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' (errno=%d %s)",
                        dest, errno, strerror(errno));
                    return -1;
                }

                /* write out a single byte */
                mfu_write(dest, out_fd, buf, 1);
            } else {
                /* this section of the destination file is all 0,
                 * seek past this section */
                if(mfu_lseek(dest, out_fd, (off_t)bytes_to_write, SEEK_CUR) == (off_t)-1) {
                    MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' (errno=%d %s)",
                        dest, errno, strerror(errno));
                    return -1;
                }
            }
        } else {
            /* write bytes to destination file */
            num_of_bytes_written = mfu_write(dest, out_fd, buf, bytes_to_write);
        }

        /* check for an error */
        if(num_of_bytes_written < 0) {
            MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s' (errno=%d %s)",
                src, dest, errno, strerror(errno));
            return -1;
        }

        /* check that we wrote the same number of bytes that we read */
        if((size_t)num_of_bytes_written != bytes_to_write) {
            MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s'",
                src, dest);
            return -1;
        }

        /* add bytes to our total (use bytes read,
         * which may be less than number written) */
        total_bytes += (size_t) num_of_bytes_read;
    }

    /* Increment the global counter. */
    mfu_copy_stats.total_size += (int64_t) total_bytes;
    mfu_copy_stats.total_bytes_copied += (int64_t) total_bytes;

#if 0
    /* force data to file system */
    if(total_bytes > 0) {
        mfu_fsync(dest, out_fd);
    }
#endif

    /* no need to truncate if sparse file is enabled,
     * since we truncated files when they were first created */
    if (mfu_copy_opts->sparse) {
        return 0;
    }

    /* if we wrote the last chunk, truncate the file */
    off_t last_written = offset + length;
    off_t file_size_offt = (off_t) file_size;
    if (last_written >= file_size_offt || file_size == 0) {
        /* Use ftruncate() here rather than truncate(), because grouplock
         * of Lustre would cause block to truncate() since the fd is different
         * from the out_fd. */
        if(mfu_ftruncate(out_fd, file_size_offt) < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to truncate destination file: %s (errno=%d %s)",
                dest, errno, strerror(errno));
            return -1;
       }
    }

    /* we don't bother closing the file because our cache does it for us */

    return 0;
}

static int mfu_copy_file_fiemap(
    const char* src,
    const char* dest,
    const int in_fd,
    const int out_fd,
    uint64_t offset,
    uint64_t length,
    uint64_t file_size,
    bool* normal_copy_required,
    mfu_copy_opts_t* mfu_copy_opts)
{
    *normal_copy_required = true;
    if (mfu_copy_opts->synchronous) {
        goto fail_normal_copy;
    }

    size_t last_ext_start = offset;
    size_t last_ext_len = 0;

    struct fiemap *fiemap = (struct fiemap*)malloc(sizeof(struct fiemap));
    if (fiemap == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Out of memory allocating fiemap");
        goto fail_normal_copy;
    }
    memset(fiemap, 0, sizeof(struct fiemap));

    fiemap->fm_start  = offset;
    fiemap->fm_length = length;
    fiemap->fm_flags  = FIEMAP_FLAG_SYNC;
    fiemap->fm_extent_count   = 0;
    fiemap->fm_mapped_extents = 0;

    struct stat sb;
    if (fstat(in_fd, &sb) < 0) {
        goto fail_normal_copy;
    }

    if (ioctl(in_fd, FS_IOC_FIEMAP, fiemap) < 0) {
        MFU_LOG(MFU_LOG_ERR, "fiemap ioctl() failed for src `%s'", src);
        goto fail_normal_copy;
    }

    size_t extents_size = sizeof(struct fiemap_extent) * (fiemap->fm_mapped_extents);

    if ((fiemap = (struct fiemap*)realloc(fiemap,sizeof(struct fiemap) +
                                  extents_size)) == NULL)
    {
        MFU_LOG(MFU_LOG_ERR, "Out of memory reallocating fiemap");
        goto fail_normal_copy;
    }

    memset(fiemap->fm_extents, 0, extents_size);
    fiemap->fm_extent_count   = fiemap->fm_mapped_extents;
    fiemap->fm_mapped_extents = 0;

    if (ioctl(in_fd, FS_IOC_FIEMAP, fiemap) < 0) {
        MFU_LOG(MFU_LOG_ERR, "fiemap ioctl() failed for src `%s'", src);
        goto fail_normal_copy;
    }

    uint64_t last_byte = offset + length;

    if (fiemap->fm_mapped_extents > 0) {
        uint64_t fe_logical = fiemap->fm_extents[0].fe_logical;
        uint64_t fe_length  = fiemap->fm_extents[0].fe_length;
        if (fe_logical < offset) {
            fiemap->fm_extents[0].fe_length -= (offset - fe_logical);
            fiemap->fm_extents[0].fe_logical = offset;
        }

        fe_logical = fiemap->fm_extents[fiemap->fm_mapped_extents - 1].fe_logical;
        fe_length  = fiemap->fm_extents[fiemap->fm_mapped_extents - 1].fe_length;
        if (fe_logical + fe_length > last_byte) {
           fiemap->fm_extents[fiemap->fm_mapped_extents - 1].fe_length -=
           fe_logical + fe_length - last_byte;
        }
    }

    *normal_copy_required = false;

    /* seek to offset in source file */
    if (mfu_lseek(src, in_fd, (off_t)last_ext_start, SEEK_SET) < 0) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in source path `%s' (errno=%d %s)",
            src, errno, strerror(errno));
        goto fail;
    }

    /* seek to offset in destination file */
    if (mfu_lseek(dest, out_fd, (off_t)last_ext_start, SEEK_SET) < 0) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' (errno=%d %s)",
            dest, errno, strerror(errno));
        goto fail;
    }

    unsigned int i;
    for (i = 0; i < fiemap->fm_mapped_extents; i++) {
        size_t ext_start;
        size_t ext_len;
        size_t ext_hole_size;

        size_t buf_size = mfu_copy_opts->block_size;
        void* buf = mfu_copy_opts->block_buf1;

        ext_start = fiemap->fm_extents[i].fe_logical;
        ext_len = fiemap->fm_extents[i].fe_length;
        ext_hole_size = ext_start - (last_ext_start + last_ext_len);

        if (ext_hole_size) {
            if (mfu_lseek(src, in_fd, (off_t)ext_start, SEEK_SET) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Couldn't seek in source path `%s' (errno=%d %s)",
                    src, errno, strerror(errno));
                goto fail;
            }
            if (mfu_lseek(dest, out_fd, (off_t)ext_hole_size, SEEK_CUR) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' (errno=%d %s)",
                    dest, errno, strerror(errno));
                goto fail;
            }
        }

        last_ext_start = ext_start;
        last_ext_len = ext_len;

        while (ext_len) {
            ssize_t num_read = mfu_read(src, in_fd, buf, MIN(ext_len, buf_size));

            if (!num_read)
                break;

            ssize_t num_written = mfu_write(dest, out_fd, buf, (size_t)num_read);

            if (num_written < 0) {
                MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s' (errno=%d %s)",
                          src, dest, errno, strerror(errno));
                goto fail;
            }
            if (num_written != num_read) {
                MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s'",
                    src, dest);
                goto fail;
            }

            ext_len -= (size_t)num_written;
            mfu_copy_stats.total_bytes_copied += (int64_t) num_written;
        }
    }

    off_t last_written = (off_t) last_byte;
    off_t file_size_offt = (off_t) file_size;
    if (last_written >= file_size_offt || file_size == 0) {
        /* Use ftruncate() here rather than truncate(), because grouplock
         * of Lustre would cause block to truncate() since the fd is different
         * from the out_fd. */
        if (mfu_ftruncate(out_fd, file_size_offt) < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to truncate destination file: %s (errno=%d %s)",
                dest, errno, strerror(errno));
            goto fail;
       }
    }

    if (last_written >= file_size_offt) {
        mfu_copy_stats.total_size += (int64_t) (file_size_offt - (off_t) offset);
    } else {
        mfu_copy_stats.total_size += (int64_t) last_byte;
    }

    free(fiemap);
    return 0;

fail:
    free(fiemap);

fail_normal_copy:
    return -1;
}

static int mfu_copy_file(
    const char* src,
    const char* dest,
    uint64_t offset,
    uint64_t length,
    uint64_t file_size,
    mfu_copy_opts_t* mfu_copy_opts)
{
    int ret;
    bool normal_copy_required;

    /* open the input file */
    int in_fd = mfu_copy_open_file(src, 1, &mfu_copy_src_cache, mfu_copy_opts);
    if (in_fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open input file `%s' (errno=%d %s)",
            src, errno, strerror(errno));
        return -1;
    }

    /* open the output file */
    int out_fd = mfu_copy_open_file(dest, 0, &mfu_copy_dst_cache, mfu_copy_opts);
    if (out_fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open output file `%s' (errno=%d %s)",
            dest, errno, strerror(errno));
        return -1;
    }

    if (mfu_copy_opts->sparse) {
        ret = mfu_copy_file_fiemap(src, dest, in_fd, out_fd, offset,
                               length, file_size, 
                               &normal_copy_required, mfu_copy_opts);
        if (!ret || !normal_copy_required) {
            return ret;
        }
    }

    ret = mfu_copy_file_normal(src, dest, in_fd, out_fd, 
            offset, length, file_size, mfu_copy_opts);

    return ret;
}

/* slices files in list at boundaries of chunk size, evenly distributes
 * chunks, and copies data from source to destination file,
 * returns 0 on success and -1 on error */
static int mfu_copy_files(mfu_flist list, uint64_t chunk_size, 
        int numpaths, const mfu_param_path* paths,
        const mfu_param_path* destpath, mfu_copy_opts_t* mfu_copy_opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* determine whether we should print status messages */
    int verbose = (mfu_debug_level >= MFU_LOG_VERBOSE);

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* barrier to ensure all procs are ready to start copy */
    MPI_Barrier(MPI_COMM_WORLD);

    /* indicate which phase we're in to user */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Copying data.");
    }
    
    /* start timer for entie operation */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_start = MPI_Wtime();
    uint64_t total_count = 0;

    /* split file list into a linked list of file sections,
     * this evenly spreads the file sections across processes */
    mfu_file_chunk* head = mfu_file_chunk_list_alloc(list, chunk_size);

    /* get a count of how many items are the chunk list */
    uint64_t list_count = mfu_file_chunk_list_size(head);

    /* allocate a flag for each element in chunk list,
     * will store 0 to mean copy of this chunk succeeded and 1 otherwise
     * to be used as input to logical OR to determine state of entire file */
    int* vals = (int*) MFU_MALLOC(list_count * sizeof(int));

    /* loop over and copy data for each file section we're responsible for */
    uint64_t i;
    const mfu_file_chunk* p = head;
    for (i = 0; i < list_count; i++) {
         /* assume we'll succeed in copying this chunk */
         vals[i] = 0;

        /* get name of destination file */
        char* dest = mfu_param_path_copy_dest(p->name, numpaths,
                paths, destpath, mfu_copy_opts);
        if (dest == NULL) {
            /* No need to copy it */
            p = p->next;
            continue;
        }

        /* add bytes to our running total */
        total_count += (uint64_t)p->length;

        /* copy portion of file corresponding to this chunk,
         * and record whether copy operation succeeded */
        int copy_rc = mfu_copy_file(p->name, dest, (uint64_t)p->offset, 
                (uint64_t)p->length, (uint64_t)p->file_size, mfu_copy_opts);
        if (copy_rc < 0) {
            /* error copying file */
            vals[i] = 1;
        }

        /* free the dest name */
        mfu_free(&dest);

        /* update pointer to next element */
        p = p->next;
    }

    /* close files */
    mfu_copy_close_file(&mfu_copy_src_cache);
    mfu_copy_close_file(&mfu_copy_dst_cache);

    /* barrier to ensure all files are closed,
     * may try to unlink bad destination files below */
    MPI_Barrier(MPI_COMM_WORLD);

    /* allocate a flag for each item in our file list */
    uint64_t size = mfu_flist_size(list);
    int* results = (int*) MFU_MALLOC(size * sizeof(int));

    /* intialize values, since not every item is represented
     * in chunk list */
    for (i = 0; i < size; i++) {
        results[i] = 0;
    }

    /* determnie which files were copied correctly */
    mfu_file_chunk_list_lor(list, head, vals, results);

    /* delete any destination file that failed to copy */
    for (i = 0; i < size; i++) {
        if (results[i] != 0) {
            /* found a file that had an error during copy,
             * compute destination name and delete it */
            const char* name = mfu_flist_file_get_name(list, i);
            const char* dest = mfu_param_path_copy_dest(name, numpaths,
                paths, destpath, mfu_copy_opts);
            if (dest != NULL) {
                /* sanity check to ensure we don't * delete the source file */
                if (strcmp(dest, name) != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to copy `%s' to `%s'", name, dest);
                    rc = -1;
#if 0
                    /* delete destination file */
                    int unlink_rc = mfu_unlink(dest);
                    if (unlink_rc != 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to unlink `%s' (errno=%d %s)",
                                  name, errno, strerror(errno)
                                );
                    }
#endif
                }

                /* free destination name */
                mfu_free(&dest);
            }
        }
    }

    /* free copy flags */
    mfu_free(&results);

    /* free the list of file chunks */
    mfu_file_chunk_list_free(&head);

    /* stop timer and report total count */
    MPI_Barrier(MPI_COMM_WORLD);
    double total_end = MPI_Wtime();

    /* print timing statistics */
    if (verbose) {
        uint64_t sum;
        MPI_Allreduce(&total_count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

        double rate = 0.0;
        double secs = total_end - total_start;
        if (secs > 0.0) {
          rate = (double)sum / secs;
        }

        /* convert bytes to units */
        double agg_size_tmp;
        const char* agg_size_units;
        mfu_format_bytes(sum, &agg_size_tmp, &agg_size_units);

        /* convert bandwidth to units */
        double agg_rate_tmp;
        const char* agg_rate_units;
        mfu_format_bw(rate, &agg_rate_tmp, &agg_rate_units);

        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Copy data: %.3lf %s (%lu bytes)",
              agg_size_tmp, agg_size_units, sum
            );
            MFU_LOG(MFU_LOG_INFO, "Copy rate: %.3lf %s (%lu bytes in %f seconds)",
              agg_rate_tmp, agg_rate_units, sum, secs
            );
        }
    }

    return rc;
}

static void mfu_sync_all(const char* msg)
{
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "%s", msg);
    }
    sync();

    MPI_Barrier(MPI_COMM_WORLD);
    double end = MPI_Wtime();

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Sync completed in %f seconds.", (end - start));
    }
}

static void print_summary(mfu_flist flist)
{
    uint64_t total_dirs    = 0;
    uint64_t total_files   = 0;
    uint64_t total_links   = 0;
    uint64_t total_unknown = 0;
    uint64_t total_bytes   = 0;

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* step through and print data */
    uint64_t idx = 0;
    uint64_t max = mfu_flist_size(flist);
    while (idx < max) {
        if (mfu_flist_have_detail(flist)) {
            /* get mode */
            mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

            /* get size */
            uint64_t size = mfu_flist_file_get_size(flist, idx);

            /* set file type */
            if (S_ISDIR(mode)) {
                total_dirs++;
            }
            else if (S_ISREG(mode)) {
                total_files++;
                total_bytes += size;
            }
            else if (S_ISLNK(mode)) {
                total_links++;
            }
            else {
                /* unknown file type */
                total_unknown++;
            }
        }
        else {
            /* get type */
            mfu_filetype type = mfu_flist_file_get_type(flist, idx);

            if (type == MFU_TYPE_DIR) {
                total_dirs++;
            }
            else if (type == MFU_TYPE_FILE) {
                total_files++;
            }
            else if (type == MFU_TYPE_LINK) {
                total_links++;
            }
            else {
                /* unknown file type */
                total_unknown++;
            }
        }

        /* go to next file */
        idx++;
    }

    /* get total directories, files, links, and bytes */
    uint64_t all_dirs, all_files, all_links, all_unknown, all_bytes;
    uint64_t all_count = mfu_flist_global_size(flist);
    MPI_Allreduce(&total_dirs,    &all_dirs,    1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_files,   &all_files,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_links,   &all_links,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_unknown, &all_unknown, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_bytes,   &all_bytes,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* convert total size to units */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Items: %llu", (unsigned long long) all_count);
        MFU_LOG(MFU_LOG_INFO, "  Directories: %llu", (unsigned long long) all_dirs);
        MFU_LOG(MFU_LOG_INFO, "  Files: %llu", (unsigned long long) all_files);
        MFU_LOG(MFU_LOG_INFO, "  Links: %llu", (unsigned long long) all_links);
        /* MFU_LOG("  Unknown: %lu", (unsigned long long) all_unknown); */

        if (mfu_flist_have_detail(flist)) {
            double agg_size_tmp;
            const char* agg_size_units;
            mfu_format_bytes(all_bytes, &agg_size_tmp, &agg_size_units);

            uint64_t size_per_file = 0.0;
            if (all_files > 0) {
                size_per_file = (uint64_t)((double)all_bytes / (double)all_files);
            }
            double size_per_file_tmp;
            const char* size_per_file_units;
            mfu_format_bytes(size_per_file, &size_per_file_tmp, &size_per_file_units);

            MFU_LOG(MFU_LOG_INFO, "Data: %.3lf %s (%.3lf %s per file)", agg_size_tmp, agg_size_units, size_per_file_tmp, size_per_file_units);
        }
    }

    return;
}

int mfu_flist_copy(mfu_flist src_cp_list, int numpaths,
        const mfu_param_path* paths, const mfu_param_path* destpath, 
        mfu_copy_opts_t* mfu_copy_opts)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
     
    /* set mfu_copy options in mfu_copy_opts_t struct */  

    /* copy the destination path to user opts structure */
    mfu_copy_opts->dest_path = MFU_STRDUP((*destpath).path);
    
    /* print note about what we're doing and the amount of files/data to be moved */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Copying to %s", mfu_copy_opts->dest_path);
    }
    mfu_flist_print_summary(src_cp_list);

    /* TODO: consider file system striping params here */
    /* hard code some configurables for now */

    /* Set default block size */
    mfu_copy_opts->block_size = FD_BLOCK_SIZE;

    /* allocate buffer to read/write files, aligned on 1MB boundaraies */
    size_t alignment = 1024*1024;
    mfu_copy_opts->block_buf1 = (char*) MFU_MEMALIGN(mfu_copy_opts->block_size, alignment);
    mfu_copy_opts->block_buf2 = (char*) MFU_MEMALIGN(mfu_copy_opts->block_size, alignment);

    /* Grab a relative and actual start time for the epilogue. */
    time(&(mfu_copy_stats.time_started));
    mfu_copy_stats.wtime_started = MPI_Wtime();

    /* Initialize statistics */
    mfu_copy_stats.total_dirs  = 0;
    mfu_copy_stats.total_files = 0;
    mfu_copy_stats.total_links = 0;
    mfu_copy_stats.total_size  = 0;
    mfu_copy_stats.total_bytes_copied = 0;

    /* Initialize file cache */
    mfu_copy_src_cache.name = NULL;
    mfu_copy_dst_cache.name = NULL;

    /* split items in file list into sublists depending on their
     * directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(src_cp_list, &levels, &minlevel, &lists);

    /* TODO: filter out files that are bigger than 0 bytes if we can't read them */

    /* create directories, from top down */
    int tmp_rc = mfu_create_directories(levels, minlevel, lists, numpaths,
            paths, destpath, mfu_copy_opts);
    if (tmp_rc < 0) {
        rc = -1;
    }

    /* operate on files in batches if batch size is given */
    uint64_t batch_size = mfu_copy_opts->batch_files;
    if (batch_size > 0) {
        /* operate in batches, get total size of list, our global
         * offset within it, and the local size of our list to
         * compute which batch our files are part of */
        uint64_t src_size   = mfu_flist_global_size(src_cp_list);
        uint64_t src_offset = mfu_flist_global_offset(src_cp_list);
        uint64_t src_count  = mfu_flist_size(src_cp_list);

        /* execute our batch copy */
        uint64_t batch_offset = 0;
        while (batch_offset < src_size) {
            /* create temporary list to copy a batch of files into */
            mfu_flist tmplist = mfu_flist_subset(src_cp_list);

            /* copy a full batch or until we run out of files */
            uint64_t count = 0;
            while (count < batch_size && (batch_offset + count) < src_size) {
                /* compute global index of this file */
                uint64_t global_idx = batch_offset + count;

                /* if this global index is in our list, check whether to copy it */
                if (src_offset <= global_idx && global_idx < (src_offset + src_count)) {
                    /* compute index of this item in our local list */
                    uint64_t idx = global_idx - src_offset;

                    /* copy item into temp list if is not a directory */
                    mfu_filetype type = mfu_flist_file_get_type(src_cp_list, idx);
                    if (type != MFU_TYPE_DIR) {
                        mfu_flist_file_copy(src_cp_list, idx, tmplist);
                    }
                }

                /* move on to next item */
                count++;
            }

            /* finish off our temp list */
            mfu_flist_summarize(tmplist);

            /* update our offset */
            batch_offset += count;

            /* if this batch is all directories, skip this part */
            uint64_t tmplist_size = mfu_flist_global_size(tmplist);
            if (tmplist_size > 0) {
                /* spread items evenly over ranks */
                mfu_flist spreadlist = mfu_flist_spread(tmplist);

                /* split items in file list into sublists depending on their
                 * directory depth */
                int levels2, minlevel2;
                mfu_flist* lists2;
                mfu_flist_array_by_depth(spreadlist, &levels2, &minlevel2, &lists2);

                /* create files and links */
                tmp_rc = mfu_create_files(levels2, minlevel2, lists2, numpaths,
                        paths, destpath, mfu_copy_opts);
                if (tmp_rc < 0) {
                    rc = -1;
                }

                /* copy data */
                tmp_rc = mfu_copy_files(spreadlist, mfu_copy_opts->chunk_size, 
                        numpaths, paths, destpath, mfu_copy_opts);
                if (tmp_rc < 0) {
                    rc = -1;
                }

                /* force data to backend to avoid the following metadata
                 * setting mismatch, which may happen on lustre */
                mfu_sync_all("Syncing data to disk.");

                /* set permissions, ownership, and timestamps if needed */
                mfu_copy_set_metadata(levels2, minlevel2, lists2, numpaths,
                        paths, destpath, mfu_copy_opts);

                /* free our lists of levels */
                mfu_flist_array_free(levels2, &lists2);

                /* free the list of spread items */
                mfu_flist_free(&spreadlist);

                /* force updates to disk */
                mfu_sync_all("Syncing updates to disk.");
            }

            /* done with our batch list */
            mfu_flist_free(&tmplist);

            /* Determine the actual and relative end time for the epilogue. */
            mfu_copy_stats.wtime_ended = MPI_Wtime();
            time(&(mfu_copy_stats.time_ended));

            /* compute time */
            double rel_time = mfu_copy_stats.wtime_ended - mfu_copy_stats.wtime_started;

            /* prep our values into buffer */
            int64_t values[5];
            values[0] = mfu_copy_stats.total_dirs;
            values[1] = mfu_copy_stats.total_files;
            values[2] = mfu_copy_stats.total_links;
            values[3] = mfu_copy_stats.total_size;
            values[4] = mfu_copy_stats.total_bytes_copied;

            /* sum values across processes */
            int64_t sums[5];
            MPI_Allreduce(values, sums, 5, MPI_INT64_T, MPI_SUM, MPI_COMM_WORLD);

            /* extract results from allreduce */
            int64_t agg_dirs   = sums[0];
            int64_t agg_files  = sums[1];
            int64_t agg_links  = sums[2];
            int64_t agg_size   = sums[3];
            int64_t agg_copied = sums[4];

            /* compute rate of copy */
            double agg_rate = (double)agg_copied / rel_time;
            if (rel_time > 0.0) {
                agg_rate = (double)agg_copied / rel_time;
            }

            if(rank == 0) {
                /* format start time */
                char starttime_str[256];
                struct tm* localstart = localtime(&(mfu_copy_stats.time_started));
                strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S", localstart);

                /* format end time */
                char endtime_str[256];
                struct tm* localend = localtime(&(mfu_copy_stats.time_ended));
                strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S", localend);

                /* total number of items */
                int64_t agg_items = agg_dirs + agg_files + agg_links;

                /* convert size to units */
                double agg_size_tmp;
                const char* agg_size_units;
                mfu_format_bytes((uint64_t)agg_size, &agg_size_tmp, &agg_size_units);

                /* convert bandwidth to units */
                double agg_rate_tmp;
                const char* agg_rate_units;
                mfu_format_bw(agg_rate, &agg_rate_tmp, &agg_rate_units);

                MFU_LOG(MFU_LOG_INFO, "Started: %s", starttime_str);
                MFU_LOG(MFU_LOG_INFO, "Current: %s", endtime_str);
                MFU_LOG(MFU_LOG_INFO, "Seconds: %.3lf", rel_time);
                MFU_LOG(MFU_LOG_INFO, "Items: %" PRId64, agg_items);
                MFU_LOG(MFU_LOG_INFO, "Data: %.3lf %s (%" PRId64 " bytes)",
                    agg_size_tmp, agg_size_units, agg_size);

                MFU_LOG(MFU_LOG_INFO, "Rate: %.3lf %s " \
                    "(%.3" PRId64 " bytes in %.3lf seconds)", \
                    agg_rate_tmp, agg_rate_units, agg_copied, rel_time);
                MFU_LOG(MFU_LOG_INFO, "Copied %" PRId64 " of %" PRId64 " items (%.3lf%%)", agg_items, src_size, (double)agg_items/(double)src_size*100.0);
            }
        }

        /* set permissions, ownership, and timestamps if needed */
        mfu_copy_set_metadata_dirs(levels, minlevel, lists, numpaths,
                paths, destpath, mfu_copy_opts);
    
        /* force updates to disk */
        mfu_sync_all("Syncing directory updates to disk.");
    } else {
        /* user does not want to batch files, so copy the whole list */

        /* create files and links */
        tmp_rc = mfu_create_files(levels, minlevel, lists, numpaths,
                paths, destpath, mfu_copy_opts);
        if (tmp_rc < 0) {
            rc = -1;
        }

        /* copy data */
        tmp_rc = mfu_copy_files(src_cp_list, mfu_copy_opts->chunk_size, 
                numpaths, paths, destpath, mfu_copy_opts);
        if (tmp_rc < 0) {
            rc = -1;
        }

        /* force data to backend to avoid the following metadata
         * setting mismatch, which may happen on lustre */
        mfu_sync_all("Syncing data to disk.");

        /* set permissions, ownership, and timestamps if needed */
        mfu_copy_set_metadata(levels, minlevel, lists, numpaths,
                paths, destpath, mfu_copy_opts);
    
        /* force updates to disk */
        mfu_sync_all("Syncing directory updates to disk.");
    }

    /* free our lists of levels */
    mfu_flist_array_free(levels, &lists);

    /* free buffers */
    mfu_free(&mfu_copy_opts->block_buf1);
    mfu_free(&mfu_copy_opts->block_buf2);

    /* Determine the actual and relative end time for the epilogue. */
    mfu_copy_stats.wtime_ended = MPI_Wtime();
    time(&(mfu_copy_stats.time_ended));

    /* compute time */
    double rel_time = mfu_copy_stats.wtime_ended - \
                      mfu_copy_stats.wtime_started;

    /* prep our values into buffer */
    int64_t values[5];
    values[0] = mfu_copy_stats.total_dirs;
    values[1] = mfu_copy_stats.total_files;
    values[2] = mfu_copy_stats.total_links;
    values[3] = mfu_copy_stats.total_size;
    values[4] = mfu_copy_stats.total_bytes_copied;

    /* sum values across processes */
    int64_t sums[5];
    MPI_Allreduce(values, sums, 5, MPI_INT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* extract results from allreduce */
    int64_t agg_dirs   = sums[0];
    int64_t agg_files  = sums[1];
    int64_t agg_links  = sums[2];
    int64_t agg_size   = sums[3];
    int64_t agg_copied = sums[4];

    /* compute rate of copy */
    double agg_rate = (double)agg_copied / rel_time;
    if (rel_time > 0.0) {
        agg_rate = (double)agg_copied / rel_time;
    }

    if(rank == 0) {
        /* format start time */
        char starttime_str[256];
        struct tm* localstart = localtime(&(mfu_copy_stats.time_started));
        strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S", localstart);

        /* format end time */
        char endtime_str[256];
        struct tm* localend = localtime(&(mfu_copy_stats.time_ended));
        strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S", localend);

        /* total number of items */
        int64_t agg_items = agg_dirs + agg_files + agg_links;

        /* convert size to units */
        double agg_size_tmp;
        const char* agg_size_units;
        mfu_format_bytes((uint64_t)agg_size, &agg_size_tmp, &agg_size_units);

        /* convert bandwidth to units */
        double agg_rate_tmp;
        const char* agg_rate_units;
        mfu_format_bw(agg_rate, &agg_rate_tmp, &agg_rate_units);

        MFU_LOG(MFU_LOG_INFO, "Started: %s", starttime_str);
        MFU_LOG(MFU_LOG_INFO, "Completed: %s", endtime_str);
        MFU_LOG(MFU_LOG_INFO, "Seconds: %.3lf", rel_time);
        MFU_LOG(MFU_LOG_INFO, "Items: %" PRId64, agg_items);
        MFU_LOG(MFU_LOG_INFO, "  Directories: %" PRId64, agg_dirs);
        MFU_LOG(MFU_LOG_INFO, "  Files: %" PRId64, agg_files);
        MFU_LOG(MFU_LOG_INFO, "  Links: %" PRId64, agg_links);
        MFU_LOG(MFU_LOG_INFO, "Data: %.3lf %s (%" PRId64 " bytes)",
            agg_size_tmp, agg_size_units, agg_size);

        MFU_LOG(MFU_LOG_INFO, "Rate: %.3lf %s " \
            "(%.3" PRId64 " bytes in %.3lf seconds)", \
            agg_rate_tmp, agg_rate_units, agg_copied, rel_time);
    }

    /* determine whether any process reported an error,
     * inputs should are either 0 or -1, so min will be -1 on any -1 */
    int all_rc;
    MPI_Allreduce(&rc, &all_rc, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    rc = all_rc;

    return rc;
}

int mfu_flist_file_sync_meta(mfu_flist src_list, uint64_t src_index, mfu_flist dst_list, uint64_t dst_index)
{
    /* assume we'll succeed */
    int rc = 0;
    int tmp_rc;

    /* get destination path */
    const char* dest_path = mfu_flist_file_get_name(dst_list, dst_index);

    /* get owner and group ids */
    uid_t src_uid = (uid_t) mfu_flist_file_get_uid(src_list, src_index);
    gid_t src_gid = (gid_t) mfu_flist_file_get_gid(src_list, src_index);

    uid_t dst_uid = (uid_t) mfu_flist_file_get_uid(dst_list, dst_index);
    gid_t dst_gid = (gid_t) mfu_flist_file_get_gid(dst_list, dst_index);

    /* update ownership on destination if needed */
    if ((src_uid != dst_uid) || (src_gid != dst_gid)) {
        tmp_rc = mfu_copy_ownership(src_list, src_index, dest_path);
        if (tmp_rc < 0) {
            rc = -1;
        }
    }

    /* update permissions on destination if needed */
    mode_t src_mode = (mode_t) mfu_flist_file_get_mode(src_list, src_index);
    mode_t dst_mode = (mode_t) mfu_flist_file_get_mode(dst_list, dst_index);
    if (src_mode != dst_mode) {
        tmp_rc = mfu_copy_permissions(src_list, src_index, dest_path);
        if (tmp_rc < 0) {
            rc = -1;
        }
    }

    /* get atime seconds and nsecs */
    uint64_t src_atime      = mfu_flist_file_get_atime(src_list, src_index);
    uint64_t src_atime_nsec = mfu_flist_file_get_atime_nsec(src_list, src_index);
    uint64_t dst_atime      = mfu_flist_file_get_atime(dst_list, dst_index);
    uint64_t dst_atime_nsec = mfu_flist_file_get_atime_nsec(dst_list, dst_index);

    /* get mtime seconds and nsecs */
    uint64_t src_mtime      = mfu_flist_file_get_mtime(src_list, src_index);
    uint64_t src_mtime_nsec = mfu_flist_file_get_mtime_nsec(src_list, src_index);
    uint64_t dst_mtime      = mfu_flist_file_get_mtime(dst_list, dst_index);
    uint64_t dst_mtime_nsec = mfu_flist_file_get_mtime_nsec(dst_list, dst_index);

    /* update atime and mtime on destination if needed */
    if ((src_atime != dst_atime) || (src_atime_nsec != dst_atime_nsec) ||
        (src_mtime != dst_mtime) || (src_mtime_nsec != dst_mtime_nsec))
    {
        tmp_rc = mfu_copy_timestamps(src_list, src_index, dest_path);
        if (tmp_rc < 0) {
            rc = -1;
        }
    }

    return rc;
}

/* return a newly allocated copy_opts structure, set default values on its fields */
mfu_copy_opts_t* mfu_copy_opts_new(void)
{
    mfu_copy_opts_t* opts = (mfu_copy_opts_t*) MFU_MALLOC(sizeof(mfu_copy_opts_t));

    /* By default, assume we are not copying into a directory */
    opts->copy_into_dir = 0;

    /* By default, we want the sync option off */
    opts->do_sync       = 0;

    /* to record destination path that we'll be copying to */
    opts->dest_path     = NULL;

    /* records name of input file to read source list from (not used?) */
    opts->input_file    = NULL;

    /* By default, don't bother to preserve all attributes. */
    opts->preserve      = false;

    /* By default, don't use O_DIRECT. */
    opts->synchronous   = false;

    /* By default, don't use sparse file. */
    opts->sparse        = false;

    /* Set default chunk size */
    opts->chunk_size    = 1*1024*1024;

    /* temporaries used during the copy operation for buffers to read/write data */
    opts->block_size    = FD_BLOCK_SIZE;
    opts->block_buf1    = NULL;
    opts->block_buf2    = NULL;

    /* Lustre grouplock ID */
    opts->grouplock_id  = -1;

    /* By default, do not limit the batch size */
    opts->batch_files   = 0;

    return opts;
}

void mfu_copy_opts_delete(mfu_copy_opts_t** popts)
{
  if (popts != NULL) {
    mfu_copy_opts_t* opts = *popts;

    /* free fields allocated on opts */
    if (opts != NULL) {
      mfu_free(&opts->dest_path);
      mfu_free(&opts->input_file);
      mfu_free(&opts->block_buf1);
      mfu_free(&opts->block_buf2);
    }

    mfu_free(popts);
  }
}
