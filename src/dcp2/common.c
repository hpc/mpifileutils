/* See the file "COPYING" for the full license governing this code. */

#include "common.h"
#include "handle_args.h"

#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>

/** Where we should keep statistics related to this file copy. */
DCOPY_statistics_t DCOPY_statistics;

/** Where we should store options specified by the user. */
DCOPY_options_t DCOPY_user_opts;

/** Cache most recent open file descriptor to avoid opening / closing the same file */
DCOPY_file_cache_t DCOPY_src_cache;
DCOPY_file_cache_t DCOPY_dst_cache;

/** What rank the current process is. */
int DCOPY_global_rank;

int DCOPY_open_file(const char* file, int read_flag, DCOPY_file_cache_t* cache)
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
            bayer_close(name, fd);
            bayer_free(&cache->name);
        }
    }

    /* open the new file */
    if (read_flag) {
        int flags = O_RDONLY;
        if (DCOPY_user_opts.synchronous) {
            flags |= O_DIRECT;
        }
        newfd = bayer_open(file, flags);
    } else {
        int flags = O_WRONLY | O_CREAT;
        if (DCOPY_user_opts.synchronous) {
            flags |= O_DIRECT;
        }
        newfd = bayer_open(file, flags, DCOPY_DEF_PERMS_FILE);
    }

    /* cache the file descriptor */
    if (newfd != -1) {
        cache->name = BAYER_STRDUP(file);
        cache->fd   = newfd;
        cache->read = read_flag;
    }

    return newfd;
}

int DCOPY_close_file(DCOPY_file_cache_t* cache)
{
    int rc = 0;

    /* close file if we have one */
    char* name = cache->name;
    if (name != NULL) {
        int fd = cache->fd;

        /* if open for write, fsync */
        int read_flag = cache->read;
        if (! read_flag) {
            rc = bayer_fsync(name, fd);
        }

        /* close the file and delete the name string */
        rc = bayer_close(name, fd);
        bayer_free(&cache->name);
    }

    return rc;
}

/* copy all extended attributes from op->operand to dest_path */
void DCOPY_copy_xattrs(
    bayer_flist flist,
    uint64_t idx,
    const char* dest_path)
{
#if DCOPY_USE_XATTRS
    /* get source file name */
    const char* src_path = bayer_flist_file_get_name(flist, idx);

    /* start with a reasonable buffer, we'll allocate more as needed */
    size_t list_bufsize = 1204;
    char* list = (char*) BAYER_MALLOC(list_bufsize);

    /* get list, if list_size == ERANGE, try again */
    ssize_t list_size;
    int got_list = 0;

    /* get current estimate for list size */
    while(! got_list) {
        list_size = llistxattr(src_path, list, list_bufsize);

        if(list_size < 0) {
            if(errno == ERANGE) {
                /* buffer is too small, free our current buffer
                 * and call it again with size==0 to get new size */
                bayer_free(&list);
                list_bufsize = 0;
            }
            else if(errno == ENOTSUP) {
                /* this is common enough that we silently ignore it */
                break;
            }
            else {
                /* this is a real error */
                BAYER_LOG(BAYER_LOG_ERR, "Failed to get list of extended attributes on %s llistxattr() errno=%d %s",
                    src_path, errno, strerror(errno)
                   );
                break;
            }
        }
        else {
            if(list_size > 0 && list_bufsize == 0) {
                /* called llistxattr with size==0 and got back positive
                 * number indicating size of buffer we need to allocate */
                list_bufsize = (size_t) list_size;
                list = (char*) BAYER_MALLOC(list_bufsize);
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
            void* val = (void*) BAYER_MALLOC(val_bufsize);

            /* lookup value for name */
            ssize_t val_size;
            int got_val = 0;

            while(! got_val) {
                val_size = lgetxattr(src_path, name, val, val_bufsize);

                if(val_size < 0) {
                    if(errno == ERANGE) {
                        /* buffer is too small, free our current buffer
                         * and call it again with size==0 to get new size */
                        bayer_free(&val);
                        val_bufsize = 0;
                    }
                    else if(errno == ENOATTR) {
                        /* source object no longer has this attribute,
                         * maybe deleted out from under us */
                        break;
                    }
                    else {
                        /* this is a real error */
                        BAYER_LOG(BAYER_LOG_ERR, "Failed to get value for name=%s on %s llistxattr() errno=%d %s",
                            name, src_path, errno, strerror(errno)
                           );
                        break;
                    }
                }
                else {
                    if(val_size > 0 && val_bufsize == 0) {
                        /* called lgetxattr with size==0 and got back positive
                         * number indicating size of buffer we need to allocate */
                        val_bufsize = (size_t) val_size;
                        val = (void*) BAYER_MALLOC(val_bufsize);
                    }
                    else {
                        /* got our value, it's size is in val_size, which may be 0 */
                        got_val = 1;
                    }
                }
            }

            /* set attribute on destination object */
            if(got_val) {
                int setrc = lsetxattr(dest_path, name, val, (size_t) val_size, 0);

                if(setrc != 0) {
                    BAYER_LOG(BAYER_LOG_ERR, "Failed to set value for name=%s on %s llistxattr() errno=%d %s",
                        name, dest_path, errno, strerror(errno)
                       );
                }
            }

            /* free value string */
            bayer_free(&val);
            val_bufsize = 0;

            /* jump to next name */
            size_t namelen = strlen(name) + 1;
            name += namelen;
        }
    }

    /* free space allocated for list */
    bayer_free(&list);
    list_bufsize = 0;

    return;
#endif /* DCOPY_USE_XATTR */
}

void DCOPY_copy_ownership(
    bayer_flist flist,
    uint64_t idx,
    const char* dest_path)
{
    /* get user id and group id of file */
    uid_t uid = (uid_t) bayer_flist_file_get_uid(flist, idx);
    gid_t gid = (gid_t) bayer_flist_file_get_gid(flist, idx);

    /* note that we use lchown to change ownership of link itself, it path happens to be a link */
    if(bayer_lchown(dest_path, uid, gid) != 0) {
        /* TODO: are there other EPERM conditions we do want to report? */

        /* since the user running dcp may not be the owner of the
         * file, we could hit an EPERM error here, and the file
         * will be left with the effective uid and gid of the dcp
         * process, don't bother reporting an error for that case */
        if (errno != EPERM) {
            BAYER_LOG(BAYER_LOG_ERR, "Failed to change ownership on %s lchown() errno=%d %s",
                dest_path, errno, strerror(errno)
               );
        }
    }

    return;
}

/* TODO: condionally set setuid and setgid bits? */
void DCOPY_copy_permissions(
    bayer_flist flist,
    uint64_t idx,
    const char* dest_path)
{
    /* get mode and type */
    bayer_filetype type = bayer_flist_file_get_type(flist, idx);
    mode_t mode = (mode_t) bayer_flist_file_get_mode(flist, idx);

    /* change mode */
    if(type != BAYER_TYPE_LINK) {
        if(bayer_chmod(dest_path, mode) != 0) {
            BAYER_LOG(BAYER_LOG_ERR, "Failed to change permissions on %s chmod() errno=%d %s",
                dest_path, errno, strerror(errno)
               );
        }
    }

    return;
}

void DCOPY_copy_timestamps(
    bayer_flist flist,
    uint64_t idx,
    const char* dest_path)
{
    /* get atime seconds and nsecs */
    uint64_t atime      = bayer_flist_file_get_atime(flist, idx);
    uint64_t atime_nsec = bayer_flist_file_get_atime_nsec(flist, idx);

    /* get mtime seconds and nsecs */
    uint64_t mtime      = bayer_flist_file_get_mtime(flist, idx);
    uint64_t mtime_nsec = bayer_flist_file_get_mtime_nsec(flist, idx);

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
    if(utimensat(AT_FDCWD, dest_path, times, AT_SYMLINK_NOFOLLOW) != 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Failed to change timestamps on %s utime() errno=%d %s",
            dest_path, errno, strerror(errno)
           );
    }

#if 0
    /* TODO: see stat-time.h and get_stat_atime/mtime/ctime to read sub-second times,
     * and use utimensat to set sub-second times */
    /* as last step, change timestamps */
    if(! S_ISLNK(statbuf->st_mode)) {
        struct utimbuf times;
        times.actime  = statbuf->st_atime;
        times.modtime = statbuf->st_mtime;
        if(utime(dest_path, &times) != 0) {
            BAYER_LOG(BAYER_LOG_ERR, "Failed to change timestamps on %s utime() errno=%d %s",
                dest_path, errno, strerror(errno)
               );
        }
    }
    else {
        struct timeval tv[2];
        tv[0].tv_sec  = statbuf->st_atime;
        tv[0].tv_usec = 0;
        tv[1].tv_sec  = statbuf->st_mtime;
        tv[1].tv_usec = 0;
        if(lutimes(dest_path, tv) != 0) {
            BAYER_LOG(BAYER_LOG_ERR, "Failed to change timestamps on %s utime() errno=%d %s",
                dest_path, errno, strerror(errno)
               );
        }
    }
#endif

    return;
}

/* called by single process upon detection of a problem */
void DCOPY_abort(int code)
{
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

/* called globally by all procs to exit */
void DCOPY_exit(int code)
{
    /* CIRCLE_finalize or will this hang? */
    bayer_finalize();
    MPI_Finalize();
    exit(code);
}

/* EOF */
