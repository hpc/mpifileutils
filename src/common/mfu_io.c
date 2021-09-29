#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fcntl.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <assert.h>
#include <libgen.h>

#include "mfu.h"
#include "mfu_errors.h"

#define MFU_IO_TRIES  (5)
#define MFU_IO_USLEEP (100)

static int mpi_rank;

/* calls access, and retries a few times if we get EIO or EINTR */
int mfu_file_access(const char* path, int amode, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_access(path, amode);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_access(path, amode, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }
}

int mfu_access(const char* path, int amode)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = access(path, amode);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int daos_access(const char* path, int amode, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_access(mfu_file->dfs_sys, path, amode, 0);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* calls faccessat, and retries a few times if we get EIO or EINTR */
int mfu_file_faccessat(int dirfd, const char* path, int amode, int flags, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_faccessat(dirfd, path, amode, flags);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_faccessat(dirfd, path, amode, flags, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    } 
}

int mfu_faccessat(int dirfd, const char* path, int amode, int flags)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = faccessat(dirfd, path, amode, flags);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* Emulates faccessat for a DAOS path */
int daos_faccessat(int dirfd, const char* path, int amode, int flags, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    /* Only current working directory supported at this time */
    if (dirfd != AT_FDCWD) {
        return mfu_errno2rc(ENOTSUP);
    }

    int access_flags = (flags & AT_SYMLINK_NOFOLLOW) ? O_NOFOLLOW : 0;
    int rc = dfs_sys_access(mfu_file->dfs_sys, path, amode, access_flags);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* calls lchown, and retries a few times if we get EIO or EINTR */
int mfu_file_lchown(const char* path, uid_t owner, gid_t group, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_lchown(path, owner, group);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_lchown(path, owner, group, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }    
}

int mfu_lchown(const char* path, uid_t owner, gid_t group)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = lchown(path, owner, group);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int daos_lchown(const char* path, uid_t owner, gid_t group, mfu_file_t* mfu_file)
{
    /* At this time, DFS does not support updating the uid or gid.
     * These are set at the container level, not file level */
    return mfu_errno2rc(0);
}

int daos_chmod(const char *path, mode_t mode, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_chmod(mfu_file->dfs_sys, path, mode);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

int mfu_chmod(const char* path, mode_t mode)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = chmod(path, mode);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* calls chmod, and retries a few times if we get EIO or EINTR */
int mfu_file_chmod(const char* path, mode_t mode, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_chmod(path, mode);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_chmod(path, mode, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }
}

/* calls utimensat, and retries a few times if we get EIO or EINTR */
int mfu_file_utimensat(int dirfd, const char* pathname, const struct timespec times[2], int flags,
                       mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_utimensat(dirfd, pathname, times, flags);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_utimensat(dirfd, pathname, times, flags, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  pathname, mfu_file->type);
    }
}

int mfu_utimensat(int dirfd, const char* pathname, const struct timespec times[2], int flags)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = utimensat(dirfd, pathname, times, flags);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* Emulates utimensat by calling dfs_osetattr */
int daos_utimensat(int dirfd, const char* pathname, const struct timespec times[2], int flags,
                   mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    /* Only current working directory supported at this time */
    if (dirfd != AT_FDCWD) {
        return mfu_errno2rc(ENOTSUP);
    }

    int time_flags = (flags & AT_SYMLINK_NOFOLLOW) ? O_NOFOLLOW : 0;
    int rc = dfs_sys_utimens(mfu_file->dfs_sys, pathname, times, time_flags);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

int daos_stat(const char* path, struct stat* buf, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_stat(mfu_file->dfs_sys, path, 0, buf);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

int mfu_stat(const char* path, struct stat* buf) {
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = stat(path, buf);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* calls stat, and retries a few times if we get EIO or EINTR */
int mfu_file_stat(const char* path, struct stat* buf, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_stat(path, buf);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_stat(path, buf, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }
}

/* lstat a DAOS path */
int daos_lstat(const char* path, struct stat* buf, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc =  dfs_sys_stat(mfu_file->dfs_sys, path, O_NOFOLLOW, buf);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

int mfu_lstat(const char* path, struct stat* buf) {
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = lstat(path, buf);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* calls lstat, and retries a few times if we get EIO or EINTR */
int mfu_file_lstat(const char* path, struct stat* buf, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_lstat(path, buf);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_lstat(path, buf, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }
}

/* calls lstat64, and retries a few times if we get EIO or EINTR */
int mfu_lstat64(const char* path, struct stat64* buf)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = lstat64(path, buf);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int daos_mknod(const char* path, mode_t mode, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_mknod(mfu_file->dfs_sys, path, mode, 0, 0);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

int mfu_mknod(const char* path, mode_t mode, dev_t dev)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = mknod(path, mode, dev);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* call mknod, retry a few times on EINTR or EIO */
int mfu_file_mknod(const char* path, mode_t mode, dev_t dev, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_mknod(path, mode, dev);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_mknod(path, mode, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }
}

int mfu_file_remove(const char* path, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_remove(path);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_remove(path, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }
}

/* call remove, retry a few times on EINTR or EIO */
int mfu_remove(const char* path)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = remove(path);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int daos_remove(const char* path, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_remove(mfu_file->dfs_sys, path, false, NULL);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* calls realpath */
char* mfu_file_realpath(const char* path, char* resolved_path, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        char* p = mfu_realpath(path, resolved_path);
        return p;
    } else if (mfu_file->type == DFS) {
        char* p = daos_realpath(path, resolved_path, mfu_file);
        return p;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }
}

char* mfu_realpath(const char* path, char* resolved_path)
{
    char* p = realpath(path, resolved_path);
    return p;
}

char* daos_realpath(const char* path, char* resolved_path, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    /* There is currently not a reasonable way to do this */
    return NULL;
#else
    errno = ENOSYS;
    return NULL;
#endif
}

/*****************************
 * Links
 ****************************/

ssize_t daos_readlink(const char* path, char* buf, size_t bufsize, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t got_size = bufsize;
    int rc = dfs_sys_readlink(mfu_file->dfs_sys, path, buf, &got_size);
    if (rc != 0) {
        errno = rc;
    }
    return (ssize_t) got_size;
#else
    return (ssize_t) mfu_errno2rc(ENOSYS);
#endif
}

/* call readlink, retry a few times on EINTR or EIO */
ssize_t mfu_readlink(const char* path, char* buf, size_t bufsize)
{
    ssize_t rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = readlink(path, buf, bufsize);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

ssize_t mfu_file_readlink(const char* path, char* buf, size_t bufsize, mfu_file_t* mfu_file)
{
    int rc;

    if (mfu_file->type == POSIX) {
        rc = mfu_readlink(path, buf, bufsize);
    } else if (mfu_file->type == DFS) {
        rc = daos_readlink(path, buf, bufsize, mfu_file);
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }

    return rc;
}

/* emulates symlink for a DAOS symlink */
int daos_symlink(const char* oldpath, const char* newpath, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_symlink(mfu_file->dfs_sys, oldpath, newpath);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* call symlink, retry a few times on EINTR or EIO */
int mfu_symlink(const char* oldpath, const char* newpath)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = symlink(oldpath, newpath);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int mfu_file_symlink(const char* oldpath, const char* newpath, mfu_file_t* mfu_file)
{
    int rc;

    if (mfu_file->type == POSIX) {
        rc = mfu_symlink(oldpath, newpath);
    } else if (mfu_file->type == DFS) {
        rc = daos_symlink(oldpath, newpath, mfu_file);
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  oldpath, mfu_file->type);
    }

    return rc;
}

/* call hardlink, retry a few times on EINTR or EIO */
int mfu_hardlink(const char* oldpath, const char* newpath)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = link(oldpath, newpath);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/*****************************
 * Files
 ****************************/
int daos_open(const char* file, int flags, mode_t mode, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_open(mfu_file->dfs_sys, file, mode, flags, 0, 0, NULL, &(mfu_file->obj));
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* open file with specified flags and mode, retry open a few times on failure */
int mfu_open(const char* file, int flags, ...)
{
    /* extract the mode (see man 2 open) */
    int mode_set = 0;
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = va_arg(ap, mode_t);
        va_end(ap);
        mode_set = 1;
    }

    /* attempt to open file */
    int fd = -1;
    errno = 0;
    if (mode_set) {
        fd = open(file, flags, mode);
    }
    else {
        fd = open(file, flags);
    }

    /* if open failed, try a few more times */
    if (fd < 0) {
        /* try again */
        int tries = MFU_IO_TRIES;
        while (tries && fd < 0) {
            /* sleep a bit before consecutive tries */
            usleep(MFU_IO_USLEEP);

            /* open again */
            errno = 0;
            if (mode_set) {
                fd = open(file, flags, mode);
            }
            else {
                fd = open(file, flags);
            }
            tries--;
        }

         /* if we still don't have a valid file, consider it an error */
         if (fd < 0) {
             /* we could abort, but probably don't want to here */
         }
    }
    return fd;
}

/* Open a file.
 * Return 0 on success, -1 on error */
int mfu_file_open(const char* file, int flags, mfu_file_t* mfu_file, ...)
{
    /* extract the mode (see man 2 open) */
    int mode_set = 0;
    mode_t mode  = 0;
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, mfu_file);
        mode = va_arg(ap, mode_t);
        va_end(ap);
        mode_set = 1;
    }

    int rc = 0;

    if (mfu_file->type == POSIX) {
        if (mode_set) {
            mfu_file->fd = mfu_open(file, flags, mode);
        } else {
            mfu_file->fd = mfu_open(file, flags);
        }
        if (mfu_file->fd < 0) {
            rc = -1;
        }
    } else if (mfu_file->type == DFS) {
        daos_open(file, flags, mode, mfu_file);
#ifdef DAOS_SUPPORT
        if (mfu_file->obj == NULL) {
            rc = -1;
        }
#endif
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  file, mfu_file->type);
    }

    return rc;
}

/* release an open object */
int daos_close(const char* file, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_close(mfu_file->obj);
    if (rc == 0) {
        mfu_file->obj = NULL;
    }
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* close file */
int mfu_close(const char* file, int fd)
{
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    int rc = close(fd);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int mfu_file_close(const char* file, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_close(file, mfu_file->fd);
        if (rc == 0) {
            mfu_file->fd = -1;
        }
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_close(file, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  file, mfu_file->type);
    }
}

int daos_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence)
{
#ifdef DAOS_SUPPORT
    if (whence == SEEK_SET) {
        mfu_file->offset = (daos_off_t)pos;
    } else {
        MFU_ABORT(-1, "daos_lseek whence type not known: %d", whence);
    }
    return 0;
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* seek file descriptor to specified position */
off_t mfu_lseek(const char* file, int fd, off_t pos, int whence)
{
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    off_t rc = lseek(fd, pos, whence);
    if (rc == (off_t)-1) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

off_t mfu_file_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence)
{
    if (mfu_file->type == POSIX) {
        off_t rc = mfu_lseek(file, mfu_file->fd, pos, whence);
        return rc;
    } else if (mfu_file->type == DFS) {
        off_t rc = daos_lseek(file, mfu_file, pos, whence);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  file, mfu_file->type);
    }
}

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t mfu_file_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        ssize_t got_size = mfu_read(file, mfu_file->fd, buf, size);
        return got_size;
    } else if (mfu_file->type == DFS) {
        ssize_t got_size = daos_read(file, buf, size, mfu_file);
        return got_size;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  file, mfu_file->type);
    }
}

ssize_t daos_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t got_size = size;
    int rc = dfs_sys_read(mfu_file->dfs_sys, mfu_file->obj, buf, mfu_file->offset, &got_size, NULL);
    if (rc != 0) {
        errno = rc;
        return -1;
    } else {
        /* update file pointer with number of bytes read */
        mfu_file->offset += (daos_off_t)got_size;
    }
    return (ssize_t)got_size;
#else
    return (ssize_t)mfu_errno2rc(ENOSYS);
#endif
}

ssize_t mfu_read(const char* file, int fd, void* buf, size_t size)
{
    int tries = MFU_IO_TRIES;
    ssize_t n = 0;
    while ((size_t)n < size) {
        errno = 0;
        ssize_t rc = read(fd, (char*) buf + n, size - (size_t)n);
        if (rc > 0) {
            /* read some data */
            n += rc;
            tries = MFU_IO_TRIES;

            /* return, even if we got a short read */
            return n;
        }
        else if (rc == 0) {
            /* EOF */
            return n;
        }
        else {   /* (rc < 0) */
            /* something worth printing an error about */
            tries--;
            if (tries <= 0) {
                /* too many failed retries, give up */
                MFU_ABORT(-1, "Failed to read file %s errno=%d (%s)",
                            file, errno, strerror(errno)
                           );
            }

            /* sleep a bit before consecutive tries */
            usleep(MFU_IO_USLEEP);
        }
    }
    return n;
}

/* reliable write to file descriptor (retries, if necessary, until hard error) */
ssize_t mfu_file_write(const char* file, const void* buf, size_t size, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        ssize_t num_bytes_written = mfu_write(file, mfu_file->fd, buf, size);
        return num_bytes_written;
    } else if (mfu_file->type == DFS) {
        ssize_t num_bytes_written = daos_write(file, buf, size, mfu_file);
        return num_bytes_written;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  file, mfu_file->type);
    }
}

ssize_t mfu_write(const char* file, int fd, const void* buf, size_t size)
{
    int tries = MFU_IO_TRIES;
    ssize_t n = 0;
    while ((size_t)n < size) {
        errno = 0;
        ssize_t rc = write(fd, (const char*) buf + n, size - (size_t)n);
        if (rc > 0) {
            /* wrote some data */
            n += rc;
            tries = MFU_IO_TRIES;
        }
        else if (rc == 0) {
            /* something bad happened, print an error and abort */
            MFU_ABORT(-1, "Failed to write file %s errno=%d (%s)",
                        file, errno, strerror(errno)
                       );
        }
        else {   /* (rc < 0) */
            /* something worth printing an error about */
            tries--;
            if (tries <= 0) {
                /* too many failed retries, give up */
                MFU_ABORT(-1, "Failed to write file %s errno=%d (%s)",
                            file, errno, strerror(errno)
                           );
            }

            /* sleep a bit before consecutive tries */
            usleep(MFU_IO_USLEEP);
        }
    }
    return n;
}

ssize_t daos_write(const char* file, const void* buf, size_t size, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t write_size = size;
    int rc = dfs_sys_write(mfu_file->dfs_sys, mfu_file->obj, buf, mfu_file->offset, &write_size, NULL);
    if (rc != 0) {
        errno = rc;
        return -1;
    } else {
        /* update file pointer with number of bytes written */
        mfu_file->offset += write_size;
    }
    return (ssize_t)write_size;
#else
    return (ssize_t)mfu_errno2rc(ENOSYS);
#endif
}

/* reliable pread from file descriptor (retries, if necessary, until hard error) */
ssize_t mfu_file_pread(const char* file, void* buf, size_t size, off_t offset, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        ssize_t rc = mfu_pread(file, mfu_file->fd, buf, size, offset);
        return rc;
    } else if (mfu_file->type == DFS) {
        ssize_t rc = daos_pread(file, buf, size, offset, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
            file, mfu_file->type);
    }
}

ssize_t daos_pread(const char* file, void* buf, size_t size, off_t offset, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t got_size = size;
    int rc = dfs_sys_read(mfu_file->dfs_sys, mfu_file->obj, buf, offset, &got_size, NULL);
    if (rc != 0) {
        errno = rc;
        /* return -1 if dfs_sys_read encounters error */
        return -1;
    }
    return (ssize_t)got_size;
#else
    return (ssize_t)mfu_errno2rc(ENOSYS);
#endif
}

ssize_t mfu_pread(const char* file, int fd, void* buf, size_t size, off_t offset)
{
    int tries = MFU_IO_TRIES;
    while (1) {
        ssize_t rc = pread(fd, (char*) buf, size, offset);
        if (rc > 0) {
            /* read some data */
            return rc;
        }
        else if (rc == 0) {
            /* EOF */
            return rc;
        }
        else {   /* (rc < 0) */
            /* something worth printing an error about */
            tries--;
            if (tries <= 0) {
                /* too many failed retries, give up */
                MFU_ABORT(-1, "Failed to read file %s errno=%d (%s)",
                    file, errno, strerror(errno));
            }

            /* sleep a bit before consecutive tries */
            usleep(MFU_IO_USLEEP);
        }
    }
}

ssize_t mfu_file_pwrite(const char* file, const void* buf, size_t size, off_t offset, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        ssize_t rc = mfu_pwrite(file, mfu_file->fd, buf, size, offset);
        return rc;
    } else if (mfu_file->type == DFS) {
        ssize_t rc = daos_pwrite(file, buf, size, offset, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
            file, mfu_file->type);
    }
}

ssize_t mfu_pwrite(const char* file, int fd, const void* buf, size_t size, off_t offset)
{
    int tries = MFU_IO_TRIES;
    while (1) {
        ssize_t rc = pwrite(fd, (const char*) buf, size, offset);
        if (rc > 0) {
            /* wrote some data */
            return rc;
        }
        else if (rc == 0) {
            /* didn't write anything, but not an error either */
            return rc;
        }
        else { /* (rc < 0) */
            /* something worth printing an error about */
            tries--;
            if (tries <= 0) {
                /* too many failed retries, give up */
                MFU_ABORT(-1, "Failed to write file %s errno=%d (%s)",
                    file, errno, strerror(errno));
            }

            /* sleep a bit before consecutive tries */
            usleep(MFU_IO_USLEEP);
        }
    }
}

ssize_t daos_pwrite(const char* file, const void* buf, size_t size, off_t offset, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t write_size = size;
    int rc = dfs_sys_write(mfu_file->dfs_sys, mfu_file->obj, buf, offset, &write_size, NULL);
    if (rc != 0) {
        errno = rc;
        /* report -1 if dfs_sys_write encounters error */
        return -1;
    }
    return (ssize_t)write_size;
#else
    return (ssize_t)mfu_errno2rc(ENOSYS);
#endif
}

/* truncate a file */
int mfu_file_truncate(const char* file, off_t length, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_truncate(file, length);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_truncate(file, length, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}

/* truncate a file */
int mfu_truncate(const char* file, off_t length)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = truncate(file, length);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int daos_truncate(const char* file, off_t length, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_punch(mfu_file->dfs_sys, file, length, DFS_MAX_FSIZE);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

int daos_ftruncate(mfu_file_t* mfu_file, off_t length)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_punch(mfu_file->dfs, mfu_file->obj, length, DFS_MAX_FSIZE);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

int mfu_ftruncate(int fd, off_t length)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = ftruncate(fd, length);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* ftruncate a file */
int mfu_file_ftruncate(mfu_file_t* mfu_file, off_t length)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_ftruncate(mfu_file->fd, length);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_ftruncate(mfu_file, length);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}

/* unlink a file */
int mfu_file_unlink(const char* file, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_unlink(file);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_unlink(file, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    } 
}

int daos_unlink(const char* file, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_remove(mfu_file->dfs_sys, file, false, NULL);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* delete a file */
int mfu_unlink(const char* file)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = unlink(file);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* force flush of written data */
int mfu_fsync(const char* file, int fd)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = fsync(fd);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/*****************************
 * Directories
 ****************************/

/* get current working directory, abort if fail or buffer too small */
void mfu_getcwd(char* buf, size_t size)
{
    errno = 0;
    char* p = getcwd(buf, size);
    if (p == NULL) {
        MFU_ABORT(-1, "Failed to get current working directory errno=%d (%s)",
                    errno, strerror(errno)
                   );
    }
}

int daos_mkdir(const char* dir, mode_t mode, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_mkdir(mfu_file->dfs_sys, dir, mode, 0);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

int mfu_mkdir(const char* dir, mode_t mode)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = mkdir(dir, mode);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

/* create directory, retry a few times on EINTR or EIO */
int mfu_file_mkdir(const char* dir, mode_t mode, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_mkdir(dir, mode);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_mkdir(dir, mode, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  dir, mfu_file->type);
    }
}

int mfu_file_rmdir(const char* dir, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_rmdir(dir);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_rmdir(dir, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  dir, mfu_file->type);
    }
}

/* remove directory, retry a few times on EINTR or EIO */
int mfu_rmdir(const char* dir)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = rmdir(dir);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int daos_rmdir(const char* dir, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_remove_type(mfu_file->dfs_sys, dir, false, S_IFDIR, NULL);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* open directory. The entry itself is not cached in mfu_file->dir_hash */
DIR* daos_opendir(const char* dir, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    DIR* dirp = NULL;
    int rc = dfs_sys_opendir(mfu_file->dfs_sys, dir, 0, &dirp);
    if (rc != 0) {
        errno = rc;
        dirp = NULL;
    }
    return dirp;
#else
    errno = ENOSYS;
    return NULL;
#endif
}

/* open directory, retry a few times on EINTR or EIO */
DIR* mfu_opendir(const char* dir)
{
    DIR* dirp;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    dirp = opendir(dir);
    if (dirp == NULL) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return dirp;
}

/* open directory, retry a few times on EINTR or EIO */
DIR* mfu_file_opendir(const char* dir, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        DIR* dirp = mfu_opendir(dir);
        return dirp;
    } else if (mfu_file->type == DFS) {
        DIR* dirp = daos_opendir(dir, mfu_file);
        return dirp;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  dir, mfu_file->type);
    }
}

/* close dir. This is not cached in mfu_file->dir_hash */
int daos_closedir(DIR* dirp, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_closedir(dirp);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}

/* close directory, retry a few times on EINTR or EIO */
int mfu_closedir(DIR* dirp)
{
    int rc;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    rc = closedir(dirp);
    if (rc < 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return rc;
}

int mfu_file_closedir(DIR* dirp, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_closedir(dirp);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_closedir(dirp, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}

struct dirent* daos_readdir(DIR* dirp, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    struct dirent* dirent = NULL;
    int rc = dfs_sys_readdir(mfu_file->dfs_sys, dirp, &dirent);
    if (rc != 0) {
        errno = rc;
        dirent = NULL;
    }
    return dirent;
#else
    errno = ENOSYS;
    return NULL;
#endif
}

/* read directory entry, retry a few times on ENOENT, EIO, or EINTR */
struct dirent* mfu_readdir(DIR* dirp)
{
    /* read next directory entry, retry a few times */
    struct dirent* entry;
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    entry = readdir(dirp);
    if (entry == NULL) {
        if (errno == EINTR || errno == EIO || errno == ENOENT) {
            tries--;
            if (tries > 0) {
                /* sleep a bit before consecutive tries */
                usleep(MFU_IO_USLEEP);
                goto retry;
            }
        }
    }
    return entry;
}

struct dirent* mfu_file_readdir(DIR* dirp, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        struct dirent* entry = mfu_readdir(dirp);
        return entry;
    } else if (mfu_file->type == DFS) {
        struct dirent* entry = daos_readdir(dirp, mfu_file);
        return entry;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}

/* list xattrs (link interrogation) */
ssize_t mfu_file_llistxattr(const char* path, char* list, size_t size, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        ssize_t rc = mfu_llistxattr(path, list, size);
        return rc;
    } else if (mfu_file->type == DFS) {
        ssize_t rc = daos_llistxattr(path, list, size, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}

ssize_t mfu_llistxattr(const char* path, char* list, size_t size)
{
    ssize_t rc = llistxattr(path, list, size);
    return rc;
}

/* DAOS wrapper for dfs_listxattr that adjusts return
 * codes and errno to be similar to POSIX llistxattr */
ssize_t daos_llistxattr(const char* path, char* list, size_t size, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t got_size = size;
    int rc = dfs_sys_listxattr(mfu_file->dfs_sys, path, list, &got_size, O_NOFOLLOW);
    if (rc != 0) {
        errno = rc;
    }
    return (ssize_t) got_size;
#else
    return (ssize_t) mfu_errno2rc(ENOSYS);
#endif
}

/* list xattrs (link dereference) */
ssize_t mfu_file_listxattr(const char* path, char* list, size_t size, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        ssize_t rc = mfu_listxattr(path, list, size);
        return rc;
    } else if (mfu_file->type == DFS) {
        ssize_t rc = daos_listxattr(path, list, size, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}

ssize_t mfu_listxattr(const char* path, char* list, size_t size)
{
    ssize_t rc = listxattr(path, list, size);
    return rc;
}

/* DAOS wrapper for dfs_listxattr that adjusts return
 * codes and errno to be similar to POSIX listxattr */
ssize_t daos_listxattr(const char* path, char* list, size_t size, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t got_size = size;
    int rc = dfs_sys_listxattr(mfu_file->dfs_sys, path, list, &got_size, 0);
    if (rc != 0) {
        errno = rc;
    }
    return (ssize_t) got_size;
#else
    return (ssize_t) mfu_errno2rc(ENOSYS);
#endif
}

/* get xattrs (link interrogation) */
ssize_t mfu_file_lgetxattr(const char* path, const char* name, void* value, size_t size, mfu_file_t* mfu_file)
{
   if (mfu_file->type == POSIX) {
        ssize_t rc = mfu_lgetxattr(path, name, value, size);
        return rc;
    } else if (mfu_file->type == DFS) {
        ssize_t rc = daos_lgetxattr(path, name, value, size, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    } 
}

ssize_t mfu_lgetxattr(const char* path, const char* name, void* value, size_t size)
{
    ssize_t rc = lgetxattr(path, name, value, size);
    return rc;
}

ssize_t daos_lgetxattr(const char* path, const char* name, void* value, size_t size, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t got_size = size;
    int rc = dfs_sys_getxattr(mfu_file->dfs_sys, path, name, value, &got_size, O_NOFOLLOW);
    if (rc != 0) {
        errno = rc;
    }
    return (ssize_t) got_size;
#else
    return (ssize_t) mfu_errno2rc(ENOSYS);
#endif
}

/* get xattrs (link dereference) */
ssize_t mfu_file_getxattr(const char* path, const char* name, void* value, size_t size, mfu_file_t* mfu_file)
{
   if (mfu_file->type == POSIX) {
        ssize_t rc = mfu_getxattr(path, name, value, size);
        return rc;
    } else if (mfu_file->type == DFS) {
        ssize_t rc = daos_getxattr(path, name, value, size, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    } 
}

ssize_t mfu_getxattr(const char* path, const char* name, void* value, size_t size)
{
    ssize_t rc = getxattr(path, name, value, size);
    return rc;
}

ssize_t daos_getxattr(const char* path, const char* name, void* value, size_t size, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    daos_size_t got_size = size;
    int rc = dfs_sys_getxattr(mfu_file->dfs_sys, path, name, value, &got_size, 0);
    if (rc != 0) {
        errno = rc;
    }
    return (ssize_t) got_size;
#else
    return (ssize_t) mfu_errno2rc(ENOSYS);
#endif
}

/* set xattrs (link interrogation) */
int mfu_file_lsetxattr(const char* path, const char* name, const void* value, size_t size, int flags,
                       mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_lsetxattr(path, name, value, size, flags);
        return rc;
    } else if (mfu_file->type == DFS) {
        int rc = daos_lsetxattr(path, name, value, size, flags, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}

int mfu_lsetxattr(const char* path, const char* name, const void* value, size_t size, int flags)
{
    int rc = lsetxattr(path, name, value, size, flags);
    return rc;
}

int daos_lsetxattr(const char* path, const char* name, const void* value, size_t size, int flags,
                   mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_sys_setxattr(mfu_file->dfs_sys, path, name, value, size, flags, O_NOFOLLOW);
    return mfu_errno2rc(rc);
#else
    return mfu_errno2rc(ENOSYS);
#endif
}
