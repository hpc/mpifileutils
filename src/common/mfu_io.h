/* defines reliable I/O functions */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_IO_H
#define MFU_IO_H

/* TODO: do we need to do this? */
/* Make sure we're using 64 bit file handling. */
#ifdef _FILE_OFFSET_BITS
#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE 1
#endif

#ifndef __USE_LARGEFILE64
#define __USE_LARGEFILE64
#endif

#ifndef _LARGEFILE_SOURCE
#define _LARGEFILE_SOURCE
#endif

#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <daos.h>
#include <daos_fs.h>
//#include "mfu.h"
#include "mfu_param_path.h"
//#include "mfu.h"

/* Intent is to wrap all POSIX I/O routines used by mfu tools.  May
 * abort on fatal conditions to avoid checking condition at every call.
 * May also automatically retry on things like EINTR. */

/* TODO: fix this */
/* do this to avoid warning about undefined stat64 struct */
struct stat64;

/* input and output function ptrs to specify io function to be used
 * for the source or destination (i.e posix_open, daos_open, etc) */
typedef struct io_funcs_t {
    /* open */
    /* lstat */
    /* opendir */
    /* readdir */
    /* closedir */
    /* close */
    /* mkdir */
    /* chmod */
    /* lseek */
    /* ftruncate */
    /* mknod */
    /* read */
    /* write */
} io_funcs;
io_funcs mfu_io;

/*****************************
 * Any object
 ****************************/

/* calls access, and retries a few times if we get EIO or EINTR */
int mfu_access(const char* path, int amode);

/* calls lchown, and retries a few times if we get EIO or EINTR */
int mfu_lchown(const char* path, uid_t owner, gid_t group);

/* calls chmod, and retries a few times if we get EIO or EINTR */
int daos_chmod(const char* path, mode_t mode, mfu_file_t* mfu_file);
int posix_chmod(const char* path, mode_t mode);
int mfu_file_chmod(const char* path, mode_t mode, mfu_file_t* mfu_file);

/* calls utimensat, and retries a few times if we get EIO or EINTR */
int mfu_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags);

/* calls lstat, and retries a few times if we get EIO or EINTR */
int mfu_file_lstat(const char* path, struct stat* buf, mfu_file_t* mfu_file);

/* only dcp1 calls mfu_lstat64, is it necessary? */
int mfu_lstat64(const char* path, struct stat64* buf);

/* posix version of stat */
int posix_lstat(const char* path, struct stat* buf);

/* daos version of stat */
int daos_stat(const char* path, struct stat* buf, mfu_file_t* mfu_file);

/* call mknod, retry a few times on EINTR or EIO */
int mfu_file_mknod(const char* path, mode_t mode, dev_t dev, mfu_file_t* mfu_file);
/* just a noop, since there is no daos_mknod */
int daos_mknod(const char* path, mode_t mode, dev_t dev, mfu_file_t* mfu_file);
int posix_mknod(const char* path, mode_t mode, dev_t dev);

/* call remove, retry a few times on EINTR or EIO */
int mfu_remove(const char* path);

/* open directory, retry a few times on EINTR or EIO */
DIR* mfu_file_opendir(const char* dir, mfu_file_t* mfu_file);
DIR* posix_opendir(const char* dir);
DIR* daos_opendir(const char* dir, mfu_file_t* mfu_file);

/* read directory entry, retry a few times on ENOENT, EIO, or EINTR */
struct dirent* mfu_file_readdir(DIR* dirp, mfu_file_t* mfu_file);
struct dirent* posix_readdir(DIR* dirp);
struct dirent* daos_readdir(DIR* dirp, mfu_file_t* mfu_file);

/* close directory, retry a few times on EINTR or EIO */
int mfu_file_closedir(DIR* dirp, mfu_file_t* mfu_file);
int posix_closedir(DIR* dirp);
int daos_closedir(DIR* dirp, mfu_file_t* mfu_file);

/*****************************
 * Links
 ****************************/

/* call readlink, retry a few times on EINTR or EIO */
ssize_t mfu_readlink(const char* path, char* buf, size_t bufsize);

/* call symlink, retry a few times on EINTR or EIO */
int mfu_symlink(const char* oldpath, const char* newpath);

/*****************************
 * Files
 ****************************/

/* open file with specified flags and mode, retry open a few times on failure */
void mfu_file_open(const char* file, int flags, mfu_file_t* mfu_file, ...);
void daos_open(const char* file, int flags, int* mode_set, mode_t mode, mfu_file_t* mfu_file);
void posix_open(const char* file, int flags, int* mode_set, mode_t mode, mfu_file_t* mfu_file);

/* close file */
int mfu_file_close(const char* file, mfu_file_t* mfu_file);
int daos_close(const char* file, mfu_file_t* mfu_file);
int posix_close(const char* file, mfu_file_t* mfu_file);

/* seek file descriptor to specified position */
off_t mfu_file_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence);
/* actually a noop since daos doesn't have an lseek */
off_t daos_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence);
off_t posix_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence);

/* reliable read from opened file descriptor (retries, if necessary, until hard error) */
ssize_t mfu_file_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file);
ssize_t daos_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file);
ssize_t posix_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file);

/* reliable write to opened file descriptor (retries, if necessary, until hard error) */
ssize_t mfu_file_write(const char* file, const void* buf, size_t size, mfu_file_t* mfu_file);
ssize_t daos_write(const char* file, const void* buf, size_t size, mfu_file_t* mfu_file);
ssize_t posix_write(const char* file, const void* buf, size_t size, mfu_file_t* mfu_file);

/* truncate a file */
int mfu_truncate(const char* file, off_t length);

/* ftruncate a file */
int mfu_file_ftruncate(mfu_file_t* mfu_file, off_t length);
int daos_ftruncate(mfu_file_t* mfu_file, off_t length);
int posix_ftruncate(mfu_file_t* mfu_file, off_t length);

/* delete a file */
int mfu_unlink(const char* file);

/* force flush of written data */
int mfu_fsync(const char* file, int fd);

/*****************************
 * Directories
 ****************************/

/* get current working directory, abort if fail or buffer too small */
void mfu_getcwd(char* buf, size_t size);

int daos_mkdir(const char* dir, mode_t mode, mfu_file_t* mfu_file);
/* create directory, retry a few times on EINTR or EIO */
int posix_mkdir(const char* dir, mode_t mode);
int mfu_file_mkdir(const char* dir, mode_t mode, mfu_file_t* mfu_file);

/* remove directory, retry a few times on EINTR or EIO */
int mfu_rmdir(const char* dir);

#endif /* MFU_IO_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
