/* defines reliable I/O functions */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef BAYER_IO_H
#define BAYER_IO_H

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

/* Intent is to wrap all POSIX I/O routines used by bayer tools.  May
 * abort on fatal conditions to avoid checking condition at every call.
 * May also automatically retry on things like EINTR. */

/*****************************
 * Any object
 ****************************/

/* calls lstat, and retries a few times if we get EIO or EINTR */
int bayer_lstat(const char* path, struct stat* buf);

/* calls lstat, and retries a few times if we get EIO or EINTR */
int bayer_lstat64(const char* path, struct stat64* buf);

/* call mknod, retry a few times on EINTR or EIO */
int bayer_mknod(const char* path, mode_t mode, dev_t dev);

/*****************************
 * Links
 ****************************/

/* call readlink, retry a few times on EINTR or EIO */
ssize_t bayer_readlink(const char* path, char* buf, size_t bufsize);

/* call symlink, retry a few times on EINTR or EIO */
int bayer_symlink(const char* oldpath, const char* newpath);

/*****************************
 * Files
 ****************************/

/* open file with specified flags and mode, retry open a few times on failure */
int bayer_open(const char* file, int flags, ...);

/* close file */
int bayer_close(const char* file, int fd);

/* seek file descriptor to specified position */
int bayer_lseek(const char* file, int fd, off_t pos, int whence);

/* reliable read from opened file descriptor (retries, if necessary, until hard error) */
ssize_t bayer_read(const char* file, int fd, void* buf, size_t size);

/* reliable write to opened file descriptor (retries, if necessary, until hard error) */
ssize_t bayer_write(const char* file, int fd, const void* buf, size_t size);

/* delete a file */
int bayer_unlink(const char* file);

/*****************************
 * Directories
 ****************************/

/* get current working directory, abort if fail or buffer too small */
void bayer_getcwd(char* buf, size_t size);

/* create directory, retry a few times on EINTR or EIO */
int bayer_mkdir(const char* dir, mode_t mode);

/* remove directory, retry a few times on EINTR or EIO */
int bayer_rmdir(const char* dir);

/* open directory, retry a few times on EINTR or EIO */
DIR* bayer_opendir(const char* dir);

/* close directory, retry a few times on EINTR or EIO */
int bayer_closedir(DIR* dirp);

/* read directory entry, retry a few times on ENOENT, EIO, or EINTR */
struct dirent* bayer_readdir(DIR* dirp);

#endif /* BAYER_IO_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
