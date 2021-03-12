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

#ifdef DAOS_SUPPORT
#include <daos.h>
#include <daos_fs.h>
#include <daos_fs_sys.h>
#endif

#include "mfu_param_path.h"

/* Intent is to wrap all POSIX I/O routines used by mfu tools.  May
 * abort on fatal conditions to avoid checking condition at every call.
 * May also automatically retry on things like EINTR. */

/* TODO: fix this */
/* do this to avoid warning about undefined stat64 struct */
struct stat64;

/*****************************
 * Any object
 ****************************/

/* calls access, and retries a few times if we get EIO or EINTR */
int mfu_file_access(const char* path, int amode, mfu_file_t* mfu_file);
int mfu_access(const char* path, int amode);
int daos_access(const char* path, int amode, mfu_file_t* mfu_file);

/* calls faccessat, and retries a few times if we get EIO or EINTR */
int mfu_file_faccessat(int dirfd, const char* path, int amode, int flags, mfu_file_t* mfu_file);
int mfu_faccessat(int dirfd, const char* path, int amode, int flags);
int daos_faccessat(int dirfd, const char* path, int amode, int flags, mfu_file_t* mfu_file);

/* calls lchown, and retries a few times if we get EIO or EINTR */
int mfu_file_lchown(const char* path, uid_t owner, gid_t group, mfu_file_t* mfu_file);
int mfu_lchown(const char* path, uid_t owner, gid_t group);
int daos_lchown(const char* path, uid_t owner, gid_t group, mfu_file_t* mfu_file);

/* calls chmod, and retries a few times if we get EIO or EINTR */
int daos_chmod(const char* path, mode_t mode, mfu_file_t* mfu_file);
int mfu_chmod(const char* path, mode_t mode);
int mfu_file_chmod(const char* path, mode_t mode, mfu_file_t* mfu_file);

/* calls utimensat, and retries a few times if we get EIO or EINTR */
int mfu_file_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags,
                       mfu_file_t* mfu_file);
int mfu_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags);
int daos_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags,
                   mfu_file_t* mfu_file);

/* calls lstat, and retries a few times if we get EIO or EINTR */
int mfu_file_lstat(const char* path, struct stat* buf, mfu_file_t* mfu_file);
int mfu_lstat(const char* path, struct stat* buf);
int daos_lstat(const char* path, struct stat* buf, mfu_file_t* mfu_file);

/* only dcp1 calls mfu_lstat64, is it necessary? */
int mfu_lstat64(const char* path, struct stat64* buf);

/* calls stat, and retries a few times if we get EIO or EINTR */
int mfu_file_stat(const char* path, struct stat* buf, mfu_file_t* mfu_file);
int mfu_stat(const char* path, struct stat* buf);
int daos_stat(const char* path, struct stat* buf, mfu_file_t* mfu_file);

/* call mknod, retry a few times on EINTR or EIO */
int mfu_file_mknod(const char* path, mode_t mode, dev_t dev, mfu_file_t* mfu_file);
int daos_mknod(const char* path, mode_t mode, mfu_file_t* mfu_file);
int mfu_mknod(const char* path, mode_t mode, dev_t dev);

/* call remove, retry a few times on EINTR or EIO */
int mfu_file_remove(const char* path, mfu_file_t* mfu_file);
int mfu_remove(const char* path);
int daos_remove(const char* path, mfu_file_t* mfu_file);

/* read directory entry, retry a few times on ENOENT, EIO, or EINTR */
struct dirent* mfu_file_readdir(DIR* dirp, mfu_file_t* mfu_file);
struct dirent* mfu_readdir(DIR* dirp);
struct dirent* daos_readdir(DIR* dirp, mfu_file_t* mfu_file);

/* list xattrs (link interrogation) */
ssize_t mfu_file_llistxattr(const char* path, char* list, size_t size, mfu_file_t* mfu_file);
ssize_t mfu_llistxattr(const char* path, char* list, size_t size);
ssize_t daos_llistxattr(const char* path, char* list, size_t size, mfu_file_t* mfu_file);

/* list xatters (link dereference */
ssize_t mfu_file_listxattr(const char* path, char* list, size_t size, mfu_file_t* mfu_file);
ssize_t mfu_listxattr(const char* path, char* list, size_t size);
ssize_t daos_listxattr(const char* path, char* list, size_t size, mfu_file_t* mfu_file);

/* get xattrs (link interrogation) */
ssize_t mfu_file_lgetxattr(const char* path, const char* name, void* value, size_t size, mfu_file_t* mfu_file);
ssize_t mfu_lgetxattr(const char* path, const char* name, void* value, size_t size);
ssize_t daos_lgetxattr(const char* path, const char* name, void* value, size_t size, mfu_file_t* mfu_file);

/* get xattrs (link dereference) */
ssize_t mfu_file_getxattr(const char* path, const char* name, void* value, size_t size, mfu_file_t* mfu_file);
ssize_t mfu_getxattr(const char* path, const char* name, void* value, size_t size);
ssize_t daos_getxattr(const char* path, const char* name, void* value, size_t size, mfu_file_t* mfu_file);

/* set xattrs (link interrogation) */
int mfu_file_lsetxattr(const char* path, const char* name, const void* value, size_t size, int flags,
                               mfu_file_t* mfu_file);
int mfu_lsetxattr(const char* path, const char* name, const void* value, size_t size, int flags);
int daos_lsetxattr(const char* path, const char* name, const void* value, size_t size, int flags,
                           mfu_file_t* mfu_file);

/* calls realpath */
char* mfu_file_realpath(const char* path, char* resolved_path, mfu_file_t* mfu_file);
char* mfu_realpath(const char* path, char* resolved_path);
char* daos_realpath(const char* path, char* resolved_path, mfu_file_t* mfu_file);

/*****************************
 * Links
 ****************************/

/* call readlink, retry a few times on EINTR or EIO */
ssize_t mfu_file_readlink(const char* path, char* buf, size_t bufsize, mfu_file_t* mfu_file);
ssize_t daos_readlink(const char* path, char* buf, size_t bufsize, mfu_file_t* mfu_file);
ssize_t mfu_readlink(const char* path, char* buf, size_t bufsize);

/* call hardlink, retry a few times on EINTR or EIO */
int mfu_hardlink(const char* oldpath, const char* newpath);

/* call symlink, retry a few times on EINTR or EIO */
int mfu_file_symlink(const char* oldpath, const char* newpath, mfu_file_t* mfu_file);
int daos_symlink(const char* oldpath, const char* newpath, mfu_file_t* mfu_file);
int mfu_symlink(const char* oldpath, const char* newpath);

/*****************************
 * Files
 ****************************/

/* open file with specified flags and mode, retry open a few times on failure */
int mfu_file_open(const char* file, int flags, mfu_file_t* mfu_file, ...);
int daos_open(const char* file, int flags, mode_t mode, mfu_file_t* mfu_file);
int mfu_open(const char* file, int flags, ...);

/* close file */
int mfu_file_close(const char* file, mfu_file_t* mfu_file);
int daos_close(const char* file, mfu_file_t* mfu_file);
int mfu_close(const char* file, int fd);

/* seek file descriptor to specified position */
off_t mfu_file_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence);
/* actually a noop since daos doesn't have an lseek */
int daos_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence);
off_t mfu_lseek(const char* file, int fd, off_t pos, int whence);

/* reliable read from opened file descriptor (retries, if necessary, until hard error) */
ssize_t mfu_file_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file);
ssize_t daos_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file);
ssize_t mfu_read(const char* file, int fd, void* buf, size_t size);

/* reliable write to opened file descriptor (retries, if necessary, until hard error) */
ssize_t mfu_file_write(const char* file, const void* buf, size_t size, mfu_file_t* mfu_file);
ssize_t daos_write(const char* file, const void* buf, size_t size, mfu_file_t* mfu_file);
ssize_t mfu_write(const char* file, int fd, const void* buf, size_t size);

/* pread from opened file descriptor (may return short read) */
ssize_t mfu_file_pread(const char* file, void* buf, size_t size, off_t offset, mfu_file_t* mfu_file);
ssize_t daos_pread(const char* file, void* buf, size_t size, off_t offset, mfu_file_t* mfu_file);
ssize_t mfu_pread(const char* file, int fd, void* buf, size_t size, off_t offset);

/* pwrite to opened file descriptor (may return short write) */
ssize_t mfu_file_pwrite(const char* file, const void* buf, size_t size, off_t offset, mfu_file_t* mfu_file);
ssize_t daos_pwrite(const char* file, const void* buf, size_t size, off_t offset, mfu_file_t* mfu_file);
ssize_t mfu_pwrite(const char* file, int fd, const void* buf, size_t size, off_t offset);

/* truncate a file */
int mfu_file_truncate(const char* file, off_t length, mfu_file_t* mfu_file);
int mfu_truncate(const char* file, off_t length);
int daos_truncate(const char* file, off_t length, mfu_file_t* mfu_file);

/* ftruncate a file */
int mfu_file_ftruncate(mfu_file_t* mfu_file, off_t length);
int daos_ftruncate(mfu_file_t* mfu_file, off_t length);
int mfu_ftruncate(int fd, off_t length);

/* delete a file */
int mfu_file_unlink(const char* file, mfu_file_t* mfu_file);
int daos_unlink(const char* file, mfu_file_t* mfu_file);
int mfu_unlink(const char* file);

/* force flush of written data */
int mfu_fsync(const char* file, int fd);

/*****************************
 * Directories
 ****************************/

/* get current working directory, abort if fail or buffer too small */
void mfu_getcwd(char* buf, size_t size);

/* create directory, retry a few times on EINTR or EIO */
int mfu_file_mkdir(const char* dir, mode_t mode, mfu_file_t* mfu_file);
int mfu_mkdir(const char* dir, mode_t mode);
int daos_mkdir(const char* dir, mode_t mode, mfu_file_t* mfu_file);

/* remove directory, retry a few times on EINTR or EIO */
int mfu_file_rmdir(const char* dir, mfu_file_t* mfu_file);
int mfu_rmdir(const char* dir);
int daos_rmdir(const char* dir, mfu_file_t* mfu_file);

/* open directory, retry a few times on EINTR or EIO */
DIR* mfu_file_opendir(const char* dir, mfu_file_t* mfu_file);
DIR* mfu_opendir(const char* dir);
DIR* daos_opendir(const char* dir, mfu_file_t* mfu_file);

/* close directory, retry a few times on EINTR or EIO */
int mfu_file_closedir(DIR* dirp, mfu_file_t* mfu_file);
int mfu_closedir(DIR* dirp);
int daos_closedir(DIR* dirp, mfu_file_t* mfu_file);

#endif /* MFU_IO_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
