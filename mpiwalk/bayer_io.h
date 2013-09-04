/* defines reliable I/O functions */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef BAYER_IO_H
#define BAYER_IO_H

#include <stdlib.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <dirent.h>

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

/* get current working directory, abort if fail or buffer too small */
void bayer_getcwd(char* buf, size_t size);

/* calls lstat, and retries a few times if we get EIO or EINTR */
int bayer_lstat(const char* path, struct stat* buf);

/* calls lstat, and retries a few times if we get ENOENT, EIO, or EINTR */
struct dirent* bayer_readdir(DIR* dirp);

#endif /* BAYER_IO_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
