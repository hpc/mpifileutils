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
