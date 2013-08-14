#include "bayer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

/* get current working directory, abort if fail or buffer too small */
void bayer_getcwd(char* buf, size_t size)
{
  char* p = getcwd(buf, size);
  if (p == NULL) {
    bayer_abort(-1, "Failed to get current working directory errno=%d (%s)\n",
      errno, strerror(errno)
    );
  }
}

/* calls lstat, and retries a few times if we get EIO or EINTR */
int bayer_lstat(const char* path, struct stat* buf)
{
  int count = 5;
retry:
  errno = 0;
  int rc = lstat(path, buf);
  if (rc != 0) {
    if (errno == EINTR || errno == EIO) {
      count--;
      if (count > 0) {
        goto retry;
      }
    }
  }
  return rc;
}

/* calls lstat, and retries a few times if we get ENOENT, EIO, or EINTR */
struct dirent* bayer_readdir(DIR* dirp)
{
  /* read next directory entry, retry a few times */
  int count = 5;
retry:
  errno = 0;
  struct dirent* entry = readdir(dirp);
  if (entry == NULL) {
    if (errno == EINTR || errno == EIO || errno == ENOENT) {
      count--;
      if (count > 0) {
        goto retry;
      }
    }
  }
  return entry;
}
