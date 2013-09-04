#include "bayer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define BAYER_IO_TRIES  (5)
#define BAYER_OPEN_USLEEP (100)

/* open file with specified flags and mode, retry open a few times on failure */
int bayer_open(const char* file, int flags, ...)
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
    } else {
        fd = open(file, flags);
    }

    /* if open failed, try a few more times */
    if (fd < 0) {
        /* try again */
        int tries = BAYER_IO_TRIES;
        while (tries && fd < 0) {
            /* sleep between consecutive tries */
            usleep(BAYER_OPEN_USLEEP);

            /* open again */
            errno = 0;
            if (mode_set) { 
                fd = open(file, flags, mode);
            } else {
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

/* close file */
int bayer_close(const char* file, int fd)
{
    int tries = BAYER_IO_TRIES;
retry:
    errno = 0;
    int rc = close(fd);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                goto retry;
            }
        }
    }
    return rc;
}

/* seek file descriptor to specified position */
int bayer_lseek(const char* file, int fd, off_t pos, int whence)
{
    int tries = BAYER_IO_TRIES;
retry:
    errno = 0;
    off_t rc = lseek(fd, pos, whence);
    if (rc == (off_t)-1) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                goto retry;
            }
        }
    }
    return rc;
}

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t bayer_read(const char* file, int fd, void* buf, size_t size)
{
    int tries = BAYER_IO_TRIES;
    ssize_t n = 0;
    while (n < size) {
        int rc = read(fd, (char*) buf + n, size - n);
        if (rc > 0) {
            /* read some data */
            n += rc;
        } else if (rc == 0) {
            /* EOF */
            return n;
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }

            /* something worth printing an error about */
            tries--;
            if (tries <= 0) {
                /* too many failed retries, give up */
                bayer_abort(-1, "Failed to read file %s errno=%d (%s)\n",
                    file, errno, strerror(errno)
                );
            }
        }
    }
    return n;
}

/* reliable write to file descriptor (retries, if necessary, until hard error) */
ssize_t bayer_write(const char* file, int fd, const void* buf, size_t size)
{
    int tries = 10;
    ssize_t n = 0;
    while (n < size) {
        ssize_t rc = write(fd, (char*) buf + n, size - n);
        if (rc > 0) {
            /* wrote some data */
            n += rc;
        } else if (rc == 0) {
            /* something bad happened, print an error and abort */
            bayer_abort(-1, "Failed to write file %s errno=%d (%s)\n",
                file, errno, strerror(errno)
            );
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }

            /* something worth printing an error about */
            tries--;
            if (tries <= 0) {
                /* too many failed retries, give up */
                bayer_abort(-1, "Failed to write file %s errno=%d (%s)\n",
                    file, errno, strerror(errno)
                );
            }
        }
    }
    return n;
}

/* delete a file */
int bayer_unlink(const char* file)
{
    int rc;
    int tries = BAYER_IO_TRIES;
retry:
    rc = unlink(file);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
                goto retry;
            }
        }
    }
    return rc;
}

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
    int rc;
    int tries = BAYER_IO_TRIES;
retry:
    rc = lstat(path, buf);
    if (rc != 0) {
        if (errno == EINTR || errno == EIO) {
            tries--;
            if (tries > 0) {
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
    struct dirent* entry;
    int tries = BAYER_IO_TRIES;
retry:
    entry = readdir(dirp);
    if (entry == NULL) {
        if (errno == EINTR || errno == EIO || errno == ENOENT) {
            tries--;
            if (tries > 0) {
                goto retry;
            }
        }
    }
    return entry;
}
