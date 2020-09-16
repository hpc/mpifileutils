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

#ifdef DAOS_SUPPORT
#include <gurt/common.h>
#include <gurt/hash.h>
#endif

#include "mfu.h"

#define MFU_IO_TRIES  (5)
#define MFU_IO_USLEEP (100)

static int mpi_rank;

#ifdef DAOS_SUPPORT
struct d_hash_table *dir_hash;

struct mfu_dir_hdl {
        d_list_t	entry;
        dfs_obj_t	*oh;
        char		name[PATH_MAX];
};

static inline struct mfu_dir_hdl* hdl_obj(d_list_t *rlink)
{
        return container_of(rlink, struct mfu_dir_hdl, entry);
}

static bool key_cmp(struct d_hash_table *htable, d_list_t *rlink,
	const void *key, unsigned int ksize)
{
        struct mfu_dir_hdl *hdl = hdl_obj(rlink);

        return (strcmp(hdl->name, (const char *)key) == 0);
}

static void rec_free(struct d_hash_table *htable, d_list_t *rlink)
{
        struct mfu_dir_hdl *hdl = hdl_obj(rlink);

        assert(d_hash_rec_unlinked(&hdl->entry));
        dfs_release(hdl->oh);
        free(hdl);
}

static d_hash_table_ops_t hdl_hash_ops = {
        .hop_key_cmp	= key_cmp,
        .hop_rec_free	= rec_free
};

static dfs_obj_t* lookup_insert_dir(const char *name, mfu_file_t* mfu_file)
{
        struct mfu_dir_hdl *hdl;
        d_list_t *rlink;
        int rc;

	if (dir_hash == NULL) {
	    rc = d_hash_table_create(0, 16, NULL, &hdl_hash_ops, &dir_hash);
	    if (rc) {
		    fprintf(stderr, "Failed to initialize dir hashtable");
		    return NULL;
	    }
	}

        rlink = d_hash_rec_find(dir_hash, name, strlen(name));
        if (rlink != NULL) {
                hdl = hdl_obj(rlink);
                return hdl->oh;
        }

        hdl = calloc(1, sizeof(struct mfu_dir_hdl));
        if (hdl == NULL)
		return NULL;

        strncpy(hdl->name, name, PATH_MAX-1);
        hdl->name[PATH_MAX-1] = '\0';

        rc = dfs_lookup(mfu_file->dfs, name, O_RDWR, &hdl->oh, NULL, NULL);
	if (rc) {
		fprintf(stderr, "dfs_lookup() of %s Failed", name);
		return NULL;
	}

        rc = d_hash_rec_insert(dir_hash, hdl->name, strlen(hdl->name),
                               &hdl->entry, true);
	if (rc)
		return NULL;

        return hdl->oh;
}

static int parse_filename(const char* path, char** _obj_name, char** _cont_name)
{
	char *f1 = NULL;
	char *f2 = NULL;
	char *fname = NULL;
	char *cont_name = NULL;
	int rc = 0;

	if (path == NULL || _obj_name == NULL || _cont_name == NULL)
		return -EINVAL;

	if (strcmp(path, "/") == 0) {
		*_cont_name = strdup("/");
		if (*_cont_name == NULL)
			return -ENOMEM;
		*_obj_name = NULL;
		return 0;
	}

	f1 = strdup(path);
	if (f1 == NULL) {
                rc = -ENOMEM;
                goto out;
        }

	f2 = strdup(path);
	if (f2 == NULL) {
                rc = -ENOMEM;
                goto out;
        }

	fname = basename(f1);
	cont_name = dirname(f2);

	if (cont_name[0] == '.' || cont_name[0] != '/') {
		char cwd[1024];

		if (getcwd(cwd, 1024) == NULL) {
                        rc = -ENOMEM;
                        goto out;
                }

		if (strcmp(cont_name, ".") == 0) {
			cont_name = strdup(cwd);
			if (cont_name == NULL) {
                                rc = -ENOMEM;
                                goto out;
                        }
		} else {
			char *new_dir = calloc(strlen(cwd) + strlen(cont_name)
					       + 1, sizeof(char));
			if (new_dir == NULL) {
                                rc = -ENOMEM;
                                goto out;
                        }

			strcpy(new_dir, cwd);
			if (cont_name[0] == '.') {
				strcat(new_dir, &cont_name[1]);
			} else {
				strcat(new_dir, "/");
				strcat(new_dir, cont_name);
			}
			cont_name = new_dir;
		}
		*_cont_name = cont_name;
	} else {
		*_cont_name = strdup(cont_name);
		if (*_cont_name == NULL) {
                        rc = -ENOMEM;
                        goto out;
                }
	}

	*_obj_name = strdup(fname);
	if (*_obj_name == NULL) {
		free(*_cont_name);
		*_cont_name = NULL;
                rc = -ENOMEM;
                goto out;
	}

out:
	if (f1)
		free(f1);
	if (f2)
		free(f2);
	return rc;
}
#endif

/* calls access, and retries a few times if we get EIO or EINTR */
int mfu_file_access(const char* path, int amode, mfu_file_t* mfu_file)
{
    if (mfu_file->type == POSIX) {
        int rc = mfu_access(path, amode);
        return rc;
    } else if (mfu_file->type == DAOS) {
        int rc = daos_access(path, amode);
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

int daos_access(const char* path, int amode)
{
#ifdef DAOS_SUPPORT
    /* noop becuase daos have an access call */
    return 0;
#endif
}

/* calls lchown, and retries a few times if we get EIO or EINTR */
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

int daos_chmod(const char *path, mode_t mode, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    dfs_obj_t *parent = NULL;
    int rc = dfs_lookup(mfu_file->dfs, dir_name, O_RDWR, &parent, NULL, NULL);
    if (parent != NULL) {
        rc = dfs_chmod(mfu_file->dfs, parent, name, mode);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_chmod %s failed (%d %s)",
                    name, rc, strerror(rc));
            errno = rc;
            rc = -1;
        }
    } else {
        MFU_LOG(MFU_LOG_ERR, "dfs_lookup %s failed", dir_name);
        errno = ENOENT;
        rc = -1;
    }
    return rc;
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
    } else if (mfu_file->type == DAOS) {
        int rc = daos_chmod(path, mode, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  path, mfu_file->type);
    }
}

/* calls utimensat, and retries a few times if we get EIO or EINTR */
int mfu_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags)
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

int daos_stat(const char* path, struct stat* buf, mfu_file_t* mfu_file) {
#ifdef DAOS_SUPPORT
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    dfs_obj_t *parent = NULL;
    int rc;
    if (mfu_file->only_daos) {
        rc = dfs_lookup(mfu_file->dfs, dir_name, O_RDWR, &parent, NULL, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup %s failed", dir_name);
            errno = ENOENT;
            rc = -1;
        }
    } else {
        parent = lookup_insert_dir(dir_name, mfu_file);
    }
    rc = dfs_stat(mfu_file->dfs, parent, name, buf);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_stat %s failed (%d %s)",
                name, rc, strerror(rc));
        errno = rc;
        rc = -1;
    }
    return rc;
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
    } else if (mfu_file->type == DAOS) {
        int rc = daos_stat(path, buf, mfu_file);
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

/* use a noop, daos does not have a mknod function */
int daos_mknod(const char* path, mode_t mode, dev_t dev, mfu_file_t* mfu_file)
{
    return 0;
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
    } else if (mfu_file->type == DAOS) {
        int rc = daos_mknod(path, mode, dev, mfu_file);
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

/*****************************
 * Links
 ****************************/

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
void daos_open(const char* file, int flags, mode_t mode, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(file, &name, &dir_name);
    assert(dir_name);

    dfs_obj_t *parent = NULL;
    int rc = dfs_lookup(mfu_file->dfs, dir_name, O_RDWR, &parent, NULL, NULL);
    if (parent != NULL) {
        rc = dfs_open(mfu_file->dfs, parent, name,
                      S_IFREG | mode, flags,
                      0, 0, NULL, &(mfu_file->obj));
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_open %s failed (%d %s)",
                    name, rc, strerror(rc));
            errno = rc;
            rc = -1;
        }
    } else {
        MFU_LOG(MFU_LOG_ERR, "dfs_lookup %s failed", dir_name);
        errno = ENOENT;
        rc = -1;
    }
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

void mfu_file_open(const char* file, int flags, mfu_file_t* mfu_file, ...)
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

    if (mfu_file->type == POSIX) {
        if (mode_set) {
            mfu_file->fd = mfu_open(file, flags, mode);
        } else {
            mfu_file->fd = mfu_open(file, flags);
        }
    } else if (mfu_file->type == DAOS) {
        daos_open(file, flags, mode, mfu_file);
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  file, mfu_file->type);
    }
}

/* release an open object */
int daos_close(const char* file, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    int rc = dfs_release(mfu_file->obj);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_close %s failed (%d %s)",
                file, rc, strerror(rc));
        errno = rc;
        rc = -1;
    }
    return rc;
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
    } else if (mfu_file->type == DAOS) {
        int rc = daos_close(file, mfu_file);
#ifdef DAOS_SUPPORT
        if (rc == 0) {
            mfu_file->obj = NULL;
        }
#endif
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
    } else if (mfu_file->type == DAOS) {
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
    } else if (mfu_file->type == DAOS) {
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
    daos_size_t got_size;

    d_sg_list_t sgl;
    d_iov_t     iov;

    sgl.sg_nr = 1;
    d_iov_set(&iov, buf, size);
    sgl.sg_iovs = &iov;
    sgl.sg_nr_out = 1;
    sgl.sg_iovs[0].iov_len = size;

    int rc = dfs_read(mfu_file->dfs, mfu_file->obj, &sgl, mfu_file->offset, &got_size, NULL); 
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_read %s failed (%d %s)",
                file, rc, strerror(rc));
        errno = rc;
        return -1;
    }
    mfu_file->offset += (daos_off_t)got_size;
    return (ssize_t)got_size;
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
    } else if (mfu_file->type == DAOS) {
        ssize_t num_bytes_written = daos_write(file, buf, size, mfu_file);
        return num_bytes_written;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  file, mfu_file->type);
    }
}

ssize_t mfu_write(const char* file, int fd, const void* buf, size_t size)
{
    int tries = 10;
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
    d_sg_list_t sgl;
    d_iov_t     iov;

    sgl.sg_nr = 1;
    d_iov_set(&iov, buf, size);
    sgl.sg_iovs = &iov;
    sgl.sg_nr_out = 1;
    sgl.sg_iovs[0].iov_len = size;

    int rc = dfs_write(mfu_file->dfs, mfu_file->obj, &sgl, mfu_file->offset, NULL); 
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_write %s failed (%d %s)",
                file, rc, strerror(rc));
        errno = rc;
        return -1;
    }
    mfu_file->offset += (daos_off_t)size;
    return (ssize_t)size;
#endif
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

/* TODO: need to fix this function for dfs */
int daos_ftruncate(mfu_file_t* mfu_file, off_t length)
{
#ifdef DAOS_SUPPORT
    daos_off_t offset = (daos_off_t) length;
    int rc = dfs_punch(mfu_file->dfs, mfu_file->obj, offset, DFS_MAX_FSIZE);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_punch failed (%d)",
                rc, strerror(rc));
        errno = rc;
        rc = -1;
    }
    return rc;
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
    } else if (mfu_file->type == DAOS) {
        int rc = daos_ftruncate(mfu_file, length);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
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

int daos_mkdir(const char* dir, mode_t mode, mfu_file_t* mfu_file) {
#ifdef DAOS_SUPPORT
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(dir, &name, &dir_name);
    assert(dir_name);

    /* Need to lookup parent directory in DFS */
    dfs_obj_t *parent = NULL;
    int rc = dfs_lookup(mfu_file->dfs, dir_name, O_RDWR, &parent, NULL, NULL);

    /* only call mkdir if the dir_name is not the root DFS directory */
    if (strcmp(dir_name, "/") != 0) {
        rc = dfs_mkdir(mfu_file->dfs, parent, name, mode, 0);
    }

    if (rc) {
        errno = rc;
        rc = -1;
    }

    return rc;
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
    } else if (mfu_file->type == DAOS) {
        int rc = daos_mkdir(dir, mode, mfu_file);
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

#define NUM_DIRENTS 24

#ifdef DAOS_SUPPORT
struct dfs_mfu_t {
    dfs_obj_t *dir;
    struct dirent ents[NUM_DIRENTS];
    daos_anchor_t anchor;
    int num_ents;
};
#endif

DIR* daos_opendir(const char* dir, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    struct dfs_mfu_t* dirp = calloc(1, sizeof(*dirp));
    if (dirp == NULL) {
        errno = ENOMEM;
        return NULL;
    }
    int rc = dfs_lookup(mfu_file->dfs, dir, O_RDWR, &dirp->dir, NULL, NULL);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_lookup %s failed", dir);
        errno = ENOENT;
        free(dirp);
        return NULL;
    }
    return (DIR *)dirp;
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
    } else if (mfu_file->type == DAOS) {
        DIR* dirp = daos_opendir(dir, mfu_file);
        return dirp;
    } else {
        MFU_ABORT(-1, "File type not known: %s type=%d",
                  dir, mfu_file->type);
    }
}

int daos_closedir(DIR* _dirp, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    struct dfs_mfu_t *dirp = (struct dfs_mfu_t *)_dirp;
    int rc = dfs_release(dirp->dir);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_release failed (%d)",
                rc, strerror(rc));
        errno = rc;
        rc = -1;
    }
    free(dirp);
    return rc;
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
    } else if (mfu_file->type == DAOS) {
        int rc = daos_closedir(dirp, mfu_file);
        return rc;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}

struct dirent* daos_readdir(DIR* _dirp, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    struct dfs_mfu_t *dirp = (struct dfs_mfu_t *)_dirp;
    if (dirp->num_ents) {
        goto ret;
    }
    dirp->num_ents = NUM_DIRENTS;
    int rc;
    while (!daos_anchor_is_eof(&dirp->anchor)) {
        rc = dfs_readdir(mfu_file->dfs, dirp->dir,
                         &dirp->anchor, &dirp->num_ents,
	                 dirp->ents);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_readdir failed (%d %s)", rc, strerror(rc));
            dirp->num_ents = 0;
            errno = ENOENT;
            return NULL;
        }
        if (dirp->num_ents == 0) {
	    continue;
        }
	goto ret;
    }
    assert(daos_anchor_is_eof(&dirp->anchor));
    return NULL;
ret:
    dirp->num_ents--;
    return &dirp->ents[dirp->num_ents];
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
    } else if (mfu_file->type == DAOS) {
        struct dirent* entry = daos_readdir(dirp, mfu_file);
        return entry;
    } else {
        MFU_ABORT(-1, "File type not known, type=%d",
                  mfu_file->type);
    }
}
