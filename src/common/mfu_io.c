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

#include <gurt/common.h>
#include <gurt/hash.h>
#include "mfu.h"

#define MFU_IO_TRIES  (5)
#define MFU_IO_USLEEP (100)

static int mpi_rank;
dfs_t *dfs1;
dfs_t *dfs2;
dfs_t *dfs;
extern daos_path dpath;
struct d_hash_table *dir_hash;

struct mfu_dir_hdl {
        d_list_t	entry;
        dfs_obj_t	*oh;
        char		name[PATH_MAX];
};

static inline struct mfu_dir_hdl *
hdl_obj(d_list_t *rlink)
{
        return container_of(rlink, struct mfu_dir_hdl, entry);
}

static bool
key_cmp(struct d_hash_table *htable, d_list_t *rlink,
	const void *key, unsigned int ksize)
{
        struct mfu_dir_hdl *hdl = hdl_obj(rlink);

        return (strcmp(hdl->name, (const char *)key) == 0);
}

static void
rec_free(struct d_hash_table *htable, d_list_t *rlink)
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

static dfs_obj_t *
lookup_insert_dir(const char *name)
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

        rc = dfs_lookup(dfs, name, O_RDWR, &hdl->oh, NULL, NULL);
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

static int
parse_filename(const char *path, char **_obj_name, char **_cont_name)
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

/* calls access, and retries a few times if we get EIO or EINTR */
int mfu_access(const char* path, int amode)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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

/* calls lchown, and retries a few times if we get EIO or EINTR */
int mfu_lchown(const char* path, uid_t owner, gid_t group)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    int rc;
    dfs_obj_t *parent = NULL;
    char *name = NULL, *dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);
    rc = dfs_lookup(mfu_file->dfs, dir_name, O_RDWR, &parent, NULL, NULL);
    //parent = lookup_insert_dir(dir_name);
    if (parent == NULL) {
        fprintf(stderr, "dfs_lookup %s failed \n", dir_name);
	return ENOENT;
    }
    rc = dfs_chmod(mfu_file->dfs, parent, name, mode);
    if (rc) {
        fprintf(stderr, "dfs_chmod %s failed (%d)\n", name, rc);
    }
    return rc;
}

int posix_chmod(const char* path, mode_t mode)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    int rc;
    if (mfu_file->type == POSIX) {
        rc = posix_chmod(path, mode);
    } else if (mfu_file->type == DAOS) {
        rc = daos_chmod(path, mode, mfu_file);
    }
    return rc;
}

/* calls utimensat, and retries a few times if we get EIO or EINTR */
int mfu_utimensat(int dirfd, const char *pathname, const struct timespec times[2], int flags)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    int rc;
    dfs_obj_t *parent = NULL;
    char *name = NULL, *dir_name = NULL;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    parse_filename(path, &name, &dir_name);
    assert(dir_name);
    rc = dfs_lookup(mfu_file->dfs, dir_name, O_RDWR, &parent, NULL, NULL);
    //parent = lookup_insert_dir(dir_name);
    if (parent == NULL) {
        fprintf(stderr, "dfs_lookup %s failed \n", dir_name);
	return ENOENT;
    }

    rc = dfs_stat(mfu_file->dfs, parent, name, buf);
    if (rc) {
        fprintf(stderr, "dfs_stat %s failed (%d)\n", name, rc);
    }
    return rc;
}

int posix_lstat(const char* path, struct stat* buf) {
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    int rc;
    if (mfu_file->type == POSIX) {
        rc = posix_lstat(path, buf);
    } else if (mfu_file->type == DAOS) {
        rc = daos_stat(path, buf, mfu_file);
    }
    return rc;
}

/* calls lstat64, and retries a few times if we get EIO or EINTR */
int mfu_lstat64(const char* path, struct stat64* buf)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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

int posix_mknod(const char* path, mode_t mode, dev_t dev)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    int rc;
    if (mfu_file->type == POSIX) {
        rc = posix_mknod(path, mode, dev);
    } else if (mfu_file->type == DAOS) {
        rc = daos_mknod(path, mode, dev, mfu_file);
    }
    return rc;
}

/* call remove, retry a few times on EINTR or EIO */
int mfu_remove(const char* path)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
 *  * Links
 *   ****************************/

/* call readlink, retry a few times on EINTR or EIO */
ssize_t mfu_readlink(const char* path, char* buf, size_t bufsize)
{
    ssize_t rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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

/*****************************
 *  * Files
 *   ****************************/
void daos_open(const char* file, int flags,
               int* mode_set, mode_t mode, mfu_file_t* mfu_file)
{

//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    int rc; 
    dfs_obj_t *parent = NULL;
    char *name = NULL, *dir_name = NULL;

    parse_filename(file, &name, &dir_name);

    assert(dir_name);

    rc = dfs_lookup(mfu_file->dfs, dir_name, O_RDWR, &parent, NULL, NULL);
    //parent = lookup_insert_dir(dir_name);
    if (parent == NULL) {
        fprintf(stderr, "dfs_lookup %s failed \n", dir_name);
    }

    rc = dfs_open(mfu_file->dfs, parent, name,
                  S_IFREG | S_IWUSR | S_IRUSR, O_RDWR | O_CREAT,
                  0, 0, NULL, &(mfu_file->obj));
    if (rc) {
        fprintf(stderr, "dfs_open %s failed (%d)\n", name, rc);
    }
}

/* open file with specified flags and mode, retry open a few times on failure */
void posix_open(const char* file, int flags, int* mode_set,
                mode_t mode, mfu_file_t* mfu_file)
{
    /* attempt to open file */
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    mfu_file->fd = -1;
    errno = 0;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    if (mode_set) {
        mfu_file->fd = open(file, flags, mode);
    }
    else {
        mfu_file->fd = open(file, flags);
    }

    /* if open failed, try a few more times */
    if (mfu_file->fd < 0) {
        /* try again */
        int tries = MFU_IO_TRIES;
        while (tries && mfu_file->fd < 0) {
            /* sleep a bit before consecutive tries */
            usleep(MFU_IO_USLEEP);

            /* open again */
            errno = 0;
            if (mode_set) {
                mfu_file->fd = open(file, flags, mode);
            }
            else {
                mfu_file->fd = open(file, flags);
            }
            tries--;
        }

       /* if we still don't have a valid file, consider it an error */
       if (mfu_file->fd < 0) {
           /* we could abort, but probably don't want to here */
       }
    }
}

void mfu_file_open(const char* file, int flags, mfu_file_t* mfu_file, ...)
{
    /* extract the mode (see man 2 open) */
    int mode_set = 0;
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, mfu_file);
        mode = va_arg(ap, mode_t);
        va_end(ap);
        mode_set = 1;
    }
    if (mfu_file->type == POSIX) {
        posix_open(file, flags, &mode_set, mode, mfu_file);
    } else if (mfu_file->type == DAOS) {
        daos_open(file, flags, &mode_set, mode, mfu_file);
    }
}

/* release an open object */
int daos_close(const char* file, mfu_file_t* mfu_file)
{
    int rc;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    rc = dfs_release(mfu_file->obj);
    return rc;
}

/* close file */
int posix_close(const char* file, mfu_file_t* mfu_file)
{
    int tries = MFU_IO_TRIES;
retry:
    errno = 0;
    int rc = close(mfu_file->fd);
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
    int rc;
    if (mfu_file->type == POSIX) {
        rc = posix_close(file, mfu_file);
    } else if (mfu_file->type == DAOS) {
        rc = daos_close(file, mfu_file);
    }
    return rc;
}

off_t daos_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence)
{
    /* noop becuase daos have an lseek */
    return 0;
}

/* seek file descriptor to specified position */
off_t posix_lseek(const char* file, mfu_file_t* mfu_file, off_t pos, int whence)
{
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
retry:
    errno = 0;
    off_t rc = lseek(mfu_file->fd, pos, whence);
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
    off_t rc;
    if (mfu_file->type == POSIX) {
        rc = posix_lseek(file, mfu_file, pos, whence);
    } else if (mfu_file->type == DAOS) {
        rc = daos_lseek(file, mfu_file, pos, whence);
    }
    return rc;
}

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t mfu_file_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file)
{
    ssize_t got_size;
    if (mfu_file->type == POSIX) {
        got_size = posix_read(file, buf, size, mfu_file);
    } else if (mfu_file->type == DAOS) {
        got_size = daos_read(file, buf, size, mfu_file);
    }
    return got_size;
}

ssize_t daos_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file)
{
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    daos_size_t got_size;
    ssize_t size_read;
    int rc;
    mfu_file->sgl->sg_iovs[0].iov_len = size;
    rc = dfs_read(mfu_file->dfs, mfu_file->obj, mfu_file->sgl, mfu_file->offset, &got_size, NULL); 
    size_read = (ssize_t)got_size;
    if (rc) {
        printf("READ FAIL %d with file: %s\n", rc, file);
    }
    return (ssize_t)got_size;
}

ssize_t posix_read(const char* file, void* buf, size_t size, mfu_file_t* mfu_file)
{
    int tries = MFU_IO_TRIES;
    ssize_t n = 0;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    while ((size_t)n < size) {
        errno = 0;
        ssize_t rc = read(mfu_file->fd, (char*) buf + n, size - (size_t)n);
        if (rc > 0) {
            /* read some data */
            n += rc;
            tries = MFU_IO_TRIES;
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
    ssize_t num_bytes_written;
    if (mfu_file->type == POSIX) {
        num_bytes_written = posix_write(file, buf, size, mfu_file);
    } else if (mfu_file->type == DAOS) {
        num_bytes_written = daos_write(file, buf, size, mfu_file);
    }
    return num_bytes_written;
}

ssize_t posix_write(const char* file, const void* buf, size_t size, mfu_file_t* mfu_file)
{
    int tries = 10;
    ssize_t n = 0;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    while ((size_t)n < size) {
        errno = 0;
        ssize_t rc = write(mfu_file->fd, (const char*) buf + n, size - (size_t)n);
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
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    int rc;
    ssize_t num_of_bytes_written;
    mfu_file->sgl->sg_iovs[0].iov_len = size;
    rc = dfs_write(mfu_file->dfs, mfu_file->obj, mfu_file->sgl, mfu_file->offset, NULL); 
    if (rc) {
        printf("DFS WRITE FAIL %d with file: %s\n", rc, file);
    }
    /* TODO: maybe need a loop like mfu_write? */
    num_of_bytes_written = (ssize_t)size;
    return num_of_bytes_written;
}

/* truncate a file */
int mfu_truncate(const char* file, off_t length)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    int rc;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
    daos_off_t offset = (daos_off_t) length;
    rc = dfs_punch(mfu_file->dfs, mfu_file->obj, offset, DFS_MAX_FSIZE);
    if (rc) {
        fprintf(stderr, "dfs_punch failed (%d)\n", rc);
	return rc;
    }
    return rc;
}

int posix_ftruncate(mfu_file_t* mfu_file, off_t length)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
retry:
    errno = 0;
    rc = ftruncate(mfu_file->fd, length);
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
    int rc;
    if (mfu_file->type == POSIX) {
        rc = posix_ftruncate(mfu_file, length);
    } else if (mfu_file->type == DAOS) {
        rc = daos_ftruncate(mfu_file, length);
    }
    return rc;
}

/* delete a file */
int mfu_unlink(const char* file)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
 *  * Directories
 *   ****************************/

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
    int rc;
    dfs_obj_t *parent = NULL;
    char *name = NULL, *dir_name = NULL;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);

    parse_filename(dir, &name, &dir_name);

    assert(dir_name);
    //parent = lookup_insert_dir(dir_name);
    rc = dfs_lookup(dfs, dir_name, O_RDWR, &parent, NULL, NULL);
    /*if (parent == NULL) {
        fprintf(stderr, "dfs_lookup %s failed \n", dir_name);
	return ENOENT;
    }*/
    rc = dfs_mkdir(dfs, parent, name, S_IRWXU, 0);
    if (rc) {
//        fprintf(stderr, "dfs_mkdir %s failed (%d)\n", name, rc);
    }
    return rc;
}

int posix_mkdir(const char* dir, mode_t mode)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    int rc;
    if (mfu_file->type == POSIX) {
        rc = posix_mkdir(dir, mode);
    } else if (mfu_file->type == DAOS) {
        rc = daos_mkdir(dir, mode, mfu_file);
    }
    return rc;
}

/* remove directory, retry a few times on EINTR or EIO */
int mfu_rmdir(const char* dir)
{
    int rc;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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

struct dfs_mfu_t {
	dfs_obj_t *dir;
	struct dirent ents[NUM_DIRENTS];
	daos_anchor_t anchor;
	int num_ents;
};

DIR* daos_opendir(const char* dir, mfu_file_t* mfu_file)
{
    struct dfs_mfu_t *dirp = NULL;
    int rc;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);

    dirp = calloc(1, sizeof(*dirp));
    if (dirp == NULL)
	    return NULL;

    rc = dfs_lookup(mfu_file->dfs, dir, O_RDWR, &dirp->dir, NULL, NULL);
    if (rc) {
	    fprintf(stderr, "dfs_lookup %s failed (%d)\n", dir, rc);
	    return NULL;
    }

    return (DIR *)dirp;
}

/* open directory, retry a few times on EINTR or EIO */
DIR* posix_opendir(const char* dir)
{
    DIR* dirp;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    DIR* dirp;
    if (mfu_file->type == POSIX) {
        dirp = posix_opendir(dir);
    } else if (mfu_file->type == DAOS) {
        dirp = daos_opendir(dir, mfu_file);
    }
    return dirp;
}

int daos_closedir(DIR* _dirp, mfu_file_t* mfu_file)
{
    int rc;
    struct dfs_mfu_t *dirp = (struct dfs_mfu_t *)_dirp;

    rc = dfs_release(dirp->dir);
    if (rc)
	    return rc;
    free(dirp);
    return rc;
}

/* close directory, retry a few times on EINTR or EIO */
int posix_closedir(DIR* dirp)
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
    int rc;
    if (mfu_file->type == POSIX) {
        rc = posix_closedir(dirp);
    } else if (mfu_file->type == DAOS) {
        rc = daos_closedir(dirp, mfu_file);
    }
    return rc;
}

struct dirent* daos_readdir(DIR* _dirp, mfu_file_t* mfu_file)
{
    int rc;
    struct dfs_mfu_t *dirp = (struct dfs_mfu_t *)_dirp;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);

    if (dirp->num_ents)
	    goto ret;

    dirp->num_ents = NUM_DIRENTS;

    while (!daos_anchor_is_eof(&dirp->anchor)) {
	    rc = dfs_readdir(mfu_file->dfs, dirp->dir, &dirp->anchor, &dirp->num_ents,
			     dirp->ents);
	    if (rc)
		    return NULL;

	    if (dirp->num_ents == 0)
		    continue;
	    goto ret;
    }

    assert(daos_anchor_is_eof(&dirp->anchor));
    return NULL;

ret:
    dirp->num_ents--;
    return &dirp->ents[dirp->num_ents];
}

/* read directory entry, retry a few times on ENOENT, EIO, or EINTR */
struct dirent* posix_readdir(DIR* dirp)
{
    /* read next directory entry, retry a few times */
    struct dirent* entry;
    int tries = MFU_IO_TRIES;
//    printf ("%d: __FUNCTION__ = %s\n", mpi_rank, __FUNCTION__);
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
    struct dirent* entry;
    if (mfu_file->type == POSIX) {
        entry = posix_readdir(dirp);
    } else if (mfu_file->type == DAOS) {
        entry = daos_readdir(dirp, mfu_file);
    }
    return entry; 
}
