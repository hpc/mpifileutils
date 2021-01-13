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

#ifdef DAOS_SUPPORT
#include <gurt/common.h>
#include <gurt/hash.h>
#endif

#include "mfu.h"

#define MFU_IO_TRIES  (5)
#define MFU_IO_USLEEP (100)

static int mpi_rank;

#ifdef DAOS_SUPPORT
/* Handle for a hash table entry */
struct daos_dir_hdl {
    d_list_t    entry;
    dfs_obj_t*  oh;
    char*       name;
};

/* Return a newly allocated daos_dir_hdl structure */
static struct daos_dir_hdl* daos_dir_hdl_new(void)
{
    struct daos_dir_hdl* hdl = (struct daos_dir_hdl*) MFU_MALLOC(sizeof(struct daos_dir_hdl));
    hdl->oh = NULL;
    hdl->name = NULL;

    return hdl;
}

/* free a daos_dir_hdl structure */
static void daos_dir_hdl_delete(struct daos_dir_hdl** phdl)
{
    if (phdl != NULL) {
        struct daos_dir_hdl* hdl = *phdl;
        if (hdl->oh != NULL) {
            dfs_release(hdl->oh);
        }
        mfu_free(&hdl->name);
        mfu_free(phdl);
    }
}

/* Get the daos_dir_hdl from its entry */
static inline struct daos_dir_hdl* hdl_obj(d_list_t* rlink)
{
    return container_of(rlink, struct daos_dir_hdl, entry);
}

/* Simple string comparison of hdl->name as the key */
static bool key_cmp(struct d_hash_table* htable, d_list_t* rlink, 
        const void* key, unsigned int ksize)
{
    struct daos_dir_hdl* hdl = hdl_obj(rlink);

    return (strcmp(hdl->name, (const char *)key) == 0);
}

/* Since we only delete entries when we are finished with them,
 * this should always return true so rec_free is called */
static bool rec_decref(struct d_hash_table* htable, d_list_t* rlink)
{
    return true;
}

/* Free a hash entry. Called when the table is destroyed */
static void rec_free(struct d_hash_table* htable, d_list_t* rlink)
{
    struct daos_dir_hdl* hdl = hdl_obj(rlink);

    assert(d_hash_rec_unlinked(&hdl->entry));
    daos_dir_hdl_delete(&hdl);
}

/* Operations for the hash table */
static d_hash_table_ops_t hdl_hash_ops = {
    .hop_key_cmp    = key_cmp,
    .hop_rec_decref = rec_decref,
    .hop_rec_free   = rec_free
};

/* Caches calls to dfs_lookup and returns lookups from the cache.
 * On error, sets errno and returns NULL */
static dfs_obj_t* daos_hash_lookup(const char* name, mfu_file_t* mfu_file)
{
    struct daos_dir_hdl* hdl;
    d_list_t* rlink;
    int rc;

    /* Make sure the hash is initialized */
    if (mfu_file->dfs_hash == NULL) {
        rc = d_hash_table_create(D_HASH_FT_NOLOCK, 16, NULL, &hdl_hash_ops, &mfu_file->dfs_hash);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "Failed to initialize dir hashtable");
            errno = ENOMEM;
            return NULL;
        }
    }

    /* If cached, return it */
    rlink = d_hash_rec_find(mfu_file->dfs_hash, name, strlen(name));
    if (rlink != NULL) {
        hdl = hdl_obj(rlink);
        return hdl->oh;
    }

    /* Create a new entry */
    hdl = daos_dir_hdl_new();
    if (hdl == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialze hash entry");
        daos_dir_hdl_delete(&hdl);
        errno = ENOMEM;
        return NULL;
    }

    /* Allocate space for name, up to PATH_MAX,
     * leaving 1 extra for the null terminator */
    size_t name_len = strnlen(name, PATH_MAX);
    if (name_len > PATH_MAX-1) {
        daos_dir_hdl_delete(&hdl);
        errno = ENAMETOOLONG;
        return NULL;
    }
    hdl->name = MFU_STRDUP(name);

    /* Lookup the object handle */
    rc = dfs_lookup(mfu_file->dfs, name, O_RDWR, &hdl->oh, NULL, NULL);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_lookup() of %s Failed", name);
        daos_dir_hdl_delete(&hdl);
        errno = rc;
        return NULL;
    }

    /* Store this entry in the hash.
     * Since we have already called d_hash_rec_find,
     * pass exclusive=false to avoid another find being called */
    rc = d_hash_rec_insert(mfu_file->dfs_hash, hdl->name, name_len,
                            &hdl->entry, false);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "Failed to insert hash entry");
        daos_dir_hdl_delete(&hdl);
        errno = ENOMEM;
        return NULL;
    }

    /* Return the object */
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

#endif /* DAOS_SUPPORT */

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
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    } else {
        rc = dfs_access(mfu_file->dfs, parent, name, amode);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_access %s failed (%d %s)",
                    name, rc, strerror(rc));
            errno = rc;
            rc = -1;
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);
    
    return rc;
#else
    return 0;
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
        errno = ENOTSUP;
        return -1;
    }

    /* Only real user and group IDs supported at this time */
    if (flags & AT_EACCESS) {
        errno = ENOTSUP;
        return -1;
    }

    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    } else {
        /* Get the mode of the object */
        mode_t mode;
        dfs_obj_t* obj;
        int lookup_flags = O_RDWR;
        if (flags & AT_SYMLINK_NOFOLLOW) {
            lookup_flags |= O_NOFOLLOW;
        }
        rc = dfs_lookup_rel(mfu_file->dfs, parent, name, lookup_flags, &obj, &mode, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s/%s failed", dir_name, name);
            errno = rc;
            rc = -1;
        } else {
            /* return success for links, since dfs_access does not have proper support */
            if (!S_ISLNK(mode)) {
                rc = dfs_access(mfu_file->dfs, parent, name, amode);
                if (rc) {
                    MFU_LOG(MFU_LOG_ERR, "dfs_access %s failed (%d %s)",
                            name, rc, strerror(rc));
                    errno = rc;
                    rc = -1;
                }
            }

            /* Release the obj */
            int tmp_rc = dfs_release(obj);
            if (tmp_rc && (rc != -1)) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release %s failed (%d %s)",
                        name, tmp_rc, strerror(tmp_rc));
                errno = tmp_rc;
                rc = -1;
            }
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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
#ifdef DAOS_SUPPORT
    /* At this time, DFS does not support updating the uid or gid.
     * These are set at the container level, not file level */
    return 0;
#else
    return 0;
#endif
}

int daos_chmod(const char *path, mode_t mode, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    } else {
        rc = dfs_chmod(mfu_file->dfs, parent, name, mode);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_chmod %s failed (%d %s)",
                    name, rc, strerror(rc));
            errno = rc;
            rc = -1;
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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
        errno = ENOTSUP;
        return -1;
    }

    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(pathname, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    } else {
        /* Lookup the object */
        dfs_obj_t* obj;
        int lookup_flags = O_RDWR;
        if (flags & AT_SYMLINK_NOFOLLOW) {
            lookup_flags |= O_NOFOLLOW;
        }
        rc = dfs_lookup_rel(mfu_file->dfs, parent, name, lookup_flags, &obj, NULL, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s failed", pathname);
            errno = rc;
            rc = -1;
        } else {
            /* Set the times on the obj */
            if (rc != -1) {
                struct stat stbuf;
                stbuf.st_atim = times[0];
                stbuf.st_mtim = times[1];
                rc = dfs_osetattr(mfu_file->dfs, obj, &stbuf, DFS_SET_ATTR_ATIME | DFS_SET_ATTR_MTIME);
                if (rc) {
                    MFU_LOG(MFU_LOG_ERR, "dfs_osetattr %s failed", pathname);
                    errno = rc;
                    rc = -1;
                }
            }

            /* Release the obj */
            int tmp_rc = dfs_release(obj);
            if (tmp_rc && (rc != -1)) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release %s failed (%d %s)",
                        pathname, tmp_rc, strerror(tmp_rc));
                errno = tmp_rc;
                rc = -1;
            }
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
#endif
}

/* Since dfs_stat() performs like lstat(), this is emulated. */
int daos_stat(const char* path, struct stat* buf, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
        goto out_free_name;
    }

    /* Lookup name within the parent */
    dfs_obj_t* obj;
    rc = dfs_lookup_rel(mfu_file->dfs, parent, name, O_RDWR, &obj, NULL, buf);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s / %s failed", dir_name, name);
        errno = rc;
        rc = -1;
        goto out_free_name;
    }

    dfs_release(obj);
out_free_name:
    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    } else {
        /* Stat the path.
         * dfs_stat interrogates the link itself */
        rc = dfs_stat(mfu_file->dfs, parent, name, buf);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_stat %s failed (%d %s)",
                    name, rc, strerror(rc));
            errno = rc;
            rc = -1;
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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

/* emulates mknod with dfs_open, dfs_release */
int daos_mknod(const char* path, mode_t mode, dev_t dev, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    /* Only regular files are supported at this time */
    mode_t dfs_mode = mode | S_IFREG;
    mode_t filetype = dfs_mode & S_IFMT;
    if (filetype != S_IFREG) {
        MFU_LOG(MFU_LOG_ERR, "Invalid entry type (not a file)");
        errno = EINVAL;
        return -1;
    }

    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    }
    else {
        /* create regular file */
        rc = dfs_open(mfu_file->dfs, parent, name,
                      dfs_mode, O_CREAT | O_EXCL,
                      0, 0, NULL, &(mfu_file->obj));
        if (rc) {
            /* Avoid excessive logging */
            if (rc != EEXIST) {
                MFU_LOG(MFU_LOG_ERR, "dfs_open %s failed (%d %s)",
                        name, rc, strerror(rc));
            }
            errno = rc;
            rc = -1;
        }
        else {
            /* close the file */
            rc = dfs_release(mfu_file->obj);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release %s failed (%d %s)",
                        path, rc, strerror(rc));
                errno = rc;
                rc = -1;
            }
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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
    return NULL;
#endif
}

/*****************************
 * Links
 ****************************/

/* emulates readlink with dfs_lookup, dfs_get_symlink_value */
ssize_t daos_readlink(const char* path, char* buf, size_t bufsize, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    daos_size_t got_size = (daos_size_t) bufsize;

    /* Lookup the parent directory first, since it is likely cached */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        got_size = -1;
    } else { 
        /* Lookup the symlink within the parent */
        dfs_obj_t* sym_obj;
        int lookup_flags = O_RDWR | O_NOFOLLOW;
        int rc = dfs_lookup_rel(mfu_file->dfs, parent, name, lookup_flags, &sym_obj, NULL, NULL);
        if (sym_obj == NULL) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s failed", path);
            errno = rc;
            got_size = -1;
        } else {
            /* Read the symlink value. This also makes sure it is S_IFLNK */
            rc = dfs_get_symlink_value(sym_obj, buf, &got_size);
            if (rc) {
                errno = rc;
                got_size = -1;
            } else {
                /* got_size includes the NULL terminator, but mfu_file_readlink
                * expects that it does not */
                got_size--;
            }

            /* Release the symlink */
            rc = dfs_release(sym_obj);
            if (rc && (got_size != -1)) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release %s failed (%d %s)",
                        path, rc, strerror(rc));
                errno = rc;
                rc = -1;
            }
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return (ssize_t) got_size;

#else
    return (ssize_t) 0;
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
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(newpath, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    } else {
        /* open/create the symlink */
        rc = dfs_open(mfu_file->dfs, parent, name,
                      S_IFLNK, O_CREAT | O_EXCL,
                      0, 0, oldpath, &(mfu_file->obj));
        if (rc) {
            /* Avoid excessive logging */
            if (rc != EEXIST) {
                MFU_LOG(MFU_LOG_ERR, "dfs_open %s failed (%d %s)",
                        name, rc, strerror(rc));
            }
            errno = rc;
            rc = -1;
        } else {
            /* close the symlink */
            rc = dfs_release(mfu_file->obj);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release %s failed (%d %s)",
                        newpath, rc, strerror(rc));
                errno = rc;
                rc = -1;
            }
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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
    /* Only regular files are supported at this time */
    mode_t dfs_mode = mode | S_IFREG;
    if (!S_ISREG(dfs_mode)) {
        MFU_LOG(MFU_LOG_ERR, "Invalid entry type (not a file)");
        errno = EINVAL;
        return -1;
    }

    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(file, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    } else {
        mode_t obj_mode;
        rc = dfs_lookup_rel(mfu_file->dfs, parent, name, O_RDWR, &(mfu_file->obj),
                            &obj_mode, NULL);
        if (!rc) {
            /* lookup found an obj */
            if (flags & O_CREAT && flags & O_EXCL) {
                /* ... but it should not have */
                dfs_release(mfu_file->obj);
                errno = EEXIST;
                rc = -1;
            } else if (!S_ISREG(obj_mode)) {
                /* ... but the obj isn't a file */
                dfs_release(mfu_file->obj);
                MFU_LOG(MFU_LOG_ERR, "Invalid entry type (not a file)");
                errno = EINVAL;
                rc = -1;
            } else {
                /* good */
            }
        } else if (rc == ENOENT && flags & O_CREAT) {
            /* call dfs_open so it can be created */
            rc = dfs_open(mfu_file->dfs, parent, name,
                          dfs_mode, flags,
                          0, 0, NULL, &(mfu_file->obj));
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_open %s failed (%d %s)",
                        name, rc, strerror(rc));
                errno = rc;
                rc = -1;
            }
        } else if (rc) {
            /* this is actually an error */
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s/%s failed", dir_name, name);
            errno = rc;
            rc = -1;
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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
    } else if (mfu_file->type == DFS) {
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
#else
    return 0;
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
#else
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
    /* record address and size of user buffer in io vector */
    d_iov_t iov;
    d_iov_set(&iov, buf, size);

    /* define scatter-gather list for dfs_read */
    d_sg_list_t sgl;
    sgl.sg_nr = 1;
    sgl.sg_iovs = &iov;
    sgl.sg_nr_out = 1;

    /* execute read operation */
    daos_size_t got_size;
    int rc = dfs_read(mfu_file->dfs, mfu_file->obj, &sgl, mfu_file->offset, &got_size, NULL); 
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_read %s failed (%d %s)",
                file, rc, strerror(rc));
        errno = rc;
        return -1;
    }

    /* update file pointer with number of bytes read */
    mfu_file->offset += (daos_off_t)got_size;

    return (ssize_t)got_size;
#else
    return (ssize_t)0;
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
    /* record address and size of user buffer in io vector */
    d_iov_t iov;
    d_iov_set(&iov, (void*) buf, size);

    /* define scatter-gather list for dfs_write */
    d_sg_list_t sgl;
    sgl.sg_nr = 1;
    sgl.sg_iovs = &iov;
    sgl.sg_nr_out = 1;

    /* execute write operation,
     * dfs_write writes all bytes if there is no error */
    int rc = dfs_write(mfu_file->dfs, mfu_file->obj, &sgl, mfu_file->offset, NULL); 
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_write %s failed (%d %s)",
                file, rc, strerror(rc));
        errno = rc;
        return -1;
    }

    /* update file pointer with number of bytes written */
    mfu_file->offset += (daos_off_t)size;

    return (ssize_t)size;
#else
    return (ssize_t)0;
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
    /* record address and size of user buffer in io vector */
    d_iov_t iov;
    d_iov_set(&iov, buf, size);

    /* define scatter-gather list for dfs_read */
    d_sg_list_t sgl;
    sgl.sg_nr = 1;
    sgl.sg_iovs = &iov;
    sgl.sg_nr_out = 1;

    /* execute read operation */
    daos_size_t got_size;
    int rc = dfs_read(mfu_file->dfs, mfu_file->obj, &sgl, offset, &got_size, NULL); 
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_read %s failed (%d %s)",
            file, rc, strerror(rc));
        errno = rc;
        return -1;
    }

    return (ssize_t)got_size;
#else
    return (ssize_t)0;
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
    /* record address and size of user buffer in io vector */
    d_iov_t iov;
    d_iov_set(&iov, (void*) buf, size);

    /* define scatter-gather list for dfs_write */
    d_sg_list_t sgl;
    sgl.sg_nr = 1;
    sgl.sg_iovs = &iov;
    sgl.sg_nr_out = 1;

    /* execute write operation,
     * dfs_write writes all bytes if there is no error */
    int rc = dfs_write(mfu_file->dfs, mfu_file->obj, &sgl, offset, NULL); 
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_write %s failed (%d %s)",
                file, rc, strerror(rc));
        errno = rc;
        return -1;
    }

    return (ssize_t)size;
#else
    return (ssize_t)0;
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
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(file, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        errno = ENOENT;
        rc = -1;
    } else {
        /* open the obj in the parent */
        dfs_obj_t* obj;
        rc = dfs_open(mfu_file->dfs, parent, name,
                      S_IFREG, O_RDWR,
                      0, 0, NULL, &obj);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_open %s failed (%d %s)",
                    name, rc, strerror(rc));
            errno = rc;
            rc = -1;
        } else {
            /* truncate the obj */
            daos_off_t offset = (daos_off_t) length;
            rc = dfs_punch(mfu_file->dfs, obj, offset, DFS_MAX_FSIZE);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_punch failed (%d %s)",
                        rc, strerror(rc));
                errno = rc;
                rc = -1;
            }

            /* close the obj */
            int tmp_rc = dfs_release(mfu_file->obj);
            if (tmp_rc && (rc != -1)) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release %s failed (%d %s)",
                        file, tmp_rc, strerror(tmp_rc));
                errno = tmp_rc;
                rc = -1;
            }
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
#endif
}

int daos_ftruncate(mfu_file_t* mfu_file, off_t length)
{
#ifdef DAOS_SUPPORT
    daos_off_t offset = (daos_off_t) length;
    int rc = dfs_punch(mfu_file->dfs, mfu_file->obj, offset, DFS_MAX_FSIZE);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_punch failed (%d %s)",
                rc, strerror(rc));
        errno = rc;
        rc = -1;
    }
    return rc;
#else
    return 0;
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

/* emulates unlink on a DAOS file or symlink.
 * Since checking the file type would require another
 * lookup, for performance considerations,
 * this also works on directories. */
int daos_unlink(const char* file, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(file, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    } 
    else {
        /* remove the file */
        rc = dfs_remove(mfu_file->dfs, parent, name, false, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_remove failed (%d %s)",
                    rc, strerror(rc));
            errno = rc;
            rc = -1;
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(dir, &name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* only call mkdir if name is not the root DFS directory */
    if (name && strcmp(name, "/") != 0) {
        /* Lookup the parent directory */
        dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
        if (parent == NULL) {
            rc = -1;
        } else {
            /* Make the directory */
            rc = dfs_mkdir(mfu_file->dfs, parent, name, mode, 0);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_mkdir %s failed (%d %s)", 
                        name, rc, strerror(rc));
                errno = rc;
                rc = -1;
            }
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
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
    dfs_obj_t* dir;
    struct dirent ents[NUM_DIRENTS];
    daos_anchor_t anchor;
    int num_ents;
};
#endif

/* open directory. The entry itself is not cached in mfu_file->dir_hash */
DIR* daos_opendir(const char* dir, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    struct dfs_mfu_t* dirp = calloc(1, sizeof(*dirp));
    if (dirp == NULL) {
        errno = ENOMEM;
        return NULL;
    }

    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(dir, &name, &dir_name);
    assert(dir_name);

    if (!name || strcmp(name, "/") == 0) {
        /* For root, just lookup the entry */
        int rc = dfs_lookup(mfu_file->dfs, dir, O_RDWR, &dirp->dir, NULL, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup %s failed", dir);
            errno = rc;
            free(dirp);
            return NULL;
        }
    } else {
        /* for non-root, try to cache the parent */
        dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
        if (parent == NULL) {
            goto err_dirp;
        } else {
            mode_t mode;
            int rc = dfs_lookup_rel(mfu_file->dfs, parent, name, O_RDWR, &dirp->dir,
                                   &mode, NULL);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s failed", dir);
                errno = rc;
                goto err_dirp;
            } else {
                if (!S_ISDIR(mode)) {
                    errno = ENOTDIR;
                    rc = dfs_release(dirp->dir);
                    if (rc) {
                        MFU_LOG(MFU_LOG_ERR, "dfs_release %s failed (%d %s)",
                                dir, rc, strerror(rc));
                    }
                    goto err_dirp;
                }
            }
        }
    }

    mfu_free(&dir_name);
    mfu_free(&name);

    return (DIR *)dirp;

err_dirp:
    mfu_free(&dir_name);
    mfu_free(&name);
    free(dirp);
    return NULL;
#else
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
int daos_closedir(DIR* _dirp, mfu_file_t* mfu_file)
{
#ifdef DAOS_SUPPORT
    struct dfs_mfu_t *dirp = (struct dfs_mfu_t *)_dirp;
    int rc = dfs_release(dirp->dir);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "dfs_release failed (%d %s)",
                rc, strerror(rc));
        errno = rc;
        rc = -1;
    }
    free(dirp);
    return rc;
#else
    return 0;
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
#else
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
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    daos_size_t got_size = (daos_size_t) size;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        got_size = -1;
    }
    else {
        /* lookup and open name */
        dfs_obj_t* obj;
        int lookup_flags = O_RDWR | O_NOFOLLOW;
        int rc = dfs_lookup_rel(mfu_file->dfs, parent, name, lookup_flags, &obj, NULL, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s failed (%d %s)",
                    path, rc, strerror(rc));
            errno = rc;
            got_size = -1;
        } else {
            /* list the xattrs */
            rc = dfs_listxattr(mfu_file->dfs, obj, list, &got_size);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_listxattr %s failed (%d %s)",
                        path, rc, strerror(rc));
                errno = rc;
                got_size = -1;
            } else if (size == 0) {
                /* we will just return got_size */
            } else if (size < got_size) {
                errno = ERANGE;
                got_size = -1;
            }

            /* Release the obj.
             * Don't log the error if we already have a different error. */
            rc = dfs_release(obj);
            if (rc && (got_size != -1)) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release failed (%d %s)",
                        rc, strerror(rc));
                errno = rc;
                got_size = -1;
            }
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return (ssize_t) got_size;
#else
    return (ssize_t) 0;
#endif
}

/* list xattrs (link dereference) */
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
    char* name     = NULL;
    char* dir_name = NULL;
    parse_filename(path, &name, &dir_name);
    assert(dir_name);

    daos_size_t got_size = (daos_size_t) size;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        got_size = -1;
    }
    else {
        /* lookup the object */
        dfs_obj_t* obj;
        int rc = dfs_lookup_rel(mfu_file->dfs, parent, name, O_RDWR, &obj, NULL, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s failed (%d %s)",
                    path, rc, strerror(rc));
            errno = rc;
            got_size = -1;
        } else {
            /* list the xattrs of the obj */
            rc = dfs_listxattr(mfu_file->dfs, obj, list, &got_size);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_listxattr %s failed (%d %s)",
                        path, rc, strerror(rc));
                errno = rc;
                got_size = -1;
            } else if (size == 0) {
                /* we will just return got_size */
            } else if (size < got_size) {
                errno = ERANGE;
                got_size = -1;
            }
        }

        /* Release the obj */
        rc = dfs_release(obj);
        if (rc && (got_size != -1)) {
            MFU_LOG(MFU_LOG_ERR, "dfs_release failed (%d %s)",
                    rc, strerror(rc));
            errno = rc;
            got_size = -1;
        }
    }

    mfu_free(&name);
    mfu_free(&dir_name);

    return (ssize_t) got_size;
#else
    return (ssize_t) 0;
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
    char* obj_name = NULL;
    char* dir_name = NULL;
    parse_filename(path, &obj_name, &dir_name);
    assert(dir_name);

    daos_size_t got_size = (daos_size_t) size;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        got_size = -1;
    }
    else {
        /* lookup and open obj_name */
        dfs_obj_t* obj;
        int lookup_flags = O_RDWR | O_NOFOLLOW;
        int rc = dfs_lookup_rel(mfu_file->dfs, parent, obj_name, lookup_flags, &obj, NULL, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s failed (%d %s)",
                    path, rc, strerror(rc));
            errno = rc;
            got_size = -1;
        } else {
            /* get the xattr */
            rc = dfs_getxattr(mfu_file->dfs, obj, name, value, &got_size);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_getxattr %s failed (%d %s)",
                        path, rc, strerror(rc));
                errno = rc;
                got_size = -1;
            } else if (size == 0) {
                /* we will just return got_size */
            } else if (size < got_size) {
                errno = ERANGE;
                got_size = -1;
            }

            /* Release the obj */
            rc = dfs_release(obj);
            if (rc && (got_size != -1)) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release failed (%d %s)",
                        rc, strerror(rc));
                errno = rc;
                got_size = -1;
            }
        }
    }

    mfu_free(&obj_name);
    mfu_free(&dir_name);

    return (ssize_t) got_size;
#else
    return (ssize_t) 0;
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
    char* obj_name = NULL;
    char* dir_name = NULL;
    parse_filename(path, &obj_name, &dir_name);
    assert(dir_name);

    daos_size_t got_size = (daos_size_t) size;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        got_size = -1;
    }
    else {
        /* lookup the object */
        dfs_obj_t* obj;
        int rc = dfs_lookup_rel(mfu_file->dfs, parent, obj_name, O_RDWR, &obj, NULL, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s failed (%d %s)",
                    path, rc, strerror(rc));
            errno = rc;
            got_size = -1;
        } else {
            /* get the xattr of the obj */
            rc = dfs_getxattr(mfu_file->dfs, obj, name, value, &got_size);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_getxattr %s failed (%d %s)",
                        path, rc, strerror(rc));
                errno = rc;
                got_size = -1;
            } else if (size == 0) {
                /* we will just return got_size */
            } else if (size < got_size) {
                errno = ERANGE;
                got_size = -1;
            }
        }

        /* Release the obj */
        rc = dfs_release(obj);
        if (rc && (got_size != -1)) {
            MFU_LOG(MFU_LOG_ERR, "dfs_release failed (%d %s)",
                    rc, strerror(rc));
            errno = rc;
            got_size = -1;
        }
    }

    mfu_free(&obj_name);
    mfu_free(&dir_name);

    return (ssize_t) got_size;
#else
    return (ssize_t) 0;
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
    char* obj_name = NULL;
    char* dir_name = NULL;
    parse_filename(path, &obj_name, &dir_name);
    assert(dir_name);

    int rc = 0;

    /* Lookup the parent directory */
    dfs_obj_t* parent = daos_hash_lookup(dir_name, mfu_file);
    if (parent == NULL) {
        rc = -1;
    }
    else {
        /* lookup and open obj_name */
        dfs_obj_t* obj;
        int lookup_flags = O_RDWR | O_NOFOLLOW;
        rc = dfs_lookup_rel(mfu_file->dfs, parent, obj_name, lookup_flags, &obj, NULL, NULL);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "dfs_lookup_rel %s failed (%d %s)",
                    path, rc, strerror(rc));
            errno = rc;
            rc = -1;
        } else {
            /* set the xattr */
            rc = dfs_setxattr(mfu_file->dfs, obj, name, value, size, flags);
            if (rc) {
                MFU_LOG(MFU_LOG_ERR, "dfs_setxattr %s failed (%d %s)",
                        path, rc, strerror(rc));
                errno = rc;
                rc = -1;
            }

            /* Release the obj */
            int rc_rel = dfs_release(obj);
            if (rc_rel && (rc != -1)) {
                MFU_LOG(MFU_LOG_ERR, "dfs_release failed (%d %s)",
                        rc_rel, strerror(rc_rel));
                errno = rc_rel;
                rc = -1;
            }
        }
    }

    mfu_free(&obj_name);
    mfu_free(&dir_name);

    return rc;
#else
    return 0;
#endif
}
