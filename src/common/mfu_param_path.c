#define _GNU_SOURCE

#include "mfu.h"
#include "mpi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>

#include <fcntl.h>
#include <unistd.h>

/* initialize fields in param */
static void mfu_param_path_init(mfu_param_path* param)
{
    /* initialize all fields */
    if (param != NULL) {
        param->orig = NULL;
        param->path = NULL;
        param->path_stat_valid = 0;
        param->target = NULL;
        param->target_stat_valid = 0;
    }

    return;
}

/* pack all fields as 64-bit values, except for times which we
 * pack as two 64-bit values */
static size_t mfu_pack_stat_size(void)
{
    size_t size = 16 * 8;
    return size;
}

static void mfu_pack_stat(char** pptr, const struct stat* s)
{
    mfu_pack_uint64(pptr, (uint64_t) s->st_dev);
    mfu_pack_uint64(pptr, (uint64_t) s->st_ino);
    mfu_pack_uint64(pptr, (uint64_t) s->st_mode);
    mfu_pack_uint64(pptr, (uint64_t) s->st_nlink);
    mfu_pack_uint64(pptr, (uint64_t) s->st_uid);
    mfu_pack_uint64(pptr, (uint64_t) s->st_gid);
    mfu_pack_uint64(pptr, (uint64_t) s->st_rdev);
    mfu_pack_uint64(pptr, (uint64_t) s->st_size);
    mfu_pack_uint64(pptr, (uint64_t) s->st_blksize);
    mfu_pack_uint64(pptr, (uint64_t) s->st_blocks);

    uint64_t secs, nsecs;
    mfu_stat_get_atimes(s, &secs, &nsecs);
    mfu_pack_uint64(pptr, secs);
    mfu_pack_uint64(pptr, nsecs);

    mfu_stat_get_mtimes(s, &secs, &nsecs);
    mfu_pack_uint64(pptr, secs);
    mfu_pack_uint64(pptr, nsecs);

    mfu_stat_get_ctimes(s, &secs, &nsecs);
    mfu_pack_uint64(pptr, secs);
    mfu_pack_uint64(pptr, nsecs);

    return;
}

static void mfu_unpack_stat(const char** pptr, struct stat* s)
{
    uint64_t val;

    mfu_unpack_uint64(pptr, &val);
    s->st_dev = (dev_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_ino = (ino_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_mode = (mode_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_nlink = (nlink_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_uid = (uid_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_gid = (gid_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_rdev = (dev_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_size = (off_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_blksize = (blksize_t) val;

    mfu_unpack_uint64(pptr, &val);
    s->st_blocks = (blkcnt_t) val;

    uint64_t secs, nsecs;

    mfu_unpack_uint64(pptr, &secs);
    mfu_unpack_uint64(pptr, &nsecs);
    mfu_stat_set_atimes(s, secs, nsecs);

    mfu_unpack_uint64(pptr, &secs);
    mfu_unpack_uint64(pptr, &nsecs);
    mfu_stat_set_mtimes(s, secs, nsecs);

    mfu_unpack_uint64(pptr, &secs);
    mfu_unpack_uint64(pptr, &nsecs);
    mfu_stat_set_ctimes(s, secs, nsecs);

    return;
}

/* given a pointer to a string, which may be the NULL pointer,
 * return number of bytes needed to pack the string */
static size_t mfu_pack_str_size(const char* str)
{
    /* uint32_t flag to indicate whether string is non-NULL */
    size_t bytes = 4;

    /* if we have a string, add in its length */
    if (str != NULL) {
        bytes += strlen(str) + 1;
    }

    return bytes;
}

/* given a pointer to a string, which may be the NULL pointer,
 * pack the string into the specified buffer and update the
 * pointer to point to next free byte */
static void mfu_pack_str(char** pptr, const char* str)
{
    /* check whether we have a string */
    if (str != NULL) {
        /* set flag to indicate that we have string */
        mfu_pack_uint32(pptr, 1);

        /* get pointer to start of buffer */
        char* ptr = *pptr;

        /* copy in string */
        size_t len = strlen(str) + 1;
        strncpy(ptr, str, len);

        /* update caller's pointer */
        *pptr += len;
    } else {
        /* set flag to indicate that we don't have str */
        mfu_pack_uint32(pptr, 0);
    }
}

/* unpack a string from a buffer, and return as newly allocated memory,
 * caller must free returned string */
static void mfu_unpack_str(const char** pptr, char** pstr)
{
    /* assume we don't have a string */
    *pstr = NULL;

    /* unpack the flag */
    uint32_t flag;
    mfu_unpack_uint32(pptr, &flag);

    /* if we have a string, unpack the string */
    if (flag) {
        /* make a copy of the string to return to caller */
        const char* str = *pptr;
        *pstr = MFU_STRDUP(str);

        /* advance caller's pointer */
        size_t len = strlen(str) + 1;
        *pptr += len;
    }

    return;
}

/* given a pointer to a mfu_param_path, compute number of bytes
 * needed to pack this into a buffer */
static size_t mfu_pack_param_size(const mfu_param_path* param)
{
    size_t bytes = 0;

    /* flag to indicate whether orig path is defined */
    bytes += mfu_pack_str_size(param->orig);

    /* flag to indicate whether reduced path is defined */
    bytes += mfu_pack_str_size(param->path);

    /* record reduced path and stat data if we have it */
    if (param->path != NULL) {
        /* flag for stat data (uint32_t) */
        bytes += 4;

        /* bytes to hold stat data for item */
        if (param->path_stat_valid) {
            bytes += mfu_pack_stat_size();
        }
    }

    /* flag to indicate whether target path exists */
    bytes += mfu_pack_str_size(param->target);

    /* record target path and stat data if we have it */
    if (param->target != NULL) {
        /* flag for stat data (uint32_t) */
        bytes += 4;

        /* bytes to hold target stat data */
        if (param->target_stat_valid) {
            bytes += mfu_pack_stat_size();
        }
    }

    return bytes;
}

/* given a pointer to a mfu_param_path, pack into the specified
 * buffer and update the pointer to point to next free byte */
static void mfu_pack_param(char** pptr, const mfu_param_path* param)
{
    /* pack the original path */
    mfu_pack_str(pptr, param->orig);

    /* pack the reduced path and stat data */
    mfu_pack_str(pptr, param->path);

    /* pack the reduced path stat data */
    if (param->path != NULL) {
        if (param->path_stat_valid) {
            /* set flag to indicate that we have stat data */
            mfu_pack_uint32(pptr, 1);
    
            /* pack stat data */
            mfu_pack_stat(pptr, &param->path_stat);
        } else {
            /* set flag to indicate that we don't have stat data */
            mfu_pack_uint32(pptr, 0);
        }
    }

    /* pack the target path and stat data */
    mfu_pack_str(pptr, param->target);

    /* pack the target stat data */
    if (param->target != NULL) {
        if (param->target_stat_valid) {
            /* set flag to indicate that we have stat data */
            mfu_pack_uint32(pptr, 1);

            /* pack stat data */
            mfu_pack_stat(pptr, &param->target_stat);
        } else {
            /* set flag to indicate that we don't have stat data */
            mfu_pack_uint32(pptr, 0);
        }
    }

    return;
}

static void mfu_unpack_param(const char** pptr, mfu_param_path* param)
{
    /* initialize all values */
    mfu_param_path_init(param);

    /* unpack original path */
    mfu_unpack_str(pptr, &param->orig);

    /* unpack reduced path */
    mfu_unpack_str(pptr, &param->path);

    /* unpack reduce path stat data */
    if (param->path != NULL) {
        uint32_t val;
        mfu_unpack_uint32(pptr, &val);
        param->path_stat_valid = (int) val;

        if (param->path_stat_valid) {
            mfu_unpack_stat(pptr, &param->path_stat);
        }
    }

    /* unpack target path */
    mfu_unpack_str(pptr, &param->target);

    /* unpack target path stat data */
    if (param->target != NULL) {
        uint32_t val;
        mfu_unpack_uint32(pptr, &val);
        param->target_stat_valid = (int) val;

        if (param->target_stat_valid) {
            mfu_unpack_stat(pptr, &param->target_stat);
        }
    }

    return;
}

/*
 * Parse an option string provided by the user to determine
 * which xattrs to copy from source to destination.
 */
attr_copy_t parse_copy_xattrs_option(char *optarg)
{
    if (strcmp(optarg,"none") == 0) {
        return XATTR_COPY_NONE;
    }

    if (strcmp(optarg,"non-lustre") == 0) {
        return XATTR_SKIP_LUSTRE;
    }

    if (strcmp(optarg,"libattr") == 0) {
        return XATTR_USE_LIBATTR;
    }

    if (strcmp(optarg,"all") == 0) {
        return XATTR_COPY_ALL;
    }

    return XATTR_COPY_INVAL;
}

/**
 * Analyze all file path inputs and place on the work queue.
 *
 * We start off with all of the following potential options in mind and prune
 * them until we figure out what situation we have.
 *
 * Libcircle only calls this function from rank 0, so there's no need to check
 * the current rank here.
 *
 * Source must overwrite destination.
 *   - Single file to single file
 *
 * Must return an error. Impossible condition.
 *   - Single directory to single file
 *   - Many file to single file
 *   - Many directory to single file
 *   - Many directory and many file to single file
 *
 * All Sources must be placed inside destination.
 *   - Single file to single directory
 *   - Single directory to single directory
 *   - Many file to single directory
 *   - Many directory to single directory
 *   - Many file and many directory to single directory
 */

/* given an item name, determine which source path this item
 * is contained within, extract directory components from source
 * path to this item and then prepend destination prefix. */
char* mfu_param_path_copy_dest(const char* name, int numpaths,
        const mfu_param_path* paths, const mfu_param_path* destpath, 
        mfu_copy_opts_t* mfu_copy_opts, mfu_file_t* mfu_src_file,
        mfu_file_t* mfu_dst_file)
{
    /* identify which source directory this came from */
    int i;
    int idx = -1;
    for (i = 0; i < numpaths; i++) {
        /* get path for step */
        const char* path = paths[i].path;

        /* get length of source path */
        size_t len = strlen(path);

        /* see if name is a child of path */
        if (strncmp(path, name, len) == 0) {
            idx = i;
            break;
        }
    }

    /* this will happen if the named item is not a child of any
     * source paths */
    if (idx == -1) {
        return NULL;
    }

    /* create path of item */
    mfu_path* item = mfu_path_from_str(name);

    /* get source directory */
    mfu_path* src = mfu_path_from_str(paths[i].path);

    /* get number of components in item */
    int item_components = mfu_path_components(item);

    /* get number of components in source path */
    int src_components = mfu_path_components(src);

    /* if copying into directory, keep last component.
     * if path is root, keep last component.
     * otherwise cut all components listed in source path */
    int cut = src_components;
    if (mfu_copy_opts->copy_into_dir && cut > 0) {
        if (strcmp(paths[i].orig, "/") == 0) {
            cut--;
        } else if ((mfu_copy_opts->do_sync != 1) &&
            (paths[i].orig[strlen(paths[i].orig) - 1] != '/')) {
            cut--;
        }
    }

    /* compute number of components to keep */
    int keep = item_components - cut;

    /* chop prefix from item */
    mfu_path_slice(item, cut, keep);

    /* prepend destination path */
    mfu_path_prepend_str(item, destpath->path);

    /* convert to a NUL-terminated string */
    char* dest = mfu_path_strdup(item);

    /* free our temporary paths */
    mfu_path_delete(&src);
    mfu_path_delete(&item);

    return dest;
}

/* check that source and destination paths are valid */
void mfu_param_path_check_copy(uint64_t num, const mfu_param_path* paths, 
        const mfu_param_path* destpath, mfu_file_t* mfu_src_file,
        mfu_file_t* mfu_dst_file,
        int no_dereference,
        int* flag_valid,
        int* flag_copy_into_dir)
{
    /* initialize output params */
    *flag_valid = 0;
    *flag_copy_into_dir = 0;

    /* need at least two paths to have a shot at being valid */
    if (num < 1 || paths == NULL || destpath == NULL) {
        return;
    }

    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* assume path parameters are valid */
    int valid = 1;

    /* just have rank 0 check */
    if(rank == 0) {
        /* DAOS-specific error checks*/
        if (mfu_src_file->type == DFS || mfu_dst_file->type == DFS) {
            if (num != 1) {
                MFU_LOG(MFU_LOG_ERR, "Only one source can be specified when using DAOS");
                valid = 0;
                goto bcast;
            }
        }
        
        /* count number of readable source paths */
        uint64_t i;
        int num_readable = 0;
        for(i = 0; i < num; i++) {
            const char* path = paths[i].path;
            int access_rc;
            if (no_dereference) {
                /* Don't dereference symlinks in the access call */
                access_rc = mfu_file_faccessat(AT_FDCWD, path, R_OK, AT_SYMLINK_NOFOLLOW, mfu_src_file);
            } else {
                /* Do dereference symlinks in the access call */
                access_rc = mfu_file_access(path, R_OK, mfu_src_file);
            }
            if (access_rc == 0) {
                num_readable++;
            } else {
                /* found a source path that we can't read, not fatal,
                 * but print an error to notify user */
                const char* orig = paths[i].orig;
                MFU_LOG(MFU_LOG_ERR, "Could not read `%s' (errno=%d %s)",
                    orig, errno, strerror(errno));
            }
        }

        /* verify that we have at least one source path */
        if(num_readable < 1) {
            MFU_LOG(MFU_LOG_ERR, "At least one valid source must be specified");
            valid = 0;
            goto bcast;
        }

        /*
         * First we need to determine if the last argument is a file or a directory.
         * We first attempt to see if the last argument already exists on disk. If it
         * doesn't, we then look at the sources to see if we can determine what the
         * last argument should be.
         */

        bool dest_exists = false;
        bool dest_is_dir = false;
        bool dest_is_file = false;
        bool dest_is_link_to_dir = false;
        bool dest_is_link_to_file = false;
        bool dest_required_to_be_dir = false;

        /* check whether dest exists, its type, and whether it's writable */
        if(destpath->path_stat_valid) {
            /* we could stat dest path, so something is there */
            dest_exists = true;

            /* now determine its type */
            if(S_ISDIR(destpath->path_stat.st_mode)) {
                /* dest is a directory */
                dest_is_dir  = true;
            }
            else if(S_ISREG(destpath->path_stat.st_mode)) {
                /* dest is a file */
                dest_is_file = true;
            }
            else if(S_ISLNK(destpath->path_stat.st_mode)) {
                /* dest is a symlink, but to what? */
                if (destpath->target_stat_valid) {
                    /* target of the symlink exists, determine what it is */
                    if(S_ISDIR(destpath->target_stat.st_mode)) {
                        /* dest is link to a directory */
                        dest_is_link_to_dir = true;
                    }
                    else if(S_ISREG(destpath->target_stat.st_mode)) {
                        /* dest is link to a file */
                        dest_is_link_to_file = true;
                    }
                    else {
                        /* unsupported type */
                        MFU_LOG(MFU_LOG_ERR, "Unsupported filetype `%s' --> `%s'",
                            destpath->orig, destpath->target);
                        valid = 0;
                        goto bcast;
                    }
                }
                else {
                    /* dest is a link, but its target does not exist,
                     * consider this an error */
                    MFU_LOG(MFU_LOG_ERR, "Destination is broken symlink `%s'",
                        destpath->orig);
                    valid = 0;
                    goto bcast;
                }
            }
            else {
                /* unsupported type */
                MFU_LOG(MFU_LOG_ERR, "Unsupported filetype `%s'",
                    destpath->orig);
                valid = 0;
                goto bcast;
            }

            /* check that dest is writable */
            if(mfu_file_access(destpath->path, W_OK, mfu_dst_file) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Destination is not writable `%s' (errno=%d %s)",
                    destpath->path, errno, strerror(errno));
                valid = 0;
                goto bcast;
            }
        }
        else {
            /* destination does not exist, so we'll be creating it,
             * check that its parent is writable */

            /* compute parent path */
            mfu_path* parent = mfu_path_from_str(destpath->path);
            mfu_path_dirname(parent);
            char* parent_str = mfu_path_strdup(parent);
            mfu_path_delete(&parent);

            /* check that parent is writable */
            if(mfu_file_access(parent_str, W_OK, mfu_dst_file) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Destination parent directory is not writable `%s' (errno=%d %s)",
                    parent_str, errno, strerror(errno));
                valid = 0;
                mfu_free(&parent_str);
                goto bcast;
            }
            mfu_free(&parent_str);
        }

        /* determine whether caller *requires* copy into dir */

        /* TODO: if caller specifies dest/ or dest/. */

        /* if caller specifies more than one source,
         * then dest has to be a directory */
        if(num > 1) {
            dest_required_to_be_dir = true;
        }

        /* if caller requires dest to be a directory, and if dest does not
         * exist or it does it exist but it's not a directory, then abort */
        if(dest_required_to_be_dir &&
           (!dest_exists || (!dest_is_dir && !dest_is_link_to_dir)))
        {
            MFU_LOG(MFU_LOG_ERR, "Destination is not a directory `%s'",
                destpath->orig);
            valid = 0;
            goto bcast;
        }

        /* we copy into a directory if any of the following:
         *   1) user specified more than one source
         *   2) destination already exists and is a directory
         *   3) destination already exists and is a link to a directory */
        bool copy_into_dir = (dest_required_to_be_dir || dest_is_dir || dest_is_link_to_dir);
        *flag_copy_into_dir = copy_into_dir ? 1 : 0;
    }

bcast:
    /* get status from rank 0 */
    MPI_Bcast(&valid, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* set valid flag */
    *flag_valid = valid;

    /* rank 0 broadcasts whether we're copying into a directory */
    MPI_Bcast(flag_copy_into_dir, 1, MPI_INT, 0, MPI_COMM_WORLD);

    return;
}

/* free resources allocated in list of params */
static void mfu_param_path_free_list(uint64_t num, mfu_param_path* params)
{
    /* make sure we got a valid pointer */
    if (params != NULL) {
        /* free memory for each param */
        uint64_t i;
        for (i = 0; i < num; i++) {
            /* get pointer to param structure */
            mfu_param_path* param = &params[i];

            /* free memory and reinit */
            if (param != NULL) {
                /* free all mememory */
                mfu_free(&param->orig);
                mfu_free(&param->path);
                mfu_free(&param->target);

                /* initialize all fields */
                mfu_param_path_init(param);
            }
        }
    }
    return;
}

void mfu_param_path_set_all(
    uint64_t num,
    const char** paths,
    mfu_param_path* params,
    mfu_file_t* mfu_file,
    bool warn)
{
    /* get our rank and number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* determine number we should look up */
    uint64_t start, count;
    mfu_get_start_count(rank, ranks, num, &start, &count);

    /* TODO: allocate temporary params */
    mfu_param_path* p = MFU_MALLOC(count * sizeof(mfu_param_path));

    /* track maximum path length */
    uint64_t bytes = 0;

    /* process each path we're responsible for */
    uint64_t i;
    for (i = 0; i < count; i++) {
        /* get pointer to param structure */
        mfu_param_path* param = &p[i];

        /* initialize all fields */
        mfu_param_path_init(param);

        /* lookup the path */
        uint64_t path_idx = start + i;
        const char* path = paths[path_idx];
        
        /* set param fields for path */
        if (path != NULL) {
            /* make a copy of original path */
            param->orig = MFU_STRDUP(path);

            /* get absolute path and remove ".", "..", consecutive "/",
             * and trailing "/" characters */
            param->path = mfu_path_strdup_abs_reduce_str(path);

            /* get stat info for simplified path */
            if (mfu_file_lstat(param->path, &param->path_stat, mfu_file) == 0) {
                param->path_stat_valid = 1;
            }

            /* TODO: we use realpath below, which is nice since it takes out
             * ".", "..", symlinks, and adds the absolute path, however, it
             * fails if the file/directory does not already exist, which is
             * often the case for dest path. */

            /* resolve any symlinks */
            char target[PATH_MAX];
            if (mfu_file_realpath(path, target, mfu_file) != NULL) {
                /* make a copy of resolved name */
                param->target = MFU_STRDUP(target);

                /* get stat info for resolved path */
                if (mfu_file_lstat(param->target, &param->target_stat, mfu_file) == 0) {
                    param->target_stat_valid = 1;
                }
            }

            /* add in bytes needed to pack this param */
            bytes += (uint64_t) mfu_pack_param_size(param);
        }
    }

    /* TODO: eventually it would be nice to leave this data distributed,
     * however for now some tools expect all params to be defined */

    /* allgather to get bytes on each process */
    int* recvcounts = (int*) MFU_MALLOC(ranks * sizeof(int));
    int* recvdispls = (int*) MFU_MALLOC(ranks * sizeof(int));
    int sendcount = (int) bytes;
    MPI_Allgather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MPI_COMM_WORLD);

    /* compute displacements and total number of bytes that we'll receive */
    uint64_t allbytes = 0;
    int disp = 0;
    for (i = 0; i < (uint64_t) ranks; i++) {
        recvdispls[i] = disp;
        disp += recvcounts[i];
        allbytes += (uint64_t) recvcounts[i];
    }

    /* allocate memory for send and recv buffers */
    char* sendbuf = MFU_MALLOC(bytes);
    char* recvbuf = MFU_MALLOC(allbytes);

    /* pack send buffer */
    char* ptr = sendbuf;
    for (i = 0; i < count; i++) {
        mfu_pack_param(&ptr, &p[i]);
    }
    
    /* allgatherv to collect data */
    MPI_Allgatherv(sendbuf, sendcount, MPI_BYTE, recvbuf, recvcounts, recvdispls, MPI_BYTE, MPI_COMM_WORLD);

    /* unpack recv buffer into caller's params */
    ptr = recvbuf;
    for (i = 0; i < num; i++) {
        mfu_unpack_param((const char**)(&ptr), &params[i]);
    }

    /* Loop through the list of files &/or directories, and check the params 
     * struct to see if all of them are valid file names. If one is not, let 
     * the user know by printing a warning */
    if (rank == 0 && warn) {
        for (i = 0; i < num; i++) {
            /* get pointer to param structure */
            mfu_param_path* param = &params[i];
            if (param->path_stat_valid == 0) {
                /* failed to find a file at this location, let user know (may be a typo) */
                MFU_LOG(MFU_LOG_WARN, "Warning: `%s' does not exist", param->orig); 
            }
        }
    }

    /* free message buffers */
    mfu_free(&recvbuf);
    mfu_free(&sendbuf);

    /* free arrays for recv counts and displacements */
    mfu_free(&recvdispls);
    mfu_free(&recvcounts);

    /* free temporary params */
    mfu_param_path_free_list(count, p);
    mfu_free(&p);

    return;
}

/* free resources allocated in call to mfu_param_path_set_all */
void mfu_param_path_free_all(uint64_t num, mfu_param_path* params)
{
    mfu_param_path_free_list(num, params);
    return;
}

/* set fields in param according to path */
void mfu_param_path_set(const char* path, mfu_param_path* param, mfu_file_t* mfu_file, bool warn)
{
    mfu_param_path_set_all(1, &path, param, mfu_file, warn);
    return;
}

/* free memory associated with param */
void mfu_param_path_free(mfu_param_path* param)
{
    mfu_param_path_free_all(1, param);
    return;
}
