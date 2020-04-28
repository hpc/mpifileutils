/* See the file "COPYING" for the full license governing this code. */

#include "common.h"
#include "handle_args.h"

#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>

/** Where we should keep statistics related to this file copy. */
DCOPY_statistics_t DCOPY_statistics;

/** Where we should store options specified by the user. */
DCOPY_options_t DCOPY_user_opts;

/** Cache most recent open file descriptor to avoid opening / closing the same file */
DCOPY_file_cache_t DCOPY_src_cache;
DCOPY_file_cache_t DCOPY_dst_cache;

/** What rank the current process is. */
int DCOPY_global_rank;

/** Chunksize */
size_t DCOPY_chunksize;
size_t DCOPY_blocksize;

/* variables to track linked list */
DCOPY_stat_elem_t* DCOPY_list_head = NULL;
DCOPY_stat_elem_t* DCOPY_list_tail = NULL;

/** A table of function pointers used for core operation. */
void (*DCOPY_jump_table[5])(DCOPY_operation_t* op, CIRCLE_handle* handle);

void DCOPY_retry_failed_operation(DCOPY_operation_code_t target, \
                                  CIRCLE_handle* handle, \
                                  DCOPY_operation_t* op)
{
    char* new_op;

    if(DCOPY_user_opts.reliable_filesystem) {
        MFU_LOG(MFU_LOG_ERR, "Not retrying failed operation. " \
            "Reliable filesystem is specified. (op=%d chunk=%ld src=%s dst=%s)",
            target, op->chunk, op->operand, op->dest_full_path);

        DCOPY_abort(EXIT_FAILURE);
    }
    else {
        MFU_LOG(MFU_LOG_INFO, "Attempting to retry operation.");

        new_op = DCOPY_encode_operation(target, op->chunk, op->operand, \
                                        op->source_base_offset, \
                                        op->dest_base_appendix, op->file_size);

        handle->enqueue(new_op);
        mfu_free(&new_op);
    }

    return;
}

/**
 * Encode an operation code for use on the distributed queue structure.
 */
char* DCOPY_encode_operation(DCOPY_operation_code_t code, \
                             int64_t chunk, \
                             char* operand, \
                             uint16_t source_base_offset, \
                             char* dest_base_appendix, \
                             int64_t file_size)
{
    /*
     * FIXME: This requires architecture changes in libcircle -- a redesign of
     * internal queue data structures to allow void* types as queue elements
     * instead of null terminated strings. Ignoring this problem by commenting
     * out this check will likely cause silent data corruption.
     */

    /* allocate memory to encode op */
    char* op = (char*) MFU_MALLOC(CIRCLE_MAX_STRING_LEN);

    /* set pointer to next byte to write to and record number of bytes left */
    char* ptr = op;
    size_t remaining = CIRCLE_MAX_STRING_LEN;

    /* encode operation and get number of bytes required to do so */
    size_t len = strlen(operand);
    int written = snprintf(ptr, remaining, "%" PRId64 ":%" PRId64 ":%" PRIu16 ":%d:%d:%s", \
                       file_size, chunk, source_base_offset, code, (int)len, operand);

    /* snprintf returns number of bytes written excluding terminating NUL,
     * so if we're equal, we'd write one byte too many */
    if((size_t)written >= remaining) {
        MFU_LOG(MFU_LOG_ERR, \
            "Exceeded libcircle message size due to large file path. " \
            "This is a known bug in dcp that we intend to fix. Sorry!");
        DCOPY_abort(EXIT_FAILURE);
    }

    /* update pointer and number of bytes remaining,
     * note that we don't include the terminating NUL in this case */
    ptr += written;
    remaining -= (size_t) written;

    /* tack on destination base appendix if we have one */
    if(dest_base_appendix) {
        len = strlen(dest_base_appendix);
        written = snprintf(ptr, remaining, ":%d:%s", (int)len, dest_base_appendix);

        /* snprintf returns number of bytes written excluding terminating NUL,
         * so if we're equal, we'd write one byte too many */
        if((size_t)written >= remaining) {
            MFU_LOG(MFU_LOG_ERR, \
                "Exceeded libcircle message size due to large file path. " \
                "This is a known bug in dcp that we intend to fix. Sorry!");
            DCOPY_abort(EXIT_FAILURE);
        }

        /* update pointer and number of bytes remaining,
         * note that we don't include the terminating NUL in this case */
        ptr += written;
        remaining -= (size_t) written;
    }

    return op;
}

/**
 * Decode the operation code from a message on the distributed queue structure.
 */
DCOPY_operation_t* DCOPY_decode_operation(char* op)
{
    DCOPY_operation_t* ret = (DCOPY_operation_t*) malloc(sizeof(DCOPY_operation_t));

    if(sscanf(strtok(op, ":"), "%" SCNd64, &(ret->file_size)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode file size attribute");
        DCOPY_abort(EXIT_FAILURE);
    }

    if(sscanf(strtok(NULL, ":"), "%" SCNd64, &(ret->chunk)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode chunk index attribute");
        DCOPY_abort(EXIT_FAILURE);
    }

    if(sscanf(strtok(NULL, ":"), "%" SCNu16, &(ret->source_base_offset)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode source base offset attribute");
        DCOPY_abort(EXIT_FAILURE);
    }

    if(sscanf(strtok(NULL, ":"), "%d", (int*) &(ret->code)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode stage code attribute");
        DCOPY_abort(EXIT_FAILURE);
    }

    /* get number of characters in operand string */
    int op_len;
    char* str = strtok(NULL, ":");
    if(sscanf(str, "%d", &op_len) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode operand string length");
        DCOPY_abort(EXIT_FAILURE);
    }

    /* skip over digits and trailing ':' to get pointer to operand */
    char* operand = str + strlen(str) + 1;
    ret->operand = operand;

    /* if operand ends with ':', then the dest_base_appendix is next */
    int dest_base_exists = 0;
    if(operand[op_len] == ':') {
        dest_base_exists = 1;
    }

    /* NUL-terminate the operand string */
    operand[op_len] = '\0';

    ret->dest_base_appendix = NULL;
    if(dest_base_exists) {
        /* get pointer to first character of dest_base_len */
        str = operand + op_len + 1;

        /* tokenize length and scan it in */
        int dest_len;
        str = strtok(str, ":");
        if(sscanf(str, "%d", &dest_len) != 1) {
            MFU_LOG(MFU_LOG_ERR, "Could not decode destination base appendix string length");
            DCOPY_abort(EXIT_FAILURE);
        }

        /* skip over digits and trailing ':' to get pointer to
         * destination base, and NUL-terminate the string */
        char* base = str + strlen(str) + 1;
        ret->dest_base_appendix = base;
        base[dest_len] = '\0';
    }

    /* get pointer to first character past source base path, if one exists */
    const char* last_component = NULL;
    if(ret->source_base_offset < op_len) {
        last_component = ret->operand + ret->source_base_offset + 1;
    }

    /* build destination object name */
    int written;
    char dest_path_recursive[PATH_MAX];
    if(ret->dest_base_appendix == NULL) {
        if (last_component == NULL) {
            written = snprintf(dest_path_recursive, sizeof(dest_path_recursive),
                "%s", DCOPY_user_opts.dest_path);
        } else {
            written = snprintf(dest_path_recursive, sizeof(dest_path_recursive),
                "%s/%s", DCOPY_user_opts.dest_path, last_component);
        }
    }
    else {
        if (last_component == NULL) {
            written = snprintf(dest_path_recursive, sizeof(dest_path_recursive),
                "%s/%s", DCOPY_user_opts.dest_path, ret->dest_base_appendix);
        } else {
            written = snprintf(dest_path_recursive, sizeof(dest_path_recursive),
                "%s/%s/%s", DCOPY_user_opts.dest_path, ret->dest_base_appendix, last_component);
        }
    }

    /* fail if we would have overwritten the buffer */
    if((size_t)written >= sizeof(dest_path_recursive)) {
        MFU_LOG(MFU_LOG_ERR, "Destination path buffer too small");
        DCOPY_abort(EXIT_FAILURE);
    }

    /* record destination path in operation descriptor */
    ret->dest_full_path = MFU_STRDUP(dest_path_recursive);
    if(ret->dest_full_path == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to allocate full destination path");
        DCOPY_abort(EXIT_FAILURE);
    }

    return ret;
}

/* given the address of a pointer to an operation_t struct,
 * free associated memory and set pointer to NULL */
void DCOPY_opt_free(DCOPY_operation_t** optptr)
{
    if(optptr != NULL) {
        /* get pointer to operation_t struct */
        DCOPY_operation_t* opt = (*optptr);

        if(opt != NULL) {
            /* free memory and then the object itself */
            mfu_free(&opt->dest_full_path);
            mfu_free(&opt);
        }

        /* set caller's pointer to NULL to catch bugs */
        *optptr = NULL;
    }

    return;
}

int DCOPY_open_file(const char* file, int read_flag, DCOPY_file_cache_t* cache)
{
    int newfd = -1;

    /* see if we have a cached file descriptor */
    char* name = cache->name;
    if (name != NULL) {
        /* we have a cached file descriptor */
        int fd = cache->fd;
        if (strcmp(name, file) == 0 && cache->read == read_flag) {
            /* the file we're trying to open matches name and read/write mode,
             * so just return the cached descriptor */
            return fd;
        } else {
            /* the file we're trying to open is different,
             * close the old file and delete the name */
            mfu_close(name, fd);
            mfu_free(&cache->name);
        }
    }

    /* open the new file */
    if (read_flag) {
        int flags = O_RDONLY;
        if (DCOPY_user_opts.synchronous) {
            flags |= O_DIRECT;
        }
        newfd = mfu_open(file, flags);
    } else {
        int flags = O_WRONLY | O_CREAT;
        if (DCOPY_user_opts.synchronous) {
            flags |= O_DIRECT;
        }
        newfd = mfu_open(file, flags, DCOPY_DEF_PERMS_FILE);
    }

    /* cache the file descriptor */
    if (newfd != -1) {
        cache->name = MFU_STRDUP(file);
        cache->fd   = newfd;
        cache->read = read_flag;
    }

    return newfd;
}

int DCOPY_close_file(DCOPY_file_cache_t* cache)
{
    int rc = 0;

    /* close file if we have one */
    char* name = cache->name;
    if (name != NULL) {
        /* TODO: if open for write, fsync? */
        int fd = cache->fd;
        rc = mfu_close(name, fd);
        mfu_free(&cache->name);
    }

    return rc;
}

/* copy all extended attributes from op->operand to dest_path */
void DCOPY_copy_xattrs(
    DCOPY_operation_t* op,
    const struct stat64* statbuf,
    const char* dest_path)
{
#if DCOPY_USE_XATTRS
    /* get source file name */
    char* src_path = op->operand;

    /* start with a reasonable buffer, we'll allocate more as needed */
    size_t list_bufsize = 1204;
    char* list = (char*) MFU_MALLOC(list_bufsize);

    /* get list, if list_size == ERANGE, try again */
    ssize_t list_size;
    int got_list = 0;

    /* get current estimate for list size */
    while(! got_list) {
        list_size = llistxattr(src_path, list, list_bufsize);

        if(list_size < 0) {
            if(errno == ERANGE) {
                /* buffer is too small, free our current buffer
                 * and call it again with size==0 to get new size */
                mfu_free(&list);
                list_bufsize = 0;
            }
            else if(errno == ENOTSUP) {
                /* this is common enough that we silently ignore it */
                break;
            }
            else {
                /* this is a real error */
                MFU_LOG(MFU_LOG_ERR, "Failed to get list of extended attributes on %s llistxattr() errno=%d %s",
                    src_path, errno, strerror(errno)
                   );
                break;
            }
        }
        else {
            if(list_size > 0 && list_bufsize == 0) {
                /* called llistxattr with size==0 and got back positive
                 * number indicating size of buffer we need to allocate */
                list_bufsize = (size_t) list_size;
                list = (char*) MFU_MALLOC(list_bufsize);
            }
            else {
                /* got our list, it's size is in list_size, which may be 0 */
                got_list = 1;
            }
        }
    }

    /* iterate over list and copy values to new object lgetxattr/lsetxattr */
    if(got_list) {
        char* name = list;

        while(name < list + list_size) {
            /* start with a reasonable buffer,
             * allocate something bigger as needed */
            size_t val_bufsize = 1024;
            void* val = (void*) MFU_MALLOC(val_bufsize);

            /* lookup value for name */
            ssize_t val_size;
            int got_val = 0;

            while(! got_val) {
                val_size = lgetxattr(src_path, name, val, val_bufsize);

                if(val_size < 0) {
                    if(errno == ERANGE) {
                        /* buffer is too small, free our current buffer
                         * and call it again with size==0 to get new size */
                        mfu_free(&val);
                        val_bufsize = 0;
                    }
                    else if(errno == ENOATTR) {
                        /* source object no longer has this attribute,
                         * maybe deleted out from under us */
                        break;
                    }
                    else {
                        /* this is a real error */
                        MFU_LOG(MFU_LOG_ERR, "Failed to get value for name=%s on %s llistxattr() errno=%d %s",
                            name, src_path, errno, strerror(errno)
                           );
                        break;
                    }
                }
                else {
                    if(val_size > 0 && val_bufsize == 0) {
                        /* called lgetxattr with size==0 and got back positive
                         * number indicating size of buffer we need to allocate */
                        val_bufsize = (size_t) val_size;
                        val = (void*) MFU_MALLOC(val_bufsize);
                    }
                    else {
                        /* got our value, it's size is in val_size, which may be 0 */
                        got_val = 1;
                    }
                }
            }

            /* set attribute on destination object */
            if(got_val) {
                int setrc = lsetxattr(dest_path, name, val, (size_t) val_size, 0);

                if(setrc != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to set value for name=%s on %s llistxattr() errno=%d %s",
                        name, dest_path, errno, strerror(errno)
                       );
                }
            }

            /* free value string */
            mfu_free(&val);
            val_bufsize = 0;

            /* jump to next name */
            size_t namelen = strlen(name) + 1;
            name += namelen;
        }
    }

    /* free space allocated for list */
    mfu_free(&list);
    list_bufsize = 0;

    return;
#endif /* DCOPY_USE_XATTR */
}

void DCOPY_copy_ownership(
    const struct stat64* statbuf,
    const char* dest_path)
{
    /* note that we use lchown to change ownership of link itself, it path happens to be a link */
    if(mfu_lchown(dest_path, statbuf->st_uid, statbuf->st_gid) != 0) {
        /* TODO: are there other EPERM conditions we do want to report? */

        /* since the user running dcp may not be the owner of the
         * file, we could hit an EPERM error here, and the file
         * will be left with the effective uid and gid of the dcp
         * process, don't bother reporting an error for that case */
        if (errno != EPERM) {
            MFU_LOG(MFU_LOG_ERR, "Failed to change ownership on %s lchown() errno=%d %s",
                dest_path, errno, strerror(errno)
               );
        }
    }

    return;
}

/* TODO: condionally set setuid and setgid bits? */
void DCOPY_copy_permissions(
    const struct stat64* statbuf,
    const char* dest_path)
{
    /* change mode */
    if(! S_ISLNK(statbuf->st_mode)) {
        if(mfu_chmod(dest_path, statbuf->st_mode) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to change permissions on %s chmod() errno=%d %s",
                dest_path, errno, strerror(errno)
               );
        }
    }

    return;
}

void DCOPY_copy_timestamps(
    const struct stat64* statbuf,
    const char* dest_path)
{
    /* read atime, mtime, and ctime second values from stat */
    uint64_t atime_sec = (uint64_t) statbuf->st_atime;
    uint64_t mtime_sec = (uint64_t) statbuf->st_mtime;
    uint64_t ctime_sec = (uint64_t) statbuf->st_ctime;

    /* now read nanoseconds if we can */
    uint64_t atime_nsec, mtime_nsec, ctime_nsec;
#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    atime_nsec = (uint64_t) statbuf->st_atimespec.tv_nsec;
    ctime_nsec = (uint64_t) statbuf->st_ctimespec.tv_nsec;
    mtime_nsec = (uint64_t) statbuf->st_mtimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    atime_nsec = (uint64_t) statbuf->st_atim.tv_nsec;
    ctime_nsec = (uint64_t) statbuf->st_ctim.tv_nsec;
    mtime_nsec = (uint64_t) statbuf->st_mtim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    atime_nsec = (uint64_t) statbuf->st_atime_n;
    ctime_nsec = (uint64_t) statbuf->st_ctime_n;
    mtime_nsec = (uint64_t) statbuf->st_mtime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    atime_nsec = (uint64_t) statbuf->st_uatime * 1000;
    ctime_nsec = (uint64_t) statbuf->st_uctime * 1000;
    mtime_nsec = (uint64_t) statbuf->st_umtime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    atime_nsec = (uint64_t) statbuf->st_atime_usec * 1000;
    ctime_nsec = (uint64_t) statbuf->st_ctime_usec * 1000;
    mtime_nsec = (uint64_t) statbuf->st_mtime_usec * 1000;
#else
    atime_nsec = 0;
    ctime_nsec = 0;
    mtime_nsec = 0;
#endif

    /* fill in time structures */
    struct timespec times[2];
    times[0].tv_sec  = (time_t) atime_sec;
    times[0].tv_nsec = (long)   atime_nsec;
    times[1].tv_sec  = (time_t) mtime_sec;
    times[1].tv_nsec = (long)   mtime_nsec;

    /* set times with nanosecond precision using utimensat,
     * assume path is relative to current working directory,
     * if it's not absolute, and set times on link (not target file)
     * if dest_path refers to a link */
    if(utimensat(AT_FDCWD, dest_path, times, AT_SYMLINK_NOFOLLOW) != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to change timestamps on %s utime() errno=%d %s",
            dest_path, errno, strerror(errno)
           );
    }

#if 0
    /* TODO: see stat-time.h and get_stat_atime/mtime/ctime to read sub-second times,
     * and use utimensat to set sub-second times */
    /* as last step, change timestamps */
    if(! S_ISLNK(statbuf->st_mode)) {
        struct utimbuf times;
        times.actime  = statbuf->st_atime;
        times.modtime = statbuf->st_mtime;
        if(utime(dest_path, &times) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to change timestamps on %s utime() errno=%d %s",
                dest_path, errno, strerror(errno)
               );
        }
    }
    else {
        struct timeval tv[2];
        tv[0].tv_sec  = statbuf->st_atime;
        tv[0].tv_usec = 0;
        tv[1].tv_sec  = statbuf->st_mtime;
        tv[1].tv_usec = 0;
        if(lutimes(dest_path, tv) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to change timestamps on %s utime() errno=%d %s",
                dest_path, errno, strerror(errno)
               );
        }
    }
#endif

    return;
}

/* called by single process upon detection of a problem */
void DCOPY_abort(int code)
{
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

/* called globally by all procs to exit */
void DCOPY_exit(int code)
{
    /* CIRCLE_finalize or will this hang? */
    mfu_finalize();
    MPI_Finalize();
    exit(code);
}

/* EOF */
