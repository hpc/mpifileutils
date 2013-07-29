/*
 * This file contains the logic to walk all of the source objects and place
 * them on the queue.
 *
 * In the case of directories, we'll simply read the contents of a directory
 * and place each sub-object back on the queue to be treewalked again. In the
 * case of files, we chunk up each file and place it on the queue as another
 * libcircle action to be later processed by the COPY and CLEANUP stages.
 *
 * See the file "COPYING" for the full license governing this code.
 */

#include "treewalk.h"
#include "dcp.h"

#include <dirent.h>
#include <errno.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/time.h>

/** Options specified by the user. */
extern DCOPY_options_t DCOPY_user_opts;

/* given path, return level within directory tree */
static int compute_depth(const char* path)
{
    /* TODO: ignore trailing '/' */

    char* c;
    int depth = 0;
    for (c = path; *c != NULL; c++) {
        if (*c == '/') {
            depth++;
        }
    }
    return depth;
}

/**
 * This is the entry point for the "file stat stage". This function is called
 * from the jump table required for the main libcircle callbacks.
 */
void DCOPY_do_treewalk(DCOPY_operation_t* op, \
                       CIRCLE_handle* handle)
{
    struct stat64 statbuf;

    if(lstat64(op->operand, &statbuf) < 0) {
        LOG(DCOPY_LOG_DBG, "Could not get info for `%s'. errno=%d %s", op->operand, errno, strerror(errno));
        DCOPY_retry_failed_operation(TREEWALK, handle, op);
        return;
    }

    /* first check that we handle this file type */
    if(! S_ISDIR(statbuf.st_mode) &&
       ! S_ISREG(statbuf.st_mode) &&
       ! S_ISLNK(statbuf.st_mode))
    {
        if (S_ISCHR(statbuf.st_mode)) {
          LOG(DCOPY_LOG_ERR, "Encountered an unsupported file type S_ISCHR at `%s'.", op->operand);
        } else if (S_ISBLK(statbuf.st_mode)) {
          LOG(DCOPY_LOG_ERR, "Encountered an unsupported file type S_ISBLK at `%s'.", op->operand);
        } else if (S_ISFIFO(statbuf.st_mode)) {
          LOG(DCOPY_LOG_ERR, "Encountered an unsupported file type S_ISFIFO at `%s'.", op->operand);
        } else if (S_ISSOCK(statbuf.st_mode)) {
          LOG(DCOPY_LOG_ERR, "Encountered an unsupported file type S_ISSOCK at `%s'.", op->operand);
        } else {
          LOG(DCOPY_LOG_ERR, "Encountered an unsupported file type %x at `%s'.", statbuf.st_mode, op->operand);
        }
        return;
    }

    /* create new element to record file path and stat info */
    DCOPY_stat_elem_t* elem = (DCOPY_stat_elem_t*) malloc(sizeof(DCOPY_stat_elem_t));
    elem->file = strdup(op->dest_full_path);
    elem->sb = (struct stat64*) malloc(sizeof(struct stat64));
    elem->depth = compute_depth(op->dest_full_path);
    memcpy(elem->sb, &statbuf, sizeof(struct stat64));
    elem->next = NULL;

    /* append element to tail of linked list */
    if (DCOPY_list_head == NULL) {
        DCOPY_list_head = elem;
    }
    if (DCOPY_list_tail != NULL) {
        DCOPY_list_tail->next = elem;
    }
    DCOPY_list_tail = elem;

    if(S_ISDIR(statbuf.st_mode)) {
        /* LOG(DCOPY_LOG_DBG, "Stat operation found a directory at `%s'.", op->operand); */
        DCOPY_stat_process_dir(op, &statbuf, handle);
    }
    else if(S_ISREG(statbuf.st_mode)) {
        /* LOG(DCOPY_LOG_DBG, "Stat operation found a file at `%s'.", op->operand); */
        DCOPY_stat_process_file(op, &statbuf, handle);
    }
    else if(S_ISLNK(statbuf.st_mode)) {
        /* LOG(DCOPY_LOG_DBG, "Stat operation found a link at `%s'.", op->operand); */
        DCOPY_stat_process_link(op, &statbuf, handle);
    }
    else {
        LOG(DCOPY_LOG_ERR, "Encountered an unsupported file type %x at `%s'.", statbuf.st_mode, op->operand);
        DCOPY_retry_failed_operation(TREEWALK, handle, op);
        return;
    }
}

/**
 * This function copies a link.
 */
void DCOPY_stat_process_link(DCOPY_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle)
{
    const char* src_path  = op->operand;
    const char* dest_path = op->dest_full_path;

    /* read link and terminate string with NUL character */
    char path[PATH_MAX + 1];
    ssize_t rc = readlink(src_path, path, sizeof(path) - 1);

    if(rc < 0) {
        LOG(DCOPY_LOG_ERR, "Failed to read link `%s' readlink() errno=%d %s",
            src_path, errno, strerror(errno)
           );
        return;
    }

    path[rc] = '\0';

    /* create new link */
    int symrc = symlink(path, dest_path);

    if(symrc < 0) {
        LOG(DCOPY_LOG_ERR, "Failed to create link `%s' symlink() errno=%d %s",
            dest_path, errno, strerror(errno)
           );
        return;
    }

    /* set permissions on link */
    if (DCOPY_user_opts.preserve) {
        DCOPY_copy_xattrs(op, statbuf, dest_path);
        DCOPY_copy_ownership(statbuf, dest_path);
        DCOPY_copy_permissions(statbuf, dest_path);
    }

    return;
}

/**
 * This function inputs a file and creates chunk operations that get placed
 * onto the libcircle queue for future processing by the copy stage.
 */
void DCOPY_stat_process_file(DCOPY_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle)
{
    int64_t file_size = statbuf->st_size;
    int64_t chunk_index;
    int64_t num_chunks = file_size / DCOPY_CHUNK_SIZE;

    LOG(DCOPY_LOG_DBG, "File `%s' size is `%" PRId64 \
        "' with chunks `%" PRId64 "' (total `%" PRId64 "').", \
        op->operand, file_size, num_chunks, \
        num_chunks * DCOPY_CHUNK_SIZE);

    const char* dest_path = op->dest_full_path;

    /* since file systems like Lustre require xattrs to be set before file is opened,
     * we first create it with mknod and then set xattrs */

    /* create file with mknod
    * for regular files, dev argument is supposed to be ignored,
    * see makedev() to create valid dev */
    dev_t dev;
    memset(&dev, 0, sizeof(dev_t));
    int mknod_rc = mknod(dest_path, DCOPY_DEF_PERMS_FILE | S_IFREG, dev);

    if(mknod_rc < 0) {
        if(errno == EEXIST) {
            /* TODO: should we unlink and mknod again in this case? */
        }

        LOG(DCOPY_LOG_DBG, "File `%s' mknod() errno=%d %s",
            dest_path, errno, strerror(errno)
           );
    }

    /* copy extended attributes, important to do this first before
     * writing data because some attributes tell file system how to
     * stripe data, e.g., Lustre */
    if (DCOPY_user_opts.preserve) {
        DCOPY_copy_xattrs(op, statbuf, dest_path);
    }

    /* Encode and enqueue each chunk of the file for processing later. */
    for(chunk_index = 0; chunk_index < num_chunks; chunk_index++) {
        char* newop = DCOPY_encode_operation(COPY, chunk_index, op->operand, \
                                             op->source_base_offset, \
                                             op->dest_base_appendix, file_size);
        handle->enqueue(newop);
        free(newop);
    }

    /* Encode and enqueue the last partial chunk. */
    if((num_chunks * DCOPY_CHUNK_SIZE) < file_size || num_chunks == 0) {
        char* newop = DCOPY_encode_operation(COPY, chunk_index, op->operand, \
                                             op->source_base_offset, \
                                             op->dest_base_appendix, file_size);
        handle->enqueue(newop);
        free(newop);
    }
}

/**
 * This function reads the contents of a directory and generates appropriate
 * libcircle operations for every object in the directory. It then places those
 * operations on the libcircle queue and returns.
 */
void DCOPY_stat_process_dir(DCOPY_operation_t* op,
                            const struct stat64* statbuf,
                            CIRCLE_handle* handle)
{
    DIR* curr_dir;
    char* curr_dir_name;
    char* newop;

    struct dirent* curr_ent;
    char newop_path[PATH_MAX];

    const char* dest_path = op->dest_full_path;

    /* first, create the destination directory */
    LOG(DCOPY_LOG_DBG, "Creating directory: %s", dest_path);
    int rc = mkdir(dest_path, DCOPY_DEF_PERMS_DIR);
    if(rc != 0) {
        LOG(DCOPY_LOG_ERR, "Failed to create directory: %s (errno=%d %s)", \
            dest_path, errno, strerror(errno));
        return;
    }

    /* copy extended attributes on directory */
    if (DCOPY_user_opts.preserve) {
        DCOPY_copy_xattrs(op, statbuf, dest_path);
    }

    /* iterate through source directory and add items to queue */
    curr_dir = opendir(op->operand);

    if(curr_dir == NULL) {
        LOG(DCOPY_LOG_ERR, "Unable to open dir `%s'. errno=%d %s", \
            op->operand, errno, strerror(errno));

        DCOPY_retry_failed_operation(TREEWALK, handle, op);
        return;
    }
    else {
        while((curr_ent = readdir(curr_dir)) != NULL) {
            curr_dir_name = curr_ent->d_name;

            /* We don't care about . or .. */
            if((strncmp(curr_dir_name, ".", 2)) && (strncmp(curr_dir_name, "..", 3))) {

                /* build new object name */
                sprintf(newop_path, "%s/%s", op->operand, curr_dir_name);

                LOG(DCOPY_LOG_DBG, "Stat operation is enqueueing `%s'", newop_path);

                /* Distributed recursion here. */
                newop = DCOPY_encode_operation(TREEWALK, 0, newop_path, \
                                               op->source_base_offset, op->dest_base_appendix, op->file_size);
                handle->enqueue(newop);

                free(newop);
            }
        }
    }

    closedir(curr_dir);
    return;
}

/* EOF */
