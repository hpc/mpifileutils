/* See the file "COPYING" for the full license governing this code. */

/*
 * For an overview of how argument handling was originally designed in dcp,
 * please see the blog post at:
 *
 * <http://www.bringhurst.org/2012/12/16/file-copy-tool-argument-handling.html>
 */

#include "handle_args.h"
#include "treewalk.h"
#include "dcp1.h"
#include "mfu.h"

#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/** Where we should store options specified by the user. */
extern DCOPY_options_t DCOPY_user_opts;

static int num_src_params;
static mfu_param_path* src_params;
static mfu_param_path  dest_param;

/**
 * Determine if the destination path is a file or directory.
 *
 * It does this by first checking to see if an object is actually at the
 * destination path. If an object isn't already at the destination path, we
 * examine the source path(s) to determine the type of what the destination
 * path will be.
 *
 * @return true if the destination should be a directory, false otherwise.
 */

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
 *
 * @param handle the libcircle primary queue handle.
 */
void DCOPY_enqueue_work_objects(CIRCLE_handle* handle)
{
    if(DCOPY_user_opts.copy_into_dir) {
        /* copy source params into directory */
        MFU_LOG(MFU_LOG_DBG, "Infered that the destination is a directory");

        /* enqueue each source param */
        int i;
        for(i = 0; i < num_src_params; i++) {
            char* src_path = src_params[i].path;
            MFU_LOG(MFU_LOG_DBG, "Enqueueing source path `%s'", src_path);

            /* TODO: skip sources we can't read */

            /*
             * If the destination directory already exists, we want to place
             * new files inside it. To do this, we send a path fragment along
             * with the source path message and append it to the options dest
             * path whenever the options dest path is used.
             */

            /* get basename of src path. */
            mfu_path* p = mfu_path_from_str(src_path);
            mfu_path_basename(p);
            char* src_path_basename = mfu_path_strdup(p);
            mfu_path_delete(&p);

            uint16_t src_len = (uint16_t)strlen(src_path);
            char* op = DCOPY_encode_operation(TREEWALK, 0, src_path,
                                              src_len, src_path_basename, 0);
            handle->enqueue(op);
            free(op);

            mfu_free(&src_path_basename);
        }
    }
    else {
        /* to get here, there must be one source, and if dir exists,
         * is is not a directory or a link to a directory */
        MFU_LOG(MFU_LOG_DBG, "Infered that the destination is a file");

        /* TODO: if dest exists, check that it's a file or link */

        char* src_path = src_params[0].path;
        MFU_LOG(MFU_LOG_DBG, "Enqueueing single source path `%s'", src_path);

        uint16_t src_len = (uint16_t)strlen(src_path);
        char* op = DCOPY_encode_operation(TREEWALK, 0, src_path,
                                          src_len, NULL, 0);

        handle->enqueue(op);
        free(op);
    }
}

/* check that source and destination paths are valid */
static void DCOPY_check_paths(void)
{
    /* assume path parameters are valid */
    int valid = 1;

    /* just have rank 0 check */
    if(DCOPY_global_rank == 0) {
        /* count number of readable source paths */
        int i;
        int num_readable = 0;
        for(i = 0; i < num_src_params; i++) {
            char* path = src_params[i].path;
            if(mfu_access(path, R_OK) == 0) {
                num_readable++;
            }
            else {
                /* found a source path that we can't read, not fatal,
                 * but print an error to notify user */
                char* orig = src_params[i].orig;
                MFU_LOG(MFU_LOG_ERR, "Could not read `%s' errno=%d %s",
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
        if(dest_param.path_stat_valid) {
            /* we could stat dest path, so something is there */
            dest_exists = true;

            /* now determine its type */
            if(S_ISDIR(dest_param.path_stat.st_mode)) {
                /* dest is a directory */
                dest_is_dir  = true;
            }
            else if(S_ISREG(dest_param.path_stat.st_mode)) {
                /* dest is a file */
                dest_is_file = true;
            }
            else if(S_ISLNK(dest_param.path_stat.st_mode)) {
                /* dest is a symlink, but to what? */
                if (dest_param.target_stat_valid) {
                    /* target of the symlink exists, determine what it is */
                    if(S_ISDIR(dest_param.target_stat.st_mode)) {
                        /* dest is link to a directory */
                        dest_is_link_to_dir = true;
                    }
                    else if(S_ISREG(dest_param.target_stat.st_mode)) {
                        /* dest is link to a file */
                        dest_is_link_to_file = true;
                    }
                    else {
                        /* unsupported type */
                        MFU_LOG(MFU_LOG_ERR, "Unsupported filetype `%s' --> `%s'",
                            dest_param.orig, dest_param.target);
                        valid = 0;
                        goto bcast;
                    }
                }
                else {
                    /* dest is a link, but its target does not exist,
                     * consider this an error */
                    MFU_LOG(MFU_LOG_ERR, "Destination is broken symlink `%s'",
                        dest_param.orig);
                    valid = 0;
                    goto bcast;
                }
            }
            else {
                /* unsupported type */
                MFU_LOG(MFU_LOG_ERR, "Unsupported filetype `%s'",
                    dest_param.orig);
                valid = 0;
                goto bcast;
            }

            /* check that dest is writable */
            if(mfu_access(dest_param.path, W_OK) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Destination is not writable `%s'",
                    dest_param.path);
                valid = 0;
                goto bcast;
            }
        }
        else {
            /* destination does not exist, so we'll be creating it,
             * check that its parent is writable */

            /* compute parent path */
            mfu_path* parent = mfu_path_from_str(dest_param.path);
            mfu_path_dirname(parent);
            char* parent_str = mfu_path_strdup(parent);
            mfu_path_delete(&parent);

            /* check that parent is writable */
            if(mfu_access(parent_str, W_OK) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Destination parent directory is not writable `%s'",
                    parent_str);
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
        if(num_src_params > 1) {
            dest_required_to_be_dir = true;
        }

        /* if caller requires dest to be a directory, and if dest does not
         * exist or it does it exist but it's not a directory, then abort */
        if(dest_required_to_be_dir &&
           (!dest_exists || (!dest_is_dir && !dest_is_link_to_dir)))
        {
            MFU_LOG(MFU_LOG_ERR, "Destination is not a directory `%s'",
                dest_param.orig);
            valid = 0;
            goto bcast;
        }

        /* we copy into a directory if any of the following:
         *   1) user specified more than one source
         *   2) destination already exists and is a directory
         *   3) destination already exists and is a link to a directory */
        bool copy_into_dir = (dest_required_to_be_dir || dest_is_dir || dest_is_link_to_dir);
        DCOPY_user_opts.copy_into_dir = copy_into_dir ? 1 : 0;
    }

    /* get status from rank 0 */
bcast:
    MPI_Bcast(&valid, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* exit job if we found a problem */
    if(! valid) {
        if(DCOPY_global_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Exiting run");
        }
        MPI_Barrier(MPI_COMM_WORLD);
        DCOPY_exit(EXIT_FAILURE);
    }

    /* rank 0 broadcasts whether we're copying into a directory */
    MPI_Bcast(&DCOPY_user_opts.copy_into_dir, 1, MPI_INT, 0, MPI_COMM_WORLD);
}

/**
 * Parse the source and destination paths that the user has provided.
 */
void DCOPY_parse_path_args(char** argv, \
                           int optind_local, \
                           int argc)
{
    /* compute number of paths and index of last argument */
    int num_args = argc - optind_local;
    int last_arg_index = num_args + optind_local - 1;

    /* we need to have at least two paths,
     * one or more sources and one destination */
    if(argv == NULL || num_args < 2) {
        if(DCOPY_global_rank == 0) {
            DCOPY_print_usage();
            MFU_LOG(MFU_LOG_ERR, "You must specify a source and destination path");
        }

        MPI_Barrier(MPI_COMM_WORLD);
        DCOPY_exit(EXIT_FAILURE);
    }

    /* determine number of source paths */
    src_params = NULL;
    num_src_params = last_arg_index - optind_local;

    /* allocate space to record info about each source */
    size_t src_params_bytes = ((size_t) num_src_params) * sizeof(mfu_param_path);
    src_params = (mfu_param_path*) MFU_MALLOC(src_params_bytes);

    /* create new mfu_file objects */
    mfu_file_t* mfu_src_file = mfu_file_new();
    mfu_file_t* mfu_dst_file = mfu_file_new();

    /* record standardized paths and stat info for each source */
    int opt_index;
    for(opt_index = optind_local; opt_index < last_arg_index; opt_index++) {
        char* path = argv[opt_index];
        int idx = opt_index - optind_local;
        mfu_param_path_set(path, &src_params[idx], mfu_src_file, true);
    }

    /* standardize destination path */
    const char* dstpath = argv[last_arg_index];
    mfu_param_path_set(dstpath, &dest_param, mfu_dst_file, false);

    /* copy the destination path to user opts structure */
    DCOPY_user_opts.dest_path = MFU_STRDUP(dest_param.path);

    /* check that source and destinations are ok */
    DCOPY_check_paths();

    /* delete file objects */
    mfu_file_delete(&mfu_src_file);
    mfu_file_delete(&mfu_dst_file);
}

/* frees resources allocated in call to parse_path_args() */
void DCOPY_free_path_args(void)
{
    /* free memory associated with destination path */
    mfu_param_path_free(&dest_param);

    /* free memory associated with source paths */
    int i;
    for(i = 0; i < num_src_params; i++) {
        mfu_param_path_free(&src_params[i]);
    }
    num_src_params = 0;
    mfu_free(&src_params);
}

/* EOF */
