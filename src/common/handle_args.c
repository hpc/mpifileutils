/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2015, DataDirect Networks, Inc.
 * 
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/

/*
 * For an overview of how argument handling was originally designed in dcp,
 * please see the blog post at:
 *
 * <http://www.bringhurst.org/2012/12/16/file-copy-tool-argument-handling.html>
 */
#include "mfu_flist.h"
#include "handle_args.h"
#include "mfu.h"

#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct mfu_copy_params{
  int num_src_params; /* get number of src parameters */ 
  mfu_param_path* src_params; /* pointer to the src_params */
  mfu_param_path dest_param; /* dest param variable */
  int do_sync;
} mfu_copy_params;
static mfu_copy_params cp_params;
static mfu_copy_params* params_ptr = &cp_params;

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
 */

/* check that source and destination paths are valid */
static void DCOPY_check_paths(void)
{
    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* assume path parameters are valid */
    int valid = 1;

    /* just have rank 0 check */
    if(rank == 0) {
        /* count number of readable source paths */
        int i;
        int num_readable = 0;
        for(i = 0; i < params_ptr->num_src_params; i++) {
            char* path = params_ptr->src_params[i].path;
            if(mfu_access(path, R_OK) == 0) {
                num_readable++;
            }
            else {
                /* found a source path that we can't read, not fatal,
                 * but print an error to notify user */
                char* orig = params_ptr->src_params[i].orig;
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
        if(params_ptr->dest_param.path_stat_valid) {
            /* we could stat dest path, so something is there */
            dest_exists = true;

            /* now determine its type */
            if(S_ISDIR(params_ptr->dest_param.path_stat.st_mode)) {
                /* dest is a directory */
                dest_is_dir  = true;
            }
            else if(S_ISREG(params_ptr->dest_param.path_stat.st_mode)) {
                /* dest is a file */
                dest_is_file = true;
            }
            else if(S_ISLNK(params_ptr->dest_param.path_stat.st_mode)) {
                /* dest is a symlink, but to what? */
                if (params_ptr->dest_param.target_stat_valid) {
                    /* target of the symlink exists, determine what it is */
                    if(S_ISDIR(params_ptr->dest_param.target_stat.st_mode)) {
                        /* dest is link to a directory */
                        dest_is_link_to_dir = true;
                    }
                    else if(S_ISREG(params_ptr->dest_param.target_stat.st_mode)) {
                        /* dest is link to a file */
                        dest_is_link_to_file = true;
                    }
                    else {
                        /* unsupported type */
                        MFU_LOG(MFU_LOG_ERR, "Unsupported filetype `%s' --> `%s'",
                            params_ptr->dest_param.orig, params_ptr->dest_param.target);
                        valid = 0;
                        goto bcast;
                    }
                }
                else {
                    /* dest is a link, but its target does not exist,
                     * consider this an error */
                    MFU_LOG(MFU_LOG_ERR, "Destination is broken symlink `%s'",
                        params_ptr->dest_param.orig);
                    valid = 0;
                    goto bcast;
                }
            }
            else {
                /* unsupported type */
                MFU_LOG(MFU_LOG_ERR, "Unsupported filetype `%s'",
                    params_ptr->dest_param.orig);
                valid = 0;
                goto bcast;
            }

            /* check that dest is writable */
            if(mfu_access(params_ptr->dest_param.path, W_OK) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Destination is not writable `%s'",
                    params_ptr->dest_param.path);
                valid = 0;
                goto bcast;
            }
        }
        else {
            /* destination does not exist, so we'll be creating it,
             * check that its parent is writable */

            /* compute parent path */
            mfu_path* parent = mfu_path_from_str(params_ptr->dest_param.path);
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
        if(params_ptr->num_src_params > 1) {
            dest_required_to_be_dir = true;
        }

        /* if caller requires dest to be a directory, and if dest does not
         * exist or it does it exist but it's not a directory, then abort */
        if(dest_required_to_be_dir &&
           (!dest_exists || (!dest_is_dir && !dest_is_link_to_dir)))
        {
            MFU_LOG(MFU_LOG_ERR, "Destination is not a directory `%s'",
                params_ptr->dest_param.orig);
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
        if(rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Exiting run");
        }
        MPI_Barrier(MPI_COMM_WORLD);
        //DCOPY_exit(EXIT_FAILURE);
    }

    /* rank 0 broadcasts whether we're copying into a directory */
    MPI_Bcast(&DCOPY_user_opts.copy_into_dir, 1, MPI_INT, 0, MPI_COMM_WORLD);
}

/* set the src and dest path parameters, and pass in preserve
 * perms flag to tell whether or not to preserve permissions
 * and timestamps, etc */
void mfu_flist_set_copy_params(int num_src_paths,
       const char* src_path, const char* dest_path,
       int preserve, int do_sync) {
    /* get current rank */  
    int rank; 
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
   
    if (preserve) {
        DCOPY_user_opts.preserve = true;
    }

    /* set number of source paths */
    params_ptr->src_params = NULL;
    params_ptr->num_src_params = num_src_paths;
    params_ptr->do_sync = 1;
    
    /* allocate space to record info about source */
    size_t src_params_bytes = ((size_t) params_ptr->num_src_params) * sizeof(mfu_param_path);
    params_ptr->src_params = (mfu_param_path*) MFU_MALLOC(src_params_bytes);

    /* record standardized path and stat info for source */
    mfu_param_path_set(src_path, &(params_ptr->src_params[0]));

    /* standardize destination path */
    mfu_param_path_set(dest_path, &(params_ptr->dest_param));

    /* copy the destination path to user opts structure */
    DCOPY_user_opts.dest_path = MFU_STRDUP(params_ptr->dest_param.path);

    /* check that source and destinations are ok */
    DCOPY_check_paths();
} 

void mfu_flist_copy(mfu_flist src_cp_list, 
        int preserve, int do_sync) {
   /* split items in file list into sublists depending on their
    * directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(src_cp_list, &levels, &minlevel, &lists);

    /* TODO: filter out files that are bigger than 0 bytes if we can't read them */

    /* create directories, from top down */
    mfu_create_directories(levels, minlevel, lists);

    /* create files and links */
    mfu_create_files(levels, minlevel, lists);

    /* copy data */
    mfu_copy_files(src_cp_list, DCOPY_user_opts.chunk_size);

    /* close files */
    mfu_copy_close_file(&mfu_copy_src_cache);
    mfu_copy_close_file(&mfu_copy_dst_cache);

    /* set permissions, ownership, and timestamps if needed */
    mfu_copy_set_metadata(levels, minlevel, lists);

    /* free our lists of levels */
    mfu_flist_array_free(levels, &lists);
}

/**
 * Parse the source and destination paths that the user has provided.
 */
void DCOPY_parse_path_args(char** argv, \
                           int optind_local, \
                           int argc)
{
    /* get current rank */  
    int rank; 
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    
    /* compute number of paths and index of last argument */
    int num_args = argc - optind_local;
    int last_arg_index = num_args + optind_local - 1;

    /* we need to have at least two paths,
     * one or more sources and one destination */
    if(argv == NULL || num_args < 2) {
        if(rank == 0) {
            //DCOPY_print_usage();
            MFU_LOG(MFU_LOG_ERR, "You must specify a source and destination path");
        }

        MPI_Barrier(MPI_COMM_WORLD);
        //DCOPY_exit(EXIT_FAILURE);
    }

    /* determine number of source paths */
    params_ptr->src_params = NULL;
    params_ptr->num_src_params = last_arg_index - optind_local;

    /* allocate space to record info about each source */
    size_t src_params_bytes = ((size_t) params_ptr->num_src_params) * sizeof(mfu_param_path);
    params_ptr->src_params = (mfu_param_path*) MFU_MALLOC(src_params_bytes);

    /* record standardized paths and stat info for each source */
    int opt_index;
    for(opt_index = optind_local; opt_index < last_arg_index; opt_index++) {
        char* path = argv[opt_index];
        int idx = opt_index - optind_local;
        mfu_param_path_set(path, &(params_ptr->src_params[idx]));
    }

    /* standardize destination path */
    const char* dstpath = argv[last_arg_index];
    mfu_param_path_set(dstpath, &(params_ptr->dest_param));

    /* copy the destination path to user opts structure */
    DCOPY_user_opts.dest_path = MFU_STRDUP(params_ptr->dest_param.path);

    /* check that source and destinations are ok */
    DCOPY_check_paths();
}

void DCOPY_walk_paths(mfu_flist flist)
{
    /* get current rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int i;
    for (i = 0; i < params_ptr->num_src_params; i++) {
        /* get path for step */
        const char* target = params_ptr->src_params[i].path;

        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Walking %s", target);
        }

        /* walk file tree and record stat data for each item */
        int walk_stat = 1;
        int dir_perm = 0;
        mfu_flist_walk_path(target, walk_stat, dir_perm, flist);
    }
}

/* given an item name, determine which source path this item
 * is contained within, extract directory components from source
 * path to this item and then prepend destination prefix. */
char* DCOPY_build_dest(const char* name)
{
    /* identify which source directory this came from */
    int i;
    int idx = -1;
    for (i = 0; i < params_ptr->num_src_params; i++) {
        /* get path for step */
        const char* path = params_ptr->src_params[i].path;

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
    mfu_path* src = mfu_path_from_str(params_ptr->src_params[i].path);

    /* get number of components in item */
    int item_components = mfu_path_components(item);

    /* get number of components in source path */
    int src_components = mfu_path_components(src);

    /* if copying into directory, keep last component,
     * otherwise cut all components listed in source path */
    int cut = src_components;
    if (DCOPY_user_opts.copy_into_dir && cut > 0) {
        if (!params_ptr->do_sync) {
            cut--;
        }
    }

    /* compute number of components to keep */
    int keep = item_components - cut;

    /* chop prefix from item */
    mfu_path_slice(item, cut, keep);

    /* prepend destination path */
    mfu_path_prepend_str(item, params_ptr->dest_param.path);

    /* convert to a NUL-terminated string */
    char* dest = mfu_path_strdup(item);

    /* free our temporary paths */
    mfu_path_delete(&src);
    mfu_path_delete(&item);

    return dest;
}

/* frees resources allocated in call to parse_path_args() */
void DCOPY_free_path_args(void)
{
    /* free memory associated with destination path */
    mfu_param_path_free(&params_ptr->dest_param);

    /* free memory associated with source paths */
    int i;
    for(i = 0; i < params_ptr->num_src_params; i++) {
        mfu_param_path_free(&params_ptr->src_params[i]);
    }
    params_ptr->num_src_params = 0;
    mfu_free(&params_ptr->src_params);
}

int DCOPY_input_flist_skip(const char* name, void* args)
{
    /* create mfu_path from name */
    mfu_path* path = mfu_path_from_str(name);

    /* iterate over each source path */
    int i;
    for (i = 0; i < params_ptr->num_src_params; i++) {
        /* create mfu_path of source path */
        char* src_name = params_ptr->src_params[i].path;
        const char* src_path = mfu_path_from_str(src_name);

        /* check whether path is contained within or equal to
         * source path and if so, we need to copy this file */
        mfu_path_result result = mfu_path_cmp(path, src_path);
        if (result == MFU_PATH_SRC_CHILD || result == MFU_PATH_EQUAL) {
               MFU_LOG(MFU_LOG_INFO, "Need to copy %s because of %s.",
                   name, src_name);
            mfu_path_delete(&src_path);
            mfu_path_delete(&path);
            return 0;
        }
        mfu_path_delete(&src_path);
    }

    /* the path in name is not a child of any source paths,
     * so skip this file */
    MFU_LOG(MFU_LOG_INFO, "Skip %s.", name);
    mfu_path_delete(&path);
    return 1;
}

/* EOF */
