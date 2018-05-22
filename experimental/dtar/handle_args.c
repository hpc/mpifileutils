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
 * @file - handle_args.c
 *
 * @author - Feiyi Wang
 *
 */

#include "common.h"

mfu_param_path* src_params;
mfu_param_path dest_param;
int num_src_params;

static void DTAR_check_paths(void)
{
    int valid = 1;
    int i;
    int num_readable = 0;
    for (i = 0; i < num_src_params; i++) {
        char* path = src_params[i].path;
        if (mfu_access(path, R_OK) == 0) {
            num_readable++;
        }
        else {
            /* not readable */
            char* orig = src_params[i].orig;
            MFU_LOG(MFU_LOG_ERR, "Could not read '%s' errno=%d %s",
                    orig, errno, strerror(errno));
        }

    }

    /* verify we have at least one valid source */
    if (num_readable < 1) {
        MFU_LOG(MFU_LOG_ERR, "At least one valid source must be specified");
        valid = 0;
        goto bcast;
    }

    /* check destination */
    if (dest_param.path_stat_valid) {
        if (DTAR_rank == 0)
        { MFU_LOG(MFU_LOG_WARN, "Destination target exists, we will overwrite"); }
    }
    else {
        mfu_path* parent = mfu_path_from_str(dest_param.path);
        mfu_path_dirname(parent);
        char* parent_str = mfu_path_strdup(parent);
        mfu_path_delete(&parent);
        /* check if parent is writable */
        if (mfu_access(parent_str, W_OK) < 0) {
            MFU_LOG(MFU_LOG_ERR, "Destination parent directory is not wriable: '%s' ",
                    parent_str);
            valid = 0;
            mfu_free(&parent_str);
            goto bcast;
        }

        mfu_free(&parent_str);
    }

    /* at this point, we know
     * (1) destination doesn't exist
     * (2) parent directory is writable
     */

bcast:
    MPI_Bcast(&valid, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (!valid) {
        if (DTAR_global_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Exiting run");
        }
        MPI_Barrier(MPI_COMM_WORLD);
        DTAR_exit(EXIT_FAILURE);
    }
}

void DTAR_parse_path_args(int argc, char** argv, const char* dstfile)
{
    if (argv == NULL || argc < 2) {
        if (DTAR_global_rank == 0) {
            fprintf(stderr, "\nYou must provide at least one source file or directory\n");
            DTAR_exit(EXIT_FAILURE);
        }
    }

    /* allocate space to record info for each source */
    src_params = NULL;
    num_src_params = argc - 1;
    size_t src_params_bytes = ((size_t) num_src_params) * sizeof(mfu_param_path);
    src_params = (mfu_param_path*) MFU_MALLOC(src_params_bytes);

    int idx;
    for (idx = 1; idx < argc; idx++) {
        char* path = argv[idx];
        mfu_param_path_set(path, &src_params[idx - 1]);
    }

    /* standardize destination path */
    mfu_param_path_set(dstfile, &dest_param);

    /* copy destination to user opts structure */
    DTAR_user_opts.dest_path = MFU_STRDUP(dest_param.path);

    /* check that source and destination are okay */
    DTAR_check_paths();
}
