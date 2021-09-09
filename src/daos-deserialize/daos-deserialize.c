#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>

/* for daos */
#include "mfu_daos.h"
#include "mpi.h"
#include "mfu.h"

#include "mfu_errors.h"

/** Print a usage message. */
void print_usage(void)
{
    printf("\n");
    printf("Usage: daos-deserialize [options] [<h5file> <h5file> ...] || [</path/to/dir>]\n");
    printf("\n");
    printf("Options:\n");
    printf("  -p, --pool               - pool uuid for containers\n");
    printf("  -l, --cont-label         - use a label name for deserialize container\n");
    printf("  -v, --verbose            - verbose output\n");
    printf("  -q, --quiet              - quiet output\n");
    printf("  -h, --help               - print usage\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
}

/* if a directory is passed in we need to count the files in it,
 * and read them into the paths array */
static int count_files(char **argpaths, char ***paths, int *numpaths) {
    int         rc = 0;
    int         i;
    struct stat statbuf;
    int         num_files = 0;

    rc = stat(argpaths[0], &statbuf);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to stat input file"
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        rc = 1;
        goto out;
    }

    if (S_ISDIR(statbuf.st_mode)) {
        DIR *dir;
        struct dirent *entry;

        /* set a max paths so that we do not 
         * have to read directory twice,
         * once to get number of files, and
         * once to copy/save strings to paths array */
        int max_paths = 1024;

        /* use paths instead of argpaths in case directory is used */
        *paths = MFU_MALLOC(max_paths * sizeof(char*));
        if (*paths == NULL) {
            rc = ENOMEM;
            goto out;
        }

        dir = opendir(argpaths[0]);
        if (dir == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open directory"
                    MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
            rc = 1;
            goto out;
        }

        while((entry = readdir(dir)) != NULL) {
            /* don't count or copy into paths array if this
             * is not a regular file */
            struct stat stbuf;
            int len;
            char name[FILENAME_LEN];
            len = snprintf(name, FILENAME_LEN, "%s/%s", argpaths[0],
                           entry->d_name);
            if (len >= FILENAME_LEN) {
                MFU_LOG(MFU_LOG_ERR, "filename is too long");
                rc = 1;
                goto out;
            }
            rc = stat(name, &stbuf);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "failed to stat path: %s", name);
                rc = 1;
                goto out;
            }
            if (S_ISREG(stbuf.st_mode)) {   
                char *path = MFU_STRDUP(name);
                if (path == NULL) {
                    rc = ENOMEM;
                    goto out;
                }
                (*paths)[num_files] = strdup(name);
                mfu_free(&path);
                num_files++;
                if (num_files > max_paths) {
                    /* TODO: maybe there is a better way to handle this..
                     * possibly moving over to libcircle might avoid
                     * having to set a maximum for the number of files
                     * deserialized from one directory */
                    MFU_LOG(MFU_LOG_ERR, "number of files exceeds max number "
                            "allowed, aborting");
                    rc = 1;
                    goto out;
                }
            } else {
                continue;
            }
        }
        *numpaths = num_files;
        closedir(dir);
    } else {
        /* use paths instead of argpaths in case directory is used */
        *paths = MFU_MALLOC(*numpaths * sizeof(char*));
        if (*paths == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Failed to allocate paths.");
            rc = 1;
            goto out;
        }

        num_files = *numpaths;
        for (i = 0; i < *numpaths; i++) {
            (*paths)[i] = MFU_STRDUP(argpaths[i]);
            if ((*paths)[i] == NULL) {
                MFU_LOG(MFU_LOG_ERR, "Failed to allocate paths.");
                rc = 1;
                goto out;
            }
        }
    }
out:
    if (rc != 0) {
        if (*paths != NULL) {
            for (i = 0; i < num_files; i++) {
                mfu_free(&(*paths)[i]); // TODO check this pointer handling
            }
            mfu_free(paths);
        }
    }
    return rc;
}

int main(int argc, char** argv)
{
    /* assume we'll exit with success */
    int rc = 0;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    int option_index = 0;
    static struct option long_options[] = {
        {"pool"                 , required_argument , 0, 'p'},
        {"cont-label"           , required_argument , 0, 'l'},
        {"verbose"              , no_argument       , 0, 'v'},
        {"quiet"                , no_argument       , 0, 'q'},
        {"help"                 , no_argument       , 0, 'h'},
        {0                      , 0                 , 0,  0 }
    };

    /* Parse options */
    unsigned long long bytes = 0;

    /* DAOS vars */ 
    daos_args_t* daos_args = daos_args_new();    
    /* option to deserialize container with label name */
    char *cont_label = NULL;

    int usage = 0;
    while(1) {
        int c = getopt_long(
                    argc, argv, "p:l:vqh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
            case 'p':
                snprintf(daos_args->src_pool, DAOS_PROP_LABEL_MAX_LEN + 1, "%s", optarg);
                break;
            case 'l':
                cont_label = MFU_STRDUP(optarg);
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = MFU_LOG_NONE;
                break;
            case 'h':
                usage = 1;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if(rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* If we need to print the usage
     * then do so before internal processing */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    int tmp_rc;
    char** argpaths = (&argv[optind]);

    /* The remaining arguments are treated as src/dst paths */
    int numpaths = argc - optind;

    /* advance to next set of options */
    optind += numpaths;

    if (numpaths < 1 || daos_args->src_pool ==  NULL) {
        MFU_LOG(MFU_LOG_ERR, "At least one file or directory and "
                "a pool UUID is required:"
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* create an empty file list */
    mfu_flist tmplist = mfu_flist_new();

    rc = daos_init();
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialize daos");
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* Initialize some stats */
    mfu_daos_stats_t stats;
    mfu_daos_stats_init(&stats);
    mfu_daos_stats_start(&stats);

    /* TODO: I think maybe this can be adjusted to use
     * libcircle in the case that a user specifies
     * a directory, it could only add regular files
     * though, so i am not sure how easy/hard this
     * would be. I am concerned about the case where
     * there are thousands of larger files in one directory.
     * It would be nice if multiple ranks were counting/adding
     * to the flist. We may also be able to avoid the 
     * mfu_flist_spread since if we can use libcircle then
     * each rank can process a file as it receives work,
     * instead of gathering the work on rank 0 first */
    if (rank == 0) {
        uint64_t                files_generated = 0;
        hid_t                   status;
        struct                  hdf5_args hdf5;
        daos_cont_layout_t      cont_type;

        char **paths = NULL;

        /* if a directory is given then count and store paths */
        rc = count_files(argpaths, &paths, &numpaths);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to count files");
            daos_fini();
            mfu_finalize();
            MPI_Finalize();
            return 1;
        }
        
        /* make sure the number of files generated on serialization
         * matches the number of files passed into the deserialization
         * grab the first file, since each file stores this attribute */
        hdf5.file = H5Fopen(paths[0], H5F_ACC_RDONLY, H5P_DEFAULT);
        if (hdf5.file < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to open file");
            rc = 1;
        }

        hid_t files_gen_attr = H5Aopen(hdf5.file, "Files Generated", H5P_DEFAULT);
        if (files_gen_attr < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to open files generated attr");
            rc = 1;
        }

        hid_t attr_dtype = H5Aget_type(files_gen_attr);
        if (attr_dtype < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to get attr type");
            rc = 1;
        }

        status = H5Aread(files_gen_attr, attr_dtype, &files_generated); 
        if (status < 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to read files generated");
            rc = 1;
        }

        if (files_generated != numpaths) {
            MFU_LOG(MFU_LOG_ERR, "number of files for deserialization does "
                                 "not match number of files generated during "
                                 "serialization, contianer data is missing\n");
            rc = 1;
        }

        int i;
        for (i = 0; i < numpaths; i++) {
            uint64_t idx = mfu_flist_file_create(tmplist);
            mfu_flist_file_set_cont(tmplist, idx, paths[i]);
        }

        tmp_rc = daos_cont_deserialize_connect(daos_args, &hdf5, &cont_type, cont_label);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to connect to container\n");
            rc = 1;
        }

        for (i = 0; i < numpaths; i++) {
            mfu_free(&paths[i]);
        }
        mfu_free(&paths);

        /* close hdf5 */
        H5Fclose(hdf5.file);
        H5Aclose(files_gen_attr);
        H5Tclose(attr_dtype);
    }

    /* use rank 0's paths, and spread them evenly among ranks
     * Each "path" is an HDF5 file */
    mfu_flist_summarize(tmplist);
    mfu_flist newflist = mfu_flist_spread(tmplist);

    /* get size of local list for each rank */
    uint64_t size = mfu_flist_size(newflist);

    /* broadcast rank 0's pool and cont handle to everyone else */
    daos_bcast_handle(rank, &daos_args->src_poh, &daos_args->src_poh, POOL_HANDLE); 
    daos_bcast_handle(rank, &daos_args->src_coh, &daos_args->src_poh, CONT_HANDLE); 

    /* connect to each pool/cont in local list, then serialize */
    int i;
    for (i = 0; i < size; i++) {
        const char *path = mfu_flist_file_get_name(newflist, i);

        /* deserialize this hdf5 file to a DAOS container */
        tmp_rc = daos_cont_deserialize_hdlr(rank, daos_args, path, &stats);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to deserialize container (%d)", rc);
            rc = 1;
        }
    }

    /* Record end time */
    mfu_daos_stats_end(&stats);

    /* Sum and print the stats */
    mfu_daos_stats_print_sum(rank, &stats, false, true, false, false);

    mfu_flist_free(&newflist);
    mfu_free(&cont_label);

    /* don't close anything until all ranks are done using handles */
    MPI_Barrier(MPI_COMM_WORLD);

    tmp_rc = daos_cont_close(daos_args->src_coh, NULL);
    MPI_Barrier(MPI_COMM_WORLD);
    if (tmp_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
        rc = 1;
    }
    
    tmp_rc = daos_pool_disconnect(daos_args->src_poh, NULL);
    MPI_Barrier(MPI_COMM_WORLD);
    if (tmp_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to disconnect pool (%d)", rc);
        rc = 1;
    }

    /* Alert the user if there were copy errors */
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "One or more errors were detected while "
                "deserializing: " MFU_ERRF, MFU_ERRP(MFU_ERR_DAOS));
    }

    tmp_rc = daos_fini();
    if (tmp_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to finalize DAOS "DF_RC, DP_RC(rc));
        rc = 1;
    }

    mfu_finalize();

    /* shut down MPI */
    MPI_Finalize();

    if (rc != 0) {
        return 1;
    }
    return 0;
}
