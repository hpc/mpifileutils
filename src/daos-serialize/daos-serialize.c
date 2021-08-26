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
    printf("Usage: daos-serialize [options] daos://<pool>/<cont>\n");
    printf("\n");
    printf("DAOS paths can be specified as:\n");
    printf("       daos://<pool>/<cont> | <UNS path>\n");
    printf("\n");
    printf("Options:\n");
    printf("  -o  --output-path        - path to output serialized hdf5 files\n");
    printf("  -v, --verbose            - verbose output\n");
    printf("  -f, --force              - force serialization even if container has unhealthy status\n");
    printf("  -q, --quiet              - quiet output\n");
    printf("  -h, --help               - print usage\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
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
        {"output-path"          , required_argument, 0, 'o'},
        {"verbose"              , no_argument      , 0, 'v'},
        {"force"                , no_argument      , 0, 'f'},
        {"quiet"                , no_argument      , 0, 'q'},
        {"help"                 , no_argument      , 0, 'h'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    unsigned long long bytes = 0;
    int usage = 0;
    char *output_path = NULL;
    bool force_serialize = false;
    while (1) {
        int c = getopt_long(
                    argc, argv, "o:vfqh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
            case 'o':
                output_path = MFU_STRDUP(optarg);
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'f':
                force_serialize = true;
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

    char** argpaths = (&argv[optind]);
    
    /* The remaining arguments are treated as src/dst paths */
    int numpaths = argc - optind;

    /* advance to next set of options */
    optind += numpaths;

    /* Before processing, make sure we have at least one path */
    if (numpaths < 1) {
        MFU_LOG(MFU_LOG_ERR, "At least one pool and container is required:"
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    rc = daos_init();
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialize daos");
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* DAOS vars */ 
    daos_args_t* daos_args = daos_args_new();    

    int len = strlen(argpaths[0]); 

    int tmp_rc;
    tmp_rc = daos_parse_path(argpaths[0], len, &daos_args->src_pool, &daos_args->src_cont);
    if (tmp_rc != 0 || daos_args->src_cont == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to resolve DAOS path");
         rc = 1;
    }
    
    tmp_rc = daos_connect(rank, daos_args, &daos_args->src_pool,
                          &daos_args->src_cont, &daos_args->src_poh,
                          &daos_args->src_coh, force_serialize, true,
                          false, false, false, NULL, true);
    if (tmp_rc != 0) {
        daos_fini();
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* Initialize some stats */
    mfu_daos_stats_t stats;
    mfu_daos_stats_init(&stats);
    mfu_daos_stats_start(&stats);

    /* take a snapshot and walk container to get list of objects,
     * returns epoch number of snapshot */
    tmp_rc = mfu_daos_flist_walk(daos_args, daos_args->src_coh, &daos_args->src_epc, flist);
    if (tmp_rc != 0) {
        rc = 1;
    }

    /* all objects are on rank 0 at this point,
     * evenly spread them among the ranks */
    mfu_flist newflist = mfu_flist_spread(flist);

    /* get size of local list for each rank */
    uint64_t size = mfu_flist_size(newflist);

    /* serialize pool/cont to an hdf5 file */
    uint64_t files_written = 0;
    struct hdf5_args hdf5;

    /* only create a directory if one is passed in with output_path
     * option, otherwise use current working dir */
    if (output_path == NULL) {
        char cwd[FILENAME_LEN];
        getcwd(cwd, FILENAME_LEN);
        if (cwd == NULL) {
            MFU_LOG(MFU_LOG_ERR, "failed to get current working directory");
            rc = 1;
        }
        output_path = MFU_STRDUP(cwd);
    } else {
        tmp_rc = mkdir(output_path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        if (tmp_rc != 0 && errno != EEXIST) {
            MFU_LOG(MFU_LOG_ERR, "failed to create output directory");
            rc = 1;
        }
    }

    /* don't bother running if this rank doesn't have any oids */
    if (size > 0) {
        tmp_rc = daos_cont_serialize_hdlr(rank, &hdf5, output_path, &files_written,
                                          daos_args, newflist, size, &stats);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to serialize container (%d)", rc);
            rc = 1;
        }
    }

    /* sum files_written across all ranks to get total */
    uint64_t total_files_written = 0;
    MPI_Allreduce(&files_written, &total_files_written, 1, MPI_UNSIGNED,
                  MPI_SUM, MPI_COMM_WORLD);

    /* no file created if this rank received no oids */
    if (size > 0) {
        tmp_rc = daos_cont_serialize_files_generated(&hdf5, &total_files_written);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "failed to serialize files generated");
            rc = 1;
        }
    }

    /* Record end time */
    mfu_daos_stats_end(&stats);

    /* Sum and print the stats */
    mfu_daos_stats_print_sum(rank, &stats, true, false, false, false);

    /* destroy snapshot after copy */
    /* TODO consider moving this into mfu_flist_copy_daos */
    if (rank == 0) {
        daos_epoch_range_t epr;
        epr.epr_lo = daos_args->src_epc;
        epr.epr_hi = daos_args->src_epc;
        tmp_rc = daos_cont_destroy_snap(daos_args->src_coh, epr, NULL);
        if (tmp_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "DAOS destroy snapshot failed: ", MFU_ERRF,
                    MFU_ERRP(-MFU_ERR_DAOS));
            rc = 1;
        }
    }

    /* free flists */
    mfu_flist_free(&newflist);
    mfu_flist_free(&flist);

    /* free output path for hdf5 files */
    mfu_free(&output_path);

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
   
    /* free daos_args */
    daos_args_delete(&daos_args);

    /* Alert the user if there were copy errors */
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "One or more errors were detected while "
                "serializing: " MFU_ERRF, MFU_ERRP(MFU_ERR_DAOS));
    }

    tmp_rc = daos_fini();
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to finalize DAOS "DF_RC, DP_RC(rc));
        rc = 1;
    }

    int global_rc;
    MPI_Allreduce(&rc, &global_rc, 1, MPI_INT, MPI_LOR, MPI_COMM_WORLD);

    mfu_finalize();

    /* shut down MPI */
    MPI_Finalize();

    if (global_rc != 0) {
        return 1;
    }
    return 0;
}
