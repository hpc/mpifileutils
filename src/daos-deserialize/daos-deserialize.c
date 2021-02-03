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
#ifdef DAOS_SUPPORT
#include "mfu_daos.h"
#endif
#include "mpi.h"
#include "mfu.h"

#include "mfu_errors.h"

/** Print a usage message. */
void print_usage(void)
{
    printf("\n");
    printf("Usage: daos-serialize [options] <h5file> <h5file> ... \n");
    printf("\n");
    printf("Options:\n");
    printf("  -p, --pool               - pool uuid for containers\n");
    printf("  -v, --verbose            - verbose output\n");
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
        {"pool"                 , required_argument , 0, 'p'},
        {"verbose"              , no_argument       , 0, 'v'},
        {"help"                 , no_argument       , 0, 'h'},
        {0                      , 0                 , 0,  0 }
    };

    /* Parse options */
    unsigned long long bytes = 0;

    /* pool to place new containers into */
    uuid_t pool_uuid;

    int usage = 0;
    while(1) {
        int c = getopt_long(
                    argc, argv, "p:vh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
            case 'p':
                uuid_parse(optarg, pool_uuid);
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
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

    /* Before processing, make sure we have at least two paths to begin with */
    if (numpaths < 1) {
        MFU_LOG(MFU_LOG_ERR, "At least one pool and container is required:"
                MFU_ERRF, MFU_ERRP(-MFU_ERR_INVAL_ARG));
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* TODO: support input file rather than required all pool/cont paths
     * be input on cmd line */
    /* TODO: maybe support moving into multiple pools? */
    /* TODO: broadcast pool handle to all ranks if not supporting
     * multiple pools (instead of connecting on each rank)
     * or have a check that if all pools are
     * the same, then use the same handle */

    /* create an empty file list */
    mfu_flist tmplist = mfu_flist_new();

    rc = daos_init();
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialize daos");
        return 1;
    }

    /* use rank 0's paths, and spread them evenly among ranks
     * Each "path" is a DAOS pool/cont, and this tool will
     * serialize each container into its own hdf5 file */
    int i;
    if (rank == 0) {
        for (i = 0; i < numpaths; i++) {
                uint64_t idx = mfu_flist_file_create(tmplist);
                mfu_flist_file_set_cont(tmplist, idx, argpaths[i]);
        }
    }

    mfu_flist_summarize(tmplist);
    mfu_flist newflist = mfu_flist_spread(tmplist);

    /* get size of local list for each rank */
    uint64_t size = mfu_flist_size(newflist);

    daos_handle_t poh;
    uuid_t cont_uuids[size];
    daos_handle_t cohs[size];
    bool daos_no_prefix = true;
    daos_pool_info_t pool_info = {0};
    printf("SIZE: %d\n", (int)size);

#if DAOS_API_VERSION_MAJOR < 1
    rc = daos_pool_connect(pool_uuid, NULL, NULL, DAOS_PC_RW,
                           &poh, &pool_info, NULL);
#else
    rc = daos_pool_connect(pool_uuid, NULL, DAOS_PC_RW,
                           &poh, &pool_info, NULL);
#endif
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to connect to pool");
        rc = 1;
    }

    /* connect to each pool/cont in local list, then serialize */
    for (i = 0; i < size; i++) {
        char *path = mfu_flist_file_get_name(newflist, i);
        uuid_generate(cont_uuids[i]);
        char cont_uuid_str[130];
        uuid_unparse(cont_uuids[i], cont_uuid_str);
        printf("rank: %d, size: %d path: %s\n", rank, (int)size, path);


        /* deserialize this hdf5 file to a DAOS container */
        rc = cont_deserialize_hdlr(pool_uuid, cont_uuids[i],
                                   &poh, &cohs[i], path);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to deserialize container (%d)", rc);
        }

        rc = daos_cont_close(cohs[i], NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
            rc = 1;
        }
    }
    
    rc = daos_pool_disconnect(poh, NULL);
    MPI_Barrier(MPI_COMM_WORLD);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to disconnect pool (%d)", rc);
        rc = 1;
    }

    /* Alert the user if there were copy errors */
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "One or more errors were detected while "
                "serializing: " MFU_ERRF, MFU_ERRP(MFU_ERR_DAOS));
    }

    rc = daos_fini();
    if (rc != 0) {
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
