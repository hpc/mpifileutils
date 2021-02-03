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
    printf("Usage: daos-serialize [options] /<pool>/<cont> /<pool>/<cont> ... \n");
    printf("\n");
    printf("DAOS paths can be specified as:\n");
    printf("       /<pool>/<cont> | <UNS path>\n");
    printf("\n");
    printf("Options:\n");
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
        {"verbose"              , no_argument      , 0, 'v'},
        {"help"                 , no_argument      , 0, 'h'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    unsigned long long bytes = 0;
    int usage = 0;
    while(1) {
        int c = getopt_long(
                    argc, argv, "vh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
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
    /* TODO daos_init
     *      gather paths on rank 0 and put in flist
     *      mfu_flist_summarize
     *      mfu_flist_spread
     *      loop over local size list and connect to pool/cont,
     *      then call cont_serialize_hdlr
     *      disconnect form pool/cont
     *      daos_fini
     */

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

    uuid_t pool_uuids[size];
    uuid_t cont_uuids[size];
    daos_handle_t pohs[size];
    daos_handle_t cohs[size];
    bool daos_no_prefix = true;

    /* connect to each pool/cont in local list, then serialize */
    for (i = 0; i < size; i++) {
        char *path = mfu_flist_file_get_name(newflist, i);
        int len = strlen(path); 
        uuid_clear(pool_uuids[i]);
        uuid_clear(cont_uuids[i]);
        printf("rank: %d, size: %d path: %s\n", rank, (int)size, path);

        rc = daos_parse_path(path, len, &pool_uuids[i], &cont_uuids[i],
                             daos_no_prefix);
        if (rc != 0 || pool_uuids[i] == NULL || cont_uuids[i] == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Failed to resolve DAOS path");
            rc = 1;
        }

        rc = daos_connect(rank, pool_uuids[i], cont_uuids[i],
                          &pohs[i], &cohs[i], true, true, false);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to connect to DAOS");
            rc = 1;
        }

        /* serialize this pool/cont to an hdf5 file */
        rc = cont_serialize_hdlr(pool_uuids[i], cont_uuids[i],
                                 pohs[i], cohs[i]);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to serialize container (%d)", rc);
        }

        rc = daos_cont_close(cohs[i], NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
            rc = 1;
        }

        rc = daos_pool_disconnect(pohs[i], NULL);
        MPI_Barrier(MPI_COMM_WORLD);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to disconnect pool (%d)", rc);
            rc = 1;
        }
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
