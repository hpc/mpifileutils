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

#define BUFLEN 80

/** Print a usage message. */
void print_usage(void)
{
    printf("\n");
    printf("Usage: daos-gen [options]\n");
    printf("\n");
    printf("Options:\n");
    printf("  -p, --pool               - pool uuid for containers\n");
    printf("  -v, --verbose            - verbose output\n");
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
        {"pool"                 , required_argument , 0, 'p'},
        {"verbose"              , no_argument       , 0, 'v'},
        {"quiet"                , no_argument       , 0, 'q'},
        {"help"                 , no_argument       , 0, 'h'},
        {0                      , 0                 , 0,  0 }
    };

    /* Parse options */
    unsigned long long bytes = 0;

    /* DAOS vars */ 
    daos_args_t* daos_args = daos_args_new();    

    int usage = 0;
    while(1) {
        int c = getopt_long(
                    argc, argv, "p:vqh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
            case 'p':
                uuid_parse(optarg, daos_args->src_pool_uuid);
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

    rc = daos_init();
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to initialize daos");
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    daos_obj_id_t oid;
    daos_ofeat_t ofeats = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT | DAOS_OF_ARRAY;
    /* connect to pool/cont then broadcast to rest of ranks */
    if (rank == 0) {
        /* generate container UUID */
        uuid_generate(daos_args->src_cont_uuid);

        daos_pool_info_t pool_info = {0};
        daos_cont_info_t co_info = {0};
#if DAOS_API_VERSION_MAJOR < 1
        rc = daos_pool_connect(daos_args->src_pool_uuid, NULL, NULL, DAOS_PC_RW,
                               &(daos_args->src_poh), &pool_info, NULL);
#else
        rc = daos_pool_connect(daos_args->src_pool_uuid, NULL, DAOS_PC_RW,
                               &(daos_args->src_poh), &pool_info, NULL);
#endif
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to connect to pool: "DF_RC, DP_RC(rc));
            rc = 1;
        }

        /* create cont and open */
        rc = daos_cont_create(daos_args->src_poh, daos_args->src_cont_uuid, NULL, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to create cont: "DF_RC, DP_RC(rc));
            rc = 1;
        }
        rc = daos_cont_open(daos_args->src_poh, daos_args->src_cont_uuid,
                            DAOS_COO_RW, &daos_args->src_coh, &co_info, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open container: "DF_RC, DP_RC(rc));
            rc = 1;
        }
    }

    /* broadcast rank 0's pool and cont handle to everyone else */
    daos_bcast_handle(rank, &(daos_args->src_poh), &(daos_args->src_poh), POOL_HANDLE); 
    daos_bcast_handle(rank, &(daos_args->src_coh), &(daos_args->src_poh), CONT_HANDLE); 

	daos_handle_t	    oh;
	daos_array_iod_t    iod;
	d_sg_list_t         sgl;
	daos_range_t	    rg;
	d_iov_t		        iov;
	char		        buf[BUFLEN], rbuf[BUFLEN];
	daos_size_t	        array_size;

    /* TODO: generate objects/data inside of container */
    rc = daos_obj_generate_oid(daos_args->src_coh, &oid, ofeats, OC_SX, 0, 0);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to generate oid: "DF_RC, DP_RC(rc));
        rc = 1;
    }

	/** create the array */
	rc = daos_array_create(daos_args->src_coh, oid, DAOS_TX_NONE, 1, 1048576, &oh,
			               NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to create array: "DF_RC, DP_RC(rc));
        rc = 1;
    }

	memset(buf, 'A', BUFLEN);

	/** set array location */
	iod.arr_nr = 1;
	rg.rg_len = BUFLEN;
	rg.rg_idx = 0;
	iod.arr_rgs = &rg;

	/** set memory location */
	sgl.sg_nr = 1;
	d_iov_set(&iov, buf, BUFLEN);
	sgl.sg_iovs = &iov;

	/** Write */
	rc = daos_array_write(oh, DAOS_TX_NONE, &iod, &sgl, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to write to array: "DF_RC, DP_RC(rc));
        rc = 1;
    }

	rc = daos_array_get_size(oh, DAOS_TX_NONE, &array_size, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to get array size: "DF_RC, DP_RC(rc));
        rc = 1;
    }

    char uuid_str[130];
    uuid_unparse(daos_args->src_cont_uuid, uuid_str);
    printf("Successfully created container %s, array size: %d\n", uuid_str, (int)array_size);

	rc = daos_array_close(oh, NULL);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to close array: "DF_RC, DP_RC(rc));
        rc = 1;
    }

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
        MFU_LOG(MFU_LOG_ERR, "One or more errors were detected while generating "
                "data: " MFU_ERRF, MFU_ERRP(MFU_ERR_DAOS));
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
