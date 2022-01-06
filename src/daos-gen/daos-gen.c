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
#define UUID_STR_LEN 129

static uint32_t obj_id_gen = 1;

/** Print a usage message. */
void print_usage(void)
{
    printf("\n");
    printf("Usage: daos-gen [options]\n");
    printf("\n");
    printf("Options:\n");
    printf("  -p, --pool               - pool uuid for containers\n");
    printf("  -o, --num-objects  - number of objects to generate\n");
    printf("  -k, --keys-per-object - number of keys per object\n");
    printf("  -v, --verbose            - verbose output\n");
    printf("  -q, --quiet              - quiet output\n");
    printf("  -h, --help               - print usage\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
}

/* stole this from the DAOS test code for generating obj id's */
daos_obj_id_t dts_oid_gen(unsigned seed)
{
    daos_obj_id_t   oid;
    uint64_t    hdr;

    hdr = seed;
    hdr <<= 32;

    /* generate a unique and not scary long object ID */
    oid.lo  = obj_id_gen++;
    oid.lo  |= hdr;
    oid.hi  = rand() % 100;

    return oid;
}

int main(int argc, char** argv)
{
    /* assume we'll exit with success */
    int rc = 0;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank */
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    int option_index = 0;
    static struct option long_options[] = {
        {"pool"                 , required_argument , 0, 'p'},
        {"num-objects"          , required_argument , 0, 'o'},
        {"keys-per-object"      , required_argument , 0, 'k'},
        {"verbose"              , no_argument       , 0, 'v'},
        {"quiet"                , no_argument       , 0, 'q'},
        {"help"                 , no_argument       , 0, 'h'},
        {0                      , 0                 , 0,  0 }
    };

    /* TODO: currently only generates DAOS_OF_KV_FLAT data, but could be
     * updated to include more object types */

    /* Parse options */
    int num_objects = 0;
    int keys_per_object = 0;

    /* DAOS vars */ 
    daos_args_t* daos_args = daos_args_new();    

    int usage = 0;
    while(1) {
        int c = getopt_long(
                    argc, argv, "p:o:k:vqh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch(c) {
            case 'p':
                strncpy(daos_args->src_pool, optarg, DAOS_PROP_LABEL_MAX_LEN);
                break;
            case 'o':
                num_objects = atoi(optarg);
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "generating %d objects\n", num_objects);
                }
                break;
            case 'k':
                keys_per_object = atoi(optarg);
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "generating %d keys per object\n", keys_per_object);
                }
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

    /* set default number of objects and keys per object,
     * if one is not passed in */
    if (num_objects == 0) {
        num_objects = 10;
    }
    if (keys_per_object == 0) {
        keys_per_object = 20;
    }

    daos_obj_id_t       oid[num_objects];
    daos_handle_t	    oh[num_objects];
    char		        buf[BUFLEN];
    const char          *key_fmt = "key%d";
    char                key[keys_per_object];
    int                 i,j;
    char                uuid_str[UUID_STR_LEN];
    daos_ofeat_t        ofeats;

    ofeats = DAOS_OF_DKEY_UINT64 | DAOS_OF_KV_FLAT | DAOS_OF_KV_FLAT;
    /* connect to pool/cont then broadcast to rest of ranks */
    if (rank == 0) {
        /* generate container UUID */
        uuid_generate(daos_args->src_cont);
        daos_pool_info_t pool_info = {0};
        daos_cont_info_t co_info = {0};
#if DAOS_API_VERSION_MAJOR < 1
        rc = daos_pool_connect(daos_args->src_pool, NULL, NULL, DAOS_PC_RW,
                               &(daos_args->src_poh), &pool_info, NULL);
#else
        rc = daos_pool_connect(daos_args->src_pool, NULL, DAOS_PC_RW,
                               &(daos_args->src_poh), &pool_info, NULL);
#endif
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to connect to pool: "DF_RC, DP_RC(rc));
            rc = 1;
            goto out;
        }

        /* create cont and open */
        rc = daos_cont_create(daos_args->src_poh, daos_args->src_cont, NULL, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to create cont: "DF_RC, DP_RC(rc));
            rc = 1;
            goto err_cont;
        }
        rc = daos_cont_open(daos_args->src_poh, daos_args->src_cont,
                            DAOS_COO_RW, &daos_args->src_coh, &co_info, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open container: "DF_RC, DP_RC(rc));
            rc = 1;
            goto out_err;
        }
    }

    /* broadcast rank 0's pool and cont handle to everyone else */
    daos_bcast_handle(rank, &(daos_args->src_poh), &(daos_args->src_poh), POOL_HANDLE); 
    daos_bcast_handle(rank, &(daos_args->src_coh), &(daos_args->src_poh), CONT_HANDLE); 

    /* TODO: generate different types of data, and different for each key */
	memset(buf, 'A', BUFLEN);
    for (i = 0; i < num_objects; i++) {
        oid[i] = dts_oid_gen(0);
                                   
        rc = daos_obj_generate_oid(daos_args->src_coh, &oid[i], ofeats, OC_RP_XSF, 0, 0);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to generate oid: "DF_RC, DP_RC(rc));
            rc = 1;
            goto out_err;
        }

    	/** create the KV store */
        rc = daos_kv_open(daos_args->src_coh, oid[i], DAOS_OO_RW, &oh[i], NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open kv object: "DF_RC, DP_RC(rc));
            rc = 1;
            goto out_err;
        }

        /* insert keys */
        for (j = 0; j < keys_per_object; j++) {
            sprintf(key, key_fmt, j);
            rc = daos_kv_put(oh[i], DAOS_TX_NONE, 0, key, BUFLEN, buf, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to put kv object: "DF_RC, DP_RC(rc));
                rc = 1;
                goto out_err;
            }
        }

        rc = daos_kv_close(oh[i], NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close kv object: "DF_RC, DP_RC(rc));
            rc = 1;
            goto out_err;
        }
    }

    /* calculate total objects and num_keys_per object, each rank
     * generates same amount */
    if (rank == 0) {
        int total_num_objects = size * num_objects; 
        uuid_unparse(daos_args->src_cont, uuid_str);
        printf("Container UUID: %s\n\ttotal objects:%d\n"
               "\tkeys per object:%d\n", uuid_str, total_num_objects, keys_per_object);
    }

    /* don't close anything until all ranks are done using handles */
    MPI_Barrier(MPI_COMM_WORLD);

out_err:
    tmp_rc = daos_cont_close(daos_args->src_coh, NULL);
    MPI_Barrier(MPI_COMM_WORLD);
    if (tmp_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to close container (%d)", rc);
        rc = 1;
    }

err_cont:
    tmp_rc = daos_pool_disconnect(daos_args->src_poh, NULL);
    MPI_Barrier(MPI_COMM_WORLD);
    if (tmp_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to disconnect pool (%d)", rc);
        rc = 1;
    }

out:
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
