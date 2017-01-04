/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   Written by Adam Moody <moody20@llnl.gov>.
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2016, DataDirect Networks, Inc.
 *
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <openssl/sha.h>
#include <assert.h>
#include <inttypes.h>
#include "mpi.h"
#include "dtcmp.h"
#include "bayer.h"
#include "list.h"

/* number of uint64_t values in our key
 * 1 for group ID + (SHA256_DIGEST_LENGTH / 8) */
#define DDUP_KEY_SIZE 5

/* amount of data to read in order to compute hash */
#define DDUP_CHUNK_SIZE 1048576

/* create MPI datatypes for key and key and satellite data */
static void mpi_type_init(MPI_Datatype* key, MPI_Datatype* keysat)
{
    assert(SHA256_DIGEST_LENGTH == (DDUP_KEY_SIZE - 1) * 8);

    /*
     * Build MPI datatype for key.
     * 1 for group ID + (SHA256_DIGEST_LENGTH / 8)
     */
    MPI_Type_contiguous(DDUP_KEY_SIZE, MPI_UINT64_T, key);
    MPI_Type_commit(key);

    /*
     * Build MPI datatype for key + satellite
     * length of key + 1 for index in flist
     */
    MPI_Type_contiguous(DDUP_KEY_SIZE + 1, MPI_UINT64_T, keysat);
    MPI_Type_commit(keysat);
}

/* free off the MPI types */
static void mpi_type_fini(MPI_Datatype* key, MPI_Datatype* keysat)
{
    MPI_Type_free(keysat);
    MPI_Type_free(key);
}

/* create a comparison operation for our key */
static int mtcmp_cmp_init(DTCMP_Op* cmp)
{
    int i;
    DTCMP_Op series[DDUP_KEY_SIZE];
    for (i = 0; i < DDUP_KEY_SIZE; i++) {
        series[i] = DTCMP_OP_UINT64T_ASCEND;
    }
    return DTCMP_Op_create_series(DDUP_KEY_SIZE, series, cmp);
}

/* free the comparison operation */
static void mtcmp_cmp_fini(DTCMP_Op* cmp)
{
    DTCMP_Op_free(cmp);
}

/* open the specified file, read specified chunk, and close file,
 * returns -1 on any read error */
static int read_data(const char* fname, char* chunk_buf, uint64_t chunk_id,
                     uint64_t chunk_size, uint64_t file_size,
                     uint64_t* data_size)
{
    int status = 0;

    assert(chunk_id > 0);

    /* compute byte offset to read from in file */
    uint64_t offset = (chunk_id - 1) * chunk_size;

    /* zero out our buffer */
    memset(chunk_buf, 0, chunk_size);

    /* open the file */
    int fd = bayer_open(fname, O_RDONLY);
    if (fd < 0) {
        return -1;
    }

    /* seek to the correct offset */
    if (bayer_lseek(fname, fd, offset, SEEK_SET) == (off_t) - 1) {
        status = -1;
        goto out;
    }

    /* read data from file */
    ssize_t read_size = bayer_read(fname, fd, chunk_buf, chunk_size);
    if (read_size < 0) {
        status = -1;
        goto out;
    }

    /* check that we read all bytes we'd expect to read */
    if (file_size >= chunk_id * chunk_size) {
        if ((uint64_t)read_size != chunk_size) {
            /* File size has been changed */
            status = -1;
            goto out;
        }
    } else {
        if ((uint64_t)read_size != file_size - offset) {
            /* File size has been changed */
            status = -1;
            goto out;
        }
    }

    /* return number of bytes read */
    *data_size = (uint64_t)read_size;

out:
    /* close our file and return */
    bayer_close(fname, fd);
    return status;
}

struct file_item {
    SHA256_CTX ctx;
};

/* print SHA256 value to stdout */
static void dump_sha256_digest(char* digest_string, unsigned char digest[])
{
    int i;
    for (i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        sprintf(&digest_string[i * 2], "%02x", (unsigned int)digest[i]);
    }
}

int main(int argc, char** argv)
{
    uint64_t i;
    int status;
    uint64_t index;
    uint64_t file_size;

    uint64_t chunk_size = DDUP_CHUNK_SIZE;

    SHA256_CTX* ctx_ptr;
    char digest_string[SHA256_DIGEST_LENGTH * 2 + 1];
    unsigned char digest[SHA256_DIGEST_LENGTH];

    MPI_Init(NULL, NULL);
    bayer_init();

    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    bayer_debug_level = BAYER_LOG_INFO;

    static struct option long_options[] = {
        {"debug",    0, 0, 'd'},
        {0, 0, 0, 0}
    };

    /* Parse options */
    int c;
    int option_index = 0;
    while ((c = getopt_long(argc, argv, "d:", \
                            long_options, &option_index)) != -1) {
        switch (c) {
            case 'd':
                if (strncmp(optarg, "fatal", 5) == 0) {
                    bayer_debug_level = BAYER_LOG_FATAL;

                    if (rank == 0)
                        BAYER_LOG(BAYER_LOG_INFO,
                                  "Debug level set to: fatal");
                }
                else if (strncmp(optarg, "err", 3) == 0) {
                    bayer_debug_level = BAYER_LOG_ERR;

                    if (rank == 0)
                        BAYER_LOG(BAYER_LOG_INFO,
                                  "Debug level set to: "
                                  "errors");
                }
                else if (strncmp(optarg, "warn", 4) == 0) {
                    bayer_debug_level = BAYER_LOG_WARN;

                    if (rank == 0)
                        BAYER_LOG(BAYER_LOG_INFO,
                                  "Debug level set to: "
                                  "warnings");
                }
                else if (strncmp(optarg, "info", 4) == 0) {
                    bayer_debug_level = BAYER_LOG_INFO;

                    if (rank == 0)
                        BAYER_LOG(BAYER_LOG_INFO,
                                  "Debug level set to: info");
                }
                else if (strncmp(optarg, "dbg", 3) == 0) {
                    bayer_debug_level = BAYER_LOG_DBG;

                    if (rank == 0)
                        BAYER_LOG(BAYER_LOG_INFO,
                                  "Debug level set to: debug");
                }
                else {
                    if (rank == 0)
                        BAYER_LOG(BAYER_LOG_INFO,
                                  "Debug level `%s' not "
                                  "recognized. Defaulting to "
                                  "`info'.", optarg);
                }
        }
        break;
    }

    /* check that user gave us a directory */
    if (argv[optind] == NULL) {
        if (rank == 0) {
            BAYER_LOG(BAYER_LOG_ERR,
                      "You must specify a directory path");
        }
        MPI_Barrier(MPI_COMM_WORLD);
        status = -1;
        goto out;
    }

    /* get the directory name */
    const char* dir = argv[optind];

    /* create MPI datatypes */
    MPI_Datatype key;
    MPI_Datatype keysat;
    mpi_type_init(&key, &keysat);

    /* create comparison operation */
    DTCMP_Op cmp;
    mtcmp_cmp_init(&cmp);

    /* allocate buffer to read data from file */
    char* chunk_buf = (char*)BAYER_MALLOC(DDUP_CHUNK_SIZE);

    /* allocate a file list */
    bayer_flist flist = bayer_flist_new();

    /* Walk the path(s) to build the flist */
    bayer_flist_walk_path(dir, 1, 0, flist);

    /* get local number of items in flist */
    uint64_t checking_files = bayer_flist_size(flist);

    /* allocate memory to hold SHA256 context values */
    struct file_item* file_items = (struct file_item*) BAYER_MALLOC(checking_files * sizeof(*file_items));

    /* Allocate two lists of length size, where each
     * element has (DDUP_KEY_SIZE + 1) uint64_t values
     * (id, checksum, index)
     */
    size_t list_bytes = checking_files * (DDUP_KEY_SIZE + 1) * sizeof(uint64_t);
    uint64_t* list     = (uint64_t*) BAYER_MALLOC(list_bytes);
    uint64_t* new_list = (uint64_t*) BAYER_MALLOC(list_bytes);

    /* Initialize the list */
    uint64_t* ptr = list;
    uint64_t new_checking_files = 0;
    for (i = 0; i < checking_files; i++) {
        /* check that we have a regular file */
        mode_t mode = (mode_t) bayer_flist_file_get_mode(flist, i);
        if (! S_ISREG(mode)) {
            continue;
        }

        /* get the file size */
        file_size = bayer_flist_file_get_size(flist, i);
        if (file_size == 0) {
            /* Files with size zero is not interesting at all */
            continue;
        }

        /* for first pass, group all files with same file size */
        ptr[0] = file_size;

        /* we'll leave the middle part of the key unset */

        /* record our index in flist */
        ptr[DDUP_KEY_SIZE] = i;

        /* initialize the SHA256 hash state for this file */
        SHA256_Init(&file_items[i].ctx);

        /* increment our file count */
        new_checking_files++;

        /* advance to next spot in the list */
        ptr += DDUP_KEY_SIZE + 1;
    }

    /* keep track of current list size */
    checking_files = new_checking_files;

    /* allocate arrays to hold result from DTCMP_Rankv call to
     * assign group and rank values to each item */
    uint64_t output_bytes = checking_files * sizeof(uint64_t);
    uint64_t* group_id    = (uint64_t*) BAYER_MALLOC(output_bytes);
    uint64_t* group_ranks = (uint64_t*) BAYER_MALLOC(output_bytes);
    uint64_t* group_rank  = (uint64_t*) BAYER_MALLOC(output_bytes);

    /* get total number of items across all tasks */
    uint64_t sum_checking_files;
    MPI_Allreduce(&checking_files, &sum_checking_files, 1,
                  MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    uint64_t chunk_id = 0;
    while (sum_checking_files > 1) {
        /* update the chunk id we'll read from all files */
        chunk_id++;

        /* iterate over our list and compute SHA256 value for each */
        ptr = list;
        for (i = 0; i < checking_files; i++) {
            /* get the flist index for this item */
            index = ptr[DDUP_KEY_SIZE];

            /* look up file name */
            const char* fname = bayer_flist_file_get_name(flist, index);

            /* look up file size */
            file_size = bayer_flist_file_get_size(flist, index);

            /* read a chunk of data from the file into chunk_buf */
            uint64_t data_size;
            status = read_data(fname, chunk_buf, chunk_id,
                               chunk_size, file_size, &data_size);
            if (status) {
                /* File size has been changed, TODO: handle */
                printf("failed to read file %s, maybe file "
                       "size has been modified during the "
                       "process", fname);
            }

            /* update the SHA256 context for this file */
            ctx_ptr = &file_items[index].ctx;
            SHA256_Update(ctx_ptr, chunk_buf, data_size);

            /*
             * Use SHA256 value as key.
             * This is actually an hack, but SHA256_Final can't
             * be called multiple times with out changing ctx
             */
            SHA256_CTX ctx_tmp;
            memcpy(&ctx_tmp, ctx_ptr, sizeof(ctx_tmp));
            SHA256_Final((unsigned char*)(ptr + 1), &ctx_tmp);

            /* move on to next file in the list */
            ptr += DDUP_KEY_SIZE + 1;
        }

        /* Assign group ids and compute group sizes */
        uint64_t groups;
        DTCMP_Rankv(
            (int)checking_files, list,
            &groups, group_id, group_ranks, group_rank,
            key, keysat, cmp, DTCMP_FLAG_NONE, MPI_COMM_WORLD
        );

        new_checking_files = 0;
        ptr = list;
        uint64_t* new_ptr = new_list;
        for (i = 0; i < checking_files; i++) {
            /* Get index into flist for this item */
            index = ptr[DDUP_KEY_SIZE];

            /* look up file name */
            const char* fname = bayer_flist_file_get_name(flist, index);

            /* look up file size */
            file_size = bayer_flist_file_get_size(flist, index);

            ctx_ptr = &file_items[index].ctx;
            if (group_ranks[i] == 1) {
                /*
                 * Only one file in this group,
                 * bayer_flist_file_name(flist, idx) is unique
                 */
            } else if (file_size < (chunk_id * chunk_size)) {
                /*
                 * We've run out of bytes to checksum, and we
                 * still have a group size > 1
                 * bayer_flist_file_name(flist, idx) is a
                 * duplicate with other files that also have
                 * matching group_id[i]
                 */
                SHA256_Final(digest, ctx_ptr);
                dump_sha256_digest(digest_string, digest);
                printf("%s %s\n", fname, digest_string);
            } else {
                /* Have multiple files with the same checksum,
                 * but still have bytes left to read, so keep
                 * this file
                 */
                /* use new group ID to segregate files */
                new_ptr[0] = group_id[i];

                /* Copy over flist index */
                new_ptr[DDUP_KEY_SIZE] = index;

                /* got one more in the new list */
                new_checking_files++;

                /* move on to next item in new list */
                new_ptr += DDUP_KEY_SIZE + 1;

                BAYER_LOG(BAYER_LOG_DBG, "checking file "
                          "\"%s\" for chunk index %d of size %"
                          PRIu64"\n", fname, chunk_id,
                          chunk_size);
            }

            /* move on to next file in the list */
            ptr += DDUP_KEY_SIZE + 1;
        }

        /* Swap list buffers */
        uint64_t* tmp_list;
        tmp_list = list;
        list     = new_list;
        new_list = tmp_list;

        /* Update size of current list */
        checking_files = new_checking_files;

        /* Get new global list size */
        MPI_Allreduce(&checking_files, &sum_checking_files, 1,
                      MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    }

    bayer_free(&group_rank);
    bayer_free(&group_ranks);
    bayer_free(&group_id);
    bayer_free(&new_list);
    bayer_free(&list);
    bayer_free(&file_items);
    bayer_free(&chunk_buf);
    bayer_flist_free(&flist);

    mtcmp_cmp_fini(&cmp);
    mpi_type_fini(&key, &keysat);

    status = 0;

out:
    bayer_finalize();
    MPI_Finalize();
    return status;
}
