#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <openssl/sha.h>
#include <assert.h>
#include <inttypes.h>
#include "mpi.h"
#include "dtcmp.h"
#include "mfu.h"
#include "list.h"

/* number of uint64_t values in our key
 * 1 for group ID + (SHA256_DIGEST_LENGTH / 8) */
#define DDUP_KEY_SIZE 5

/* amount of data to read in order to compute hash */
#define DDUP_CHUNK_SIZE 1048576

/* Print a usage message */
static void print_usage(void)
{
    printf("\n");
    printf("Usage: ddup <dir>\n");
    printf("\n");
    printf("Options:\n");
    printf("  -d, --debug <DEBUG>  - set verbosity, one of: fatal,err,warn,info,dbg\n");
    printf("  -v, --verbose        - verbose output\n");
    printf("  -q, --quiet          - quiet output\n");
    printf("  -h, --help           - print usage\n");
    printf("\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    fflush(stdout);
}

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
    int fd = mfu_open(fname, O_RDONLY);
    if (fd < 0) {
        return -1;
    }

    /* seek to the correct offset */
    if (mfu_lseek(fname, fd, (off_t)offset, SEEK_SET) == (off_t) - 1) {
        status = -1;
        goto out;
    }

    /* read data from file */
    ssize_t read_size = mfu_read(fname, fd, chunk_buf, chunk_size);
    if (read_size < 0) {
        /* read failed */
        status = -1;
        goto out;
    }

    /* compute number of bytes we expect to read */
    ssize_t expect_size = (ssize_t) chunk_size;
    if (chunk_id * chunk_size > file_size) {
        if (offset >= file_size) {
          /* have gone past the end of the file, expect to read 0 */
          expect_size = 0;
        } else {
          /* read partial chunk */
          expect_size = (ssize_t) (file_size - offset);
        }
    }

    /* check that we read all bytes we expected */
    if (read_size != expect_size) {
        /* File size has been changed */
        status = -1;
        goto out;
    }

    /* return number of bytes read */
    *data_size = (uint64_t)read_size;

out:
    /* close our file and return */
    mfu_close(fname, fd);
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
    uint64_t file_size;

    uint64_t chunk_size = DDUP_CHUNK_SIZE;

    SHA256_CTX* ctx_ptr;

    MPI_Init(NULL, NULL);
    mfu_init();

    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    mfu_debug_level = MFU_LOG_VERBOSE;

    static struct option long_options[] = {
        {"debug",    0, 0, 'd'},
        {"verbose",  0, 0, 'v'},
        {"quiet",    0, 0, 'q'},
        {"help",     0, 0, 'h'},
        {0, 0, 0, 0}
    };

    /* Parse options */
    int usage = 0;
    int help  = 0;
    int c;
    int option_index = 0;
    while ((c = getopt_long(argc, argv, "d:vqh", \
                            long_options, &option_index)) != -1)
    {
        switch (c) {
        case 'd':
            if (strncmp(optarg, "fatal", 5) == 0) {
                mfu_debug_level = MFU_LOG_FATAL;

                if (rank == 0)
                    MFU_LOG(MFU_LOG_INFO,
                              "Debug level set to: fatal");
            }
            else if (strncmp(optarg, "err", 3) == 0) {
                mfu_debug_level = MFU_LOG_ERR;

                if (rank == 0)
                    MFU_LOG(MFU_LOG_INFO,
                              "Debug level set to: "
                              "errors");
            }
            else if (strncmp(optarg, "warn", 4) == 0) {
                mfu_debug_level = MFU_LOG_WARN;

                if (rank == 0)
                    MFU_LOG(MFU_LOG_INFO,
                              "Debug level set to: "
                              "warnings");
            }
            else if (strncmp(optarg, "info", 4) == 0) {
                mfu_debug_level = MFU_LOG_INFO;

                if (rank == 0)
                    MFU_LOG(MFU_LOG_INFO,
                              "Debug level set to: info");
            }
            else if (strncmp(optarg, "dbg", 3) == 0) {
                mfu_debug_level = MFU_LOG_DBG;

                if (rank == 0)
                    MFU_LOG(MFU_LOG_INFO,
                              "Debug level set to: debug");
            }
            else {
                if (rank == 0)
                    MFU_LOG(MFU_LOG_INFO,
                              "Debug level `%s' not "
                              "recognized. Defaulting to "
                              "`info'.", optarg);
            }
        case 'h':
            usage = 1;
            help  = 1;
        case 'v':
            mfu_debug_level = MFU_LOG_VERBOSE;
            break;
        case 'q':
            mfu_debug_level = MFU_LOG_NONE;
            break;
        case '?':
            usage = 1;
            help  = 1;
            break;
        default:
            usage = 1;
            break;
        }
    }

    /* check that user gave us one and only one directory */
    int numargs = argc - optind;
    if (numargs != 1) {
        /* missing the directory, so post a message, and print usage */
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "You must specify a directory path");
        }
        usage = 1;
    }

    /* print usage and bail if needed */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        /* set error code base on whether user requested usage or not */
        if (help) {
            status = 0;
        } else {
            status = -1;
        }
        MPI_Barrier(MPI_COMM_WORLD);
        goto out;
    }

    /* get the directory name */
    const char* dir = argv[optind];

    /* create MPI datatypes */
    MPI_Datatype key;
    MPI_Datatype keysat;
    mpi_type_init(&key, &keysat);

    /* create DTCMP comparison operation */
    DTCMP_Op cmp;
    mtcmp_cmp_init(&cmp);

    /* allocate buffer to read data from file */
    char* chunk_buf = (char*)MFU_MALLOC(DDUP_CHUNK_SIZE);

    /* allocate a file list */
    mfu_flist flist = mfu_flist_new();

    /* create new mfu_file object */
    mfu_file_t* mfu_file = mfu_file_new();

    /* Walk the path(s) to build the flist */
    mfu_flist_walk_path(dir, walk_opts, flist, mfu_file);

    /* TODO: spread list among procs? */

    /* get local number of items in flist */
    uint64_t checking_files = mfu_flist_size(flist);

    /* allocate memory to hold SHA256 context values */
    struct file_item* file_items = (struct file_item*) MFU_MALLOC(checking_files * sizeof(*file_items));

    /* Allocate two lists of length size, where each
     * element has (DDUP_KEY_SIZE + 1) uint64_t values
     * (id, checksum, index)
     */
    size_t list_bytes = checking_files * (DDUP_KEY_SIZE + 1) * sizeof(uint64_t);
    uint64_t* list     = (uint64_t*) MFU_MALLOC(list_bytes);
    uint64_t* new_list = (uint64_t*) MFU_MALLOC(list_bytes);

    /* Initialize the list */
    uint64_t* ptr = list;
    uint64_t new_checking_files = 0;
    for (i = 0; i < checking_files; i++) {
        /* check that item is a regular file */
        mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, i);
        if (! S_ISREG(mode)) {
            continue;
        }

        /* get the file size */
        file_size = mfu_flist_file_get_size(flist, i);
        if (file_size == 0) {
            /* Files with size zero are not interesting at all */
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

    /* reduce our list count based on any files filtered out above */
    checking_files = new_checking_files;

    /* allocate arrays to hold result from DTCMP_Rankv call to
     * assign group and rank values to each item */
    uint64_t output_bytes = checking_files * sizeof(uint64_t);
    uint64_t* group_id    = (uint64_t*) MFU_MALLOC(output_bytes);
    uint64_t* group_ranks = (uint64_t*) MFU_MALLOC(output_bytes);
    uint64_t* group_rank  = (uint64_t*) MFU_MALLOC(output_bytes);

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
            uint64_t idx = ptr[DDUP_KEY_SIZE];

            /* look up file name */
            const char* fname = mfu_flist_file_get_name(flist, idx);

            /* look up file size */
            file_size = mfu_flist_file_get_size(flist, idx);

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
            ctx_ptr = &file_items[idx].ctx;
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

        /* any files assigned to a group of size 1 is unique,
         * any files in groups sizes > 1 for which we've read
         * all bytes are the same, and filter all other files
         * into a new list for another iteration */
        new_checking_files = 0;
        ptr = list;
        uint64_t* new_ptr = new_list;
        for (i = 0; i < checking_files; i++) {
            /* Get index into flist for this item */
            uint64_t idx = ptr[DDUP_KEY_SIZE];

            /* look up file name */
            const char* fname = mfu_flist_file_get_name(flist, idx);

            /* look up file size */
            file_size = mfu_flist_file_get_size(flist, idx);

            /* get a pointer to the SHA256 context for this file */
            ctx_ptr = &file_items[idx].ctx;

            if (group_ranks[i] == 1) {
                /*
                 * Only one file in this group,
                 * mfu_flist_file_name(flist, idx) is unique
                 */
            } else if (file_size <= (chunk_id * chunk_size)) {
                /*
                 * We've run out of bytes to checksum, and we
                 * still have a group size > 1
                 * mfu_flist_file_name(flist, idx) is a
                 * duplicate with other files that also have
                 * matching group_id[i]
                 */
                unsigned char digest[SHA256_DIGEST_LENGTH];
                SHA256_Final(digest, ctx_ptr);

                char digest_string[SHA256_DIGEST_LENGTH * 2 + 1];
                dump_sha256_digest(digest_string, digest);
                printf("%s %s\n", fname, digest_string);
            } else {
                /* Have multiple files with the same checksum,
                 * but still have bytes left to read, so keep
                 * this file
                 */

                /* use new group ID to segregate files,
                 * this id will be unique for all files of the
                 * same size and having the same hash up to
                 * this point */
                new_ptr[0] = group_id[i];

                /* Copy over flist index into new list entry */
                new_ptr[DDUP_KEY_SIZE] = idx;

                /* got one more in the new list */
                new_checking_files++;

                /* move on to next item in new list */
                new_ptr += DDUP_KEY_SIZE + 1;

                MFU_LOG(MFU_LOG_DBG, "checking file "
                          "\"%s\" for chunk index %d of size %"
                          PRIu64"\n", fname, (int)chunk_id,
                          chunk_size);
            }

            /* move on to next file in the list */
            ptr += DDUP_KEY_SIZE + 1;
        }

        /* Swap lists */
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

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* delete file objects */
    mfu_file_delete(&mfu_file);

    mfu_free(&group_rank);
    mfu_free(&group_ranks);
    mfu_free(&group_id);
    mfu_free(&new_list);
    mfu_free(&list);
    mfu_free(&file_items);
    mfu_free(&chunk_buf);
    mfu_flist_free(&flist);

    mtcmp_cmp_fini(&cmp);
    mpi_type_fini(&key, &keysat);

    status = 0;

out:
    mfu_finalize();
    MPI_Finalize();

    return status;
}
