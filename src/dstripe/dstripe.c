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

// mpicc -g -O0 -o restripe restripe.c -llustreapi

#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <getopt.h>
#include <string.h>
#include "mpi.h"

#ifdef LUSTRE_SUPPORT
#include <lustre/lustreapi.h>
#endif

#include "mfu.h"

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dstripe [options] PATH...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -c, --count            - stripe count (default -1)\n");
    printf("  -s, --size             - stripe size in bytes (default 1MB)\n");
    printf("  -r, --report           - input file stripe info\n");
    printf("  -v, --verbose          - verbose output\n");
    printf("  -h, --help             - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

static void generate_suffix(char *suffix, const int len)
{
    const char set[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    int numchars = len - 1;

    suffix[0] = '.';

    for (int i = 1; i < numchars; i++) {
        int set_idx = (double)rand() / RAND_MAX * (sizeof(set) - 1);
        suffix[i] = set[set_idx];
    }

    suffix[len - 1] = '\0';
}

static mfu_flist filter_list(mfu_flist list, int stripe_count, int stripe_size)
{
    MPI_Barrier(MPI_COMM_WORLD);
    mfu_flist filtered = mfu_flist_subset(list);

#ifdef LUSTRE_SUPPORT
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);
    int rc = 0;

    /* account for different versions of the lov_user_md struct */
    int lumsz = lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
    struct lov_user_md *lum = NULL;

    lum = malloc(lumsz);
    if (lum == NULL) {
        printf("ran out of memory");
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    for (idx = 0; idx < size; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(list, idx);
        if (type == MFU_TYPE_FILE) {
            const char* in_path = mfu_flist_file_get_name(list, idx);
            rc = llapi_file_get_stripe(in_path, lum);
            if (rc != 0) {
                free(lum);
                lum = NULL;
                printf("retrieving file stripe information for '%s' has failed, %s\n",
                    in_path, strerror(-rc));
                fflush(stdout);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }

            /* TODO: handle if a file is already in the list? */

            /* TODO: handle when stripe count is -1 */
            if (lum->lmm_stripe_count != stripe_count || lum->lmm_stripe_size != stripe_size) {
                mfu_flist_file_copy(list, idx, filtered);
            }
        }
    }

    free(lum);
    lum = NULL;
#endif

    mfu_flist_summarize(filtered);
    return filtered;
}

static void write_file_chunk(mfu_file_chunk* p, const char* out_path)
{
    size_t chunk_size = 1024*1024;
    uint64_t base = (off_t)p->offset;
    uint64_t file_size = (off_t)p->file_size;
    const char *in_path = p->name;
    uint64_t stripe_size = (off_t)p->length;

    if (file_size == 0 || stripe_size == 0) {
        return;
    }

    /* allocate buffer */
    void* buf = MFU_MALLOC(chunk_size);
    if (buf == NULL) {
        printf("Failed to allocate buffer\n");
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* open input file for reading */
    int in_fd = mfu_open(in_path, O_RDONLY);
    if (in_fd < 0) {
        printf("Failed to open input file %s (%s)\n", in_path, strerror(errno));
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* open output file for writing */
    int out_fd = mfu_open(out_path, O_WRONLY);
    if (out_fd < 0) {
        printf("Failed to open output file %s (%s)\n", out_path, strerror(errno));
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* write data */
    uint64_t chunk_id = 0;
    uint64_t stripe_read = 0;
    while (stripe_read < stripe_size) {
        /* determine number of bytes to read */
        /* try to read a full chunk's worth of bytes */
        size_t read_size = chunk_size;

        /* if the stripe doesn't have that much left */
        uint64_t remainder = stripe_size - stripe_read;
        if (remainder < (uint64_t) read_size) {
            read_size = (size_t) remainder;
        }

        /* get byte offset to read from */
        uint64_t offset = base + (chunk_id * chunk_size);
        if (offset < file_size) {
            /* the first byte falls within the file size,
             * now check the last byte */
            uint64_t last = offset + (uint64_t) read_size;
            if (last > file_size) {
                /* the last byte is beyond the end, set read size
                 * to the most we can read */
                read_size = (size_t) (file_size - offset);
            }
        } else {
            /* the first byte we need to read is past the end of
             * the file, so don't read anything */
            read_size = 0;
        }

        /* bail if we don't have anything to read */
        if (read_size == 0) {
            break;
        }

        /* seek to correct spot in input file */
        off_t pos = (off_t) offset;
        off_t seek_rc = mfu_lseek(in_path, in_fd, pos, SEEK_SET);
        if (seek_rc == (off_t)-1) {
            printf("Failed to seek in input file %s (%s)\n", in_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* read chunk from input */
        ssize_t nread = mfu_read(in_path, in_fd, buf, read_size);

        /* check for errors */
        if (nread < 0) {
            printf("Failed to read data from input file %s (%s)\n", in_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* check for short reads */
        if (nread != read_size) {
            printf("Got a short read from input file %s\n", in_path);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* seek to correct spot in output file */
        seek_rc = mfu_lseek(out_path, out_fd, pos, SEEK_SET);
        if (seek_rc == (off_t)-1) {
            printf("Failed to seek in output file %s (%s)\n", out_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* write chunk to output */
        ssize_t nwrite = mfu_write(out_path, out_fd, buf, read_size);

        /* check for errors */
        if (nwrite < 0) {
            printf("Failed to write data to output file %s (%s)\n", out_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* check for short reads */
        if (nwrite != read_size) {
            printf("Got a short write to output file %s\n", out_path);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* go on to the next chunk in this stripe, we assume we
         * read the whole chunk size, if we didn't it's because
         * the stripe is smaller or we're at the end of the file,
         * but in either case we're done so it doesn't hurt to
         * over estimate in this calculation */
        stripe_read += (uint64_t) chunk_size;
        chunk_id++;
    }

    /* close files */
    mfu_fsync(out_path, out_fd);
    mfu_close(out_path, out_fd);
    mfu_close(in_path, in_fd);

    /* free buffer */
    mfu_free(&buf);
}

static void flist_dstripe_report(mfu_flist list)
{
#ifdef LUSTRE_SUPPORT
    MPI_Barrier(MPI_COMM_WORLD);

    uint64_t idx;
    uint64_t size = mfu_flist_size(list);
    int rc = 0;

    /* account for different versions of the lov_user_md struct */
    int lumsz = lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
    struct lov_user_md *lum = NULL;

    lum = malloc(lumsz);
    if (lum == NULL) {
        printf("ran out of memory");
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    for (idx = 0; idx < size; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(list, idx);
        if (type == MFU_TYPE_FILE) {
            const char* in_path = mfu_flist_file_get_name(list, idx);
            rc = llapi_file_get_stripe(in_path, lum);
            if (rc != 0) {
                free(lum);
                lum = NULL;
                printf("retrieving file stripe information for '%s' has failed, %s\n",
                    in_path, strerror(-rc));
                fflush(stdout);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }

            /* print stripe count and stripe size */
            printf("File \"%s\" has a stripe count of %d and a stripe size of %d bytes.\n",
                       in_path, lum->lmm_stripe_count, lum->lmm_stripe_size);
            fflush(stdout);
        }
    }

    free(lum);
    lum = NULL;
#endif
}

int main(int argc, char* argv[])
{
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and number of ranks in the job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    uint64_t idx;
    int option_index = 0;
    int usage = 0;
    int report = 0;
    int verbose = 0;
    int stripes = -1;
    unsigned long long stripe_size = 1048576;
    unsigned int numpaths = 0;
    mfu_param_path* paths = NULL;

    static struct option long_options[] = {
        {"count",    1, 0, 'c'},
        {"size",     1, 0, 's'},
        {"help",     0, 0, 'h'},
        {"report",   0, 0, 'r'},
        {0, 0, 0, 0}
    };

    while (1) {
        int c = getopt_long(argc, argv, "c:s:rhv",
                    long_options, &option_index);

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'c':
                /* stripe count */
                stripes = atoi(optarg);
                break;
            case 's':
                /* stripe size in bytes */
                if (mfu_abtoull(optarg, &stripe_size) != MFU_SUCCESS) {
                    if (rank == 0) {
                        printf("Failed to parse stripe size: %s\n", optarg);
                        fflush(stdout);
                    }
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }
                break;
            case 'r':
                /* report striping info */
		report = 1;
                break;
            case 'v':
                /* verbose output */
                verbose = 1;
                break;
            case 'h':
                /* display usage */
                usage = 1;
                break;
            case '?':
                /* display usage */
                usage = 1;
                break;
            default:
                if (rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* paths to walk come after the options */
    if (optind < argc) {
        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        char** p = &argv[optind];
        mfu_param_path_set_all((uint64_t)numpaths, (const char**)p, paths);
        optind += numpaths;
    } else {
        usage = 1;
    }

    if (usage) {
        if (rank == 0) {
            print_usage();
        }

        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* nothing to do if lustre support is disabled */
#ifndef LUSTRE_SUPPORT
    if (rank == 0) {
        printf("Lustre support is disabled\n");
        fflush(stdout);
    }

    MPI_Abort(MPI_COMM_WORLD, 1);
#endif

    /* stripe count must be -1 for all available or greater than 0 */
    if (stripes < 0 && stripes != -1) {
        if (rank == 0) {
            printf("Stripe count must be -1 for all servers or a positive value\n");
            fflush(stdout);
        }

        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* lustre requires stripe sizes to be aligned */
    if (stripe_size % 65536 != 0) {
        if (rank == 0) {
            printf("Stripe size must be a multiple of 65536\n");
            fflush(stdout);
        }

        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* TODO: verify that source / target are on Lustre */

    /* walk list of input paths and stat as we walk */
    mfu_flist flist = mfu_flist_new();
    mfu_param_path_walk(numpaths, paths, 1, flist, 0);

    /* report the stripe count in all files we found */
    if (report) {
        flist_dstripe_report(flist);
        mfu_flist_free(&flist);
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }

    /* filter down our list to files which don't meet our striping requirements */
    mfu_flist filtered = filter_list(flist, stripes, stripe_size);
    mfu_flist_free(&flist);

    /* generate a global suffix for our temp files and have each node check it's list*/
    char suffix[8];
    uint64_t retry;

    srand(time(NULL));

    do {
        uint64_t attempt = 0;

        if (rank == 0) {
            generate_suffix(suffix, sizeof(suffix));
        }

        MPI_Bcast(suffix, sizeof(suffix), MPI_CHAR, 0, MPI_COMM_WORLD);

        uint64_t size = mfu_flist_size(filtered);
        for (idx = 0; idx < size; idx++) {
            char temp_path[PATH_MAX];
            strcpy(temp_path, mfu_flist_file_get_name(filtered, idx));
            strcat(temp_path, suffix);
            if(!mfu_access(temp_path, F_OK)) {
                attempt = 1;
                break;
            }
        }

        MPI_Allreduce(&attempt, &retry, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
    } while(retry != 0);

    uint64_t size = mfu_flist_size(filtered);
#ifdef LUSTRE_SUPPORT
    /* create new files so we can restripe */
    for (idx = 0; idx < size; idx++) {
        char temp_path[PATH_MAX];
        strcpy(temp_path, mfu_flist_file_get_name(filtered, idx));
        strcat(temp_path, suffix);

        /* set striping params on new file */
        int rc = llapi_file_create(temp_path, stripe_size, 0, stripes, LOV_PATTERN_RAID0);
        if (rc < 0) {
            printf("file creation has failed, %s\n", strerror(-rc));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
#endif

    /* found a suffix, now we need to break our files into chunks based on stripe size */
    mfu_file_chunk* file_chunks = mfu_file_chunk_list_alloc(filtered, stripe_size);
    mfu_file_chunk* p = file_chunks;
    while(p != NULL) {
        char temp_path[PATH_MAX];
        strcpy(temp_path, p->name);
        strcat(temp_path, suffix);

        write_file_chunk(p, temp_path);
        p = p->next;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* remove input file and rename temp file */
    for (idx = 0; idx < size; idx++) {
        mode_t mode = (mode_t) mfu_flist_file_get_mode(filtered, idx);
        const char *in_path = mfu_flist_file_get_name(filtered, idx);
        char out_path[PATH_MAX];
        strcpy(out_path, in_path);
        strcat(out_path, suffix);

        if (mfu_chmod(out_path, (mode_t) mode) != 0) {
            printf("Failed to chmod file %s (%s)", out_path, strerror(errno));
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        if (mfu_unlink(in_path) != 0) {
            printf("Failed to remove input file %s\n", in_path);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        if (rename(out_path, in_path) != 0) {
            printf("Failed to rename file %s to %s\n", out_path, in_path);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* wait for everyone to finish */
    MPI_Barrier(MPI_COMM_WORLD);

    mfu_file_chunk_list_free(&file_chunks);
    mfu_flist_free(&filtered);
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
