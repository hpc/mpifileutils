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

/**
 * @file dtar.c - parallel tar main file
 *
 * @author - Feiyi Wang
 *
 *
 */
#include <string.h>
#include <getopt.h>
#include "common.h"


/* The opts_blocksize option is optional and is used to specify
 * a blocksize for compression.
 * The opts_memory option is optional and is used to specify
 * memory limit for compression for each process in compression.
 * The opts_compress option is used to specify whether
 * compression/decompression should be used */

static int     opts_create    = 0;
static int     opts_compress  = 0;
static int     opts_verbose   = 0;
static int     opts_debug     = 0;
static int     opts_extract   = 0;
static int     opts_preserve  = 0;
static char*   opts_tarfile   = NULL;
static size_t  opts_chunksize = 1024 * 1024;
static int     opts_blocksize = 9;
static ssize_t opts_memory    = -1;

static void print_usage(void)
{
    printf("\n");
    printf("Usage: drm [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -c, --create            - create archive\n");
    printf("  -j, --compress          - compress archive\n");
    printf("  -x, --extract           - extract archive\n");
    printf("  -p, --preserve          - preserve attributes\n");
    printf("  -s, --chunksize <bytes> - chunk size (bytes)\n");
    printf("  -f, --file <filename>   - target output file\n");
    printf("  -b, --blocksize <size>  - block size (1-9)\n");
    printf("  -m, --memory <bytes>    - memory limit (bytes)\n");
    printf("  -v, --verbose           - verbose output\n");
    printf("  -y, --debug             - debug output\n");
    printf("  -h, --help              - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

int DTAR_global_rank;
DTAR_options_t DTAR_user_opts;
DTAR_writer_t DTAR_writer;
DTAR_statistics_t DTAR_statistics;
mfu_flist DTAR_flist;
uint64_t* DTAR_fsizes = NULL;
uint64_t* DTAR_offsets = NULL;
uint64_t DTAR_total = 0;
uint64_t DTAR_count = 0;
uint64_t DTAR_goffset = 0;
int DTAR_rank, DTAR_size;

static void process_flist(void)
{
    uint64_t idx;
    for (idx = 0; idx < DTAR_count; idx++) {
        DTAR_fsizes[idx] = 0;
        mfu_filetype type = mfu_flist_file_get_type(DTAR_flist, idx);
        if (type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            DTAR_fsizes[idx] = DTAR_HDR_LENGTH;
        }
        else if (type == MFU_TYPE_FILE) {
            uint64_t fsize = mfu_flist_file_get_size(DTAR_flist, idx);
            uint64_t rem = (fsize) % 512;
            if (rem == 0) {
                DTAR_fsizes[idx] = fsize + DTAR_HDR_LENGTH;
            }
            else {
                DTAR_fsizes[idx] = (fsize / 512 + 4) * 512;
            }

        }

        DTAR_offsets[idx] = DTAR_total;
        DTAR_total += DTAR_fsizes[idx];
    }
}


static void update_offsets(void)
{
    for (uint64_t idx = 0; idx < DTAR_count; idx++) {
        DTAR_offsets[idx] += DTAR_goffset;
    }
}

static void create_archive(char* filename)
{
    DTAR_writer_init();

    char** paths = (char**) malloc(sizeof(char*) * num_src_params);

    /* walk path to get stats info on all files */
    DTAR_flist = mfu_flist_new();
    for (int i = 0; i < num_src_params; i++) {
        mfu_flist_walk_path(src_params[i].path, 1, 0, DTAR_flist);
    }

    DTAR_count = mfu_flist_size(DTAR_flist);

    /* allocate memory for DTAR_fsizes */
    DTAR_fsizes  = (uint64_t*) MFU_MALLOC(DTAR_count * sizeof(uint64_t));
    DTAR_offsets = (uint64_t*) MFU_MALLOC(DTAR_count * sizeof(uint64_t));

    /* calculate size, offset for each file as well as global offset*/
    process_flist();
    MPI_Scan(&DTAR_total, &DTAR_goffset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    DTAR_goffset -= DTAR_total;
    update_offsets();

    /* write header first*/
    struct archive* ar = DTAR_new_archive();
    archive_write_open_fd(ar, DTAR_writer.fd_tar);

    for (uint64_t idx = 0; idx < DTAR_count; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(DTAR_flist, idx);
        if (type == MFU_TYPE_FILE || type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            DTAR_write_header(ar, idx, DTAR_offsets[idx]);
        }
    }

    DTAR_global_rank = CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL);
    CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    CIRCLE_enable_logging(loglevel);

    /* register callbacks */
    CIRCLE_cb_create(&DTAR_enqueue_copy);
    CIRCLE_cb_process(&DTAR_perform_copy);

    /* run the libcircle job */
    CIRCLE_begin();
    CIRCLE_finalize();

    uint64_t archive_size = 0;
    MPI_Allreduce(&DTAR_total, &archive_size, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    DTAR_statistics.total_size = archive_size;

    /* clean up */
    archive_write_free(ar);
    mfu_free(&DTAR_fsizes);
    mfu_free(&DTAR_offsets);
    mfu_flist_free(&DTAR_flist);
    mfu_close(DTAR_writer.name, DTAR_writer.fd_tar);
}

static void errmsg(const char* m)
{
    fprintf(stderr, "%s\n", m);
}

static void msg(const char* m)
{
    fprintf(stdout, "%s", m);
}

static int copy_data(struct archive* ar, struct archive* aw)
{
    int r;
    const void* buff;
    size_t size;
    off_t offset;
    for (;;) {
        r = archive_read_data_block(ar, &buff, &size, &offset);
        if (r == ARCHIVE_EOF) {
            return (ARCHIVE_OK);
        }
        if (r != ARCHIVE_OK) {
            return r;
        }

        r = archive_write_data_block(aw, buff, size, offset);
        if (r != ARCHIVE_OK) {
            errmsg(archive_error_string(ar));
            return r;
        }
    }
    return 0;
}

static void extract_archive(const char* filename, bool verbose, int flags)
{
    int r;

    /* initiate archive object for reading */
    struct archive* a = archive_read_new();

    /* initiate archive object for writing */
    struct archive* ext = archive_write_disk_new();
    archive_write_disk_set_options(ext, flags);

    /* we want all the format supports */
    archive_read_support_filter_bzip2(a);
    archive_read_support_filter_gzip(a);
    archive_read_support_filter_compress(a);
    archive_read_support_format_tar(a);

    archive_write_disk_set_standard_lookup(ext);

    if (filename != NULL && strcmp(filename, "-") == 0) {
        filename = NULL;
    }

    /* blocksize set to 1024K */
    if ((r = archive_read_open_filename(a, filename, 10240))) {
        errmsg(archive_error_string(a));
        exit(r);
    }

    struct archive_entry* entry;
    for (;;) {
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            break;
        }
        if (r != ARCHIVE_OK) {
            errmsg(archive_error_string(a));
            exit(r);
        }

        if (verbose) {
            msg("x ");
        }

        if (verbose) {
            msg(archive_entry_pathname(entry));
        }

        r = archive_write_header(ext, entry);
        if (r != ARCHIVE_OK) {
            errmsg(archive_error_string(a));
        } else {
            copy_data(a, ext);
        }

        if (verbose) {
            msg("\n");
        }
    }

    archive_read_close(a);
    archive_read_free(a);
}

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    mfu_init();

    MPI_Comm_rank(MPI_COMM_WORLD, &DTAR_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &DTAR_size);

    int option_index = 0;
    static struct option long_options[] = {
        {"create",    0, 0, 'c'},
        {"compress",  0, 0, 'j'},
        {"extract",   0, 0, 'x'},
        {"preserve",  0, 0, 'p'},
        {"chunksize", 1, 0, 's'},        
        {"file",      1, 0, 'f'},
        {"blocksize", 1, 0, 'b'},
        {"memory",    1, 0, 'm'},
        {"verbose",   0, 0, 'v'},
        {"debug",     0, 0, 'y'},
        {"help",      0, 0, 'h'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "cjxps:f:b:m:vyh",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        unsigned long long bytes;
        switch (c) {
            case 'c':
                opts_create = 1;
                break;
            case 'j':
                opts_compress = 1;
                break;
            case 'x':
                opts_extract = 1;
                break;
            case 'p':
                opts_preserve = 1;
                break;
            case 's':
                mfu_abtoull(optarg, &bytes);
                opts_chunksize = (size_t) bytes;
                break;
            case 'f':
                opts_tarfile = MFU_STRDUP(optarg);
                break;
            case 'b':
                opts_blocksize = atoi(optarg);
                break;
            case 'm':
                mfu_abtoull(optarg, &bytes);
                opts_memory = (ssize_t) bytes;
                break;
            case 'v':
                opts_verbose = 1;
                break;
            case 'd':
                opts_debug = 1;
                break;
            case 'h':
                usage = 1;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if (DTAR_rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    if (opts_debug) {
        mfu_debug_level = MFU_LOG_DBG;
    } else if (opts_verbose) {
        mfu_debug_level = MFU_LOG_INFO;
    } else {
        mfu_debug_level = MFU_LOG_ERR;
    }

    if (!opts_create && !opts_extract && DTAR_global_rank == 0) {
        MFU_LOG(MFU_LOG_ERR, "One of extract(x) or create(c) need to be specified");
        DTAR_exit(EXIT_FAILURE);
    }

    if (opts_create && opts_extract && DTAR_global_rank == 0) {
        MFU_LOG(MFU_LOG_ERR, "Only one of extraction(x) or create(c) can be specified");
        DTAR_exit(EXIT_FAILURE);
    }

    /* done by default */
    DTAR_user_opts.flags  = ARCHIVE_EXTRACT_TIME;
    DTAR_user_opts.flags |= ARCHIVE_EXTRACT_OWNER;
    DTAR_user_opts.flags |= ARCHIVE_EXTRACT_PERM;
    DTAR_user_opts.flags |= ARCHIVE_EXTRACT_ACL;
    DTAR_user_opts.flags |= ARCHIVE_EXTRACT_FFLAGS;

    if (opts_preserve) {
        DTAR_user_opts.flags |= ARCHIVE_EXTRACT_XATTR;
        DTAR_user_opts.preserve = 1;
        if (DTAR_global_rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Creating archive with extended attributes");
        }
    }

    DTAR_user_opts.chunk_size = opts_chunksize;

    /* init statistics */
    DTAR_statistics.total_dirs  = 0;
    DTAR_statistics.total_files = 0;
    DTAR_statistics.total_links = 0;
    DTAR_statistics.total_size  = 0;
    DTAR_statistics.total_bytes_copied = 0;

    if (DTAR_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Chunk size = %" PRIu64, DTAR_user_opts.chunk_size);
    }

    time(&(DTAR_statistics.time_started));
    DTAR_statistics.wtime_started = MPI_Wtime();
    if (opts_compress && opts_create) {
        DTAR_parse_path_args(argc, argv, opts_tarfile);
        struct stat st;
        char fname[50];
        char fname1[50];
        strncpy(fname1, opts_tarfile, 50);
        strncpy(fname, opts_tarfile, 50);
        if ((stat(strcat(fname, ".bz2"), &st) == 0)) {
            if (DTAR_rank == 0) {
                printf("Output file already exists\n");
            }
            exit(0);
        }
        create_archive(opts_tarfile);
        //dbz2_compress(opts_blocksize, opts_tarfile, opts_memory);
        remove(fname1);
    }
    else if (opts_create) {
        DTAR_parse_path_args(argc, argv, opts_tarfile);
        create_archive(opts_tarfile);
    }
    else if (opts_extract && opts_compress) {
        char fname[50];
        char fname_out[50];
        strncpy(fname, opts_tarfile, 50);
        strncpy(fname_out, opts_tarfile, 50);
        size_t len = strlen(fname_out);
        fname_out[len - 4] = '\0';
        printf("The file name is:%s %s %d", fname, fname_out, (int)len);
        struct stat st;
        if ((stat(fname_out, &st) == 0)) {
            if (DTAR_rank == 0) {
                printf("Output file already exists\n");
            }
            exit(0);
        }
        //decompress(fname, fname_out);
        remove(fname);
        extract_archive(fname_out, opts_verbose, DTAR_user_opts.flags);
    }
    else if (opts_extract) {
        extract_archive(opts_tarfile, opts_verbose, DTAR_user_opts.flags);
    }
    else {
        if (DTAR_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Neither creation or extraction is specified");
            DTAR_exit(EXIT_FAILURE);
        }
    }

    DTAR_statistics.wtime_ended = MPI_Wtime();
    time(&(DTAR_statistics.time_ended));

    /* free context */
    MFU_LOG(MFU_LOG_ERR, "Rank %d before epilogue\n", DTAR_rank);
    DTAR_epilogue();
    DTAR_exit(EXIT_SUCCESS);

    return 0;
}
