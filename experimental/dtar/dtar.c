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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#define _LARGEFILE64_SOURCE

#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <mpi.h>
#include <libcircle.h>
#include <archive.h>
#include <archive_entry.h>
#include <string.h>
#include <getopt.h>

#include "mfu.h"

#define DTAR_HDR_LENGTH 1536

typedef enum {
    COPY_DATA
} DTAR_operation_code_t;

/* common structures */

typedef struct {
    size_t  chunk_size;
    size_t  block_size;
    char*   dest_path;
    bool    preserve;
    int     flags;
} DTAR_options_t;

typedef struct {
    const char* name;
    int fd_tar;
    int flags;
} DTAR_writer_t;

typedef struct {
    uint64_t total_dirs;
    uint64_t total_files;
    uint64_t total_links;
    uint64_t total_size;
    uint64_t total_bytes_copied;
    double  wtime_started;
    double  wtime_ended;
    time_t  time_started;
    time_t  time_ended;
} DTAR_statistics_t;

typedef struct {
    uint64_t file_size;
    uint64_t chunk_index;
    uint64_t offset;
    DTAR_operation_code_t code;
    char* operand;
} DTAR_operation_t;

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

mfu_param_path* src_params;
mfu_param_path dest_param;
int num_src_params;

static void DTAR_abort(int code)
{
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

static void DTAR_exit(int code)
{
    mfu_finalize();
    MPI_Finalize();
    exit(code);
}

mfu_flist DTAR_flist;
uint64_t* DTAR_offsets = NULL;
int DTAR_global_rank;
DTAR_options_t DTAR_user_opts;
DTAR_writer_t DTAR_writer;
DTAR_statistics_t DTAR_statistics;
uint64_t DTAR_count = 0;
int DTAR_rank, DTAR_size;

static void DTAR_write_header(struct archive* ar, uint64_t idx, uint64_t offset)
{
    /* allocate and entry for this item */
    struct archive_entry* entry = archive_entry_new();

    /* get file name for this item */
    /* fill up entry, FIXME: the uglyness of removing leading slash */
    const char* fname = mfu_flist_file_get_name(DTAR_flist, idx);
    archive_entry_copy_pathname(entry, &fname[1]);

    if (DTAR_user_opts.preserve) {
        struct archive* source = archive_read_disk_new();
        archive_read_disk_set_standard_lookup(source);
        int fd = open(fname, O_RDONLY);
        if (archive_read_disk_entry_from_file(source, entry, fd, NULL) != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "archive_read_disk_entry_from_file(): %s", archive_error_string(ar));
        }
        archive_read_free(source);
        close(fd);
    } else {
        /* TODO: read stat info from mfu_flist */
        struct stat stbuf;
        mfu_lstat(fname, &stbuf);
        archive_entry_copy_stat(entry, &stbuf);

        /* set user name of owner */
        const char* uname = mfu_flist_file_get_username(DTAR_flist, idx);
        archive_entry_set_uname(entry, uname);

        /* set group name */
        const char* gname = mfu_flist_file_get_groupname(DTAR_flist, idx);
        archive_entry_set_gname(entry, gname);
    }

    /* write entry info to archive */
    struct archive* dest = archive_write_new();
    archive_write_set_format_pax(dest);

    if (archive_write_open_fd(dest, DTAR_writer.fd_tar) != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "archive_write_open_fd(): %s", archive_error_string(ar));
    }

    /* seek to offset in tar archive for this file */
    lseek(DTAR_writer.fd_tar, offset, SEEK_SET);

    /* write header for this item */
    if (archive_write_header(dest, entry) != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "archive_write_header(): %s", archive_error_string(ar));
    }

    archive_entry_free(entry);
    archive_write_free(dest);
}

static char* DTAR_encode_operation(DTAR_operation_code_t code, const char* operand,
                            uint64_t fsize, uint64_t chunk_idx, uint64_t offset)
{
    size_t opsize = (size_t) CIRCLE_MAX_STRING_LEN;
    char* op = (char*) MFU_MALLOC(opsize);
    size_t len = strlen(operand);

    int written = snprintf(op, opsize,
                           "%" PRIu64 ":%" PRIu64 ":%" PRIu64 ":%d:%d:%s",
                           fsize, chunk_idx, offset, code, (int) len, operand);

    if (written >= opsize) {
        MFU_LOG(MFU_LOG_ERR, "Exceed libcirlce message size");
        DTAR_abort(EXIT_FAILURE);
    }

    return op;
}

static DTAR_operation_t* DTAR_decode_operation(char* op)
{
    DTAR_operation_t* ret = (DTAR_operation_t*) MFU_MALLOC(
                                sizeof(DTAR_operation_t));

    if (sscanf(strtok(op, ":"), "%" SCNu64, &(ret->file_size)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode file size attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->chunk_index)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode chunk index attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->offset)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode source base offset attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%d", (int*) & (ret->code)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode stage code attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* get number of characters in operand string */
    int op_len;
    char* str = strtok(NULL, ":");
    if (sscanf(str, "%d", &op_len) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode operand string length.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* skip over digits and trailing ':' to get pointer to operand */
    char* operand = str + strlen(str) + 1;
    operand[op_len] = '\0';
    ret->operand = operand;

    return ret;
}

static void DTAR_enqueue_copy(CIRCLE_handle* handle)
{
    for (uint64_t idx = 0; idx < DTAR_count; idx++) {
        /* add copy work only for files */
        mfu_filetype type = mfu_flist_file_get_type(DTAR_flist, idx);
        if (type == MFU_TYPE_FILE) {
            /* get name and size of file */
            const char* name = mfu_flist_file_get_name(DTAR_flist, idx);
            uint64_t size = mfu_flist_file_get_size(DTAR_flist, idx);

            /* compute offset for first byte of file content */
            uint64_t dataoffset = DTAR_offsets[idx] + DTAR_HDR_LENGTH;

            /* compute number of chunks */
            uint64_t num_chunks = size / DTAR_user_opts.chunk_size;
            for (uint64_t chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
                char* newop = DTAR_encode_operation(
                                  COPY_DATA, name, size, chunk_idx, dataoffset);
                handle->enqueue(newop);
                mfu_free(&newop);
            }

            /* create copy work for possibly last item */
            if (num_chunks * DTAR_user_opts.chunk_size < size || num_chunks == 0) {
                char* newop = DTAR_encode_operation(
                                  COPY_DATA, name, size, num_chunks, dataoffset);
                handle->enqueue(newop);
                mfu_free(&newop);
            }
        }
    }
}

static void DTAR_perform_copy(CIRCLE_handle* handle)
{
    char opstr[CIRCLE_MAX_STRING_LEN];
    char iobuf[FD_BLOCK_SIZE];

    int out_fd = DTAR_writer.fd_tar;

    handle->dequeue(opstr);
    DTAR_operation_t* op = DTAR_decode_operation(opstr);

    uint64_t in_offset = DTAR_user_opts.chunk_size * op->chunk_index;
    int in_fd = open(op->operand, O_RDONLY);

    ssize_t num_of_bytes_read = 0;
    ssize_t num_of_bytes_written = 0;
    ssize_t total_bytes_written = 0;

    uint64_t out_offset = op->offset + in_offset;

    lseek(in_fd, in_offset, SEEK_SET);
    lseek(out_fd, out_offset, SEEK_SET);

    while (total_bytes_written < DTAR_user_opts.chunk_size) {
        num_of_bytes_read = read(in_fd, &iobuf[0], sizeof(iobuf));
        if (!num_of_bytes_read) { break; }
        num_of_bytes_written = write(out_fd, &iobuf[0], num_of_bytes_read);
        total_bytes_written += num_of_bytes_written;
    }

    uint64_t num_chunks = op->file_size / DTAR_user_opts.chunk_size;
    uint64_t rem = op->file_size - DTAR_user_opts.chunk_size * num_chunks;
    uint64_t last_chunk = (rem) ? num_chunks : num_chunks - 1;

    /* handle last chunk */
    if (op->chunk_index == last_chunk) {
        int padding = 512 - (int) (op->file_size % 512);
        if (padding > 0 && padding != 512) {
            char* buff = (char*) calloc(padding, sizeof(char));
            write(out_fd, buff, padding);
        }
    }

    close(in_fd);
    mfu_free(&op);
}

static void DTAR_epilogue(void)
{
    double rel_time = DTAR_statistics.wtime_ended - \
                      DTAR_statistics.wtime_started;
    if (DTAR_rank == 0) {
        char starttime_str[256];
        struct tm* localstart = localtime(&(DTAR_statistics.time_started));
        strftime(starttime_str, 256, "%b-%d-%Y, %H:%M:%S", localstart);

        char endtime_str[256];
        struct tm* localend = localtime(&(DTAR_statistics.time_ended));
        strftime(endtime_str, 256, "%b-%d-%Y, %H:%M:%S", localend);

        /* add two 512 blocks at the end */
        DTAR_statistics.total_size += 512 * 2;

        /* convert bandwidth to unit */
        double agg_rate_tmp;
        double agg_rate = (double) DTAR_statistics.total_size / rel_time;
        const char* agg_rate_units;
        mfu_format_bytes(agg_rate, &agg_rate_tmp, &agg_rate_units);

        MFU_LOG(MFU_LOG_INFO, "Started:    %s", starttime_str);
        MFU_LOG(MFU_LOG_INFO, "Completed:  %s", endtime_str);
        MFU_LOG(MFU_LOG_INFO, "Total archive size: %" PRIu64, DTAR_statistics.total_size);
        MFU_LOG(MFU_LOG_INFO, "Rate: %.3lf %s " \
                "(%.3" PRIu64 " bytes in %.3lf seconds)", \
                agg_rate_tmp, agg_rate_units, DTAR_statistics.total_size, rel_time);
    }
}

static void mfu_flist_archive_create_libcircle(mfu_flist flist, const char* archivefile)
{
    DTAR_flist = flist;

    /* TODO: stripe the archive file if on parallel file system */

    /* create the archive file */
    DTAR_writer.name = archivefile;
    DTAR_writer.flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_LARGEFILE;
    DTAR_writer.fd_tar = open(archivefile, DTAR_writer.flags, 0664);

    /* get number of items in our portion of the list */
    DTAR_count = mfu_flist_size(DTAR_flist);

    /* allocate memory for file sizes and offsets */
    uint64_t* fsizes = (uint64_t*) MFU_MALLOC(DTAR_count * sizeof(uint64_t));
    DTAR_offsets     = (uint64_t*) MFU_MALLOC(DTAR_count * sizeof(uint64_t));

    /* compute local offsets for each item and total
     * bytes we're contributing to the archive */
    uint64_t idx;
    uint64_t offset = 0;
    for (idx = 0; idx < DTAR_count; idx++) {
        /* assume the item takes no space */
        fsizes[idx] = 0;

        /* identify item type to compute its size in the archive */
        mfu_filetype type = mfu_flist_file_get_type(DTAR_flist, idx);
        if (type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            /* directories and symlinks only need the header */
            fsizes[idx] = DTAR_HDR_LENGTH;
        } else if (type == MFU_TYPE_FILE) {
            /* regular file requires a header, plus file content,
             * and things are packed into blocks of 512 bytes */
            uint64_t fsize = mfu_flist_file_get_size(DTAR_flist, idx);

            /* determine whether file size is integer multiple of 512 bytes */
            uint64_t rem = fsize % 512;
            if (rem == 0) {
                /* file content is multiple of 512 bytes, so perfect fit */
                fsizes[idx] = fsize + DTAR_HDR_LENGTH;
            } else {
                /* TODO: check and explain this math */
                fsizes[idx] = (fsize / 512 + 4) * 512;
            }

        }

        /* increment our local offset for this item */
        DTAR_offsets[idx] = offset;
        offset += fsizes[idx];
    }

    /* execute scan to figure our global base offset in the archive file */
    uint64_t global_offset = 0;
    MPI_Scan(&offset, &global_offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    global_offset -= offset;

    /* update offsets for each of our file to their global offset */
    for (idx = 0; idx < DTAR_count; idx++) {
        DTAR_offsets[idx] += global_offset;
    }

    /* create an archive */
    struct archive* ar = archive_write_new();

    archive_write_set_format_pax(ar);

    int r = archive_write_open_fd(ar, DTAR_writer.fd_tar);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "archive_write_open_fd(): %s", archive_error_string(ar));
        DTAR_abort(EXIT_FAILURE);
    }

    /* write headers for our files */
    for (idx = 0; idx < DTAR_count; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(DTAR_flist, idx);
        if (type == MFU_TYPE_FILE || type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            DTAR_write_header(ar, idx, DTAR_offsets[idx]);
        }
    }

    /* prepare libcircle */
    DTAR_global_rank = CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL);
    CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    CIRCLE_enable_logging(loglevel);

    /* register callbacks */
    CIRCLE_cb_create(&DTAR_enqueue_copy);
    CIRCLE_cb_process(&DTAR_perform_copy);

    /* run the libcircle job to copy data into archive file */
    CIRCLE_begin();
    CIRCLE_finalize();

    /* compute total bytes copied */
    uint64_t archive_size = 0;
    MPI_Allreduce(&offset, &archive_size, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    DTAR_statistics.total_size = archive_size;

    /* clean up */
    mfu_free(&fsizes);
    mfu_free(&DTAR_offsets);

    /* close archive file */
    archive_write_free(ar);
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
    const void* buff;
    size_t size;
    off_t offset;
    for (;;) {
        int r = archive_read_data_block(ar, &buff, &size, &offset);
        if (r == ARCHIVE_EOF) {
            return ARCHIVE_OK;
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

static void DTAR_check_paths(int numparams, mfu_param_path* srcparams, mfu_param_path destparam)
{
    int valid = 1;
    int i;
    int num_readable = 0;
    for (i = 0; i < numparams; i++) {
        char* path = srcparams[i].path;
        if (mfu_access(path, R_OK) == 0) {
            num_readable++;
        } else {
            /* not readable */
            char* orig = srcparams[i].orig;
            MFU_LOG(MFU_LOG_ERR, "Could not read '%s' errno=%d %s",
                    orig, errno, strerror(errno));
        }
    }

    /* verify we have at least one valid source */
    if (num_readable < 1) {
        MFU_LOG(MFU_LOG_ERR, "At least one valid source must be specified");
        valid = 0;
        goto bcast;
    }

    /* check destination */
    if (destparam.path_stat_valid) {
        if (DTAR_rank == 0) {
            MFU_LOG(MFU_LOG_WARN, "Destination target exists, we will overwrite");
        }
    } else {
        /* compute path to parent of destination archive */
        mfu_path* parent = mfu_path_from_str(destparam.path);
        mfu_path_dirname(parent);
        char* parent_str = mfu_path_strdup(parent);
        mfu_path_delete(&parent);

        /* check if parent is writable */
        if (mfu_access(parent_str, W_OK) < 0) {
            MFU_LOG(MFU_LOG_ERR, "Destination parent directory is not wriable: '%s' ",
                    parent_str);
            valid = 0;
            mfu_free(&parent_str);
            goto bcast;
        }

        mfu_free(&parent_str);
    }

    /* at this point, we know
     * (1) destination doesn't exist
     * (2) parent directory is writable
     */

bcast:
    MPI_Bcast(&valid, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (! valid) {
        if (DTAR_global_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Exiting run");
        }
        MPI_Barrier(MPI_COMM_WORLD);
        DTAR_exit(EXIT_FAILURE);
    }
}

static void DTAR_parse_path_args(int numpaths, const char** pathlist, const char* dstfile)
{
    if (pathlist == NULL || numpaths < 1) {
        if (DTAR_global_rank == 0) {
            fprintf(stderr, "\nYou must provide at least one source file or directory\n");
            DTAR_exit(EXIT_FAILURE);
        }
    }

    /* allocate space to record info for each source */
    src_params = NULL;
    num_src_params = numpaths;
    size_t src_params_bytes = ((size_t) num_src_params) * sizeof(mfu_param_path);
    src_params = (mfu_param_path*) MFU_MALLOC(src_params_bytes);

    /* process each source path */
    mfu_param_path_set_all(numpaths, pathlist, src_params);

    /* standardize destination path */
    mfu_param_path_set(dstfile, &dest_param);

    /* copy destination to user opts structure */
    DTAR_user_opts.dest_path = MFU_STRDUP(dest_param.path);

    /* check that source and destination are okay */
    DTAR_check_paths(num_src_params, src_params, dest_param);
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dtar [options] <source ...>\n");
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

int main(int argc, const char** argv)
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
                usage = 1;
        }
    }

    /* print usage if we need to */
    if (usage) {
        if (DTAR_rank == 0) {
            print_usage();
        }
        mfu_finalize();
        MPI_Finalize();
        return 0;
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

    /* when creating a tarbll, we require a file name */
    if (opts_create && opts_tarfile == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Must specify a file name(-f)");
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

    /* adjust pointers to start of paths */
    int numpaths = argc - optind;
    const char** pathlist = &argv[optind];

    time(&(DTAR_statistics.time_started));
    DTAR_statistics.wtime_started = MPI_Wtime();
    if (opts_create) {
        /* check that input paths are valid
         * (also set src_params and dest_param) */
        DTAR_parse_path_args(numpaths, pathlist, opts_tarfile);

        /* walk path to get stats info on all files */
        mfu_flist flist = mfu_flist_new();
        mfu_flist_walk_param_paths(num_src_params, src_params, 1, 0, flist);

        /* create the archive file */
        mfu_flist_archive_create_libcircle(flist, opts_tarfile);

        /* compress archive file */
        if (opts_compress) {
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
            //dbz2_compress(opts_blocksize, opts_tarfile, opts_memory);
            remove(fname1);
        }

        /* free the file list */
        mfu_flist_free(&flist);
    } else if (opts_extract) {
        char* tarfile = opts_tarfile;
        if (opts_compress) {
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
            tarfile = fname_out;
        }
        extract_archive(tarfile, opts_verbose, DTAR_user_opts.flags);
    } else {
        if (DTAR_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Neither creation or extraction is specified");
            DTAR_exit(EXIT_FAILURE);
        }
    }

    DTAR_statistics.wtime_ended = MPI_Wtime();
    time(&(DTAR_statistics.time_ended));

    /* free context */
    mfu_free(&opts_tarfile);
    MFU_LOG(MFU_LOG_ERR, "Rank %d before epilogue\n", DTAR_rank);
    DTAR_epilogue();

    DTAR_exit(EXIT_SUCCESS);
}
