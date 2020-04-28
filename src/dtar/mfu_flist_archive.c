
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
#include "mfu_flist_archive.h"

#define DTAR_HDR_LENGTH 1536

typedef enum {
    COPY_DATA
} DTAR_operation_code_t;

/* common structures */

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

mfu_param_path* src_params;
mfu_param_path dest_param;
int num_src_params;

mfu_flist DTAR_flist;
uint64_t* DTAR_offsets = NULL;
mfu_archive_options_t DTAR_user_opts;
DTAR_writer_t DTAR_writer;
DTAR_statistics_t DTAR_statistics;
uint64_t DTAR_count = 0;
int DTAR_rank;

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

    /* TODO: Seems to be a bug here potentially leading to corrupted
     * archive files.  archive_write_free also writes two blocks of
     * NULL bytes at the end of an archive file, however, each rank
     * will have a different view of the length of the file, so one
     * rank may write its NULL blocks over top of the actual data
     * written by another rank */

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
    DTAR_operation_t* ret = (DTAR_operation_t*) MFU_MALLOC(sizeof(DTAR_operation_t));

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
        if (! num_of_bytes_read) {
            break;
        }
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

void mfu_param_path_check_archive(int numparams, mfu_param_path* srcparams, mfu_param_path destparam, int* valid)
{
    /* TODO: need to parallize this, rather than have every rank do the test */

    /* assume paths are valid */
    *valid = 1;

    /* count number of source paths that we can read */
    int i;
    int num_readable = 0;
    for (i = 0; i < numparams; i++) {
        char* path = srcparams[i].path;
        if (mfu_access(path, R_OK) == 0) {
            /* found one that we can read */
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
        *valid = 0;
        goto bcast;
    }

    /* copy destination to user opts structure */
    DTAR_user_opts.dest_path = MFU_STRDUP(dest_param.path);

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
            *valid = 0;
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
    MPI_Bcast(valid, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (! *valid) {
        if (DTAR_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Exiting run");
        }
        MPI_Barrier(MPI_COMM_WORLD);
        DTAR_exit(EXIT_FAILURE);
    }
}

static void mfu_flist_archive_create_libcircle(mfu_flist flist, const char* archivefile, mfu_archive_options_t* opts)
{
    DTAR_flist = flist;
    DTAR_user_opts = *opts;

    MPI_Comm_rank(MPI_COMM_WORLD, &DTAR_rank);

    /* TODO: stripe the archive file if on parallel file system */

    /* init statistics */
    DTAR_statistics.total_dirs  = 0;
    DTAR_statistics.total_files = 0;
    DTAR_statistics.total_links = 0;
    DTAR_statistics.total_size  = 0;
    DTAR_statistics.total_bytes_copied = 0;

    time(&(DTAR_statistics.time_started));
    DTAR_statistics.wtime_started = MPI_Wtime();

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
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL | CIRCLE_TERM_TREE);
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

    DTAR_statistics.wtime_ended = MPI_Wtime();
    time(&(DTAR_statistics.time_ended));

    /* print stats */
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

    /* clean up */
    mfu_free(&fsizes);
    mfu_free(&DTAR_offsets);

    /* close archive file */
    archive_write_free(ar);
    mfu_close(DTAR_writer.name, DTAR_writer.fd_tar);
}

void mfu_flist_archive_create(mfu_flist flist, const char* archivefile, mfu_archive_options_t* opts)
{
    mfu_flist_archive_create_libcircle(flist, archivefile, opts);
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

void mfu_flist_archive_extract(const char* filename, bool verbose, int flags)
{
    int r;

    /* TODO: this needs to be parallelized */

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
