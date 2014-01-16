/*
 * common.c
 *
 *  Created on: Jan 13, 2014
 *      Author: fwang2
 */

#include "common.h"

void DTAR_writer_init() {
    char * filename = DTAR_user_opts.dest_path;
    DTAR_writer.name = filename;
    DTAR_writer.flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_LARGEFILE;
    DTAR_writer.fd_tar = open(filename, DTAR_writer.flags, 0664);

}

void DTAR_abort(int code) {
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

void DTAR_exit(int code) {
    bayer_finalize();
    MPI_Finalize();
    exit(code);
}


struct archive * DTAR_new_archive() {
    struct archive *a = archive_write_new();
    archive_write_set_format_pax(a);
    int r = archive_write_open_fd(a, DTAR_writer.fd_tar);
    if ( r != ARCHIVE_OK) {
        BAYER_LOG(BAYER_LOG_ERR, "archive_write_open_fd(): %s", archive_error_string(a));
        return NULL;
    }

    return a;
}

void DTAR_write_header(struct archive *ar, uint64_t idx, uint64_t offset)
{

    const char * fname = bayer_flist_file_get_name(DTAR_flist, idx);

    /* fill up entry, FIXME: the uglyness of removing leading slash */
    struct archive_entry *entry = archive_entry_new();
    archive_entry_copy_pathname(entry, &fname[1]);
    struct archive * source = archive_read_disk_new();
    archive_read_disk_set_standard_lookup(source);
    int fd = open(fname, O_RDONLY);
    if (archive_read_disk_entry_from_file(source, entry, fd, NULL) != ARCHIVE_OK) {
        BAYER_LOG(BAYER_LOG_ERR, "archive_read_disk_entry_from_file(): %s", archive_error_string(ar));
    }
    archive_read_free(source);

    /* write entry info to archive */
    struct archive* dest = archive_write_new();
    archive_write_set_format_pax(dest);

    if (archive_write_open_fd(dest, DTAR_writer.fd_tar) != ARCHIVE_OK) {
        BAYER_LOG(BAYER_LOG_ERR, "archive_write_open_fd(): %s", archive_error_string(ar));
    }

    lseek64(DTAR_writer.fd_tar, offset, SEEK_SET);

    if (archive_write_header(dest, entry) != ARCHIVE_OK) {
        BAYER_LOG(BAYER_LOG_ERR, "archive_write_header(): %s", archive_error_string(ar));
    }
    archive_entry_free(entry);
    archive_write_free(dest);

}

void DTAR_enqueue_copy(CIRCLE_handle *handle) {
    for (uint64_t idx = 0; idx < DTAR_count; idx++) {
        bayer_filetype type = bayer_flist_file_get_type(DTAR_flist, idx);
        /* add copy work only for files */
        if (type == BAYER_TYPE_FILE) {
            uint64_t dataoffset = DTAR_offsets[idx] + DTAR_HDR_LENGTH;
            const char * name = bayer_flist_file_get_name(DTAR_flist, idx);
            uint64_t size = bayer_flist_file_get_size(DTAR_flist, idx);

            /* compute number of chunks */
            uint64_t num_chunks = size / DTAR_user_opts.chunk_size;
            for (uint64_t chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
                char* newop = DTAR_encode_operation(
                        COPY_DATA, name, size, chunk_idx, dataoffset);
                handle->enqueue(newop);
                bayer_free(&newop);

            }

            /* create copy work for possibly last item */
            if (num_chunks * DTAR_user_opts.chunk_size < size || num_chunks == 0) {
                char* newop = DTAR_encode_operation(
                        COPY_DATA, name, size, num_chunks, dataoffset);
                handle->enqueue(newop);
                bayer_free(&newop);
            }
        }
    }
}

void DTAR_perform_copy(CIRCLE_handle* handle) {
    char opstr[CIRCLE_MAX_STRING_LEN];
    char iobuf[FD_BLOCK_SIZE];

    int out_fd = DTAR_writer.fd_tar;

    handle->dequeue(opstr);
    DTAR_operation_t* op = DTAR_decode_operation(opstr);

    uint64_t in_offset = DTAR_user_opts.chunk_size * op->chunk_index;
    int in_fd = open64(op->operand, O_RDONLY);

    ssize_t num_of_bytes_read = 0;
    ssize_t num_of_bytes_written = 0;
    ssize_t total_bytes_written = 0;

    uint64_t out_offset = op->offset + in_offset;

    lseek64(in_fd, in_offset, SEEK_SET);
    lseek64(out_fd, out_offset, SEEK_SET);

    while (total_bytes_written < DTAR_user_opts.chunk_size) {
        num_of_bytes_read = read(in_fd, &iobuf[0], sizeof(iobuf));
        if (!num_of_bytes_read) break;
        num_of_bytes_written = write(out_fd, &iobuf[0], num_of_bytes_read);
        total_bytes_written += num_of_bytes_written;
    }

    int num_chunks = op->file_size / DTAR_user_opts.chunk_size;
    int rem = op->file_size - DTAR_user_opts.chunk_size * num_chunks;
    int last_chunk = (rem)? num_chunks : num_chunks - 1;

    /* handle last chunk */
    if (op->chunk_index == last_chunk) {
        int padding = 512 - op->file_size % 512;
        if (padding > 0) {
            char * buff = (char*) calloc(padding, sizeof(char));
            write(out_fd, buff, padding);
        }
    }

    close(in_fd);
    bayer_free(&op);
}


char * DTAR_encode_operation( DTAR_operation_code_t code, const char* operand,
        uint64_t fsize, uint64_t chunk_idx, uint64_t offset)
{

    size_t opsize = (size_t) CIRCLE_MAX_STRING_LEN;
    char* op = (char*) BAYER_MALLOC(opsize);
    size_t len = strlen(operand);

    int written = snprintf(op, opsize,
            "%" PRIu64 ":%" PRIu64 ":%" PRIu64 ":%d:%d:%s",
            fsize, chunk_idx, offset, code, (int) len, operand);

    if (written >= opsize) {
        BAYER_LOG(BAYER_LOG_ERR, "Exceed libcirlce message size");
        DTAR_abort(EXIT_FAILURE);
    }

    return op;

}

DTAR_operation_t* DTAR_decode_operation(char *op) {

    DTAR_operation_t* ret = (DTAR_operation_t*) BAYER_MALLOC(
            sizeof(DTAR_operation_t));

    if (sscanf(strtok(op, ":"), "%" SCNu64, &(ret->file_size)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode file size attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->chunk_index)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode chunk index attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->offset)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode source base offset attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%d", (int*) &(ret->code)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode stage code attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* get number of characters in operand string */
    int op_len;
    char* str = strtok(NULL, ":");
    if (sscanf(str, "%d", &op_len) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode operand string length.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* skip over digits and trailing ':' to get pointer to operand */
    char* operand = str + strlen(str) + 1;
    operand[op_len] = '\0';
    ret->operand = operand;

    return ret;

}
