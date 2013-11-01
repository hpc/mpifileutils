/* See the file "COPYING" for the full license governing this code. */

#include "dtar.h"

#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>

extern DTAR_options_t DTAR_user_opts;
extern DTAR_writer_t DTAR_writer;

int DTAR_open_input_fd(DTAR_operation_t* op, off64_t offset, off64_t len)
{
    const char* path = op->operand;

    int in_fd = open64(path, O_RDONLY | O_NOATIME);

    if (in_fd < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "In DTAR_open_input_fd in_fd is invalid\n");
        return in_fd;
    }

    return in_fd;
}

int DTAR_perform_copy(
    DTAR_operation_t* op,
    int in_fd,
    int out_fd,
    off64_t offset)
{
    ssize_t num_of_bytes_read = 0;
    ssize_t num_of_bytes_written = 0;
    ssize_t total_bytes_written = 0;

    char io_buf[FD_BLOCK_SIZE];

    if (lseek64(in_fd, offset, SEEK_SET) < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Couldn't seek in source path `%s'. errno=%d %s", op->operand,
                errno, strerror(errno));
        /* Handle operation requeue in parent function. */
        return -1;
    }

    if (lseek64(out_fd, offset + op->offset, SEEK_SET) < 0) {
        BAYER_LOG(BAYER_LOG_ERR,
                "Couldn't seek in destination path (source is `%s'). errno=%d %s",
                op->operand, errno, strerror(errno));
        return -1;
    }

    while (total_bytes_written <= DTAR_CHUNK_SIZE) {

        num_of_bytes_read = read(in_fd, &io_buf[0], sizeof(io_buf));

        if (!num_of_bytes_read) {
            break;
        }

        num_of_bytes_written = write(out_fd, &io_buf[0],
                (size_t) num_of_bytes_read);

        if (num_of_bytes_written != num_of_bytes_read) {
            BAYER_LOG(BAYER_LOG_ERR, "Write error when copying from `%s'. errno=%d %s",
                    op->operand, errno, strerror(errno));
            return -1;
        }

        total_bytes_written += num_of_bytes_written;
    }

    int num_chunks = op->file_size / DTAR_CHUNK_SIZE;
    int rem = op->file_size - DTAR_CHUNK_SIZE * num_chunks;
    int last_chunk = (rem) ? num_chunks : num_chunks - 1;

    if (op->chunk == last_chunk) {

        int padding = 512 - op->file_size % 512;
        if (padding > 0) {
            char * buff_null = (char*) calloc(padding, sizeof(char));
            int num_of_bytes_written = write(out_fd, buff_null,
                    (size_t) padding);
        }
    }

    return 1;
}

void DTAR_do_copy(DTAR_operation_t* op)
{
    off64_t offset = DTAR_CHUNK_SIZE * op->chunk;

    int in_fd = DTAR_open_input_fd(op, offset, DTAR_CHUNK_SIZE);

    if (in_fd < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "In DTAR_do_copy in_fd is invalid\n");
        return;
    }

    int out_fd = DTAR_writer.fd_tar;

    if (out_fd < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "In DTAR_do_copy in_fd in invalid\n");
        return;
    }

printf("Writing data at %llu, chunk %d of %s\n", (unsigned long long)op->offset + offset, op->chunk, op->operand);
    if (DTAR_perform_copy(op, in_fd, out_fd, offset) < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "In DTAR_do_copy perform copy failed\n");
        return;
    }

    if (close(in_fd) < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "In DTAR_do_copy close failed\n");
    }

    return;
}

/* EOF */
