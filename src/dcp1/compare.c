/* See the file "COPYING" for the full license governing this code. */

#include "compare.h"
#include "dcp1.h"

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/** Options specified by the user. */
extern DCOPY_options_t DCOPY_user_opts;

/** Statistics to gather for summary output. */
extern DCOPY_statistics_t DCOPY_statistics;

/*
 * Perform the compare on this chunk.
 */
static int DCOPY_perform_compare(DCOPY_operation_t* op,
                          int in_fd,
                          int out_fd,
                          off_t offset)
{
    /* seek to offset in source file */
    if(mfu_lseek(op->operand, in_fd, offset, SEEK_SET) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in source path `%s' errno=%d %s",
            op->operand, errno, strerror(errno));
        /* Handle operation requeue in parent function. */
        return -1;
    }

    /* seek to offset in destination file */
    if(mfu_lseek(op->dest_full_path, out_fd, offset, SEEK_SET) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' errno=%d %s",
            op->dest_full_path, errno, strerror(errno));
        return -1;
    }

    /* get buffer info */
    size_t buf_size = DCOPY_user_opts.block_size;
    void* src_buf = DCOPY_user_opts.block_buf1;
    void* dest_buf = DCOPY_user_opts.block_buf2;

    /* compare bytes */
    size_t total_bytes = 0;
    size_t chunk_size = DCOPY_user_opts.chunk_size;
    while(total_bytes <= chunk_size) {
        /* determine number of bytes that we can read = max(buf size, remaining chunk) */
        size_t left_to_read = chunk_size - total_bytes;
        if (left_to_read > buf_size) {
            left_to_read = buf_size;
        }

        /* read data from source and destination */
        ssize_t num_of_in_bytes = mfu_read(op->operand, in_fd, src_buf, left_to_read);
        ssize_t num_of_out_bytes = mfu_read(op->dest_full_path, out_fd, dest_buf, left_to_read);

        /* check that we got the same number of bytes from each */
        if(num_of_in_bytes != num_of_out_bytes) {
            MFU_LOG(MFU_LOG_DBG, "Source byte count `%zu' does not match " \
                "destination byte count '%zu' of total file size `%zu'",
                num_of_in_bytes, num_of_out_bytes, op->file_size);

            return -1;
        }

        /* check for EOF */
        if(!num_of_in_bytes) {
            break;
        }

        /* check that buffers are the same */
        if(memcmp(src_buf, dest_buf, (size_t) num_of_in_bytes) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Compare mismatch when copying from file `%s'",
                op->operand);

            return -1;
        }

        /* add bytes to our total */
        total_bytes += (size_t) num_of_in_bytes;
    }

    return 1;
}

/* The entrance point to the compare operation. */
void DCOPY_do_compare(DCOPY_operation_t* op,
                      CIRCLE_handle* handle)
{
    /* open source file */
    //int in_fd = mfu_open(op->operand, O_RDONLY | O_NOATIME);
    int in_fd = DCOPY_open_file(op->operand, 1, &DCOPY_src_cache);
    if(in_fd < 0) {
        /* seems like we should retry the COMPARE here, may be overkill to COPY */
        MFU_LOG(MFU_LOG_ERR, "Failed to open source file for compare `%s' errno=%d %s",
            op->operand, errno, strerror(errno));

        DCOPY_retry_failed_operation(COPY, handle, op);
        return;
    }

    /* compute starting byte offset */
    off_t chunk_size = (off_t) DCOPY_user_opts.chunk_size;
    off_t offset = chunk_size * op->chunk;

    /* hint that we'll read from file sequentially */
    posix_fadvise(in_fd, offset, chunk_size, POSIX_FADV_SEQUENTIAL);

    /* open destination file */
    //int out_fd = mfu_open(op->dest_full_path, O_RDONLY | O_NOATIME);
    int out_fd = DCOPY_open_file(op->dest_full_path, 1, &DCOPY_dst_cache);
    if(out_fd < 0) {
        /* assume destination file does not exist, try copy again */
        MFU_LOG(MFU_LOG_ERR, "Failed to open destination file for compare " \
            "`%s' errno=%d %s", op->dest_full_path, errno, strerror(errno));

        DCOPY_retry_failed_operation(COPY, handle, op);
        return;
    }

    /* hint that we'll read from file sequentially */
    posix_fadvise(out_fd, offset, chunk_size, POSIX_FADV_SEQUENTIAL);

    /* compare bytes */
    if(DCOPY_perform_compare(op, in_fd, out_fd, offset) < 0) {
        /* found incorrect data, try copy again */
        MFU_LOG(MFU_LOG_ERR, "Corrupt data detected, retrying copy from `%s' to `%s'",
            op->operand, op->dest_full_path);

        DCOPY_retry_failed_operation(COPY, handle, op);
        return;
    }

#if 0
    /* close destination file */
    if(mfu_close(op->dest_full_path, out_fd) < 0) {
        MFU_LOG(MFU_LOG_DBG, "Close on destination file failed `%s' errno=%d %s",
            op->dest_full_path, errno, strerror(errno));
    }

    /* close source file */
    if(mfu_close(op->operand, in_fd) < 0) {
        MFU_LOG(MFU_LOG_DBG, "Close on source file failed `%s' errno=%d %s",
            op->operand, errno, strerror(errno));
    }
#endif

    return;
}

/* EOF */
