/* See the file "COPYING" for the full license governing this code. */

#include "copy.h"
#include "treewalk.h"
#include "dcp1.h"

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>

/** Options specified by the user. */
extern DCOPY_options_t DCOPY_user_opts;

/** Statistics to gather for summary output. */
extern DCOPY_statistics_t DCOPY_statistics;

/** Cache most recent open file descriptors. */
extern DCOPY_file_cache_t DCOPY_file_cache;

/*
 * Encode and enqueue the cleanup stage for this chunk so the file is
 * truncated and (if specified via getopt) permissions are preserved.
 */
static void DCOPY_enqueue_cleanup_stage(DCOPY_operation_t* op,
                                 CIRCLE_handle* handle)
{
    char* newop;

    newop = DCOPY_encode_operation(CLEANUP, op->chunk, op->operand,
                                   op->source_base_offset,
                                   op->dest_base_appendix, op->file_size);

    handle->enqueue(newop);
    free(newop);
}

/*
 * Perform the actual copy on this chunk and increment the global statistics
 * counter.
 */
static int DCOPY_perform_copy(DCOPY_operation_t* op,
                       int in_fd,
                       int out_fd,
                       off_t offset)
{
    /* seek to offset in source file */
    if(mfu_lseek(op->operand, in_fd, offset, SEEK_SET) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in source path `%s' errno=%d %s", \
            op->operand, errno, strerror(errno));
        /* Handle operation requeue in parent function. */
        return -1;
    }

    /* seek to offset in destination file */
    if(mfu_lseek(op->dest_full_path, out_fd, offset, SEEK_SET) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' errno=%d %s", \
            op->dest_full_path, errno, strerror(errno));
        /* Handle operation requeue in parent function. */
        return -1;
    }

    /* get buffer */
    size_t buf_size = DCOPY_user_opts.block_size;
    void* buf = DCOPY_user_opts.block_buf1;

    /* write data */
    size_t total_bytes = 0;
    size_t chunk_size = DCOPY_user_opts.chunk_size;
    while(total_bytes <= chunk_size) {
        /* determine number of bytes that we can read = max(buf size, remaining chunk) */
        size_t left_to_read = chunk_size - total_bytes;
        if(left_to_read > buf_size) {
            left_to_read = buf_size;
        }

        /* read data from source file */
        ssize_t num_of_bytes_read = mfu_read(op->operand, in_fd, buf, left_to_read);

        /* check for EOF */
        if(!num_of_bytes_read) {
            break;
        }

        /* compute number of bytes to write */
        size_t bytes_to_write = (size_t) num_of_bytes_read;
        if(DCOPY_user_opts.synchronous) {
            /* O_DIRECT requires particular write sizes,
             * ok to write beyond end of file so long as
             * we truncate in cleanup step */
            size_t remainder = buf_size - (size_t) num_of_bytes_read;
            if(remainder > 0) {
                /* zero out the end of the buffer for security,
                 * don't want to leave data from another file at end of
                 * current file if we fail before truncating */
                char* bufzero = ((char*)buf + num_of_bytes_read);
                memset(bufzero, 0, remainder);
            }

            /* assumes buf_size is magic size for O_DIRECT */
            bytes_to_write = buf_size;
        }

        /* write data to destination file */
        ssize_t num_of_bytes_written = mfu_write(op->dest_full_path, out_fd, buf,
                                     bytes_to_write);

        /* check that we wrote the same number of bytes that we read */
        if(num_of_bytes_written < 0) {
            MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s' errno=%d %s",
                op->operand, op->dest_full_path, errno, strerror(errno));
            /* Handle operation requeue in parent function. */
            return -1;
        }

        /* check that we wrote the same number of bytes that we read */
        if((size_t)num_of_bytes_written != bytes_to_write) {
            MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s'",
                op->operand, op->dest_full_path);
            /* Handle operation requeue in parent function. */
            return -1;
        }

        /* add bytes to our total (use bytes read,
         * which may be less than number written) */
        total_bytes += (size_t) num_of_bytes_read;
    }

#if 0
    /* force data to file system */
    if(total_bytes > 0) {
        mfu_fsync(op->dest_full_path, out_fd);
    }
#endif

    /* Increment the global counter. */
    DCOPY_statistics.total_bytes_copied += (int64_t) total_bytes;

    MFU_LOG(MFU_LOG_DBG, "Wrote `%zu' bytes at segment `%" PRId64 \
        "', offset `%" PRId64 "' (`%" PRId64 "' total)",
        total_bytes, op->chunk, (int64_t)DCOPY_user_opts.chunk_size * op->chunk,
        DCOPY_statistics.total_bytes_copied);

    return 1;
}

/* The entrance point to the copy operation. */
void DCOPY_do_copy(DCOPY_operation_t* op,
                   CIRCLE_handle* handle)
{
    /* open the input file */
    int in_fd = DCOPY_open_file(op->operand, 1, &DCOPY_src_cache);
    if(in_fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open input file `%s' errno=%d %s",
            op->operand, errno, strerror(errno));

        DCOPY_retry_failed_operation(COPY, handle, op);
        return;
    }

    /* compute starting byte offset */
    off_t chunk_size = (off_t) DCOPY_user_opts.chunk_size;
    off_t offset = chunk_size * op->chunk;

    /* hint that we'll read from file sequentially */
//    posix_fadvise(in_fd, offset, chunk_size, POSIX_FADV_SEQUENTIAL);

    /* open the output file */
    int out_fd = DCOPY_open_file(op->dest_full_path, 0, &DCOPY_dst_cache);
    if(out_fd < 0) {
        /* If the force option is specified, try to unlink the destination and
         * reopen before doing the optional requeue. */
        if(DCOPY_user_opts.force) {
            mfu_unlink(op->dest_full_path);
            out_fd = DCOPY_open_file(op->dest_full_path, 0, &DCOPY_dst_cache);
        }

        /* requeue operation */
        if(out_fd < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open output file `%s' errno=%d %s",
                op->dest_full_path, errno, strerror(errno));

            DCOPY_retry_failed_operation(COPY, handle, op);
            return;
        }
    }

    /* copy data */
    if(DCOPY_perform_copy(op, in_fd, out_fd, offset) < 0) {
        /* we already reported an error in perform_copy */
        DCOPY_retry_failed_operation(COPY, handle, op);
        return;
    }

    DCOPY_enqueue_cleanup_stage(op, handle);

    return;
}

/* EOF */
