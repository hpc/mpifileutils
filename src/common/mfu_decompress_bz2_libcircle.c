#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#define _LARGEFILE64_SOURCE
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sysinfo.h>
#include <string.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "mpi.h"
#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <utime.h>
#include <bzlib.h>
#include <inttypes.h>
#include <errno.h>

#include "libcircle.h"
#include "mfu.h"
#include "mfu_bz2.h"

#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)

static int64_t block_meta;
static int64_t block_total;
static int64_t block_size;
static char* src_name;
static char* dst_name;
static int fd;
static int fd_out;

/* This function creates work for libcircle if we are using decompression
 * The function reads the trailer, which stores the offset of all the
 * blocks and creates a string of the form:
 *    block_no:offset:next_offset(offset of next block.
 * It pushes this string into the queue */
static void DBz2_decompEnqueue(CIRCLE_handle* handle)
{
    int64_t block_num = 0;
    while (block_num < block_total) {
        /* seek to metadata for this block */
        off_t pos = (off_t) (block_meta + block_num * 16);
        off_t lseek_rc = mfu_lseek(src_name, fd, pos, SEEK_SET);
        if (lseek_rc == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to seek to block metadata in source file: %s offset=%lx errno=%d (%s)",
                src_name, pos, errno, strerror(errno));
            //rc = MFU_FAILURE;
            break;
        }
    
        /* read offset of block */
        uint64_t net_offset;
        ssize_t nread = mfu_read(src_name, fd, &net_offset, 8);
        if (nread != 8) {
            MFU_LOG(MFU_LOG_ERR, "Failed to read block offset from source file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                src_name, pos, nread, 8, errno, strerror(errno));
            //rc = MFU_FAILURE;
            break;
        }

        /* read length of block */
        uint64_t net_length;
        nread = mfu_read(src_name, fd, &net_length, 8);
        if (nread != 8) {
            MFU_LOG(MFU_LOG_ERR, "Failed to read block length from source file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                src_name, pos+8, nread, 8, errno, strerror(errno));
            //rc = MFU_FAILURE;
            break;
        }
    
        /* convert values from network to host order */
        int64_t offset = mfu_ntoh64(net_offset);
        int64_t length = mfu_ntoh64(net_length);

        /* define work element as block_id : start_offset : end_offset */
        char* newop = (char*)MFU_MALLOC(sizeof(char) * 50);
        sprintf(newop, "%"PRId64":%" PRId64 ":%" PRId64, block_num, offset, length);

        /* enqueue work item in libcircle */
        handle->enqueue(newop);

        mfu_free(&newop);

        /* increment count of blocks processed */
        block_num++;
    }
}

/* function to perfrom libcircle processing in decompression */
static void DBz2_decompDequeue(CIRCLE_handle* handle)
{
    /* pick item off of queue */
    char newop[50];
    handle->dequeue(newop);

    /* parse newop to get block_no, offset and offset of next block */
    int64_t block_num;
    int64_t offset;
    int64_t length;
    char* token = strtok(newop, ":");
    sscanf(token, "%" PRId64, &block_num);
    token = strtok(NULL, ":");
    sscanf(token, "%"PRId64, &offset);
    token = strtok(NULL, ":");
    sscanf(token, "%"PRId64, &length);

    /* compute max size of buffer to hold uncompressed data:
     * BZ2 scheme requires block_size + 1% + 600 bytes */
    unsigned int outSize = (unsigned int)block_size;

    /* allocate input and output buffers */
    char* obuf = (char*) MFU_MALLOC(outSize);
    char* ibuf = (char*) MFU_MALLOC(length);

    /* seek to start of compressed block in source file */
    int lseek_rc = mfu_lseek(src_name, fd, offset, SEEK_SET);
    if (lseek_rc == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to seek to block in source file: %s offset=%lx errno=%d (%s)",
            src_name, offset, errno, strerror(errno));
        //rc = MFU_FAILURE;
        //break;
    }

    /* Read compressed block from source file */
    ssize_t inSize = mfu_read(src_name, fd, (char*)ibuf, length);
    if (inSize != (ssize_t)length) {
        MFU_LOG(MFU_LOG_ERR, "Failed to read block from source file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
            src_name, offset, inSize, length, errno, strerror(errno));
        //rc = MFU_FAILURE;
        //break;
    }
    
    /* Perform decompression */
    int ret = BZ2_bzBuffToBuffDecompress(obuf, &outSize, ibuf, inSize, 0, 0);
    if (ret != 0) {
        MFU_LOG(MFU_LOG_ERR, "Error in decompression");
        //rc = MFU_FAILURE;
        //break;
    }
    
    /* compute offset to start of uncompressed block in target file */
    int64_t in_offset = block_num * block_size;
    
    /* seek to position to write block in target file */
    lseek_rc = mfu_lseek(dst_name, fd_out, in_offset, SEEK_SET);
    if (lseek_rc == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to seek to block in target file: %s offset=%lx errno=%d (%s)",
            dst_name, in_offset, errno, strerror(errno));
        //rc = MFU_FAILURE;
        //break;
    }

    /* write decompressed block to target file */
    ssize_t nwritten = mfu_write(dst_name, fd_out, obuf, outSize);
    if (nwritten != outSize) {
        MFU_LOG(MFU_LOG_ERR, "Failed to write block in target file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
            dst_name, in_offset, nwritten, outSize, errno, strerror(errno));
        //rc = MFU_FAILURE;
        //break;
    }

    /* free buffers */
    mfu_free(&ibuf);
    mfu_free(&obuf);

    return;
}

int mfu_decompress_bz2_libcircle(const char* src, const char* dst)
{
    int rc = MFU_SUCCESS;

    /* make a copy of the target file name */
    src_name = MFU_STRDUP(src);
    dst_name = MFU_STRDUP(dst);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* open compressed file for reading */
    fd = mfu_open(src_name, O_RDONLY);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for reading: %s errno=%d (%s)",
            src_name, errno, strerror(errno));
    }

    /* check that all processes were able to open the file */
    if (! mfu_alltrue(fd >= 0, MPI_COMM_WORLD)) {
        /* some process failed to open so bail with error,
         * if we opened ok, close file */
        if (fd >= 0) {
            mfu_close(src_name, fd);
        }
        return MFU_FAILURE;
    }

    /* assume that we'll read the footer */
    int footer_flag = 1;

    /* have arnk 0 read footer from file */
    struct stat st;
    uint64_t footer[7] = {0};
    if (rank == 0) {
        /* seek to read footer from end of file */
        size_t footer_size = 6 * 8;
        off_t lseek_rc = mfu_lseek(src_name, fd, -footer_size, SEEK_END);
        if (lseek_rc == (off_t)-1) {
            /* failed to seek to position to read footer */
            footer_flag = 0;
            MFU_LOG(MFU_LOG_ERR, "Failed to seek to read footer: %s errno=%d (%s)",
                src_name, errno, strerror(errno));
        }

        /* read footer from end of file */
        uint64_t file_footer[6] = {0};
        ssize_t read_rc = mfu_read(src_name, fd, file_footer, footer_size);
        if (read_rc == footer_size) {
            /* got the footer, convert fields from network to host order */
            footer[0] = mfu_ntoh64(file_footer[0]);
            footer[1] = mfu_ntoh64(file_footer[1]);
            footer[2] = mfu_ntoh64(file_footer[2]);
            footer[3] = mfu_ntoh64(file_footer[3]);
            footer[4] = mfu_ntoh64(file_footer[4]);
            footer[5] = mfu_ntoh64(file_footer[5]);
        } else {
            /* failed to read footer */
            footer_flag = 0;
            MFU_LOG(MFU_LOG_ERR, "Failed to read footer: %s errno=%d (%s)",
                src_name, errno, strerror(errno));
        }

        /* get size of file */
        int lstat_rc = mfu_lstat(src_name, &st);
        if (lstat_rc == 0) {
            footer[6] = (uint64_t) st.st_size;
        } else {
            /* failed to stat file for file size */
            footer_flag = 0;
            MFU_LOG(MFU_LOG_ERR, "Failed to stat file: %s errno=%d (%s)",
                src_name, errno, strerror(errno));
        }
    }

    /* broadcast footer to all ranks */
    MPI_Bcast(&footer_flag, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&footer, 8, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* check whether we read the footer successfully */
    if (! footer_flag) {
        /* failed to read footer for some reason */
        mfu_close(src_name, fd);
        return MFU_FAILURE;
    }

    /* extract values from footer into local variables */
    block_meta  = (int64_t)footer[0];         /* offset to start of block metadata */
    block_total = (int64_t)footer[1];         /* number of blocks */
    block_size          = (int64_t)footer[2]; /* max uncompressed size of a block */
                                              /* (int64_t)footer[3] - unused - uncompressed size of all blocks */
    uint64_t version    = footer[4];          /* dbz2 file format footer version */
    uint64_t magic      = footer[5];          /* dbz2 file format magic value */
                                              /* (int64_t)footer[6] - unused - file size of compressed file */

    /* check that we got correct magic value */
    if (magic != 0x3141314131413141) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Source file does not seem to be a dbz2 file: %s",
                src_name);
        }
        mfu_close(src_name, fd);
        return MFU_FAILURE;
    }

    /* check that we got correct version number */
    if (version != 1) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Source dbz2 file has unsupported version (%llu): %s",
                (unsigned long long)version, src_name);
        }
        mfu_close(src_name, fd);
        return MFU_FAILURE;
    }

    /* open destination file for writing */
    fd_out = mfu_create_fully_striped(dst_name, FILE_MODE);
    if (fd_out < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing: %s errno=%d (%s)",
            dst_name, errno, strerror(errno));
    }

    /* check that all processes were able to open the file */
    if (! mfu_alltrue(fd_out >= 0, MPI_COMM_WORLD)) {
        /* some process failed to open so bail with error,
         * if we opened ok, close file */
        if (fd_out >= 0) {
            mfu_close(dst_name, fd_out);
        }
        mfu_close(src_name, fd);
        return MFU_FAILURE;
    }

    /* Actual call to libcircle and callback fuction setting */
    CIRCLE_init(0, NULL, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&DBz2_decompEnqueue);
    CIRCLE_cb_process(&DBz2_decompDequeue);
    CIRCLE_begin();
    CIRCLE_finalize();

    /* close source and target files */
    mfu_close(dst_name, fd_out);
    mfu_close(src_name, fd);

    mfu_free(&dst_name);
    mfu_free(&src_name);

    return rc;
}
