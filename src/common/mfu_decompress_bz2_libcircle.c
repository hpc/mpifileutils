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

#include "libcircle.h"
#include "mfu.h"
#include "mfu_bz2.h"

#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
#ifndef O_BINARY
#define O_BINARY 0
#endif

int64_t block_size;
char fname[50];
char fname_out[50];
int fd;
int fd_out;

/* This function creates work for libcircle if we are using decompression
 * The function reads the trailer, which stores the offset of all the
 * blocks and creates a string of the form:
 *    block_no:offset:next_offset(offset of next block.
 * It pushes this string into the queue */
static void DBz2_decompEnqueue(CIRCLE_handle* handle)
{
    /* The last 8 bytes of the trailer stores the location or offset of the start of the trailer */
    int64_t trailer_begin;
    int64_t last_offset = (int64_t)lseek64(fd, -8, SEEK_END);
    mfu_read(fname, fd, &trailer_begin, 8);
    MFU_LOG(MFU_LOG_INFO, "trailer begins at:%" PRId64 "\n", trailer_begin);

    /* seek to start of trailer */
    int64_t start;
    lseek64(fd, trailer_begin, SEEK_SET);

    /* read offset of start value */
    mfu_read(fname, fd, &start, 8);

    int64_t block_num = 0;
    int64_t end = -1;
    while (1) {
        /* when the offset of the trailer is read,
         * all block offsets have been read and queued */
        if (end == trailer_begin) {
            break;
        }

        /* read offset of end */
        mfu_read(fname, fd, &end, 8);

        /* define work element as block_id : start_offset : end_offset */
        char* newop = (char*)MFU_MALLOC(sizeof(char) * 50);
        sprintf(newop, "%"PRId64":%" PRId64 ":%" PRId64, block_num, start, end);
        MFU_LOG(MFU_LOG_DBG, "Start and End at:%" PRId64 "%" PRId64 "\n", start, end);
        MFU_LOG(MFU_LOG_INFO, "The queued item is:%s\n", newop);

        /* enqueue work item in libcircle */
        handle->enqueue(newop);

        /* increment count of blocks processed */
        block_num++;
        MFU_LOG(MFU_LOG_INFO, "Blocks queued=%" PRId64 "\n", block_num);

        /* advance offset */
        start = end;
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
    int64_t next_offset;
    char* token = strtok(newop, ":");
    sscanf(token, "%" PRId64, &block_num);
    token = strtok(NULL, ":");
    sscanf(token, "%"PRId64, &offset);
    token = strtok(NULL, ":");
    sscanf(token, "%"PRId64, &next_offset);

    /* find length of compressed block */
    unsigned int length = (unsigned int)(next_offset - offset);

    /* compute offset to start of uncompressed block in target file */
    int64_t in_offset = block_num * block_size;

    /* compute max size of buffer to hold uncompressed data:
     * BZ2 scheme requires block_size + 1% + 600 bytes */
    unsigned int outSize = (unsigned int)((block_size * 10 * 1.01) + 600);
    MFU_LOG(MFU_LOG_INFO, "Block size=%" PRId64 "\n", block_size);

    /* allocate input and output buffers */
    char* obuf = (char*) MFU_MALLOC(sizeof(char) * outSize);
    char* ibuf = (char*) MFU_MALLOC(sizeof(char) * length);
    MFU_LOG(MFU_LOG_INFO, "Block specs:block_no=%" PRId64 "offset=%" PRId64 "next_offset%" PRId64 "length %u\n", block_num, offset, next_offset, length);


    /* Read from correct position of compressed file */
    lseek64(fd, offset, SEEK_SET);
    ssize_t inSize = mfu_read(fname, fd, (char*)ibuf, length);
    MFU_LOG(MFU_LOG_DBG, "The string is=%s\n", ibuf);

    /* Perform decompression */
    int ret = BZ2_bzBuffToBuffDecompress(obuf, &outSize, ibuf, length, 0, 0);
    MFU_LOG(MFU_LOG_DBG, "After Decompresssion=%s,size=%uret=%d\n", obuf, outSize, ret);
    /*     if(ret!=0)
            {
                    printf("Error in compression for rank %d",rank);
                    MPI_Finalize();
                    exit(1);
            }    */

    /*write result to correct offset in file*/
    lseek64(fd_out, in_offset, SEEK_SET);
    mfu_write(fname_out, fd_out, obuf, outSize);

    /* free buffers */
    mfu_free(&ibuf);
    mfu_free(&obuf);
}

void mfu_decompress_bz2(const char* src_name, const char* fname_op)
{
    /* make a copy of the target file name */
    strncpy(fname,     src_name, 49);
    strncpy(fname_out, fname_op, 49);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* open compressed file for reading */
    MFU_LOG(MFU_LOG_INFO, "The file name is:%s\n", fname);
    fd = mfu_open(fname, O_RDONLY | O_BINARY | O_LARGEFILE);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for reading rank %d", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* the 3rd character stored in the file gives the block size used for compression */
    char size_str[5];
    mfu_read(fname, fd, (char*)size_str, 4 * sizeof(char));
    int bc_size = size_str[3] - '0';
    MFU_LOG(MFU_LOG_INFO, "The block size %d\n", bc_size);

    /* seek back to start of source file */
    lseek64(fd, 0, SEEK_SET);

    struct stat st;
    stat(fname, &st);

    /* Open file for writing and change permissions */
    if (rank == 0) {
        /* open output file for writing */
        fd_out = mfu_open(fname_out, O_CREAT | O_RDWR | O_TRUNC | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* TODO: should we be setting this on the target file instead? */

        /* set timestamps on file */
        struct utimbuf uTimBuf;
        uTimBuf.actime  = st.st_atime;
        uTimBuf.modtime = st.st_mtime;
        utime(fname, &uTimBuf);

        /* set mode and group on file */
        chmod(fname, st.st_mode);
        chown(fname, st.st_uid, st.st_gid);
    }

    /* wait for rank 0 to complete */
    MPI_Barrier(MPI_COMM_WORLD);

    /* rest of ranks open output file */
    if (rank != 0) {
        fd_out = mfu_open(fname_out, O_RDWR | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* compute block size in bytes */
    block_size = bc_size * 100 * 1024;

    /* Actual call to libcircle and callback fuction setting */
    CIRCLE_init(0, NULL, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&DBz2_decompEnqueue);
    CIRCLE_cb_process(&DBz2_decompDequeue);
    CIRCLE_begin();
    CIRCLE_finalize();

    /* close source and target files */
    mfu_close(fname_out, fd_out);
    mfu_close(fname, fd);
}
