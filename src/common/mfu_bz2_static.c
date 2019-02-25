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

void mfu_compress_bz2_static(const char* src_name, const char* dst_name, int b_size)
{
    /* get rank and size of communicator */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* read stat info for source file */
    struct stat st;
    int64_t filesize = 0;
    if (rank == 0) {
        /* TODO: check that stat succeeds */
        mfu_lstat(src_name, &st);

        filesize = (int64_t) st.st_size;
    }
    MPI_Bcast(&filesize, 1, MPI_INT64_T, 0, MPI_COMM_WORLD);

    /* TODO: check that b_size is in range of [1,9] */
    /* compute block size in bytes */
    int64_t block_size = (int64_t)b_size * 100 * 1024;

    /* compute total number of blocks in the file */
    int64_t tot_blocks = (int64_t)(filesize) / block_size;
    if (tot_blocks * block_size < filesize) {
        tot_blocks++;
    }

    /* compute max number of blocks this process will handle */
    int64_t blocks_per_rank = tot_blocks / ranks;
    if (blocks_per_rank * ranks < tot_blocks) {
        blocks_per_rank++;
    }

    /* open the source file for reading */
    int fd = mfu_open(src_name, O_RDONLY | O_BINARY | O_LARGEFILE);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for reading rank %d", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* The file for output is opened and options set */
    int fd_out = -1;
    if (rank == 0) {
        /* open file */
        fd_out = mfu_open(dst_name, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* wait for rank 0 to finish operations */
    MPI_Barrier(MPI_COMM_WORLD);

    /* have rest of ranks open the file */
    if (rank != 0) {
        fd_out = mfu_open(dst_name, O_WRONLY | O_BINARY | O_LARGEFILE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* BZ2 requires block size + 1% plus 600 bytes */
    int64_t comp_buff_size = 1.01 * block_size + 600;

    /* define amount of memory to use for compressing data */
    size_t bufsize = 128 * 1024 * 1024;
    if (bufsize < (size_t)comp_buff_size) {
        bufsize = (size_t)comp_buff_size;
    }

    /* compute number of blocks we can fit in buffer */
    uint64_t blocks_per_buffer = bufsize / comp_buff_size;

    /* compute number of waves to finish file */
    int64_t blocks_per_wave = ranks * blocks_per_buffer;
    int64_t num_waves = tot_blocks / blocks_per_wave;
    if (num_waves * blocks_per_wave < tot_blocks) {
        num_waves += 1;
    }

    /* allocate array to track offsets into compressed file where we
     * write each of our compressed blocks */
    uint64_t* my_offsets = (uint64_t*) MFU_MALLOC(blocks_per_rank * sizeof(uint64_t));
    uint64_t* my_lengths = (uint64_t*) MFU_MALLOC(blocks_per_rank * sizeof(uint64_t));

    /* array to store offsets and totals of each set of blocks */
    uint64_t* block_lengths = (uint64_t*) MFU_MALLOC(blocks_per_buffer * sizeof(int64_t));
    uint64_t* block_offsets = (uint64_t*) MFU_MALLOC(blocks_per_buffer * sizeof(int64_t));
    uint64_t* block_totals  = (uint64_t*) MFU_MALLOC(blocks_per_buffer * sizeof(int64_t));

    /* allocate a compression buffer for each block */
    char** a = (char**) MFU_MALLOC(blocks_per_buffer * sizeof(char*));
    for (int i = 0; i < blocks_per_buffer; i++) {
        a[i] = (char*) MFU_MALLOC(comp_buff_size * sizeof(char));
    }

    /* allocate buffer to read data from source file */
    char* ibuf = (char*) MFU_MALLOC(sizeof(char) * block_size);

    /* Call libcircle in a loop to work each wave as a single instance of libcircle */
    int64_t my_blocks = 0;
    int64_t last_offset = 0;
    int64_t blocks_processed = 0;
    for (int64_t blocks_done = 0; blocks_done < tot_blocks; blocks_done += blocks_per_wave) {
        for (int k = 0; k < blocks_per_buffer; k++) {
            block_lengths[k] = 0;
            block_offsets[k] = 0;
        }

        for (int k = 0; k < blocks_per_buffer; k++) {
            /* compute block number for this process */
            int64_t block_no = blocks_processed + rank;

            /* compute starting offset in source file to read from */
            off_t pos = block_no * block_size;
            if (pos < filesize) {
                /* seek to offset in source file for this block */
                mfu_lseek(src_name, fd, pos, SEEK_SET);

                /* compute number of bytes to read from input file */
                size_t nread = (size_t) block_size;
                size_t remainder = (size_t) (filesize - pos);
                if (remainder < nread) {
                    nread = remainder;
                }

                /* read block from input file */
                ssize_t inSize = mfu_read(src_name, fd, ibuf, nread);

                /* Guaranteed max output size after compression for bz2 */
                unsigned int outSize = (unsigned int)((block_size * 1.01) + 600);

                /* compress block from read buffer into next compression buffer */
                int ret = BZ2_bzBuffToBuffCompress(a[k], &outSize, ibuf, (int)inSize, b_size, 0, 30);
                if (ret != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Error in compression for rank %d", rank);
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }

                /* return size of buffer */
                block_lengths[k] = (uint64_t) outSize;
            }

            blocks_processed += ranks;
        }

        MPI_Exscan(block_lengths,    block_offsets, blocks_per_buffer, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        MPI_Allreduce(block_lengths, block_totals,  blocks_per_buffer, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

        /* Each process writes out the blocks it processed in current wave at the correct offset */
        for (int k = 0; k < blocks_per_buffer; k++) {
            if (block_lengths[k] > 0) {
                /* compute offset into compressed file for our block */
                off_t pos = last_offset + block_offsets[k];

                /* record our offset */
                my_offsets[my_blocks] = (uint64_t) pos;
                my_lengths[my_blocks] = block_lengths[k];
                my_blocks++;

                /* seek to position and write out block */
                mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
                mfu_write(dst_name, fd_out, a[k], block_lengths[k]);

                /* update offset for next set of blocks */
                last_offset += block_totals[k];
            }
        }
    }

    /* free read buffer */
    mfu_free(&ibuf);

    /* free memory regions used to store compress blocks */
    for (int i = 0; i < blocks_per_buffer; i++) {
        mfu_free(&a[i]);
    }
    mfu_free(&a);

    /* End of all waves */
    MPI_Barrier(MPI_COMM_WORLD);

    /* TODO: gather these to rank 0 for writing */
    /* Each process writes the offset of all blocks processed by it at corect location in trailer */
    for (int k = 0; k < my_blocks; k++) {
        int64_t block_no = ranks * k + rank;
        off_t pos = last_offset + block_no * 16;
        mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
        mfu_write(dst_name, fd_out, &my_offsets[k], 8);
        mfu_write(dst_name, fd_out, &my_lengths[k], 8);
    }

    /* root writes the locaion of trailer start to last 8 bytes of the file */
    if (rank == 0) {
        /* TODO: also record block size for computing offsets when decompressing file */
        off_t pos = last_offset + tot_blocks * 16;
        mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
        mfu_write(dst_name, fd_out, &last_offset, 8);
    }

    mfu_free(&block_totals);
    mfu_free(&block_offsets);
    mfu_free(&block_lengths);

    /* close source and target files */
    //mfu_fsync(dst_name, fd_out);
    mfu_close(dst_name, fd_out);
    mfu_close(src_name, fd);

    if (rank == 0) {
        /* set mode and group */
        mfu_chmod(dst_name, st.st_mode);
        mfu_lchown(dst_name, st.st_uid, st.st_gid);

        /* set timestamps, mode, and group */
        struct utimbuf uTimBuf;
        uTimBuf.actime  = st.st_atime;
        uTimBuf.modtime = st.st_mtime;
        utime(dst_name, &uTimBuf);
    }
}

void mfu_decompress_bz2_static(const char* src_name, const char* dst_name)
{
    /* get rank and size of communicator */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* open compressed file for reading */
    int fd = mfu_open(src_name, O_RDONLY | O_BINARY | O_LARGEFILE);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for reading rank %d", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* read block size from file */
    struct stat st;
    int bc_size;
    int64_t filesize;
    if (rank == 0) {
        /* the 3rd character stored in the file gives the block size used for compression */
        char size_str[5];
        mfu_read(src_name, fd, (char*)size_str, 4 * sizeof(char));
        bc_size = size_str[3] - '0';
    
        /* seek back to start of source file */
        mfu_lseek(src_name, fd, 0, SEEK_SET);
    
        mfu_lstat(src_name, &st);

        filesize = (int64_t) st.st_size;
    }
    MPI_Bcast(&bc_size,  1, MPI_INT,     0, MPI_COMM_WORLD);
    MPI_Bcast(&filesize, 1, MPI_INT64_T, 0, MPI_COMM_WORLD);

    /* Open file for writing and change permissions */
    int fd_out = -1;
    if (rank == 0) {
        /* open output file for writing */
        fd_out = mfu_open(dst_name, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* wait for rank 0 to complete */
    MPI_Barrier(MPI_COMM_WORLD);

    /* rest of ranks open output file */
    if (rank != 0) {
        fd_out = mfu_open(dst_name, O_WRONLY | O_BINARY | O_LARGEFILE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* compute block size in bytes */
    int64_t block_size = bc_size * 100 * 1024;

    /* The last 8 bytes of the trailer stores the location or offset of the start of the trailer */
    int64_t trailer_begin;
    if (rank == 0) {
        mfu_lseek(src_name, fd, -8, SEEK_END);
        mfu_read(src_name, fd, &trailer_begin, 8);
    }
    MPI_Bcast(&trailer_begin, 1, MPI_INT64_T, 0, MPI_COMM_WORLD);

    /* compute number of blocks */
    int64_t total_blocks = (filesize - trailer_begin - 8) / 16;

    size_t bufsize = (size_t) (block_size * 1.01 + 600);
    char* obuf = (char*) MFU_MALLOC(bufsize);
    char* ibuf = (char*) MFU_MALLOC(bufsize);

    int64_t processed_blocks = 0;
    while (processed_blocks < total_blocks) {
        /* compute block id we'll process in this loop */
        int64_t block_no = processed_blocks + rank;

        /* seek to start of block */
        off_t pos = (off_t) (trailer_begin + block_no * 16);
        mfu_lseek(src_name, fd, pos, SEEK_SET);
    
        /* read offset of start value */
        int64_t offset, length;
        mfu_read(src_name, fd, &offset, 8);
        mfu_read(src_name, fd, &length, 8);

        /* compute max size of buffer to hold uncompressed data:
         * BZ2 scheme requires block_size + 1% + 600 bytes */
        unsigned int outSize = (unsigned int)block_size;

        /* Read from correct position of compressed file */
        mfu_lseek(src_name, fd, offset, SEEK_SET);
        ssize_t inSize = mfu_read(src_name, fd, (char*)ibuf, length);

        /* Perform decompression */
        int ret = BZ2_bzBuffToBuffDecompress(obuf, &outSize, ibuf, length, 0, 0);
        if (ret != 0) {
            printf("Error in decompression for rank %d",rank);
            //MPI_Finalize();
            exit(1);
        }

        /* compute offset to start of uncompressed block in target file */
        int64_t in_offset = block_no * block_size;

        /* write result to correct offset in file */
        mfu_lseek(dst_name, fd_out, in_offset, SEEK_SET);
        mfu_write(dst_name, fd_out, obuf, outSize);

        processed_blocks += ranks;
    }

    /* free buffers */
    mfu_free(&ibuf);
    mfu_free(&obuf);

    /* close source and target files */
    //mfu_fsync(dst_name, fd_out);
    mfu_close(dst_name, fd_out);
    mfu_close(src_name, fd);

    if (rank == 0) {
        /* set mode and group on file */
        mfu_chmod(dst_name, st.st_mode);
        mfu_lchown(dst_name, st.st_uid, st.st_gid);

        /* set timestamps on file */
        struct utimbuf uTimBuf;
        uTimBuf.actime  = st.st_atime;
        uTimBuf.modtime = st.st_mtime;
        utime(dst_name, &uTimBuf);
    }
}
