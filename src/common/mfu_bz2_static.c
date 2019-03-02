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

int mfu_compress_bz2_static(const char* src_name, const char* dst_name, int b_size)
{
    int rc = MFU_SUCCESS;

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
    int64_t bwt_size = (int64_t)b_size * 100 * 1000;

    /* slice file into integer number of blocks based on bwt size */
    int64_t bwt_per_block = (10 * 1024 * 1024) / bwt_size;
    int64_t block_size = bwt_per_block * bwt_size;

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

#if LUSTRE_SUPPORT
    /* The file for output is opened and options set */
    int fd_out = -1;
    if (rank == 0) {
        mfu_stripe_set(dst_name, 1024*1024, -1);
    }

    /* wait for rank 0 to finish operations */
    MPI_Barrier(MPI_COMM_WORLD);

    /* have rest of ranks open the file */
    fd_out = mfu_open(dst_name, O_WRONLY | O_BINARY | O_LARGEFILE);
    if (fd_out < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
#else
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
#endif

    /* given original data of size B, BZ2 compressed data can take up to B * 1.01 + 600 bytes,
     * we use 2% to be on safe side */
    int64_t comp_buff_size = (int64_t) (1.02 * (double)block_size + 600.0);

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
                unsigned int outSize = (unsigned int)comp_buff_size;

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
                my_offsets[my_blocks] = mfu_hton64((uint64_t)pos);
                my_lengths[my_blocks] = mfu_hton64(block_lengths[k]);
                my_blocks++;

                /* seek to position and write out block */
                mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
                mfu_write(dst_name, fd_out, a[k], block_lengths[k]);
            }

            /* update offset for next set of blocks */
            last_offset += block_totals[k];
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

/*
    size_t footer_offset_size = 10 * 1024 * 1024;
    size_t single_pair_size = 16 * ranks;
    if (footer_offset_size < single_pair_size) {
        footer_offset_size = single_pair_size;
    }
    int64_t pairs_per_gather_step = footer_offset_size / single_pair_size;
    for (i = 0; i < pairs_per_gather_step; i++) {
    }
*/

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
        /* convert header fields to network order */
        uint64_t footer[5];
        footer[0] = mfu_hton64(last_offset); /* offset to start of block metadata */
        footer[1] = mfu_hton64(tot_blocks);  /* number of blocks in the file */
        footer[2] = mfu_hton64(block_size);  /* max size of uncompressed block */
        footer[3] = mfu_hton64(1);           /* file version number */
        footer[4] = mfu_hton64(0x3141314131413141); /* magic number (repeating pi: 3.141) */

        /* TODO: also record block size for computing offsets when decompressing file */
        off_t pos = last_offset + tot_blocks * 16;
        mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
        mfu_write(dst_name, fd_out, footer, 5 * 8);
    }

    mfu_free(&block_totals);
    mfu_free(&block_offsets);
    mfu_free(&block_lengths);

    /* close source and target files */
    mfu_fsync(dst_name, fd_out);
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

    return rc;
}

int mfu_decompress_bz2_static(const char* src_name, const char* dst_name)
{
    int rc = MFU_SUCCESS;

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
    uint64_t footer[6];
    if (rank == 0) {
        /* read footer from end of file */
        uint64_t file_footer[5];
        mfu_lseek(src_name, fd, -40, SEEK_END);
        mfu_read(src_name, fd, file_footer, 40);

        /* convert fields from network to host order */
        footer[0] = mfu_ntoh64(file_footer[0]);
        footer[1] = mfu_ntoh64(file_footer[1]);
        footer[2] = mfu_ntoh64(file_footer[2]);
        footer[3] = mfu_ntoh64(file_footer[3]);
        footer[4] = mfu_ntoh64(file_footer[4]);

        /* seek back to start of source file */
        mfu_lseek(src_name, fd, 0, SEEK_SET);
    
        /* get size of file */
        mfu_lstat(src_name, &st);
        footer[5] = (uint64_t) st.st_size;
    }
    MPI_Bcast(&footer, 6, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* extract values from footer into local variables */
    int64_t last_offset  = (int64_t)footer[0];
    int64_t total_blocks = (int64_t)footer[1];
    int64_t block_size   = (int64_t)footer[2];
    uint64_t version     = footer[3];
    uint64_t magic       = footer[4];
    int64_t filesize     = (int64_t)footer[5];

    if (magic != 0x3141314131413141) {
        /* error! */
    }

    if (version != 1) {
        /* error! */
    }

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

    size_t bufsize = (size_t) ((double)block_size * 1.02 + 600.0);
    char* obuf = (char*) MFU_MALLOC(bufsize);
    char* ibuf = (char*) MFU_MALLOC(bufsize);

    int64_t processed_blocks = 0;
    while (processed_blocks < total_blocks) {
        /* compute block id we'll process in this loop */
        int64_t block_no = processed_blocks + rank;

        if (block_no < total_blocks) {
            /* seek to start of block */
            off_t pos = (off_t) (last_offset + block_no * 16);
            mfu_lseek(src_name, fd, pos, SEEK_SET);
        
            /* read offset of start value */
            uint64_t net_offset, net_length;
            mfu_read(src_name, fd, &net_offset, 8);
            mfu_read(src_name, fd, &net_length, 8);
    
            /* convert values from network to host order */
            int64_t offset = mfu_ntoh64(net_offset);
            int64_t length = mfu_ntoh64(net_length);
    
            /* compute max size of buffer to hold uncompressed data:
             * BZ2 scheme requires block_size + 1% + 600 bytes */
            unsigned int outSize = (unsigned int)block_size;
    
            /* Read from correct position of compressed file */
            mfu_lseek(src_name, fd, offset, SEEK_SET);
            ssize_t inSize = mfu_read(src_name, fd, (char*)ibuf, length);
            if (inSize != (ssize_t)length) {
                MFU_LOG(MFU_LOG_ERR, "Failed to read expected amount of data at %lx expected %llu got %llu", pos, length, inSize);
                exit(1);
            }
    
            /* Perform decompression */
            int ret = BZ2_bzBuffToBuffDecompress(obuf, &outSize, ibuf, length, 0, 0);
            if (ret != 0) {
                MFU_LOG(MFU_LOG_ERR, "Error in decompression");
                //MPI_Finalize();
                exit(1);
            }
    
            /* compute offset to start of uncompressed block in target file */
            int64_t in_offset = block_no * block_size;
    
            /* write result to correct offset in file */
            mfu_lseek(dst_name, fd_out, in_offset, SEEK_SET);
            mfu_write(dst_name, fd_out, obuf, outSize);
        }

        processed_blocks += ranks;
    }

    /* free buffers */
    mfu_free(&ibuf);
    mfu_free(&obuf);

    /* close source and target files */
    mfu_fsync(dst_name, fd_out);
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

    return rc;
}
