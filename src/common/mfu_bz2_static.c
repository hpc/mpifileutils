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

int mfu_compress_bz2_static(const char* src_name, const char* dst_name, int b_size)
{
    int rc = MFU_SUCCESS;

    /* get rank and size of communicator */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* read stat info for source file */
    struct stat st;
    int stat_flag = 1;
    int64_t filesize = 0;
    if (rank == 0) {
        /* stat file to get file size */
        int lstat_rc = mfu_lstat(src_name, &st);
        if (lstat_rc == 0) {
            filesize = (int64_t) st.st_size;
        } else {
            /* failed to stat file for file size */
            stat_flag = 0;
            MFU_LOG(MFU_LOG_ERR, "Failed to stat file: %s errno=%d (%s)",
                src_name, errno, strerror(errno));
        }
    }

    /* broadcast filesize to all ranks */
    MPI_Bcast(&stat_flag, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&filesize, 1, MPI_INT64_T, 0, MPI_COMM_WORLD);

    /* check that we could stat file */
    if (! stat_flag) {
        return MFU_FAILURE;
    }

    /* ensure that b_size is in range of [1,9] */
    if (b_size < 1) {
        b_size = 1;
    }
    if (b_size > 9) {
        b_size = 9;
    }

    /* compute block size in bytes */
    int64_t bwt_size = (int64_t)b_size * 100 * 1000;

    /* slice file into integer number of blocks based on bwt size */
    int64_t max_block_size = 10 * 1024 * 1024;
    int64_t bwt_per_block = max_block_size / bwt_size;
    int64_t block_size = bwt_per_block * bwt_size;

    /* compute total number of blocks in the file */
    int64_t tot_blocks = filesize / block_size;
    if (tot_blocks * block_size < filesize) {
        tot_blocks++;
    }

    /* compute max number of blocks this process will handle */
    int64_t blocks_per_rank = tot_blocks / ranks;
    if (blocks_per_rank * ranks < tot_blocks) {
        blocks_per_rank++;
    }

    /* open the source file for reading */
    int fd = mfu_open(src_name, O_RDONLY | O_LARGEFILE);
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

    /* open destination file for writing */
    int fd_out = mfu_create_fully_striped(dst_name, FILE_MODE);
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
        /* initialize our counts to 0 for this wave */
        for (int k = 0; k < blocks_per_buffer; k++) {
            block_lengths[k] = 0;
            block_offsets[k] = 0;
        }

        /* compress blocks */
        for (int k = 0; k < blocks_per_buffer; k++) {
            /* compute block number for this process */
            int64_t block_no = blocks_processed + rank;

            /* compute starting offset in source file to read from */
            off_t pos = block_no * block_size;
            if (pos < filesize) {
                /* seek to offset in source file for this block */
                off_t lseek_rc = mfu_lseek(src_name, fd, pos, SEEK_SET);
                if (lseek_rc == (off_t)-1) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to seek in source file: %s offset=%lx errno=%d (%s)",
                        src_name, pos, errno, strerror(errno));
                    rc = MFU_FAILURE;
                    continue;
                }

                /* compute number of bytes to read from input file */
                size_t nread = (size_t) block_size;
                size_t remainder = (size_t) (filesize - pos);
                if (remainder < nread) {
                    nread = remainder;
                }

                /* read block from input file */
                ssize_t inSize = mfu_read(src_name, fd, ibuf, nread);
                if (inSize != nread) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to read from source file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                        src_name, pos, inSize, nread, errno, strerror(errno));
                    rc = MFU_FAILURE;
                    continue;
                }

                /* Guaranteed max output size after compression for bz2 */
                unsigned int outSize = (unsigned int)comp_buff_size;

                /* compress block from read buffer into next compression buffer */
                int ret = BZ2_bzBuffToBuffCompress(a[k], &outSize, ibuf, (int)inSize, b_size, 0, 30);
                if (ret != 0) {
                    MFU_LOG(MFU_LOG_ERR, "Error in compression for rank %d", rank);
                    rc = MFU_FAILURE;
                    continue;
                }

                /* return size of buffer */
                block_lengths[k] = (uint64_t) outSize;
            }

            blocks_processed += ranks;
        }

        /* execute scan and allreduce to compute offsets in compressed file
         * for each of our blocks in this wave */
        MPI_Exscan(block_lengths,    block_offsets, blocks_per_buffer, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        MPI_Allreduce(block_lengths, block_totals,  blocks_per_buffer, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

        /* Each process writes out the blocks it processed in current wave at the correct offset */
        for (int k = 0; k < blocks_per_buffer; k++) {
            /* write out our block if we have one,
             * this assumes a compressed block with consume
             * at least 1 byte, which is ensured by BZ2 */
            if (block_lengths[k] > 0) {
                /* compute offset into compressed file for our block */
                off_t pos = last_offset + block_offsets[k];

                /* record our offset */
                my_offsets[my_blocks] = mfu_hton64((uint64_t)pos);
                my_lengths[my_blocks] = mfu_hton64(block_lengths[k]);
                my_blocks++;

                /* seek to position in destination file for this block */
                off_t lseek_rc = mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
                if (lseek_rc == (off_t)-1) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to seek to compressed block in target file: %s offset=%lx errno=%d (%s)",
                        dst_name, pos, errno, strerror(errno));
                    rc = MFU_FAILURE;
                }

                /* write out block */
                ssize_t nwritten = mfu_write(dst_name, fd_out, a[k], block_lengths[k]);
                if (nwritten != block_lengths[k]) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to write compressed block to target file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                        dst_name, pos, nwritten, block_lengths[k], errno, strerror(errno));
                    rc = MFU_FAILURE;
                }
            }

            /* update offset for next set of blocks */
            last_offset += block_totals[k];
        }
    }

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

    /* TODO: gather these in larger blocks to rank 0 for writing,
     * tight interleaving as written will not perform well with
     * byte range locking on lustre */

    /* Each process writes the offset of all blocks processed by it at corect location in trailer */
    for (int k = 0; k < my_blocks; k++) {
        /* seek to metadata location for this block */
        int64_t block_no = ranks * k + rank;
        off_t pos = last_offset + block_no * 16;
        off_t lseek_rc = mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
        if (lseek_rc == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to seek to block metadata in target file: %s offset=%lx errno=%d (%s)",
                dst_name, pos, errno, strerror(errno));
            rc = MFU_FAILURE;
        }

        /* write offset of block in destination file */
        ssize_t nwritten = mfu_write(dst_name, fd_out, &my_offsets[k], 8);
        if (nwritten != 8) {
            MFU_LOG(MFU_LOG_ERR, "Failed to write block offset to target file: %s pos=%lx got=%d expected=%d errno=%d (%s)",
                dst_name, pos, nwritten, 8, errno, strerror(errno));
            rc = MFU_FAILURE;
        }

        /* write length of block in destination file */
        nwritten = mfu_write(dst_name, fd_out, &my_lengths[k], 8);
        if (nwritten != 8) {
            MFU_LOG(MFU_LOG_ERR, "Failed to write block length to target file: %s pos=%lx got=%d expected=%d errno=%d (%s)",
                dst_name, pos+8, nwritten, 8, errno, strerror(errno));
            rc = MFU_FAILURE;
        }
    }

    /* root writes the locaion of trailer start to last 8 bytes of the file */
    if (rank == 0) {
        /* convert header fields to network order */
        uint64_t footer[6];
        footer[0] = mfu_hton64(last_offset); /* offset to start of block metadata */
        footer[1] = mfu_hton64(tot_blocks);  /* number of blocks in the file */
        footer[2] = mfu_hton64(block_size);  /* max size of uncompressed block */
        footer[3] = mfu_hton64(filesize);    /* size with all blocks decompressed */
        footer[4] = mfu_hton64(1);           /* file version number */
        footer[5] = mfu_hton64(0x3141314131413141); /* magic number (repeating pi: 3.141) */

        /* seek to position to write footer */
        off_t pos = last_offset + tot_blocks * 16;
        off_t lseek_rc = mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
        if (lseek_rc == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to seek to footer in target file: %s offset=%lx errno=%d (%s)",
                dst_name, pos, errno, strerror(errno));
            rc = MFU_FAILURE;
        }

        /* write footer */
        size_t footer_size = 6 * 8;
        ssize_t nwritten = mfu_write(dst_name, fd_out, footer, footer_size);
        if (nwritten != footer_size) {
            MFU_LOG(MFU_LOG_ERR, "Failed to write footer to target file: %s pos=%lx got=%d expected=%d errno=%d (%s)",
                dst_name, pos, nwritten, footer_size, errno, strerror(errno));
            rc = MFU_FAILURE;
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* free read buffer */
    mfu_free(&ibuf);

    /* free memory regions used to store compress blocks */
    for (int i = 0; i < blocks_per_buffer; i++) {
        mfu_free(&a[i]);
    }
    mfu_free(&a);

    mfu_free(&block_totals);
    mfu_free(&block_offsets);
    mfu_free(&block_lengths);

    mfu_free(&my_lengths);
    mfu_free(&my_offsets);

    /* close source and target files */
    mfu_fsync(dst_name, fd_out);
    mfu_close(dst_name, fd_out);
    mfu_close(src_name, fd);

    MPI_Barrier(MPI_COMM_WORLD);

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

    /* check that all processes wrote successfully */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        /* TODO: delete target file? */
        rc = MFU_FAILURE;
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
    int fd = mfu_open(src_name, O_RDONLY | O_LARGEFILE);
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
    int64_t block_meta  = (int64_t)footer[0]; /* offset to start of block metadata */
    int64_t block_total = (int64_t)footer[1]; /* number of blocks */
    int64_t block_size  = (int64_t)footer[2]; /* max uncompressed size of a block */
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
    int fd_out = mfu_create_fully_striped(dst_name, FILE_MODE);
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

    /* for an uncompressed block of size block_size, BZ2 compression
     * may compressed this to block_size * 1.01 + 600.  User 2% for
     * some breathing room when allocating buffer to read in compressed
     * block */
    size_t bufsize = (size_t) ((double)block_size * 1.02 + 600.0);
    char* obuf = (char*) MFU_MALLOC(bufsize);
    char* ibuf = (char*) MFU_MALLOC(bufsize);

    int64_t processed_blocks = 0;
    while (processed_blocks < block_total) {
        /* compute block id we'll process in this loop */
        int64_t block_no = processed_blocks + rank;

        /* read block, decompress, write to target file if in range */
        if (block_no < block_total) {
            /* seek to metadata for this block */
            off_t pos = (off_t) (block_meta + block_no * 16);
            off_t lseek_rc = mfu_lseek(src_name, fd, pos, SEEK_SET);
            if (lseek_rc == (off_t)-1) {
                MFU_LOG(MFU_LOG_ERR, "Failed to seek to block metadata in source file: %s offset=%lx errno=%d (%s)",
                    src_name, pos, errno, strerror(errno));
                rc = MFU_FAILURE;
                break;
            }
        
            /* read offset of block */
            uint64_t net_offset;
            ssize_t nread = mfu_read(src_name, fd, &net_offset, 8);
            if (nread != 8) {
                MFU_LOG(MFU_LOG_ERR, "Failed to read block offset from source file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                    src_name, pos, nread, 8, errno, strerror(errno));
                rc = MFU_FAILURE;
                break;
            }

            /* read length of block */
            uint64_t net_length;
            nread = mfu_read(src_name, fd, &net_length, 8);
            if (nread != 8) {
                MFU_LOG(MFU_LOG_ERR, "Failed to read block length from source file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                    src_name, pos+8, nread, 8, errno, strerror(errno));
                rc = MFU_FAILURE;
                break;
            }
    
            /* convert values from network to host order */
            int64_t offset = mfu_ntoh64(net_offset);
            int64_t length = mfu_ntoh64(net_length);
    
            /* compute max size of buffer to hold uncompressed data:
             * BZ2 scheme requires block_size + 1% + 600 bytes */
            unsigned int outSize = (unsigned int)block_size;
    
            /* seek to start of compressed block in source file */
            lseek_rc = mfu_lseek(src_name, fd, offset, SEEK_SET);
            if (lseek_rc == (off_t)-1) {
                MFU_LOG(MFU_LOG_ERR, "Failed to seek to block in source file: %s offset=%lx errno=%d (%s)",
                    src_name, offset, errno, strerror(errno));
                rc = MFU_FAILURE;
                break;
            }

            /* Read compressed block from source file */
            ssize_t inSize = mfu_read(src_name, fd, (char*)ibuf, length);
            if (inSize != (ssize_t)length) {
                MFU_LOG(MFU_LOG_ERR, "Failed to read block from source file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                    src_name, offset, inSize, length, errno, strerror(errno));
                rc = MFU_FAILURE;
                break;
            }
    
            /* Perform decompression */
            int ret = BZ2_bzBuffToBuffDecompress(obuf, &outSize, ibuf, inSize, 0, 0);
            if (ret != 0) {
                MFU_LOG(MFU_LOG_ERR, "Error in decompression");
                rc = MFU_FAILURE;
                break;
            }
    
            /* compute offset to start of uncompressed block in target file */
            int64_t in_offset = block_no * block_size;
    
            /* seek to position to write block in target file */
            lseek_rc = mfu_lseek(dst_name, fd_out, in_offset, SEEK_SET);
            if (lseek_rc == (off_t)-1) {
                MFU_LOG(MFU_LOG_ERR, "Failed to seek to block in target file: %s offset=%lx errno=%d (%s)",
                    dst_name, in_offset, errno, strerror(errno));
                rc = MFU_FAILURE;
                break;
            }

            /* write decompressed block to target file */
            ssize_t nwritten = mfu_write(dst_name, fd_out, obuf, outSize);
            if (nwritten != outSize) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write block in target file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                    dst_name, in_offset, nwritten, outSize, errno, strerror(errno));
                rc = MFU_FAILURE;
                break;
            }
        }

        processed_blocks += ranks;
    }

    /* wait for everyone to finish */
    MPI_Barrier(MPI_COMM_WORLD);

    /* free buffers */
    mfu_free(&ibuf);
    mfu_free(&obuf);

    /* close source and target files */
    mfu_fsync(dst_name, fd_out);
    mfu_close(dst_name, fd_out);
    mfu_close(src_name, fd);

    MPI_Barrier(MPI_COMM_WORLD);

    /* have rank 0 set meta data on target file */
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

    /* check that all processes wrote successfully */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        /* TODO: delete target file? */
        rc = MFU_FAILURE;
    }

    return rc;
}
