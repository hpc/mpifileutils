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

struct block_info {
    unsigned int length;
    int rank;
    int64_t offset;
    int64_t sno;
};

static struct block_info* my_blocks;
static int64_t blocks_processed = 0;
static char** a;
static int64_t my_prev_blocks = 0;
static int64_t blocks_pn_pw;
static int64_t block_size;
static int64_t comp_buff_size;
static int64_t blocks_done = 0;
static int64_t wave_blocks;
static int64_t tot_blocks;
static int bc_size;
static int64_t filesize;
static char* src_name;
static char* dst_name;
static int fd;
static int fd_out;
static int64_t my_tot_blocks = 0;

/* To use libcircle, this function creates work for compression.
 * It simply puts the all the block numbers for the file in the queue. */
static void DBz2_Enqueue(CIRCLE_handle* handle)
{
    char* newop;
    for (int i = 0; i < wave_blocks; i++) {
        /* compute block id */
        int64_t block_no = (int64_t)i + (int64_t)blocks_done;
        if (block_no >= tot_blocks) {
            break;
        }

        /* encode block id as string */
        newop = (char*)MFU_MALLOC(sizeof(char) * 10);
        sprintf(newop, "%" PRId64, block_no);

        /* enqueue this block */
        handle->enqueue(newop);

        MFU_LOG(MFU_LOG_INFO, "Blocks queued=%" PRId64 "\n", block_no);

        mfu_free(&newop);
    }
}

/* process each compression block */
static void DBz2_Dequeue(CIRCLE_handle* handle)
{
    /* used to check whether memory is full because the number of blocks
    to be processed in a wave have been completed for this wave */

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* dequeue an item */
    char newop[10];
    handle->dequeue(newop);

    /* extract block id */
    int64_t block_no;
    sscanf(newop, "%" PRId64, &block_no);

    /* compute starting offset in source file to read from */
    off_t pos = block_no * block_size;

    /* seek to offset in source file for this block */
    off_t lseek_rc = mfu_lseek(src_name, fd, pos, SEEK_SET);
    if (lseek_rc == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to seek in source file: %s offset=%lx errno=%d (%s)",
            src_name, pos, errno, strerror(errno));
        //rc = MFU_FAILURE;
        //continue;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* compute number of bytes to read from input file */
    size_t nread = (size_t) block_size;
    size_t remainder = (size_t) (filesize - pos);
    if (remainder < nread) {
        nread = remainder;
    }

    /* allocate a buffer to hold data from file */
    char* ibuf = MFU_MALLOC(nread);

    /* read block from input file */
    ssize_t inSize = mfu_read(src_name, fd, ibuf, nread);
    if (inSize != nread) {
        MFU_LOG(MFU_LOG_ERR, "Failed to read from source file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
            src_name, pos, inSize, nread, errno, strerror(errno));
        //rc = MFU_FAILURE;
        //continue;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* Guaranteed max output size after compression for bz2 */
    unsigned int outSize = (unsigned int)comp_buff_size;

    /* compress block from read buffer into next compression buffer */
    int ret = BZ2_bzBuffToBuffCompress(a[blocks_processed], &outSize, ibuf, (int)inSize, bc_size, 0, 30);
    if (ret != 0) {
        MFU_LOG(MFU_LOG_ERR, "Error in compression for rank %d", rank);
        //rc = MFU_FAILURE;
        //continue;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* set metadata for the compressed block */
    my_blocks[my_tot_blocks].sno    = block_no;
    my_blocks[my_tot_blocks].length = outSize;
    my_blocks[my_tot_blocks].rank   = rank;

    /* increment count of blocks we have processed */
    blocks_processed++;
    my_tot_blocks++;
    MFU_LOG(MFU_LOG_INFO, "Processed block %" PRId64 ",num processed=%" PRId64 ",rank=%d, blocks per wave=%" PRId64 "\n", block_no, blocks_processed, rank, blocks_pn_pw);

    /* free read buffer */
    mfu_free(&ibuf);
}

static void find_wave_size(int64_t size, int opts_memory)
{
    /* get process memory limit from rlimit, if one is set */
    struct rlimit limit;
    getrlimit(RLIMIT_DATA, &limit);
    MFU_LOG(MFU_LOG_INFO, "The limit is %lld %lld\n", (long long)limit.rlim_cur, (long long)limit.rlim_max);

    /* identify free memory on the node */
    struct sysinfo info;
    sysinfo(&info);
    MFU_LOG(MFU_LOG_INFO, "The free and total ram are:%lu,%lu", info.freeram, info.totalram);
    MFU_LOG(MFU_LOG_INFO, "The block size is:%" PRId64, size);

    /* TODO: what about other procs on the same node? */

    /* set our memory limit to minimum of rlimit and free memory */
    int64_t mem_limit = info.freeram;
    if ((unsigned long)limit.rlim_cur < info.freeram) {
        mem_limit = (int64_t)limit.rlim_cur;
    }

    /* go lower still if user gave us a lower limit */
    if (opts_memory > 0 && opts_memory < mem_limit) {
        mem_limit = (int64_t)opts_memory;
    }

    /* memory is computed as the mem_limit is max memory for a process.
     * We leave 2% of totalram free, then 8*size +400*1024 is the
     * memory required to do compression, keep 128B for variables,etc.
     * The block itself must be in memory before compression. */
    int64_t wave_size_approx = mem_limit - (int64_t)info.totalram * 2 / 100 - 8 * size - 400 * 1024 - 128 - size;
    int64_t wave_size = wave_size_approx - 2 * tot_blocks * sizeof(struct block_info);
    blocks_pn_pw = (int64_t)(0.4 * wave_size / comp_buff_size);
    if (blocks_pn_pw > 800) {
        blocks_pn_pw = 800;
    }
    //int blocks_n_w=(int)blocks_pn_pw;

    /* find minimum across all processes */
    MPI_Allreduce(&blocks_pn_pw, &wave_blocks, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
}

int mfu_compress_bz2_libcircle(const char* src, const char* dst, int b_size, ssize_t opts_memory)
{
    int rc = MFU_SUCCESS;

    /* copy source and target file names */
    src_name = MFU_STRDUP(src);
    dst_name = MFU_STRDUP(dst);

    /* get rank and size of the communicator */
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* read stat info for source file */
    struct stat st;
    int stat_flag = 1;
    filesize = 0;
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
        mfu_free(&src_name);
        mfu_free(&dst_name);
        return MFU_FAILURE;
    }

    /* open the source file for reading */
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
        mfu_free(&src_name);
        mfu_free(&dst_name);
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
        mfu_free(&src_name);
        mfu_free(&dst_name);
        mfu_close(src_name, fd);
        return MFU_FAILURE;
    }

    /* ensure that b_size is in range of [1,9] */
    if (b_size < 1) {
        b_size = 1;
    }
    if (b_size > 9) {
        b_size = 9;
    }
    bc_size = b_size;

    /* compute block size in bytes */
    block_size = (int64_t)b_size * 100 * 1000;

    /* compute total number of blocks in the file */
    tot_blocks = filesize / block_size;
    if (tot_blocks * block_size < filesize) {
        tot_blocks++;
    }

    /* given original data of size B, BZ2 compressed data can take up to B * 1.01 + 600 bytes,
     * we use 2% to be on safe side */
    comp_buff_size = (int64_t) (1.02 * (double)block_size + 600.0);

    /* compute number of blocks we can handle in a wave
     * based on allowed memory per process */
    find_wave_size(block_size, opts_memory);

    blocks_processed = 0;
    my_prev_blocks   = 0;
    blocks_done      = 0;
    my_tot_blocks    = 0;

    /* compute number of waves to finish file */
    int64_t num_waves = tot_blocks / wave_blocks;
    if (num_waves * wave_blocks < tot_blocks) {
        num_waves += 1;
    }

    /* stores metadata of all blocks processed by this process */
    my_blocks = (struct block_info*)MFU_MALLOC(sizeof(struct block_info) * blocks_pn_pw * num_waves);
    struct block_info** this_wave_blocks = (struct block_info**)MFU_MALLOC(sizeof(struct block_info*)*wave_blocks);

    MPI_Datatype metatype, oldtypes[3];
    MPI_Aint offsets[3], extent, lb;
    int blockcounts[3];

    offsets[0]     = 0;
    oldtypes[0]    = MPI_UNSIGNED;
    blockcounts[0] = 1;
    MPI_Type_get_extent(MPI_UNSIGNED, &lb, &extent);

    offsets[1]     = extent;
    oldtypes[1]    = MPI_INT;
    blockcounts[1] = 1;
    MPI_Type_get_extent(MPI_INT, &lb, &extent);

    offsets[2]     = extent + offsets[1];
    oldtypes[2]    = MPI_INT64_T;
    blockcounts[2] = 2;

    MPI_Type_create_struct(3, blockcounts, offsets, oldtypes, &metatype);
    MPI_Type_commit(&metatype);

    struct block_info rbuf[wave_blocks];

    /* allocate a compression buffer for each block */
    a = (char**)MFU_MALLOC(sizeof(char*) * wave_blocks);
    for (int i = 0; i < wave_blocks; i++) {
        a[i] = (char*)MFU_MALLOC(comp_buff_size);
        //memset(a[i],1,comp_buff_size*sizeof(char));
    }

    /* Call libcircle in a loop to work each wave as a single instance of libcircle */
    int64_t last_offset = 0;
    for (blocks_done = 0; blocks_done < tot_blocks; blocks_done += wave_blocks) {
        /* compute number of blocks in this wave */
        int blocks_for_wave = wave_blocks;
        int blocks_remaining = tot_blocks - blocks_done;
        if (blocks_remaining < blocks_for_wave) {
            blocks_for_wave = blocks_remaining;
        }

        /* execute libcircle to compress blocks */
        CIRCLE_init(0, NULL, CIRCLE_DEFAULT_FLAGS);
        CIRCLE_cb_create(&DBz2_Enqueue);
        CIRCLE_cb_process(&DBz2_Dequeue);
        CIRCLE_begin();
        CIRCLE_finalize();

        /* gather the number of blocks processed by each process in this wave */
        int rcount[size];
        MPI_Gather(&blocks_processed, 1, MPI_INT, rcount, 1, MPI_INT, 0, MPI_COMM_WORLD);

        /* actual number of blocks processed by all processes in this wave */
        int64_t actual_wave_blocks = 0;
        for (int k = 0; k < size; k++) {
            actual_wave_blocks += (int64_t)rcount[k];
        }

        /* compute displacements array for gatherv */
        int displs[size];
        displs[0] = 0;
        for (int k = 1; k < size; k++) {
            displs[k] = displs[k - 1] + rcount[k - 1];
        }

        /* Gather metadata of all blocks processed in this wave */
        MPI_Gatherv(&my_blocks[my_prev_blocks], blocks_processed, metatype, rbuf, rcount, displs, metatype, 0, MPI_COMM_WORLD);

        /* compute the offset of all blocks processed in current wave */
        if (rank == 0) {
            for (int k = 0; k < actual_wave_blocks; k++) {
                this_wave_blocks[rbuf[k].sno - blocks_done] = &rbuf[k];
            }

            this_wave_blocks[0]->offset = last_offset;

            for (int k = 1; k < actual_wave_blocks; k++) {
                this_wave_blocks[k]->offset = this_wave_blocks[k - 1]->offset + this_wave_blocks[k - 1]->length;
            }

            last_offset = this_wave_blocks[actual_wave_blocks - 1]->offset + this_wave_blocks[actual_wave_blocks - 1]->length;
        }

        /* provide info about the offset of coressponding blocks to process that processed it */
        MPI_Scatterv(&rbuf, rcount, displs, metatype, &my_blocks[my_prev_blocks], blocks_processed, metatype, 0, MPI_COMM_WORLD);

        /* Each process writes out the blocks it processed in current wave at the correct offset */
        for (int k = 0; k < blocks_processed; k++) {
            /* compute offset into compressed file for our block */
            off_t pos = my_blocks[my_prev_blocks + k].offset;

            /* seek to position in destination file for this block */
            off_t lseek_rc = mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
            if (lseek_rc == (off_t)-1) {
                MFU_LOG(MFU_LOG_ERR, "Failed to seek to compressed block in target file: %s offset=%lx errno=%d (%s)",
                    dst_name, pos, errno, strerror(errno));
                rc = MFU_FAILURE;
            }

            /* write out block */
            size_t my_length = (size_t) my_blocks[my_prev_blocks + k].length;
            ssize_t nwritten = mfu_write(dst_name, fd_out, a[k], my_length);
            if (nwritten != my_length) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write compressed block to target file: %s offset=%lx got=%d expected=%d errno=%d (%s)",
                    dst_name, pos, nwritten, my_length, errno, strerror(errno));
                rc = MFU_FAILURE;
            }
        }

        my_prev_blocks = my_tot_blocks;
        blocks_processed = 0;
    }

    /* free buffers used to hold compressed data */
    for (int i = 0; i < wave_blocks; i++) {
        mfu_free(&a[i]);
    }
    mfu_free(&a);

    /* End of all waves */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Broadcast offset of start of trailer */
    MPI_Bcast(&last_offset, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);

    /* Each process writes the offset of all blocks processed by it at corect location in trailer */
    for (int k = 0; k < my_tot_blocks; k++) {
        /* seek to metadata location for this block */
        off_t pos = last_offset + my_blocks[k].sno * 16;
        off_t lseek_rc = mfu_lseek(dst_name, fd_out, pos, SEEK_SET);
        if (lseek_rc == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to seek to block metadata in target file: %s offset=%lx errno=%d (%s)",
                dst_name, pos, errno, strerror(errno));
            rc = MFU_FAILURE;
        }

        /* write offset of block in destination file */
        int64_t my_offset = my_blocks[k].offset;
        int64_t net_offset = mfu_hton64(my_offset);
        ssize_t nwritten = mfu_write(dst_name, fd_out, &net_offset, 8);
        if (nwritten != 8) {
            MFU_LOG(MFU_LOG_ERR, "Failed to write block offset to target file: %s pos=%lx got=%d expected=%d errno=%d (%s)",
                dst_name, pos, nwritten, 8, errno, strerror(errno));
            rc = MFU_FAILURE;
        }

        /* write length of block in destination file */
        int64_t my_length = my_blocks[k].length;
        int64_t net_length = mfu_hton64(my_length);
        nwritten = mfu_write(dst_name, fd_out, &net_length, 8);
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

    /* free memory for compress blocks */
    mfu_free(&this_wave_blocks);
    mfu_free(&my_blocks);

    /* close source and target files */
    mfu_fsync(dst_name, fd_out);
    mfu_close(dst_name, fd_out);
    mfu_close(src_name, fd);

    /* ensure that everyone has closed and synced before updating timestamps */
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

    mfu_free(&src_name);
    mfu_free(&dst_name);

    return rc;
}
