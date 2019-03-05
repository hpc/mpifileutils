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

struct block_info {
    unsigned int length;
    int rank;
    int64_t offset;
    int64_t sno;
};

struct block_info* my_blocks;
int64_t blocks_processed = 0;
char** a;
int64_t my_prev_blocks = 0;
int64_t blocks_pn_pw;
int64_t block_size;
int64_t blocks_done = 0;
int64_t wave_blocks;
int64_t tot_blocks;
int bc_size;
int rank;
char fname[50];
char fname_out[50];
int fd;
int fd_out;
int64_t my_tot_blocks = 0;

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
    }
}

/* process each compression block */
static void DBz2_Dequeue(CIRCLE_handle* handle)
{
    /* used to check whether memory is full because the number of blocks
    to be processed in a wave have been completed for this wave */

    /* dequeue an item */
    char newop[10];
    handle->dequeue(newop);

    /* extract block id */
    int64_t block_no;
    sscanf(newop, "%" PRId64, &block_no);

    /* seek to offset in source file for this block */
    lseek64(fd, block_no * block_size, SEEK_SET);

    /* read block from input file */
    char* ibuf = (char*)MFU_MALLOC(sizeof(char) * block_size); /*Input buffer*/
    ssize_t inSize = mfu_read(fname, fd, (char*)ibuf, (size_t)block_size);

    /* Guaranteed max output size */
    unsigned int outSize = (unsigned int)((block_size * 1.01) + 600);
    //a[blocks_processed]=malloc(sizeof(char)*outSize);

    /* compress block from read buffer into next compression buffer */
    int ret = BZ2_bzBuffToBuffCompress(a[blocks_processed], &outSize, ibuf, (int)inSize, bc_size, 0, 30);
    MFU_LOG(MFU_LOG_DBG, "After compresssion=%s,size=%u\n", a[blocks_processed], outSize);
    if (ret != 0) {
        MFU_LOG(MFU_LOG_ERR, "Error in compression for rank %d", rank);
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

static void find_wave_size(int b_size, int opts_memory)
{
    /* setting this for libcircle? */
    bc_size = b_size;

    /* compute block size in bytes */
    block_size = (int64_t)b_size * 100 * 1024;

    /* get process memory limit from rlimit, if one is set */
    struct rlimit limit;
    getrlimit(RLIMIT_DATA, &limit);
    MFU_LOG(MFU_LOG_INFO, "The limit is %lld %lld\n", (long long)limit.rlim_cur, (long long)limit.rlim_max);

    /* identify free memory on the node */
    struct sysinfo info;
    sysinfo(&info);
    MFU_LOG(MFU_LOG_INFO, "The free and total ram are:%lu,%lu", info.freeram, info.totalram);
    MFU_LOG(MFU_LOG_INFO, "The block size is:%" PRId64, block_size);

    /* TODO: what about other procs on the same node? */

    /* set our memory limit to minimum of rlimit and free memory */
    int64_t mem_limit;
    if ((unsigned long)limit.rlim_cur < info.freeram) {
        mem_limit = (int64_t)limit.rlim_cur;
    } else {
        mem_limit = (int64_t)info.freeram;
    }

    /* go lower still if user gave us a lower limit */
    if (opts_memory < mem_limit && opts_memory > 0) {
        mem_limit = (int64_t)opts_memory;
    }

    /* memory is computed as the mem_limit is max memory for a process.
     * We leave 2% of totalram free, then 8*block_size +400*1024 is the
     * memory required to do compression, keep 128B for variables,etc.
     * The block itself must be in memory before compression. */
    int64_t wave_size_approx = mem_limit - (int64_t)info.totalram * 2 / 100 - 8 * block_size - 400 * 1024 - 128 - block_size;
    int64_t comp_buff_size = 1.01 * block_size + 600; /* Compressed buffer size is the max size of a compressed block */
    int64_t waves_blocks_approx = wave_size_approx / comp_buff_size;
    int64_t wave_size = wave_size_approx - 2 * tot_blocks * sizeof(struct block_info);
    blocks_pn_pw = (int64_t)(0.4 * wave_size / comp_buff_size);
    if (blocks_pn_pw > 800) {
        blocks_pn_pw = 800;
    }
    //int blocks_n_w=(int)blocks_pn_pw;

    /* find minimum across all processes */
    MPI_Allreduce(&blocks_pn_pw, &wave_blocks, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
}

int mfu_compress_bz2_libcircle(const char* src_name, const char* dst_name, int b_size, ssize_t opts_memory)
{
    int rc = MFU_SUCCESS;

    /* set rank global variable */
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* get rank and size of the communicator */
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* read stat info for source file on rank 0 */
    struct stat st;
    int64_t filesize = 0;
    if (rank == 0) {
        /* TODO: check that stat succeeds */
        mfu_lstat(src_name, &st);
        filesize = (int64_t) st.st_size;
    }

    /* broadcast filesize to all ranks */
    MPI_Bcast(&filesize, 1, MPI_INT64_T, 0, MPI_COMM_WORLD);

    /* compute block size in bytes */
    block_size = (int64_t)b_size * 100 * 1024;

    /* compute total number of blocks in the file */
    int64_t tot_blocks = filesize / block_size;
    if (tot_blocks * block_size < filesize) {
        tot_blocks++;
    }

    /* compute number of blocks we can handle in a wave
     * based on allowed memory per process */
    find_wave_size(b_size, opts_memory);

    /* open the source file for readhing */
    fd = mfu_open(src_name, O_RDONLY | O_BINARY | O_LARGEFILE);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for reading: %s errno=%d (%s)",
            src_name, errno, strerror(errno));
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* The file for output is opened and options set */
    if (rank == 0) {
        /* open file */
        fd_out = mfu_open(dst_name, O_CREAT | O_RDWR | O_TRUNC | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing: %s errno=%d (%s)",
                dst_name, errno, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* wait for rank 0 to finish operations */
    MPI_Barrier(MPI_COMM_WORLD);

    /* have rest of ranks open the file */
    if (rank != 0) {
        fd_out = mfu_open(dst_name, O_RDWR | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing: %s errno=%d (%s)",
                dst_name, errno, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    blocks_processed = 0;
    my_prev_blocks   = 0;
    blocks_done      = 0;
    my_tot_blocks    = 0;

    /* given original data of size B, BZ2 compressed data can take up to B * 1.01 + 600 bytes,
     * we use 2% to be on safe side */
    int64_t comp_buff_size = (int64_t) (1.02 * (double)block_size + 600.0);

    /* compute number of waves to finish file */
    int64_t num_waves = tot_blocks / wave_blocks;
    if (num_waves * wave_blocks < tot_blocks) {
        num_waves += 1;
    }

    /* stores metadata of all blocks processed by this process */
    my_blocks = (struct block_info*)MFU_MALLOC(sizeof(struct block_info) * blocks_pn_pw * num_waves);
    struct block_info** this_wave_blocks = (struct block_info**)MFU_MALLOC(sizeof(struct block_info*)*wave_blocks);

    MPI_Datatype metatype, oldtypes[3];
    MPI_Aint offsets[3], extent;
    int blockcounts[3];

    offsets[0]     = 0;
    oldtypes[0]    = MPI_UNSIGNED;
    blockcounts[0] = 1;
    MPI_Type_extent(MPI_UNSIGNED, &extent);

    offsets[1]     = extent;
    oldtypes[1]    = MPI_INT;
    blockcounts[1] = 1;
    MPI_Type_extent(MPI_INT, &extent);

    offsets[2]     = extent + offsets[1];
    oldtypes[2]    = MPI_INT64_T;
    blockcounts[2] = 2;

    MPI_Type_struct(3, blockcounts, offsets, oldtypes, &metatype);
    MPI_Type_commit(&metatype);

    struct block_info rbuf[wave_blocks];

    /* Call libcircle in a loop to work each wave as a single instance of libcircle */
    int64_t last_offset = 0;
    for (blocks_done = 0; blocks_done < tot_blocks; blocks_done += wave_blocks) {
        /* compute number of blocksi n this wave */
        int blocks_for_wave;
        if ((tot_blocks - blocks_done) < wave_blocks) {
            blocks_for_wave = tot_blocks - blocks_done;
        } else {
            blocks_for_wave = wave_blocks;
        }

        /* allocate a compression buffer for each block */
        a = (char**)MFU_MALLOC(sizeof(char*)*blocks_for_wave);
        for (int i = 0; i < blocks_for_wave; i++) {
            a[i] = (char*)MFU_MALLOC(comp_buff_size * sizeof(char));
            //memset(a[i],1,comp_buff_size*sizeof(char));
        }

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

        //memset(this_wave_blocks,0,sizeof(this_wave_blocks));
        MPI_Barrier(MPI_COMM_WORLD);

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
            lseek64(fd_out, my_blocks[my_prev_blocks + k].offset, SEEK_SET);
            MFU_LOG(MFU_LOG_DBG, "Data to be written=%s,%u\n", a[k], my_blocks[my_prev_blocks + k].length);
            mfu_write(fname_out, fd_out, a[k], my_blocks[my_prev_blocks + k].length);
        }

        my_prev_blocks = my_tot_blocks;
        blocks_processed = 0;
        for (int i = 0; i < blocks_for_wave; i++) {
            mfu_free(&a[i]);
        }
        mfu_free(&a);
    }

    /* End of all waves */
    MPI_Barrier(MPI_COMM_WORLD);

    /* Broadcast offset of start of trailer */
    MPI_Bcast(&last_offset, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);

    /* Each process writes the offset of all blocks processed by it at corect location in trailer */
    for (int k = 0; k < my_tot_blocks; k++) {
        lseek64(fd_out, last_offset + my_blocks[k].sno * 8, SEEK_SET);
        int64_t this_offset = (int64_t)my_blocks[k].offset;
        mfu_write(fname_out, fd_out, &this_offset, 8);
    }

    /* root writes the locaion of trailer start to last 8 bytes of the file */
    if (rank == 0) {
        lseek64(fd_out, last_offset + tot_blocks * 8, SEEK_SET);
        int64_t trailer_offset = (int64_t)last_offset;
        mfu_write(fname_out, fd_out, &trailer_offset, 8);
    }

    /* free memory for compress blocks */
    mfu_free(&my_blocks);

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
