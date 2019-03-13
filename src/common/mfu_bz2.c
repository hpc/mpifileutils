#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <errno.h>

#include "mpi.h"
#include "mfu.h"

int mfu_compress_bz2_libcircle(const char* src_name, const char* dst_name, int b_size, ssize_t opts_memory);
int mfu_compress_bz2_static(const char* src_name, const char* dst_name, int b_size);

int mfu_decompress_bz2_libcircle(const char* src_name, const char* dst_name);
int mfu_decompress_bz2_static(const char* src_name, const char* dst_name);

int mfu_compress_bz2(const char* src_name, const char* dst_name, int b_size)
{
    //return mfu_compress_bz2_libcircle(src_name, dst_name, b_size, 0);
    return mfu_compress_bz2_static(src_name, dst_name, b_size);
}


int mfu_decompress_bz2(const char* src_name, const char* dst_name)
{
    //return mfu_decompress_bz2_libcircle(src_name, dst_name);
    return mfu_decompress_bz2_static(src_name, dst_name);
}

int mfu_create_fully_striped(const char* name, mode_t mode)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

#if LUSTRE_SUPPORT
    /* have rank 0 create the file with striping */
    if (rank == 0) {
        mfu_stripe_set(name, 1024*1024, -1);
    }

    /* wait for rank 0 to finish operations */
    MPI_Barrier(MPI_COMM_WORLD);

    /* open the file */
    int fd = mfu_open(name, O_WRONLY);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing: %s errno=%d (%s)",
            name, errno, strerror(errno));
    }
#else
    /* The file for output is opened and options set */
    int fd = -1;
    if (rank == 0) {
        /* open file */
        fd = mfu_open(name, O_WRONLY | O_CREAT | O_TRUNC, mode);
        if (fd < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing: %s errno=%d (%s)",
                name, errno, strerror(errno));
        }
    }

    /* wait for rank 0 to finish operations */
    MPI_Barrier(MPI_COMM_WORLD);

    /* have rest of ranks open the file */
    if (rank != 0) {
        fd = mfu_open(name, O_WRONLY);
        if (fd < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing: %s errno=%d (%s)",
                name, errno, strerror(errno));
        }
    }
#endif

    return fd;
}
