#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#include <errno.h>

/* for statfs */
#include <sys/vfs.h>

/* for LL_SUPER_MAGIC */
#if LUSTRE_SUPPORT
#include <lustre/lustre_user.h>
#endif /* LUSTRE_SUPPORT */

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

static int mfu_create_output(const char* name, mode_t mode)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

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

    return fd;
}

int mfu_create_fully_striped(const char* name, mode_t mode)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

#if LUSTRE_SUPPORT
    /* have rank 0 check whether the file will be on Lustre */
    int on_lustre = 0;
    if (rank == 0) {
        /* get directory path for file */
        mfu_path* dirpath = mfu_path_from_str(name);
        mfu_path_dirname(dirpath);
        char* dirstr = mfu_path_strdup(dirpath);

        /* statfs the directory */
        errno = 0;
        struct statfs fs_stat;
        if (statfs(dirstr, &fs_stat) == 0) {
            /* set to 1 if this path is on lustre, 0 otherwise */
            on_lustre = (fs_stat.f_type == LL_SUPER_MAGIC);
        } else {
            MFU_LOG(MFU_LOG_ERR, "Failed to statfs: `%s' errno=%d (%s)",
                dirstr, errno, strerror(errno));
        }

        /* free the directory name */
        mfu_free(&dirstr);
        mfu_path_delete(&dirpath);
    }

    /* broadcast result from rank 0 */
    MPI_Bcast(&on_lustre, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* if on lustre, set striping while opening file,
     * otherwise fallback to something basic */
    int fd = -1;
    if (on_lustre) {
        /* have rank 0 create the file with striping */
        if (rank == 0) {
            mfu_stripe_set(name, 1024*1024, -1);
        }
    
        /* wait for rank 0 to finish operations */
        MPI_Barrier(MPI_COMM_WORLD);
    
        /* open the file */
        fd = mfu_open(name, O_WRONLY);
        if (fd < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing: %s errno=%d (%s)",
                name, errno, strerror(errno));
        }
    } else {
        fd = mfu_create_output(name, mode);
    }
#else
    int fd = mfu_create_output(name, mode);
#endif

    return fd;
}
