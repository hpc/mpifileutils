/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2015, DataDirect Networks, Inc.
 * 
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/

#include "mfu_flist.h"
#include "mfu_flist.c"

#include "dcp.h"

#include "handle_args.h"

#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/param.h>

#include <linux/fs.h>
#include <linux/fiemap.h>

/** Options specified by the user. */
extern DCOPY_options_t DCOPY_user_opts;

/** Statistics to gather for summary output. */
extern DCOPY_statistics_t DCOPY_statistics;

/** Cache most recent open file descriptors. */
extern DCOPY_file_cache_t DCOPY_file_cache;

#if 0
static void DCOPY_reduce_init(void)
{
    int64_t agg_dirs   = DCOPY_statistics.total_dirs;
    int64_t agg_files  = DCOPY_statistics.total_files;
    int64_t agg_links  = DCOPY_statistics.total_links;
    int64_t agg_size   = DCOPY_statistics.total_size;
    int64_t agg_copied = DCOPY_statistics.total_bytes_copied;

    int64_t values[6];
    values[0] = agg_dirs + agg_files + agg_links;
    values[1] = agg_dirs;
    values[2] = agg_files;
    values[3] = agg_links;
    values[4] = agg_size;
    values[5] = agg_copied;

    CIRCLE_reduce(&values, sizeof(values));
}

static void DCOPY_reduce_op(const void* buf1, size_t size1, const void* buf2, size_t size2)
{
    int64_t values[6];

    const int64_t* a = (const int64_t*) buf1;
    const int64_t* b = (const int64_t*) buf2;

    int i;
    for (i = 0; i < 6; i++) {
        values[i] = a[i] + b[i];
    }

    CIRCLE_reduce(&values, sizeof(values));
}

static void DCOPY_reduce_fini(const void* buf, size_t size)
{
    const int64_t* a = (const int64_t*) buf;

    /* convert size to units */
    uint64_t agg_copied = (uint64_t) a[5];
    double agg_copied_tmp;
    const char* agg_copied_units;
    mfu_format_bytes(agg_copied, &agg_copied_tmp, &agg_copied_units);

    MFU_LOG(MFU_LOG_INFO,
        "Items created %" PRId64 ", Data copied %.3lf %s ...",
        a[0], agg_copied_tmp, agg_copied_units);
//        "Items %" PRId64 ", Dirs %" PRId64 ", Files %" PRId64 ", Links %" PRId64 ", Bytes %.3lf %s",
//        a[0], a[1], a[2], a[3], agg_size_tmp, agg_size_units);
}
#endif

static int64_t DCOPY_sum_int64(int64_t val)
{
    long long val_ull = (long long) val;
    long long sum;
    MPI_Allreduce(&val_ull, &sum, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    return (int64_t) sum;
}

/**
 * Print out information on the results of the file copy.
 */
static void DCOPY_epilogue(void)
{
    double rel_time = DCOPY_statistics.wtime_ended - \
                      DCOPY_statistics.wtime_started;
    int64_t agg_dirs   = DCOPY_sum_int64(DCOPY_statistics.total_dirs);
    int64_t agg_files  = DCOPY_sum_int64(DCOPY_statistics.total_files);
    int64_t agg_links  = DCOPY_sum_int64(DCOPY_statistics.total_links);
    int64_t agg_size   = DCOPY_sum_int64(DCOPY_statistics.total_size);
    int64_t agg_copied = DCOPY_sum_int64(DCOPY_statistics.total_bytes_copied);
    double agg_rate = (double)agg_copied / rel_time;

    if(DCOPY_global_rank == 0) {
        char starttime_str[256];
        struct tm* localstart = localtime(&(DCOPY_statistics.time_started));
        strftime(starttime_str, 256, "%b-%d-%Y,%H:%M:%S", localstart);

        char endtime_str[256];
        struct tm* localend = localtime(&(DCOPY_statistics.time_ended));
        strftime(endtime_str, 256, "%b-%d-%Y,%H:%M:%S", localend);

        int64_t agg_items = agg_dirs + agg_files + agg_links;

        /* convert size to units */
        double agg_size_tmp;
        const char* agg_size_units;
        mfu_format_bytes((uint64_t)agg_size, &agg_size_tmp, &agg_size_units);

        /* convert bandwidth to units */
        double agg_rate_tmp;
        const char* agg_rate_units;
        mfu_format_bw(agg_rate, &agg_rate_tmp, &agg_rate_units);

        MFU_LOG(MFU_LOG_INFO, "Started: %s", starttime_str);
        MFU_LOG(MFU_LOG_INFO, "Completed: %s", endtime_str);
        MFU_LOG(MFU_LOG_INFO, "Seconds: %.3lf", rel_time);
        MFU_LOG(MFU_LOG_INFO, "Items: %" PRId64, agg_items);
        MFU_LOG(MFU_LOG_INFO, "  Directories: %" PRId64, agg_dirs);
        MFU_LOG(MFU_LOG_INFO, "  Files: %" PRId64, agg_files);
        MFU_LOG(MFU_LOG_INFO, "  Links: %" PRId64, agg_links);
        MFU_LOG(MFU_LOG_INFO, "Data: %.3lf %s (%" PRId64 " bytes)",
            agg_size_tmp, agg_size_units, agg_size);

        MFU_LOG(MFU_LOG_INFO, "Rate: %.3lf %s " \
            "(%.3" PRId64 " bytes in %.3lf seconds)", \
            agg_rate_tmp, agg_rate_units, agg_copied, rel_time);
    }

    /* free memory allocated to parse user params */
    DCOPY_free_path_args();

    /* free file I/O buffer */
    mfu_free(&DCOPY_user_opts.block_buf2);
    mfu_free(&DCOPY_user_opts.block_buf1);

    return;
}

static int create_directory(mfu_flist list, uint64_t idx)
{
    /* get name of directory */
    const char* name = mfu_flist_file_get_name(list, idx);

    /* get destination name */
    char* dest_path = DCOPY_build_dest(name);

    /* No need to copy it */
    if (dest_path == NULL) {
        return 0;
    }

   /* create the destination directory */
    MFU_LOG(MFU_LOG_DBG, "Creating directory `%s'", dest_path);
    int rc = mfu_mkdir(dest_path, DCOPY_DEF_PERMS_DIR);
    if(rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to create directory `%s' (errno=%d %s)", \
            dest_path, errno, strerror(errno));
        mfu_free(&dest_path);
        return -1;
    }

    /* we do this now in case there are Lustre attributes for
     * creating / striping files in the directory */

    /* copy extended attributes on directory */
    if (DCOPY_user_opts.preserve) {
        DCOPY_copy_xattrs(list, idx, dest_path);
    }

    /* increment our directory count by one */
    DCOPY_statistics.total_dirs++;

    /* free the directory name */
    mfu_free(&dest_path);

    return 0;
}

/* create directories, we work from shallowest level to the deepest
 * with a barrier in between levels, so that we don't try to create
 * a child directory until the parent exists */
static int create_directories(int levels, int minlevel, mfu_flist* lists)
{
    int rc = 0;

    int verbose = (mfu_debug_level <= MFU_LOG_INFO);

    /* indicate to user what phase we're in */
    if (DCOPY_global_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating directories.");
    }

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* work from shallowest level to deepest level */
    int level;
    for (level = 0; level < levels; level++) {
        /* time how long this takes */
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* create each directory we have at this level */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* check whether we have a directory */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_DIR) {
                /* create the directory */
                int tmp_rc = create_directory(list, idx);
                if (tmp_rc != 0) {
                    rc = tmp_rc;
                }

                count++;
            }
        }

        /* wait for all procs to finish before we start
         * creating directories at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        /* stop our timer */
        double end = MPI_Wtime();

         /* print statistics */
        if (verbose) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            double secs = end - start;
            if (secs > 0.0) {
              rate = (double)sum / secs;
            }
            if (rank == 0) {
                printf("  level=%d min=%lu max=%lu sum=%lu rate=%f/sec secs=%f\n",
                  (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
                );
                fflush(stdout);
            }
        }
    }

    return rc;
}

static int create_link(mfu_flist list, uint64_t idx)
{
    /* get source name */
    const char* src_path = mfu_flist_file_get_name(list, idx);

    /* get destination name */
    const char* dest_path = DCOPY_build_dest(src_path);

    /* No need to copy it */
    if (dest_path == NULL) {
        return 0;
    }

    /* read link target */
    char path[PATH_MAX + 1];
    ssize_t rc = mfu_readlink(src_path, path, sizeof(path) - 1);

    if(rc < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to read link `%s' readlink() errno=%d %s",
            src_path, errno, strerror(errno)
        );
        mfu_free(&dest_path);
        return -1;
    }

    /* ensure that string ends with NUL */
    path[rc] = '\0';

    /* create new link */
    int symrc = mfu_symlink(path, dest_path);

    if(symrc < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to create link `%s' symlink() errno=%d %s",
            dest_path, errno, strerror(errno)
        );
        mfu_free(&dest_path);
        return -1;
    }

    /* TODO: why not do this later? */

    /* set permissions on link */
    if (DCOPY_user_opts.preserve) {
        DCOPY_copy_xattrs(list, idx, dest_path);
        DCOPY_copy_ownership(list, idx, dest_path);
        DCOPY_copy_permissions(list, idx, dest_path);
    }

    /* free destination path */
    mfu_free(&dest_path);

    /* increment our directory count by one */
    DCOPY_statistics.total_links++;

    return 0;
}

static int create_file(mfu_flist list, uint64_t idx)
{
    /* get source name */
    const char* src_path = mfu_flist_file_get_name(list, idx);

    /* get destination name */
    const char* dest_path = DCOPY_build_dest(src_path);

    /* No need to copy it */
    if (dest_path == NULL) {
        return 0;
    }

    /* since file systems like Lustre require xattrs to be set before file is opened,
     * we first create it with mknod and then set xattrs */

    /* create file with mknod
    * for regular files, dev argument is supposed to be ignored,
    * see makedev() to create valid dev */
    dev_t dev;
    memset(&dev, 0, sizeof(dev_t));
    int mknod_rc = mfu_mknod(dest_path, DCOPY_DEF_PERMS_FILE | S_IFREG, dev);

    if(mknod_rc < 0) {
        if(errno == EEXIST) {
            /* TODO: should we unlink and mknod again in this case? */
        }

        MFU_LOG(MFU_LOG_ERR, "File `%s' mknod() errno=%d %s",
            dest_path, errno, strerror(errno)
        );
    }

    /* copy extended attributes, important to do this first before
     * writing data because some attributes tell file system how to
     * stripe data, e.g., Lustre */
    if (DCOPY_user_opts.preserve) {
        DCOPY_copy_xattrs(list, idx, dest_path);
    }

    /* Truncate destination files to 0 bytes when sparse file is enabled,
     * this is because we will not overwrite sections corresponding to holes
     * and we need those to be set to 0 */
    if (DCOPY_user_opts.sparse) {
        /* truncate destination file to 0 bytes */
        struct stat st;
        int status = mfu_lstat(dest_path, &st);
        if (status == 0) {
            /* destination exists, truncate it to 0 bytes */
            status = truncate64(dest_path, 0);
            if (status) {
                MFU_LOG(MFU_LOG_ERR, "Failed to truncate destination file: %s (errno=%d %s)",
                          dest_path, errno, strerror(errno));
            }
        } else if (errno == -ENOENT) {
            /* destination does not exist, which is fine */
            status = 0;
        } else {
            /* had an error stating destination file */
            MFU_LOG(MFU_LOG_ERR, "mfu_lstat() file: %s (errno=%d %s)",
                      dest_path, errno, strerror(errno));
        }

        if (status) {
            /* do we need to abort here? */
            //return;
        }
    }

    /* free destination path */
    mfu_free(&dest_path);

    /* increment our file count by one */
    DCOPY_statistics.total_files++;

    return 0;
}

static int create_files(int levels, int minlevel, mfu_flist* lists)
{
    int rc = 0;

    int verbose = (mfu_debug_level <= MFU_LOG_INFO);

    /* indicate to user what phase we're in */
    if (DCOPY_global_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating files.");
    }

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* TODO: we don't need to have a barrier between levels */

    int level;
    for (level = 0; level < levels; level++) {
        /* time how long this takes */
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* iterate over items and set write bit on directories if needed */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* get type of item */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);

            /* process files and links */
            if (type == MFU_TYPE_FILE) {
                /* TODO: skip file if it's not readable */
                create_file(list, idx);
                count++;
            } else if (type == MFU_TYPE_LINK) {
                create_link(list, idx);
                count++;
            }
        }

        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        /* stop our timer */
        double end = MPI_Wtime();

        /* print timing statistics */
        if (verbose) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            double secs = end - start;
            if (secs > 0.0) {
              rate = (double)sum / secs;
            }
            if (rank == 0) {
                printf("  level=%d min=%lu max=%lu sum=%lu rate=%f secs=%f\n",
                  (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
                );
                fflush(stdout);
            }
        }
    }

    return rc;
}

/* return 1 if entire buffer is 0, return 0 if any byte is not 0,
 * we avoid writing NULL blocks when supporting sparse files */
static int is_all_null(const char* buf, uint64_t buf_size)
{
    uint64_t i;
    for (i = 0; i < buf_size; i++) {
        if (buf[i] != 0) {
            return 0;
        }
    }
    return 1;
}

/* when using sparse files, we need to write the last byte if the
 * hole is adjacent to EOF, so we need to detect whether we're at
 * the end of the file */
static int is_eof(const char* file, int fd)
{
    /* read one byte from fd to determine whether this is EOF.
     * This is not efficient, but it is the only reliable way */
    char buf[1];
    ssize_t num_of_bytes_read = mfu_read(file, fd, buf, 1);

    /* return if we detect EOF */
    if(! num_of_bytes_read) {
        return 1;
    }

    /* otherwise, we're not at EOF yet, seek back one byte */
    if(mfu_lseek(file, fd, -1, SEEK_CUR) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in path `%s' errno=%d %s",
                  file, errno, strerror(errno));
        return -1;
    }
    return 0;
}

static int copy_file_normal(
    const char* src,
    const char* dest,
    const int in_fd,
    const int out_fd,
    off_t offset,
    off_t length,
    uint64_t file_size)
{
    /* hint that we'll read from file sequentially */
//    posix_fadvise(in_fd, offset, chunk_size, POSIX_FADV_SEQUENTIAL);

    /* seek to offset in source file */
    if(mfu_lseek(src, in_fd, offset, SEEK_SET) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in source path `%s' errno=%d %s", \
            src, errno, strerror(errno));
        return -1;
    }

    /* seek to offset in destination file */
    if(mfu_lseek(dest, out_fd, offset, SEEK_SET) == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' errno=%d %s", \
            dest, errno, strerror(errno));
        return -1;
    }

    /* get buffer */
    size_t buf_size = DCOPY_user_opts.block_size;
    void* buf = DCOPY_user_opts.block_buf1;

    /* write data */
    size_t total_bytes = 0;
    while(total_bytes <= length) {
        /* determine number of bytes that we
         * can read = max(buf size, remaining chunk) */
        size_t left_to_read = length - total_bytes;
        if(left_to_read > buf_size) {
            left_to_read = buf_size;
        }

        /* read data from source file */
        ssize_t num_of_bytes_read = mfu_read(src, in_fd, buf, left_to_read);

        /* check for EOF */
        if(! num_of_bytes_read) {
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

        /* Write data to destination file.
         * Do nothing for a hole in the middle of a file,
         * because write of next chunk will create one for us.
         * Write only the last byte to create the hole,
         * if the hole is next to EOF. */
        ssize_t num_of_bytes_written = bytes_to_write;
        if (DCOPY_user_opts.sparse && is_all_null(buf, bytes_to_write)) {
            /* TODO: isn't there a better way to know if we're at EOF,
             * e.g., by using file size? */
            /* determine whether we're at the end of the file */
            int end_of_file = is_eof(src, in_fd);
            if (end_of_file < 0) {
                /* hit an error while looking for EOF */
                return -1;
            }

            /* if we're at the end of the file, write out a byte,
             * otherwise just seek out destination file pointer
             * ahead without writing anything */
            if (end_of_file) {
                /* seek to last byte position in file */
                if(mfu_lseek(dest, out_fd, bytes_to_write - 1, SEEK_CUR) == (off_t)-1) {
                    MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' errno=%d %s", \
                        dest, errno, strerror(errno));
                    return -1;
                }

                /* write out a single byte */
                mfu_write(dest, out_fd, buf, 1);
            } else {
                /* this section of the destination file is all 0,
                 * seek past this section */
                if(mfu_lseek(dest, out_fd, bytes_to_write, SEEK_CUR) == (off_t)-1) {
                    MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' errno=%d %s", \
                        dest, errno, strerror(errno));
                    return -1;
                }
            }
        } else {
            /* write bytes to destination file */
            num_of_bytes_written = mfu_write(dest, out_fd, buf, bytes_to_write);
        }

        /* check for an error */
        if(num_of_bytes_written < 0) {
            MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s' errno=%d %s",
                src, dest, errno, strerror(errno));
            return -1;
        }

        /* check that we wrote the same number of bytes that we read */
        if((size_t)num_of_bytes_written != bytes_to_write) {
            MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s'",
                src, dest);
            return -1;
        }

        /* add bytes to our total (use bytes read,
         * which may be less than number written) */
        total_bytes += (size_t) num_of_bytes_read;
    }

    /* Increment the global counter. */
    DCOPY_statistics.total_size += (int64_t) total_bytes;
    DCOPY_statistics.total_bytes_copied += (int64_t) total_bytes;

#if 0
    /* force data to file system */
    if(total_bytes > 0) {
        mfu_fsync(dest, out_fd);
    }
#endif

    /* no need to truncate if sparse file is enabled,
     * since we truncated files when they were first created */
    if (DCOPY_user_opts.sparse) {
        return 0;
    }

    /* if we wrote the last chunk, truncate the file */
    off_t last_written = offset + length;
    off_t file_size_offt = (off_t) file_size;
    if (last_written >= file_size_offt || file_size == 0) {
       /*
        * Use ftruncate() here rather than truncate(), because grouplock
        * of Lustre would cause block to truncate() since the fd is different
        * from the out_fd.
        */
        if(ftruncate(out_fd, file_size_offt) < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to truncate destination file: %s (errno=%d %s)",
                dest, errno, strerror(errno));
            return -1;
       }
    }

    /* we don't bother closing the file because our cache does it for us */

    return 0;
}

static int copy_file_fiemap(
    const char* src,
    const char* dest,
    const int in_fd,
    const int out_fd,
    uint64_t offset,
    uint64_t length,
    uint64_t file_size,
    bool* normal_copy_required)
{
    *normal_copy_required = true;
    if (DCOPY_user_opts.synchronous) {
        goto fail_normal_copy;
    }

    size_t last_ext_start = offset;
    size_t last_ext_len = 0;

    struct fiemap *fiemap = (struct fiemap*)malloc(sizeof(struct fiemap));
    if (fiemap == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Out of memory allocating fiemap\n");
        goto fail_normal_copy;
    }

    memset(fiemap, 0, sizeof(struct fiemap));
    fiemap->fm_start  = offset;
    fiemap->fm_length = length;
    fiemap->fm_flags  = FIEMAP_FLAG_SYNC;
    fiemap->fm_extent_count   = 0;
    fiemap->fm_mapped_extents = 0;

    struct stat sb;
    if (fstat(in_fd, &sb) < 0) {
        goto fail_normal_copy;
    }

    if (ioctl(in_fd, FS_IOC_FIEMAP, fiemap) < 0) {
        MFU_LOG(MFU_LOG_ERR, "fiemap ioctl() failed for src %s\n", src);
        goto fail_normal_copy;
    }

    size_t extents_size = sizeof(struct fiemap_extent) * (fiemap->fm_mapped_extents);

    if ((fiemap = (struct fiemap*)realloc(fiemap,sizeof(struct fiemap) +
                                  extents_size)) == NULL)
    {
        MFU_LOG(MFU_LOG_ERR, "Out of memory reallocating fiemap\n");
        goto fail_normal_copy;
    }

    memset(fiemap->fm_extents, 0, extents_size);
    fiemap->fm_extent_count   = fiemap->fm_mapped_extents;
    fiemap->fm_mapped_extents = 0;

    if (ioctl(in_fd, FS_IOC_FIEMAP, fiemap) < 0) {
        MFU_LOG(MFU_LOG_ERR, "fiemap ioctl() failed for src %s\n", src);
        goto fail_normal_copy;
    }

    uint64_t last_byte = offset + length;

    if (fiemap->fm_mapped_extents > 0) {
        uint64_t fe_logical = fiemap->fm_extents[0].fe_logical;
        uint64_t fe_length  = fiemap->fm_extents[0].fe_length;
        if (fe_logical < offset) {
            fiemap->fm_extents[0].fe_length -= (offset - fe_logical);
            fiemap->fm_extents[0].fe_logical = offset;
        }

        fe_logical = fiemap->fm_extents[fiemap->fm_mapped_extents - 1].fe_logical;
        fe_length  = fiemap->fm_extents[fiemap->fm_mapped_extents - 1].fe_length;
        if (fe_logical + fe_length > last_byte) {
           fiemap->fm_extents[fiemap->fm_mapped_extents - 1].fe_length -=
           fe_logical + fe_length - last_byte;
        }
    }

    *normal_copy_required = false;

    /* seek to offset in source file */
    if (mfu_lseek(src, in_fd, (off_t)last_ext_start, SEEK_SET) < 0) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in source path `%s' errno=%d %s", \
            src, errno, strerror(errno));
        goto fail;
    }

    /* seek to offset in destination file */
    if (mfu_lseek(dest, out_fd, (off_t)last_ext_start, SEEK_SET) < 0) {
        MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' errno=%d %s", \
            dest, errno, strerror(errno));
        goto fail;
    }

    unsigned int i;
    for (i = 0; i < fiemap->fm_mapped_extents; i++) {
        size_t ext_start;
        size_t ext_len;
        size_t ext_hole_size;

        size_t buf_size = DCOPY_user_opts.block_size;
        void* buf = DCOPY_user_opts.block_buf1;

        ext_start = fiemap->fm_extents[i].fe_logical;
        ext_len = fiemap->fm_extents[i].fe_length;
        ext_hole_size = ext_start - (last_ext_start + last_ext_len);

        //printf("HOLE SIZE: %zu" PRId32, ext_hole_size);

        if (ext_hole_size) {
            if (mfu_lseek(src, in_fd, (off_t)ext_start, SEEK_SET) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Couldn't seek in source path `%s' errno=%d %s", \
                    src, errno, strerror(errno));
                goto fail;
            }
            if (mfu_lseek(dest, out_fd, (off_t)ext_hole_size, SEEK_CUR) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Couldn't seek in destination path `%s' errno=%d %s", \
                    dest, errno, strerror(errno));
                goto fail;
            }
        }

        last_ext_start = ext_start;
        last_ext_len = ext_len;

        while (ext_len) {
            ssize_t num_read = mfu_read(src, in_fd, buf, MIN(ext_len, buf_size));

            if (!num_read)
                break;

            ssize_t num_written = mfu_write(dest, out_fd, buf, (size_t)num_read);

            if (num_written < 0) {
                MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s' errno=%d %s",
                          src, dest, errno, strerror(errno));
                goto fail;
            }
            if (num_written != num_read) {
                MFU_LOG(MFU_LOG_ERR, "Write error when copying from `%s' to `%s'",
                    src, dest);
                goto fail;
            }

            ext_len -= (size_t)num_written;
            DCOPY_statistics.total_bytes_copied += (int64_t) num_written;
        }
    }

    off_t last_written = (off_t) last_byte;
    off_t file_size_offt = (off_t) file_size;
    if (last_written >= file_size_offt || file_size == 0) {
       /*
        * Use ftruncate() here rather than truncate(), because grouplock
        * of Lustre would cause block to truncate() since the fd is different
        * from the out_fd.
        */
        if (ftruncate(out_fd, file_size_offt) < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to truncate destination file: %s (errno=%d %s)",
                dest, errno, strerror(errno));
            goto fail;
       }
    }

    if (last_written >= file_size_offt) {
        DCOPY_statistics.total_size += (int64_t) (file_size_offt - (off_t) offset);
    } else {
        DCOPY_statistics.total_size += (int64_t) last_byte;
    }

    free(fiemap);
    return 0;

fail:
    free(fiemap);

fail_normal_copy:
    return -1;
}

static int copy_file(
    const char* src,
    const char* dest,
    uint64_t offset,
    uint64_t length,
    uint64_t file_size)
{
    int ret;
    bool normal_copy_required;

    /* open the input file */
    int in_fd = DCOPY_open_file(src, 1, &DCOPY_src_cache);
    if (in_fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open input file `%s' errno=%d %s",
            src, errno, strerror(errno));
        return -1;
    }

    /* open the output file */
    int out_fd = DCOPY_open_file(dest, 0, &DCOPY_dst_cache);
    if (out_fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open output file `%s' errno=%d %s",
            dest, errno, strerror(errno));
        return -1;
    }

    if (DCOPY_user_opts.sparse) {
        ret = copy_file_fiemap(src, dest, in_fd, out_fd, offset,
                               length, file_size, &normal_copy_required);
        if (!ret || !normal_copy_required) {
            return ret;
        }
    }

    return copy_file_normal(src, dest, in_fd, out_fd, offset, length, file_size);
}

/* After receiving all incoming chunks, process open and write their chunks 
 * to the files. The process which writes the last chunk to each file also 
 * truncates the file to correct size.  A 0-byte file still has one chunk. */
static void copy_files(mfu_flist list)
{
    /* indicate which phase we're in to user */
    if (DCOPY_global_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Copying data.");
    }

    /* get chunk size for copying files */
    uint64_t chunk_size = (uint64_t)DCOPY_user_opts.chunk_size;

    /* split file list into a linked list of file sections,
     * this evenly spreads the file sections across processes */
    mfu_file_chunk* p = mfu_file_chunk_list_alloc(list, chunk_size);

    /* loop over and copy data for each file section we're responsible for */
    while (p != NULL) {
        /* get name of destination file */
        char* dest_path = DCOPY_build_dest(p->name);

        /* No need to copy it */
        if (dest_path == NULL) {
            continue;
        }

        /* call copy_file for each element of the copy_elem linked list of structs */
        copy_file(p->name, dest_path, (off_t)p->offset, (off_t)p->length, p->file_size);

        /* free the dest name */
        mfu_free(&dest_path);

        /* update pointer to next element */
        p = p->next;
    }

    /* free the linked list */
    mfu_file_chunk_list_free(&p);
}

/* iterate through list of files and set ownership, timestamps,
 * and permissions starting from deepest level and working upwards,
 * we go in this direction in case updating a file updates its
 * parent directory */
static void DCOPY_set_metadata(int levels, int minlevel, mfu_flist* lists)
{
    if (DCOPY_global_rank == 0) {
        if(DCOPY_user_opts.preserve) {
            MFU_LOG(MFU_LOG_INFO, "Setting ownership, permissions, and timestamps.");
        }
        else {
            MFU_LOG(MFU_LOG_INFO, "Fixing permissions.");
        }
    }

    /* now set timestamps on files starting from deepest level */
    int level;
    for (level = levels-1; level >= 0; level--) {
        /* get list at this level */
        mfu_flist list = lists[level];

        /* cycle through our list of items and set timestamps
         * for each one at this level */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        for (idx = 0; idx < size; idx++) {
            /* get type of item */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);

            /* we've already set these properties for links,
             * so we can skip those here */
            if (type == MFU_TYPE_LINK) {
                continue;
            }

            /* TODO: skip file if it's not readable */

            /* get destination name of item */
            const char* name = mfu_flist_file_get_name(list, idx);
            char* dest = DCOPY_build_dest(name);

            /* No need to copy it */
            if (dest == NULL) {
                continue;
            }

            if(DCOPY_user_opts.preserve) {
                DCOPY_copy_ownership(list, idx, dest);
                DCOPY_copy_permissions(list, idx, dest);
                DCOPY_copy_timestamps(list, idx, dest);
            }
            else {
                /* TODO: set permissions based on source permissons
                 * masked by umask */
                DCOPY_copy_permissions(list, idx, dest);
            }

            /* free destination item */
            mfu_free(&dest);
        }
        
        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }

    return;
}

/**
 * Print the current version.
 */
static void DCOPY_print_version(void)
{
    fprintf(stdout, "%s-%s\n", PACKAGE_NAME, PACKAGE_VERSION);
}

/**
 * Print a usage message.
 */
void DCOPY_print_usage(void)
{
    /* The compare option isn't really effective because it often
     * reads from the page cache and not the disk, which gives a
     * false sense of validation.  Also, it tends to thrash the
     * metadata server with lots of extra open/close calls.  Plan
     * is to delete it here, and rely on dcmp instead.  For now
     * we just hide it as an option. */

    printf("\n");
    printf("Usage: dcp [options] source target\n");
    printf("       dcp [options] source ... target_dir\n");
    printf("\n");
    printf("Options:\n");
    /* printf("  -c, --compare       - read data back after writing to compare\n"); */
    printf("  -d, --debug <level> - specify debug verbosity level (default info)\n");
    printf("  -f, --force         - delete destination file if error on open\n");
#ifdef LUSTRE_SUPPORT
    printf("  -g, --groulock      - use Lustre grouplock when reading/writing file\n");
#endif
    printf("  -i, --input <file>  - read list from file\n");
    printf("  -p, --preserve      - preserve permissions, ownership, timestamps, extended attributes\n");
    printf("  -s, --synchronous   - use synchronous read/write calls (O_DIRECT)\n");
    printf("  -S, --sparse        - create sparse files when possible\n");
    printf("  -v, --version       - print version info\n");
    printf("  -h, --help          - print usage\n");
    printf("\n");
    printf("Level: dbg,info,warn,err,fatal\n");
    printf("\n");
    fflush(stdout);
}

int main(int argc, \
         char** argv)
{
    int c;
    int option_index = 0;

    MPI_Init(&argc, &argv);
    mfu_init();

    MPI_Comm_rank(MPI_COMM_WORLD, &DCOPY_global_rank);

    /* Initialize our processing library and related callbacks. */
    /* This is a bit of chicken-and-egg problem, because we'd like
     * to have our rank to filter output messages below but we might
     * also want to set different libcircle flags based on command line
     * options -- for now just pass in the default flags */
#if 0
    DCOPY_global_rank = CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&DCOPY_add_objects);
    CIRCLE_cb_process(&DCOPY_process_objects);
    CIRCLE_cb_reduce_init(&DCOPY_reduce_init);
    CIRCLE_cb_reduce_op(&DCOPY_reduce_op);
    CIRCLE_cb_reduce_fini(&DCOPY_reduce_fini);
#endif

    /* Initialize statistics */
    DCOPY_statistics.total_dirs  = 0;
    DCOPY_statistics.total_files = 0;
    DCOPY_statistics.total_links = 0;
    DCOPY_statistics.total_size  = 0;
    DCOPY_statistics.total_bytes_copied = 0;

    /* Initialize file cache */
    DCOPY_src_cache.name = NULL;
    DCOPY_dst_cache.name = NULL;

    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;
    mfu_debug_level = MFU_LOG_INFO;

    /* By default, don't unlink destination files if an open() fails. */
    DCOPY_user_opts.force = false;

    /* By default, don't bother to preserve all attributes. */
    DCOPY_user_opts.preserve = false;

    /* By default, don't use O_DIRECT. */
    DCOPY_user_opts.synchronous = false;

    /* By default, don't use sparse file. */
    DCOPY_user_opts.sparse = false;

    /* Set default chunk size */
    DCOPY_user_opts.chunk_size = DCOPY_CHUNK_SIZE;

    /* Set default block size */
    DCOPY_user_opts.block_size = FD_BLOCK_SIZE;

    /* By default, don't have iput file. */
    DCOPY_user_opts.input_file = NULL;

    static struct option long_options[] = {
        {"debug"                , required_argument, 0, 'd'},
        {"force"                , no_argument      , 0, 'f'},
        {"grouplock"            , required_argument, 0, 'g'},
        {"help"                 , no_argument      , 0, 'h'},
        {"input"                , required_argument, 0, 'i'},
        {"preserve"             , no_argument      , 0, 'p'},
        {"synchronous"          , no_argument      , 0, 's'},
        {"sparse"               , no_argument      , 0, 'S'},
        {"version"              , no_argument      , 0, 'v'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    while((c = getopt_long(argc, argv, "d:fg:hi:pusSv", \
                           long_options, &option_index)) != -1) {
        switch(c) {

            case 'd':
                if(strncmp(optarg, "fatal", 5) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_FATAL;
                    mfu_debug_level = MFU_LOG_FATAL;

                    if(DCOPY_global_rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: fatal");
                    }

                }
                else if(strncmp(optarg, "err", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_ERR;
                    mfu_debug_level = MFU_LOG_ERR;

                    if(DCOPY_global_rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: errors");
                    }

                }
                else if(strncmp(optarg, "warn", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN;
                    mfu_debug_level = MFU_LOG_WARN;

                    if(DCOPY_global_rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: warnings");
                    }

                }
                else if(strncmp(optarg, "info", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN; /* we back off a level on CIRCLE verbosity */
                    mfu_debug_level = MFU_LOG_INFO;

                    if(DCOPY_global_rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: info");
                    }

                }
                else if(strncmp(optarg, "dbg", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_DBG;
                    mfu_debug_level = MFU_LOG_DBG;

                    if(DCOPY_global_rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level set to: debug");
                    }

                }
                else {
                    if(DCOPY_global_rank == 0) {
                        MFU_LOG(MFU_LOG_INFO, "Debug level `%s' not recognized. " \
                            "Defaulting to `info'.", optarg);
                    }
                }

                break;

            case 'f':
                DCOPY_user_opts.force = true;

                if(DCOPY_global_rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Deleting destination on errors.");
                }

                break;

#ifdef LUSTRE_SUPPORT
            case 'g':
                DCOPY_user_opts.grouplock_id = atoi(optarg);

                if(DCOPY_global_rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "groulock ID: %d.",
                        DCOPY_user_opts.grouplock_id);
                }

                break;
#endif

            case 'h':
                if(DCOPY_global_rank == 0) {
                    DCOPY_print_usage();
                }

                DCOPY_exit(EXIT_SUCCESS);
                break;

            case 'i':
                DCOPY_user_opts.input_file = MFU_STRDUP(optarg);
                if(DCOPY_global_rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using input list.");
                }
                break;

            case 'p':
                DCOPY_user_opts.preserve = true;

                if(DCOPY_global_rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Preserving file attributes.");
                }

                break;

            case 's':
                DCOPY_user_opts.synchronous = true;

                if(DCOPY_global_rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using synchronous read/write (O_DIRECT)");
                }

                break;

            case 'S':
                DCOPY_user_opts.sparse = true;

                if(DCOPY_global_rank == 0) {
                    MFU_LOG(MFU_LOG_INFO, "Using sparse file");
                }

                break;

            case 'v':
                if(DCOPY_global_rank == 0) {
                    DCOPY_print_version();
                }

                DCOPY_exit(EXIT_SUCCESS);
                break;

            case '?':
            default:
                if(DCOPY_global_rank == 0) {
                    if(optopt == 'd') {
                        DCOPY_print_usage();
                        fprintf(stderr, "Option -%c requires an argument.\n", \
                                optopt);
                    }
                    else if(isprint(optopt)) {
                        DCOPY_print_usage();
                        fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                    }
                    else {
                        DCOPY_print_usage();
                        fprintf(stderr,
                                "Unknown option character `\\x%x'.\n",
                                optopt);
                    }
                }

                DCOPY_exit(EXIT_FAILURE);
                break;
        }
    }

    /** Parse the source and destination paths. */
    DCOPY_parse_path_args(argv, optind, argc);

    /* allocate buffer to read/write files, aligned on 1MB boundaraies */
    size_t alignment = 1024*1024;
    DCOPY_user_opts.block_buf1 = (char*) MFU_MEMALIGN(
        DCOPY_user_opts.block_size, alignment);
    DCOPY_user_opts.block_buf2 = (char*) MFU_MEMALIGN(
        DCOPY_user_opts.block_size, alignment);

    /* Grab a relative and actual start time for the epilogue. */
    time(&(DCOPY_statistics.time_started));
    DCOPY_statistics.wtime_started = MPI_Wtime();

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();
    if (DCOPY_user_opts.input_file == NULL) {
        /* walk paths and fill in file list */
        DCOPY_walk_paths(flist);
    } else {
        /* otherwise, read list of files from input, but then stat each one */
        mfu_flist input_flist = mfu_flist_new();
        mfu_flist_read_cache(DCOPY_user_opts.input_file, input_flist);
        mfu_flist_stat(input_flist, flist, DCOPY_input_flist_skip, NULL);
        mfu_flist_free(&input_flist);
    }

    /* split items in file list into sublists depending on their
     * directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(flist, &levels, &minlevel, &lists);

    /* TODO: filter out files that are bigger than 0 bytes if we can't read them */

    /* create directories, from top down */
    create_directories(levels, minlevel, lists);

    /* create files and links */
    create_files(levels, minlevel, lists);

    /* copy data */
    copy_files(flist);

    /* close files */
    DCOPY_close_file(&DCOPY_src_cache);
    DCOPY_close_file(&DCOPY_dst_cache);

    /* set permissions, ownership, and timestamps if needed */
    DCOPY_set_metadata(levels, minlevel, lists);

    /* free our lists of levels */
    mfu_flist_array_free(levels, &lists);

    /* free our file lists */
    mfu_flist_free(&flist);

    /* Determine the actual and relative end time for the epilogue. */
    DCOPY_statistics.wtime_ended = MPI_Wtime();
    time(&(DCOPY_statistics.time_ended));

    /* force updates to disk */
    if (DCOPY_global_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Syncing updates to disk.");
    }
    sync();

    /* Print the results to the user. */
    DCOPY_epilogue();

    DCOPY_exit(EXIT_SUCCESS);
}

/* EOF */
