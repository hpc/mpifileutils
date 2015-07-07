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

#include "dcp.h"

#include "handle_args.h"

#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <inttypes.h>
#include <unistd.h>
#include <errno.h>

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
    bayer_format_bytes(agg_copied, &agg_copied_tmp, &agg_copied_units);

    BAYER_LOG(BAYER_LOG_INFO,
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
        bayer_format_bytes((uint64_t)agg_size, &agg_size_tmp, &agg_size_units);

        /* convert bandwidth to units */
        double agg_rate_tmp;
        const char* agg_rate_units;
        bayer_format_bw(agg_rate, &agg_rate_tmp, &agg_rate_units);

        BAYER_LOG(BAYER_LOG_INFO, "Started: %s", starttime_str);
        BAYER_LOG(BAYER_LOG_INFO, "Completed: %s", endtime_str);
        BAYER_LOG(BAYER_LOG_INFO, "Seconds: %.3lf", rel_time);
        BAYER_LOG(BAYER_LOG_INFO, "Items: %" PRId64, agg_items);
        BAYER_LOG(BAYER_LOG_INFO, "  Directories: %" PRId64, agg_dirs);
        BAYER_LOG(BAYER_LOG_INFO, "  Files: %" PRId64, agg_files);
        BAYER_LOG(BAYER_LOG_INFO, "  Links: %" PRId64, agg_links);
        BAYER_LOG(BAYER_LOG_INFO, "Data: %.3lf %s (%" PRId64 " bytes)",
            agg_size_tmp, agg_size_units, agg_size);

        BAYER_LOG(BAYER_LOG_INFO, "Rate: %.3lf %s " \
            "(%.3" PRId64 " bytes in %.3lf seconds)", \
            agg_rate_tmp, agg_rate_units, agg_copied, rel_time);
    }

    /* free memory allocated to parse user params */
    DCOPY_free_path_args();

    /* free file I/O buffer */
    bayer_free(&DCOPY_user_opts.block_buf2);
    bayer_free(&DCOPY_user_opts.block_buf1);

    return;
}

static int create_directory(bayer_flist list, uint64_t idx)
{
    /* get name of directory */
    const char* name = bayer_flist_file_get_name(list, idx);

    /* get destination name */
    char* dest_path = DCOPY_build_dest(name);

   /* create the destination directory */
    BAYER_LOG(BAYER_LOG_DBG, "Creating directory `%s'", dest_path);
    int rc = bayer_mkdir(dest_path, DCOPY_DEF_PERMS_DIR);
    if(rc != 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Failed to create directory `%s' (errno=%d %s)", \
            dest_path, errno, strerror(errno));
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
    bayer_free(&dest_path);

    return 0;
}

/* create directories, we work from shallowest level to the deepest
 * with a barrier in between levels, so that we don't try to create
 * a child directory until the parent exists */
static int create_directories(int levels, int minlevel, bayer_flist* lists)
{
    int rc = 0;

    int verbose = (bayer_debug_level <= BAYER_LOG_INFO);

    /* indicate to user what phase we're in */
    if (DCOPY_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_INFO, "Creating directories.");
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
        bayer_flist list = lists[level];

        /* create each directory we have at this level */
        uint64_t idx;
        uint64_t size = bayer_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* check whether we have a directory */
            bayer_filetype type = bayer_flist_file_get_type(list, idx);
            if (type == BAYER_TYPE_DIR) {
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

static int create_link(bayer_flist list, uint64_t idx)
{
    /* get source name */
    const char* src_path = bayer_flist_file_get_name(list, idx);

    /* get destination name */
    const char* dest_path = DCOPY_build_dest(src_path);

    /* read link target */
    char path[PATH_MAX + 1];
    ssize_t rc = bayer_readlink(src_path, path, sizeof(path) - 1);

    if(rc < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Failed to read link `%s' readlink() errno=%d %s",
            src_path, errno, strerror(errno)
        );
        bayer_free(&dest_path);
        return -1;
    }

    /* ensure that string ends with NUL */
    path[rc] = '\0';

    /* create new link */
    int symrc = bayer_symlink(path, dest_path);

    if(symrc < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Failed to create link `%s' symlink() errno=%d %s",
            dest_path, errno, strerror(errno)
        );
        bayer_free(&dest_path);
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
    bayer_free(&dest_path);

    /* increment our directory count by one */
    DCOPY_statistics.total_links++;

    return 0;
}

static int create_file(bayer_flist list, uint64_t idx)
{
    /* get source name */
    const char* src_path = bayer_flist_file_get_name(list, idx);

    /* get destination name */
    const char* dest_path = DCOPY_build_dest(src_path);

    /* since file systems like Lustre require xattrs to be set before file is opened,
     * we first create it with mknod and then set xattrs */

    /* create file with mknod
    * for regular files, dev argument is supposed to be ignored,
    * see makedev() to create valid dev */
    dev_t dev;
    memset(&dev, 0, sizeof(dev_t));
    int mknod_rc = bayer_mknod(dest_path, DCOPY_DEF_PERMS_FILE | S_IFREG, dev);

    if(mknod_rc < 0) {
        if(errno == EEXIST) {
            /* TODO: should we unlink and mknod again in this case? */
        }

        BAYER_LOG(BAYER_LOG_ERR, "File `%s' mknod() errno=%d %s",
            dest_path, errno, strerror(errno)
        );
    }

    /* copy extended attributes, important to do this first before
     * writing data because some attributes tell file system how to
     * stripe data, e.g., Lustre */
    if (DCOPY_user_opts.preserve) {
        DCOPY_copy_xattrs(list, idx, dest_path);
    }

    /* free destination path */
    bayer_free(&dest_path);

    /* increment our directory count by one */
    DCOPY_statistics.total_files++;

    return 0;
}

static int create_files(int levels, int minlevel, bayer_flist* lists)
{
    int rc = 0;

    int verbose = (bayer_debug_level <= BAYER_LOG_INFO);

    /* indicate to user what phase we're in */
    if (DCOPY_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_INFO, "Creating files.");
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
        bayer_flist list = lists[level];

        /* iterate over items and set write bit on directories if needed */
        uint64_t idx;
        uint64_t size = bayer_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* get type of item */
            bayer_filetype type = bayer_flist_file_get_type(list, idx);

            /* process files and links */
            if (type == BAYER_TYPE_FILE) {
                create_file(list, idx);
                count++;
            } else if (type == BAYER_TYPE_LINK) {
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

static int map_chunk_to_rank(uint64_t offset, uint64_t cutoff, uint64_t chunks_per_rank)
{
    /* total number of chunks held by ranks below cutoff */
    uint64_t cutoff_coverage = cutoff * (chunks_per_rank + 1);

    /* given an offset of a chunk, identify which rank will
     * be responsible */
    int rank;
    if (offset < cutoff_coverage) {
        rank = (int) (offset / (chunks_per_rank + 1));
    } else {
        rank = (int) (cutoff + (offset - cutoff_coverage) / chunks_per_rank);
    }

    return rank;
}

static int copy_file(
    const char* src,
    const char* dest,
    uint64_t chunk_offset,
    uint64_t chunk_count,
    uint64_t file_size)
{
    /* open the input file */
    int in_fd = DCOPY_open_file(src, 1, &DCOPY_src_cache);
    if(in_fd < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Failed to open input file `%s' errno=%d %s",
            src, errno, strerror(errno));
        return -1;
    }

    /* open the output file */
    int out_fd = DCOPY_open_file(dest, 0, &DCOPY_dst_cache);
    if(out_fd < 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Failed to open output file `%s' errno=%d %s",
            dest, errno, strerror(errno));
        return -1;
    }

    /* hint that we'll read from file sequentially */
//    posix_fadvise(in_fd, offset, chunk_size, POSIX_FADV_SEQUENTIAL);

    /* get chunk size for copying files */
    size_t chunk_size = (size_t)DCOPY_user_opts.chunk_size;

    uint64_t chunks = 0;
    while (chunks < chunk_count) {

    /* get offset into file */
    off_t offset = (off_t) (chunk_size * (chunk_offset + chunks));

    /* seek to offset in source file */
    if(bayer_lseek(src, in_fd, offset, SEEK_SET) == (off_t)-1) {
        BAYER_LOG(BAYER_LOG_ERR, "Couldn't seek in source path `%s' errno=%d %s", \
            src, errno, strerror(errno));
        return -1;
    }

    /* seek to offset in destination file */
    if(bayer_lseek(dest, out_fd, offset, SEEK_SET) == (off_t)-1) {
        BAYER_LOG(BAYER_LOG_ERR, "Couldn't seek in destination path `%s' errno=%d %s", \
            dest, errno, strerror(errno));
        return -1;
    }

    /* get buffer */
    size_t buf_size = DCOPY_user_opts.block_size;
    void* buf = DCOPY_user_opts.block_buf1;

    /* write data */
    size_t total_bytes = 0;
    while(total_bytes <= chunk_size) {
        /* determine number of bytes that we
         * can read = max(buf size, remaining chunk) */
        size_t left_to_read = chunk_size - total_bytes;
        if(left_to_read > buf_size) {
            left_to_read = buf_size;
        }

        /* read data from source file */
        ssize_t num_of_bytes_read = bayer_read(src, in_fd, buf, left_to_read);

        /* check for EOF */
        if(!num_of_bytes_read) {
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

        /* write data to destination file */
        ssize_t num_of_bytes_written = bayer_write(dest, out_fd, buf,
                                     bytes_to_write);

        /* check for an error */
        if(num_of_bytes_written < 0) {
            BAYER_LOG(BAYER_LOG_ERR, "Write error when copying from `%s' to `%s' errno=%d %s",
                src, dest, errno, strerror(errno));
            return -1;
        }

        /* check that we wrote the same number of bytes that we read */
        if((size_t)num_of_bytes_written != bytes_to_write) {
            BAYER_LOG(BAYER_LOG_ERR, "Write error when copying from `%s' to `%s'",
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

    /* go to next chunk */
    chunks++;
    }

#if 0
    /* force data to file system */
    if(total_bytes > 0) {
        bayer_fsync(dest, out_fd);
    }
#endif

    /* if we wrote the last chunk, truncate the file */
    off_t last_written = (off_t) (chunk_size * (chunk_offset + chunk_count));
    off_t file_size_offt = (off_t) file_size;
    if (last_written >= file_size_offt || file_size == 0) {
        if(truncate64(dest, file_size_offt) < 0) {
            BAYER_LOG(BAYER_LOG_ERR, "Failed to truncate destination file: %s (errno=%d %s)",
                dest, errno, strerror(errno));
            return -1;
       }
    }

    /* we don't bother closing the file because our cache does it for us */

    return 0;
}

/* This is a long routine, but the idea is simple.  All tasks sum up
 * the number of file chunks they have, and those are then evenly
 * distributed amongst the processes.  After receiving all incoming
 * chunks, process open and write their chunks to the files.  The
 * process which writes the last chunk to each file also truncates
 * the file to correct size.  A 0-byte file still has one chunk. */
typedef struct copy_elem_struct {
  const char* name;
  uint64_t chunk_offset;
  uint64_t chunk_count;
  uint64_t file_size;
  struct copy_elem_struct* next;
} copy_elem;

static void copy_files(bayer_flist list)
{
    /* get our rank and number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* indicate which phase we're in to user */
    if (DCOPY_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_INFO, "Copying data.");
    }

    /* get chunk size for copying files */
    uint64_t chunk_size = (uint64_t)DCOPY_user_opts.chunk_size;

    /* total up number of file chunks for all files in our list */
    uint64_t count = 0;
    uint64_t idx;
    uint64_t size = bayer_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        /* get type of item */
        bayer_filetype type = bayer_flist_file_get_type(list, idx);

        /* if we have a file, add up its chunks */
        if (type == BAYER_TYPE_FILE) {
            /* get size of file */
            uint64_t file_size = bayer_flist_file_get_size(list, idx);

            /* compute number of chunks to copy for this file */
            uint64_t chunks = file_size / chunk_size;
            if (chunks * chunk_size < file_size || file_size == 0) {
                /* this accounts for the last chunk, which may be
                 * partial or it adds a chunk for 0-size files */
                chunks++;
            }

            /* include these chunks in our total */
            count += chunks;
        }
    }

    /* compute total number of chunks across procs */
    uint64_t total;
    MPI_Allreduce(&count, &total, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* get global offset of our first chunk */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }

    /* compute number of chunks per task, ranks below cutoff will
     * be responsible for (chunks_per_rank+1) and ranks at cutoff
     * and above are responsible for chunks_per_rank */
    uint64_t chunks_per_rank = total / (uint64_t) ranks;
    uint64_t coverage = chunks_per_rank * (uint64_t) ranks;
    uint64_t cutoff = total - coverage;

    /* TODO: replace this with DSDE */

    /* allocate an array of integers to use in alltoall,
     * we'll set a flag to 1 if we have data for that rank
     * and set it to 0 otherwise, then we'll exchange flags
     * with an alltoall */
    int* sendlist = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));
    int* recvlist = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));

    /* assume we won't send to any ranks,
     * so initialize all ranks to 0 */
    int i;
    for (i = 0; i < ranks; i++) {
        sendlist[i] = 0;
    }

    /* if we have some chunks, figure out the number of ranks
     * we'll send to and the range of rank ids, set flags to 1 */
    int send_ranks = 0;
    int first_send_rank, last_send_rank;
    if (count > 0) {
        /* compute first rank we'll send data to */
        first_send_rank = map_chunk_to_rank(offset, cutoff, chunks_per_rank);

        /* compute last rank we'll send to */
        uint64_t last_offset = offset + count - 1;
        last_send_rank  = map_chunk_to_rank(last_offset, cutoff, chunks_per_rank);

        /* set flag for each process we'll send data to */
        for (i = first_send_rank; i <= last_send_rank; i++) {
            sendlist[i] = 1;
        }

        /* compute total number of destinations we'll send to */
        send_ranks = last_send_rank - first_send_rank + 1;
    }

    /* allocate a linked list for each process we'll send to */
    copy_elem** heads = (copy_elem**) BAYER_MALLOC((size_t)send_ranks * sizeof(copy_elem*));
    copy_elem** tails = (copy_elem**) BAYER_MALLOC((size_t)send_ranks * sizeof(copy_elem*));
    uint64_t* counts  = (uint64_t*)   BAYER_MALLOC((size_t)send_ranks * sizeof(uint64_t));
    uint64_t* bytes   = (uint64_t*)   BAYER_MALLOC((size_t)send_ranks * sizeof(uint64_t));
    char** sendbufs   = (char**)      BAYER_MALLOC((size_t)send_ranks * sizeof(char*));

    /* initialize values */
    for (i = 0; i < send_ranks; i++) {
        heads[i]    = NULL;
        tails[i]    = NULL;
        counts[i]   = 0;
        bytes[i]    = 0;
        sendbufs[i] = NULL;
    }

    /* now iterate through files and build up list of chunks we'll
     * send to each task, as an optimization, we encode consecutive
     * chunks of the same file into a single unit */
    uint64_t current_offset = offset;
    for (idx = 0; idx < size; idx++) {
        /* get type of item */
        bayer_filetype type = bayer_flist_file_get_type(list, idx);

        /* if we have a file, add up its chunks */
        if (type == BAYER_TYPE_FILE) {
            /* get size of file */
            uint64_t file_size = bayer_flist_file_get_size(list, idx);

            /* compute number of chunks to copy for this file */
            uint64_t chunks = file_size / chunk_size;
            if (chunks * chunk_size < file_size || file_size == 0) {
                chunks++;
            }

            /* iterate over each chunk of this file and determine the
             * rank we should send it to */
            int prev_rank = MPI_PROC_NULL;
            uint64_t chunk_id;
            for (chunk_id = 0; chunk_id < chunks; chunk_id++) {
                /* determine which rank we should map this chunk to */
                int current_rank = map_chunk_to_rank(current_offset, cutoff, chunks_per_rank);

                /* compute index into our send_ranks arrays */
                int rank_index = current_rank - first_send_rank;

                /* if this chunk goes to a rank we've already created
                 * an element for, just update that element, otherwise
                 * create a new element */
                if (current_rank == prev_rank) {
                    /* we've already got an element started for this
                     * file and rank, just update its count field to
                     * append this element */
                    copy_elem* elem = tails[rank_index];
                    elem->chunk_count++;
                } else {
                    /* we're sending to a new rank or have the start
                     * of a new file, either way allocate a new element */
                    copy_elem* elem = (copy_elem*) BAYER_MALLOC(sizeof(copy_elem));
                    elem->name         = bayer_flist_file_get_name(list, idx);
                    elem->chunk_offset = chunk_id;
                    elem->chunk_count  = 1;
                    elem->file_size    = file_size;
                    elem->next         = NULL;

                    /* compute bytes needed to pack this item,
                     * full name NUL-terminated, chunk id,
                     * number of chunks, and file size */
                    size_t pack_size = strlen(elem->name) + 1;
                    pack_size += 3 * 8;

                    /* append element to list */
                    if (heads[rank_index] == NULL) {
                        heads[rank_index] = elem;
                    }
                    if (tails[rank_index] != NULL) {
                        tails[rank_index]->next = elem;
                    }
                    tails[rank_index] = elem;
                    counts[rank_index]++;
                    bytes[rank_index] += pack_size;

                    /* remember which rank we're sending to */
                    prev_rank = current_rank;
                }

                /* go on to our next chunk */
                current_offset++;
            }
        }
    }

    /* exchange flags with ranks so everyone knows who they'll
     * receive data from */
    MPI_Alltoall(sendlist, 1, MPI_INT, recvlist, 1, MPI_INT, MPI_COMM_WORLD);

    /* determine number of ranks that will send to us */
    int first_recv_rank = MPI_PROC_NULL;
    int recv_ranks = 0;
    for (i = 0; i < ranks; i++) {
        if (recvlist[i]) {
            /* record the first rank we'll receive from */
            if (first_recv_rank == MPI_PROC_NULL) {
                first_recv_rank = i;
            }

            /* increase our count of procs to send to us */
            recv_ranks++;
        }
    }
 
    /* determine number of messages we'll have outstanding */
    int msgs = send_ranks + recv_ranks;
    MPI_Request* request = (MPI_Request*) BAYER_MALLOC((size_t)msgs * sizeof(MPI_Request));
    MPI_Status*  status  = (MPI_Status*)  BAYER_MALLOC((size_t)msgs * sizeof(MPI_Status));

    /* create storage to hold byte counts that we'll send
     * and receive, it would be best to use uint64_t here
     * but for that, we'd need to create a datatypen,
     * with an int, we should be careful we don't overflow */
    int* send_counts = (int*) BAYER_MALLOC((size_t)send_ranks * sizeof(int));
    int* recv_counts = (int*) BAYER_MALLOC((size_t)recv_ranks * sizeof(int));

    /* initialize our send counts */
    for (i = 0; i < send_ranks; i++) {
        /* TODO: check that we don't overflow here */

        send_counts[i] = (int) bytes[i];
    }

    /* post irecv to get sizes */
    for (i = 0; i < recv_ranks; i++) {
        int recv_rank = first_recv_rank + i;
        MPI_Irecv(&recv_counts[i], 1, MPI_INT, recv_rank, 0, MPI_COMM_WORLD, &request[i]);
    }

    /* post isend to send sizes */
    for (i = 0; i < send_ranks; i++) {
        int req_id = recv_ranks + i;
        int send_rank = first_send_rank + i;
        MPI_Isend(&send_counts[i], 1, MPI_INT, send_rank, 0, MPI_COMM_WORLD, &request[req_id]);
    }

    /* wait for sizes to come in */
    MPI_Waitall(msgs, request, status);

    /* allocate memory and encode lists for sending */
    for (i = 0; i < send_ranks; i++) {
        /* allocate buffer for this destination */
        size_t sendbuf_size = (size_t) bytes[i];
        sendbufs[i] = (char*) BAYER_MALLOC(sendbuf_size);

        /* pack data into buffer */
        char* sendptr = sendbufs[i];
        copy_elem* elem = heads[i];
        while (elem != NULL) {
            /* pack file name */
            strcpy(sendptr, elem->name);
            sendptr += strlen(elem->name) + 1;

            /* pack chunk id, count, and file size */
            bayer_pack_uint64(&sendptr, elem->chunk_offset);
            bayer_pack_uint64(&sendptr, elem->chunk_count);
            bayer_pack_uint64(&sendptr, elem->file_size);

            /* go to next element */
            elem = elem->next;
        }
    }

    /* sum up total bytes that we'll receive */
    size_t recvbuf_size = 0;
    for (i = 0; i < recv_ranks; i++) {
        recvbuf_size += (size_t) recv_counts[i];
    }

    /* allocate memory for recvs */
    char* recvbuf = (char*) BAYER_MALLOC(recvbuf_size);

    /* post irecv for incoming data */
    char* recvptr = recvbuf;
    for (i = 0; i < recv_ranks; i++) {
        int recv_rank = first_recv_rank + i;
        int recv_count = recv_counts[i];
        MPI_Irecv(recvptr, recv_count, MPI_BYTE, recv_rank, 0, MPI_COMM_WORLD, &request[i]);
        recvptr += recv_count;
    }

    /* post isend to send outgoing data */
    for (i = 0; i < send_ranks; i++) {
        int req_id = recv_ranks + i;
        int send_rank = first_send_rank + i;
        int send_count = send_counts[i];
        MPI_Isend(sendbufs[i], send_count, MPI_BYTE, send_rank, 0, MPI_COMM_WORLD, &request[req_id]);
    }

    /* waitall */
    MPI_Waitall(msgs, request, status);

    /* iterate over all received data */
    const char* packptr = recvbuf;
    char* recvbuf_end = recvbuf + recvbuf_size;
    while (packptr < recvbuf_end) {
        /* unpack file name */
        const char* name = packptr;
        packptr += strlen(name) + 1;

        /* unpack chunk offset, count, and file size */
        uint64_t chunk_offset, chunk_count, file_size;
        bayer_unpack_uint64(&packptr, &chunk_offset);
        bayer_unpack_uint64(&packptr, &chunk_count);
        bayer_unpack_uint64(&packptr, &file_size);

        /* get name of destination file */
        char* dest_path = DCOPY_build_dest(name);

        /* open file, read, write, close, fsync,
         * if we have the last chunk, truncate file */
        copy_file(name, dest_path, chunk_offset, chunk_count, file_size);

        /* free the destination name */
        bayer_free(&dest_path);
    }

    /* delete memory we allocated for each destination rank */
    for (i = 0; i < send_ranks; i++) {
        /* free each of our send buffers */
        bayer_free(&sendbufs[i]);

        /* delete elements from our list */
        copy_elem* elem = heads[i];
        while (elem != NULL) {
            copy_elem* next = elem->next;
            bayer_free(&elem);
            elem = next;
        }

    }

    bayer_free(&recvbuf);
    bayer_free(&sendbufs);

    bayer_free(&recv_counts);
    bayer_free(&send_counts);

    bayer_free(&status);
    bayer_free(&request);

    /* delete structures for our list */
    bayer_free(&bytes);
    bayer_free(&counts);
    bayer_free(&tails);
    bayer_free(&heads);

    /* free our list of ranks */
    bayer_free(&sendlist);
    bayer_free(&recvlist);

    return;
}

/* iterate through list of files and set ownership, timestamps,
 * and permissions starting from deepest level and working upwards,
 * we go in this direction in case updating a file updates its
 * parent directory */
static void DCOPY_set_metadata(int levels, int minlevel, bayer_flist* lists)
{
    if (DCOPY_global_rank == 0) {
        if(DCOPY_user_opts.preserve) {
            BAYER_LOG(BAYER_LOG_INFO, "Setting ownership, permissions, and timestamps.");
        }
        else {
            BAYER_LOG(BAYER_LOG_INFO, "Fixing permissions.");
        }
    }

    /* now set timestamps on files starting from deepest level */
    int level;
    for (level = levels-1; level > 0; level--) {
        /* get list at this level */
        bayer_flist list = lists[level];

        /* cycle through our list of items and set timestamps
         * for each one at this level */
        uint64_t idx;
        uint64_t size = bayer_flist_size(list);
        for (idx = 0; idx < size; idx++) {
            /* get type of item */
            bayer_filetype type = bayer_flist_file_get_type(list, idx);

            /* we've already set these properties for links,
             * so we can skip those here */
            if (type == BAYER_TYPE_LINK) {
                continue;
            }

            /* get destination name of item */
            const char* name = bayer_flist_file_get_name(list, idx);
            char* dest = DCOPY_build_dest(name);

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
            bayer_free(&dest);
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
    printf("  -p, --preserve      - preserve permissions, ownership, timestamps, extended attributes\n");
    printf("  -s, --synchronous   - use synchronous read/write calls (O_DIRECT)\n");
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
    bayer_init();

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
    bayer_debug_level = BAYER_LOG_INFO;

    /* By default, don't unlink destination files if an open() fails. */
    DCOPY_user_opts.force = false;

    /* By default, don't bother to preserve all attributes. */
    DCOPY_user_opts.preserve = false;

    /* By default, don't use O_DIRECT. */
    DCOPY_user_opts.synchronous = false;

    /* Set default chunk size */
    DCOPY_user_opts.chunk_size = DCOPY_CHUNK_SIZE;

    /* Set default block size */
    DCOPY_user_opts.block_size = FD_BLOCK_SIZE;

    static struct option long_options[] = {
        {"debug"                , required_argument, 0, 'd'},
        {"force"                , no_argument      , 0, 'f'},
        {"help"                 , no_argument      , 0, 'h'},
        {"preserve"             , no_argument      , 0, 'p'},
        {"synchronous"          , no_argument      , 0, 's'},
        {"version"              , no_argument      , 0, 'v'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    while((c = getopt_long(argc, argv, "d:fhpusv", \
                           long_options, &option_index)) != -1) {
        switch(c) {

            case 'd':
                if(strncmp(optarg, "fatal", 5) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_FATAL;
                    bayer_debug_level = BAYER_LOG_FATAL;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: fatal");
                    }

                }
                else if(strncmp(optarg, "err", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_ERR;
                    bayer_debug_level = BAYER_LOG_ERR;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: errors");
                    }

                }
                else if(strncmp(optarg, "warn", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN;
                    bayer_debug_level = BAYER_LOG_WARN;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: warnings");
                    }

                }
                else if(strncmp(optarg, "info", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN; /* we back off a level on CIRCLE verbosity */
                    bayer_debug_level = BAYER_LOG_INFO;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: info");
                    }

                }
                else if(strncmp(optarg, "dbg", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_DBG;
                    bayer_debug_level = BAYER_LOG_DBG;

                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level set to: debug");
                    }

                }
                else {
                    if(DCOPY_global_rank == 0) {
                        BAYER_LOG(BAYER_LOG_INFO, "Debug level `%s' not recognized. " \
                            "Defaulting to `info'.", optarg);
                    }
                }

                break;

            case 'f':
                DCOPY_user_opts.force = true;

                if(DCOPY_global_rank == 0) {
                    BAYER_LOG(BAYER_LOG_INFO, "Deleting destination on errors.");
                }

                break;

            case 'h':
                if(DCOPY_global_rank == 0) {
                    DCOPY_print_usage();
                }

                DCOPY_exit(EXIT_SUCCESS);
                break;

            case 'p':
                DCOPY_user_opts.preserve = true;

                if(DCOPY_global_rank == 0) {
                    BAYER_LOG(BAYER_LOG_INFO, "Preserving file attributes.");
                }

                break;

            case 's':
                DCOPY_user_opts.synchronous = true;

                if(DCOPY_global_rank == 0) {
                    BAYER_LOG(BAYER_LOG_INFO, "Using synchronous read/write (O_DIRECT)");
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
    DCOPY_user_opts.block_buf1 = (char*) BAYER_MEMALIGN(
        DCOPY_user_opts.block_size, alignment);
    DCOPY_user_opts.block_buf2 = (char*) BAYER_MEMALIGN(
        DCOPY_user_opts.block_size, alignment);

    /* Grab a relative and actual start time for the epilogue. */
    time(&(DCOPY_statistics.time_started));
    DCOPY_statistics.wtime_started = MPI_Wtime();

    /* create an empty file list */
    bayer_flist flist = bayer_flist_new();

    /* walk paths and fill in file list */
    DCOPY_walk_paths(flist);

    /* split items in file list into sublists depending on their
     * directory depth */
    int levels, minlevel;
    bayer_flist* lists;
    bayer_flist_array_by_depth(flist, &levels, &minlevel, &lists);

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
    bayer_flist_array_free(levels, &lists);

    /* free our file lists */
    bayer_flist_free(&flist);

    /* Determine the actual and relative end time for the epilogue. */
    DCOPY_statistics.wtime_ended = MPI_Wtime();
    time(&(DCOPY_statistics.time_ended));

    /* force updates to disk */
    if (DCOPY_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_INFO, "Syncing updates to disk.");
    }
    sync();

    /* Print the results to the user. */
    DCOPY_epilogue();

    DCOPY_exit(EXIT_SUCCESS);
}

/* EOF */
