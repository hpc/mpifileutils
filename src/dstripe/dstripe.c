// mpicc -g -O0 -o restripe restripe.c -llustreapi

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <getopt.h>
#include <string.h>
#include <inttypes.h>
#include "mpi.h"

#include "mfu.h"

uint64_t create_prog_count;
uint64_t create_prog_count_total;
mfu_progress* create_prog;

uint64_t stripe_prog_bytes;
uint64_t stripe_prog_bytes_total;
mfu_progress* stripe_prog;

static void create_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute percentage of items removed */
    double percent = 0.0;
    if (create_prog_count_total > 0) {
        percent = 100.0 * (double)vals[0] / (double)create_prog_count_total;
    }

    /* compute average delete rate */
    double rate  = 0.0;
    if (secs > 0) {
        rate  = (double)vals[0] / secs;
    }

    /* compute estimated time remaining */
    double secs_remaining = -1.0;
    if (rate > 0.0) {
        secs_remaining = (double)(create_prog_count_total - vals[0]) / rate;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Created %llu files (%.2f%%) in %.3lf secs (%.3lf files/sec) %d secs remaining ...",
            vals[0], percent, secs, rate, (int)secs_remaining);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Created %llu files (%.2f%%) in %.3lf secs (%.3lf) done",
            vals[0], percent, secs, rate);
    }
}

static void stripe_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute percentage of items removed */
    double percent = 0.0;
    if (stripe_prog_bytes_total > 0) {
        percent = 100.0 * (double)vals[0] / (double)stripe_prog_bytes_total;
    }

    /* compute average delete rate */
    double rate  = 0.0;
    if (secs > 0) {
        rate  = (double)vals[0] / secs;
    }

    /* compute estimated time remaining */
    double secs_remaining = -1.0;
    if (rate > 0.0) {
        secs_remaining = (double)(stripe_prog_bytes_total - vals[0]) / rate;
    }

    /* convert bytes to units */
    double agg_size_tmp;
    const char* agg_size_units;
    mfu_format_bytes(vals[0], &agg_size_tmp, &agg_size_units);

    /* convert bandwidth to units */
    double agg_rate_tmp;
    const char* agg_rate_units;
    mfu_format_bw(rate, &agg_rate_tmp, &agg_rate_units);

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Wrote %.3lf %s (%.2f%%) in %.3lf secs (%.3lf %s) %d secs remaining ...",
            agg_size_tmp, agg_size_units, percent, secs, agg_rate_tmp, agg_rate_units, (int)secs_remaining);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Wrote %.3lf %s (%.2f%%) in %.3lf secs (%.3lf %s) done",
            agg_size_tmp, agg_size_units, percent, secs, agg_rate_tmp, agg_rate_units);
    }
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dstripe [options] PATH...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -c, --count <COUNT>    - stripe count (default -1)\n");
    printf("  -s, --size <SIZE>      - stripe size in bytes (default 1MB)\n");
    printf("  -m, --minsize <SIZE>   - minimum file size (default 0MB)\n");
    printf("  -r, --report           - display file size and stripe info\n");
    printf("      --progress <N>     - print progress every N seconds\n");
    printf("  -v, --verbose          - verbose output\n");
    printf("  -q, --quiet            - quiet output\n");
    printf("  -h, --help             - print usage\n");
    printf("\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    fflush(stdout);
    return;
}

/* generate a random, alpha suffix */
static void generate_suffix(char *suffix, const int len)
{
    const char set[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    int numchars = len - 1;

    /* first char has to be a dot */
    suffix[0] = '.';

    /* randomly fill in our array with chars */
    for (int i = 1; i < numchars; i++) {
        int set_idx = (double)rand() / RAND_MAX * (sizeof(set) - 1);
        suffix[i] = set[set_idx];
    }

    /* need to be null terminated */
    suffix[len - 1] = '\0';
}

static void generate_pretty_size(char *out, unsigned int length, uint64_t size)
{
    double size_tmp;
    const char* size_units;
    char *unit;
    unsigned int unit_len;

    mfu_format_bytes(size, &size_tmp, &size_units);
    snprintf(out, length, "%.2f %s", size_tmp, size_units);
}

/* print to stdout the stripe size and count of each file in the mfu_flist */
static void stripe_info_report(mfu_flist list)
{
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* print header */
    if (rank == 0) {
        printf("%10s %3.3s %8.8s %s\n", "Size", "Cnt", "Str Size", "File Path");
        printf("%10s %3.3s %8.8s %s\n", "----", "---", "--------", "---------");
        fflush(stdout);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* print out file info */
    for (idx = 0; idx < size; idx++) {
        mfu_filetype type = mfu_flist_file_get_type(list, idx);

        /* report striping information for regular files only */
        if (type == MFU_TYPE_FILE) {
            const char* in_path = mfu_flist_file_get_name(list, idx);
            uint64_t stripe_size = 0;
            uint64_t stripe_count = 0;
            char filesize[11];
            char stripesize[9];

            /*
             * attempt to get striping info and print it out,
             * skip the file if we can't get the striping info we seek
             */
            if (mfu_stripe_get(in_path, &stripe_size, &stripe_count) != 0) {
                continue;
            }

            /* format it nicely */
            generate_pretty_size(filesize, sizeof(filesize), mfu_flist_file_get_size(list, idx));
            generate_pretty_size(stripesize, sizeof(stripesize), stripe_size);

            /* print the row */
            printf("%10.10s %3" PRId64 " %8.8s %s\n", filesize, stripe_count, stripesize, in_path);
            fflush(stdout);
        }
    }
}

/* filter the list of files down based on the current stripe size and stripe count */
static mfu_flist filter_list(mfu_flist list, int stripe_count, uint64_t stripe_size, uint64_t min_size, uint64_t* total_count, uint64_t* total_size)
{
    /* initialize counters for file and byte count */
    uint64_t my_count = 0;
    uint64_t my_size  = 0;

    /* this is going to be a subset of the full file list */
    mfu_flist filtered = mfu_flist_subset(list);

    uint64_t idx;
    uint64_t size = mfu_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        /* we only care about regular files */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);
        if (type == MFU_TYPE_FILE) {
            /* if our file is below the minimum file size, skip it */
            uint64_t filesize = mfu_flist_file_get_size(list, idx);
            if (filesize < min_size) {
                continue;
            }

            const char* in_path = mfu_flist_file_get_name(list, idx);
            uint64_t curr_stripe_size = 0;
            uint64_t curr_stripe_count = 0;

            /*
             * attempt to get striping info,
             * skip the file if we can't get the striping info we seek
             */
            if (mfu_stripe_get(in_path, &curr_stripe_size, &curr_stripe_count) != 0) {
                continue;
            }

            /* TODO: this should probably be better */
            /* if the current stripe size or stripe count doesn't match, then a restripe the file */
            if (curr_stripe_count != stripe_count || curr_stripe_size != stripe_size) {
                mfu_flist_file_copy(list, idx, filtered);

                /* increment file count and add file size to our running total */
                my_count += 1;
                my_size  += filesize;
            }
        }
    }

    /* summarize and return the new list */
    mfu_flist_summarize(filtered);

    /* get sum of count and size */
    MPI_Allreduce(&my_count, total_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&my_size,  total_size,  1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    return filtered;
}

/* write a chunk of the file */
static void write_file_chunk(mfu_file_chunk* p, const char* out_path)
{
    size_t chunk_size = 1024*1024;
    uint64_t base = (off_t)p->offset;
    uint64_t file_size = (off_t)p->file_size;
    const char *in_path = p->name;
    uint64_t stripe_size = (off_t)p->length;

    /* if the file size is 0, there's no data to restripe */
    /* if the stripe size is 0, then there's no work to be done */
    if (file_size == 0 || stripe_size == 0) {
        return;
    }

    /* allocate buffer */
    void* buf = MFU_MALLOC(chunk_size);

    /* open input file for reading */
    int in_fd = mfu_open(in_path, O_RDONLY);
    if (in_fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open input file %s (%s)", in_path, strerror(errno));
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* open output file for writing */
    int out_fd = mfu_open(out_path, O_WRONLY);
    if (out_fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open output file %s (%s)", out_path, strerror(errno));
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* write data */
    uint64_t chunk_id = 0;
    uint64_t stripe_read = 0;
    while (stripe_read < stripe_size) {
        /* determine number of bytes to read */
        /* try to read a full chunk's worth of bytes */
        size_t read_size = chunk_size;

        /* if the stripe doesn't have that much left */
        uint64_t remainder = stripe_size - stripe_read;
        if (remainder < (uint64_t) read_size) {
            read_size = (size_t) remainder;
        }

        /* get byte offset to read from */
        uint64_t offset = base + (chunk_id * chunk_size);
        if (offset < file_size) {
            /* the first byte falls within the file size,
             * now check the last byte */
            uint64_t last = offset + (uint64_t) read_size;
            if (last > file_size) {
                /* the last byte is beyond the end, set read size
                 * to the most we can read */
                read_size = (size_t) (file_size - offset);
            }
        } else {
            /* the first byte we need to read is past the end of
             * the file, so don't read anything */
            read_size = 0;
        }

        /* bail if we don't have anything to read */
        if (read_size == 0) {
            break;
        }

        /* seek to correct spot in input file */
        off_t pos = (off_t) offset;
        off_t seek_rc = mfu_lseek(in_path, in_fd, pos, SEEK_SET);
        if (seek_rc == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to seek in input file %s (%s)", in_path, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* read chunk from input */
        ssize_t nread = mfu_read(in_path, in_fd, buf, read_size);

        /* check for errors */
        if (nread < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to read data from input file %s (%s)", in_path, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* check for short reads */
        if (nread != read_size) {
            MFU_LOG(MFU_LOG_ERR, "Got a short read from input file %s", in_path);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* seek to correct spot in output file */
        seek_rc = mfu_lseek(out_path, out_fd, pos, SEEK_SET);
        if (seek_rc == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to seek in output file %s (%s)", out_path, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* write chunk to output */
        ssize_t nwrite = mfu_write(out_path, out_fd, buf, read_size);

        /* check for errors */
        if (nwrite < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to write data to output file %s (%s)", out_path, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* check for short reads */
        if (nwrite != read_size) {
            MFU_LOG(MFU_LOG_ERR, "Got a short write to output file %s", out_path);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* update our byte count for progress messages */
        stripe_prog_bytes += read_size;
        mfu_progress_update(&stripe_prog_bytes, stripe_prog);

        /* go on to the next chunk in this stripe, we assume we
         * read the whole chunk size, if we didn't it's because
         * the stripe is smaller or we're at the end of the file,
         * but in either case we're done so it doesn't hurt to
         * over estimate in this calculation */
        stripe_read += (uint64_t) chunk_size;
        chunk_id++;
    }

    /* close files */
    mfu_fsync(out_path, out_fd);
    mfu_close(out_path, out_fd);
    mfu_close(in_path, in_fd);

    /* free buffer */
    mfu_free(&buf);
}

int main(int argc, char* argv[])
{
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and number of ranks in the job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    uint64_t idx;
    int option_index = 0;
    int usage = 0;
    int report = 0;
    unsigned int numpaths = 0;
    mfu_param_path* paths = NULL;
    unsigned long long bytes;

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    /* default to 1MB stripe size, stripe across all OSTs, and all files are candidates */
    int stripes = -1;
    uint64_t stripe_size = 1048576;
    uint64_t min_size = 0;

    static struct option long_options[] = {
        {"count",    1, 0, 'c'},
        {"size",     1, 0, 's'},
        {"minsize",  1, 0, 'm'},
        {"report",   0, 0, 'r'},
        {"progress", 1, 0, 'R'},
        {"verbose",  0, 0, 'v'},
        {"quiet",    0, 0, 'q'},
        {"help",     0, 0, 'h'},
        {0, 0, 0, 0}
    };

    while (1) {
        int c = getopt_long(argc, argv, "c:s:m:rvqh",
                    long_options, &option_index);

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'c':
                /* stripe count */
                stripes = atoi(optarg);
                break;
            case 's':
                /* stripe size in bytes */
                if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse stripe size: %s", optarg);
                    }
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }
                stripe_size = (uint64_t)bytes;
                break;
            case 'm':
                /* min file size in bytes */
                if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse minimum file size: %s", optarg);
                    }
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }
                min_size = (uint64_t)bytes;
                break;
            case 'r':
                /* report striping info */
		report = 1;
                break;
            case 'R':
                mfu_progress_timeout = atoi(optarg);
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = MFU_LOG_NONE;
                /* since process won't be printed in quiet anyway,
                 * disable the algorithm to save some overhead */
                mfu_progress_timeout = 0;
                break;
            case 'h':
                /* display usage */
                usage = 1;
                break;
            case '?':
                /* display usage */
                usage = 1;
                break;
            default:
                if (rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* check that we got a valid progress value */
    if (mfu_progress_timeout < 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Seconds in --progress must be non-negative: %d invalid", mfu_progress_timeout);
        }
        usage = 1;
    }

    /* create new mfu_file objects */
    mfu_file_t* mfu_file = mfu_file_new();

    /* paths to walk come after the options */
    if (optind < argc) {
        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        char** p = &argv[optind];
        mfu_param_path_set_all((uint64_t)numpaths, (const char**)p, paths, mfu_file, true);
        optind += numpaths;
    } else {
        usage = 1;
    }

    /* if we need to print usage, print it and exit */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_file_delete(&mfu_file);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* nothing to do if lustre support is disabled */
#ifndef LUSTRE_SUPPORT
    if (rank == 0) {
        MFU_LOG(MFU_LOG_ERR, "Lustre support is disabled.");
    }
    MPI_Abort(MPI_COMM_WORLD, 1);
#endif

    /* stripe count must be -1 for all available or greater than 0 */
    if (stripes < -1) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Stripe count must be -1 for all servers, 0 for lustre file system default, or a positive value");
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* lustre requires stripe sizes to be aligned */
    if (stripe_size > 0 && stripe_size % 65536 != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Stripe size must be a multiple of 65536");
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* TODO: verify that source / target are on Lustre */

    /* walk list of input paths and stat as we walk */
    mfu_flist flist = mfu_flist_new();

    mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist, mfu_file);

    /* filter down our list to files which don't meet our striping requirements */
    mfu_flist filtered = filter_list(flist, stripes, stripe_size, min_size, &create_prog_count_total, &stripe_prog_bytes_total);
    mfu_flist_free(&flist);

    MPI_Barrier(MPI_COMM_WORLD);

    /* report the file size and stripe count of all files we found */
    if (report) {
        /* report the files in our filtered list */
        stripe_info_report(filtered);

        /* free the paths and our list */
        mfu_flist_free(&filtered);
        mfu_param_path_free_all(numpaths, paths);
        mfu_free(&paths);

        /* delete file object */
        mfu_file_delete(&mfu_file);

        /* finalize */
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }

    /* generate a global suffix for our temp files and have each node check it's list */
    char suffix[8];
    uint64_t retry;

    /* seed our random number generator */
    srand(time(NULL));

    /* keep trying to make a valid random suffix...*/
    do {
        uint64_t attempt = 0;

        /* make rank 0 responsible for generating a random suffix */
        if (rank == 0) {
            generate_suffix(suffix, sizeof(suffix));
        }

        /* broadcast the random suffix to all ranks */
        MPI_Bcast(suffix, sizeof(suffix), MPI_CHAR, 0, MPI_COMM_WORLD);

        /* check that the file doesn't already exist */
        uint64_t size = mfu_flist_size(filtered);
        for (idx = 0; idx < size; idx++) {
            char temp_path[PATH_MAX];
            strcpy(temp_path, mfu_flist_file_get_name(filtered, idx));
            strcat(temp_path, suffix);
            if(!mfu_file_access(temp_path, F_OK, mfu_file)) {
                /* the file already exists */
                attempt = 1;
                break;
            }
        }

        /* do a reduce to figure out if a rank has a file collision */
        MPI_Allreduce(&attempt, &retry, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
    } while(retry != 0);

    /* initialize progress messages while creating files */
    create_prog_count = 0;
    create_prog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, create_progress_fn);

    /* create new files so we can restripe */
    uint64_t size = mfu_flist_size(filtered);
    for (idx = 0; idx < size; idx++) {
        char temp_path[PATH_MAX];
        strcpy(temp_path, mfu_flist_file_get_name(filtered, idx));
        strcat(temp_path, suffix);

        /* create a striped file at the temp file path */
        mfu_stripe_set(temp_path, stripe_size, stripes);

        /* update our status for file create progress */
        create_prog_count++;
        mfu_progress_update(&create_prog_count, create_prog);
    }

    /* finalize file create progress messages */
    mfu_progress_complete(&create_prog_count, &create_prog);

    MPI_Barrier(MPI_COMM_WORLD);

    /* initialize progress messages while copying data */
    stripe_prog_bytes = 0;
    stripe_prog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, stripe_progress_fn);

    /* found a suffix, now we need to break our files into chunks based on stripe size */
    mfu_file_chunk* file_chunks = mfu_file_chunk_list_alloc(filtered, stripe_size);
    mfu_file_chunk* p = file_chunks;
    while (p != NULL) {
        /* build path to temp file */
        char temp_path[PATH_MAX];
        strcpy(temp_path, p->name);
        strcat(temp_path, suffix);

        /* write each chunk in our list */
        write_file_chunk(p, temp_path);

        /* move on to next file chunk */
        p = p->next;
    }
    mfu_file_chunk_list_free(&file_chunks);

    /* finalize progress messages */
    mfu_progress_complete(&stripe_prog_bytes, &stripe_prog);

    MPI_Barrier(MPI_COMM_WORLD);

    /* remove input file and rename temp file */
    for (idx = 0; idx < size; idx++) {
        /* build path to temp file */
        const char *in_path = mfu_flist_file_get_name(filtered, idx);
        char out_path[PATH_MAX];
        strcpy(out_path, in_path);
        strcat(out_path, suffix);

        /* change the mode of the newly restriped file to be the same as the old one */
        mode_t mode = (mode_t) mfu_flist_file_get_mode(filtered, idx);
        if (mfu_file_chmod(out_path, mode, mfu_file) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to chmod file %s (%s)", out_path, strerror(errno));
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        /* rename the new, restriped file to the old name */
        if (rename(out_path, in_path) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to rename file %s to %s", out_path, in_path);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /* wait for everyone to finish */
    MPI_Barrier(MPI_COMM_WORLD);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* delete file object */
    mfu_file_delete(&mfu_file);

    /* free filtered list, path parameters */
    mfu_flist_free(&filtered);
    mfu_param_path_free_all(numpaths, paths);
    mfu_free(&paths);

    mfu_finalize();
    MPI_Finalize();

    return 0;
}
