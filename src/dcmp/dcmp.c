#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <mpi.h>
#include <libcircle.h>
#include <linux/limits.h>
#include <libgen.h>
#include <errno.h>

/* for bool type, true/false macros */
#include <stdbool.h>
#include <assert.h>

#include "bayer.h"

/* globals to hold user-input paths */
static bayer_param_path param1;
static bayer_param_path param2;

/* Print a usage message */
static void print_usage(void)
{
    printf("\n");
    printf("Usage: dcmp [options] source target\n");
    printf("\n");
    printf("Options:\n");
    printf("  -h, --help  - print usage\n");
    printf("\n");
    fflush(stdout);
}

enum {
    DCMP_STATE_INIT = 'a', /* initial state */
    DCMP_STATE_COMMON,     /* both have this file */
    DCMP_STATE_TYPE,       /* both are the same type */
    DCMP_STATE_SIZE,       /* both are regular file and have same size */
    DCMP_STATE_CONTENT,    /* both have the same data */
    DCMP_STATE_SAME,       /* both are the same file */
};

/* given a filename as the key, encode an index followed
 * by the init state */
static void
dcmp_strmap_item_init(strmap* map, const char *key, uint64_t index)
{
    /* Should be long enough for 64 bit number and a flag */
    char val[22];

    /* encode the index */
    int len = snprintf(val, sizeof(val), "%llu",
                       (unsigned long long) index);

    /* encode the state (state character and trailing NUL) */
    assert((size_t)len < (sizeof(val) - 2));
    size_t position = strlen(val);
    val[position] = DCMP_STATE_INIT;
    val[position + 1] = '\0';

    /* add item to map */
    strmap_set(map, key, val);
}

static void
dcmp_strmap_item_update(strmap* map, const char *key, char state)
{
    /* Should be long enough for 64 bit number and a flag */
    char new_val[22];

    /* lookup item from map */
    const char* val = strmap_get(map, key);

    /* copy existing index over */
    strcpy(new_val, val);

    /* set new state value */
    assert(strlen(val) < sizeof(new_val) - 2);
    size_t position = strlen(new_val) - 1;
    new_val[position] = state;
    new_val[position + 1] = '\0';

    /* reinsert item in map */
    strmap_set(map, key, new_val);
}

static int
dcmp_strmap_item_decode(strmap* map, const char *key, uint64_t *index, char *state)
{
    /* Should be long enough for 64 bit number and a flag */
    char buf[22];

    /* lookup item from map */
    const char* val = strmap_get(map, key);
    if (val == NULL) {
        return -1;
    }

    /* extract state */
    size_t position = strlen(val) - 1;
    *state = val[position];

    /* extract index */
    strcpy(buf, val);
    buf[strlen(buf) - 1] = '\0';
    *index = strtoull(buf, NULL, 0);

    return 0;
}

/* map each file name to its index in the file list and initialize
 * its state for comparison operation */
static strmap* dcmp_strmap_creat(bayer_flist list, const char* prefix)
{
    /* create a new string map to map a file name to a string
     * encoding its index and state */
    strmap* map = strmap_new();

    /* determine length of prefix string */
    size_t prefix_len = strlen(prefix);

    /* iterate over each item in the file list */
    uint64_t index = 0;
    uint64_t count = bayer_flist_size(list);
    while (index < count) {
        /* get full path of file name */
        const char* name = bayer_flist_file_get_name(list, index);

        /* ignore prefix portion of path */
        name += prefix_len;

        /* create entry for this file */
        dcmp_strmap_item_init(map, name, index);

        /* go to next item in list */
        index++;
    }

    return map;
}

/* Return -1 when error, return 0 when equal, return 1 when diff */
int _dcmp_compare_2_files(const char* src_name, const char* dst_name,
                          int src_fd, int dst_fd,
                          off_t offset, size_t size, size_t buff_size)
{
    /* assume we'll find that files contents are the same */
    int rc = 0;

    /* seek to offset in source file */
    if (bayer_lseek(src_name, src_fd, offset, SEEK_SET) == (off_t)-1) {
        return -1;
    }

    /* seek to offset in destination file */
    if(bayer_lseek(dst_name, dst_fd, offset, SEEK_SET) == (off_t)-1) {
        return -1;
    }

    /* allocate buffers to read file data */
    void* src_buf  = BAYER_MALLOC(buff_size + 1);
    void* dest_buf = BAYER_MALLOC(buff_size + 1);

    /* read and compare data from files */
    size_t total_bytes = 0;
    while(size == 0 || total_bytes <= size) {
        /* determine number of bytes to read in this iteration */
        size_t left_to_read;
        if (size == 0) {
            left_to_read = buff_size;
        } else {
            left_to_read = size - total_bytes;
            if (left_to_read > buff_size) {
                left_to_read = buff_size;
            }
        }

        /* read data from source and destination */
        ssize_t src_read = bayer_read(src_name, src_fd, src_buf,
             left_to_read);
        ssize_t dst_read = bayer_read(dst_name, dst_fd, dest_buf,
             left_to_read);

        /* check for read errors */
        if (src_read < 0 || dst_read < 0) {
            /* hit a read error */
            rc = -1;
            break;
        }

        /* check that we got the same number of bytes from each */
        if (src_read != dst_read) {
            /* one read came up shorter than the other */
            rc = 1;
            break;
        }

        /* check for EOF */
        if (!src_read) {
            /* hit end of file in both */
            break;
        }

        /* check that buffers are the same */
        if (memcmp(src_buf, dest_buf, src_read) != 0) {
            /* memory contents are different */
            rc = 1;
            break;
        }

        /* add bytes to our total */
        total_bytes += src_read;
    }

    /* free buffers */
    bayer_free(&dest_buf);
    bayer_free(&src_buf);

    return rc;
}

/* Return -1 when error, return 0 when equal, return 1 when diff */
int dcmp_compare_2_files(const char* src_name, const char* dst_name, size_t buff_size)
{
    /* open source file */
    int src_fd = bayer_open(src_name, O_RDONLY);
    if (src_fd < 0) {
        return -1;
    }

    /* open destination file */
    int dst_fd = bayer_open(dst_name, O_RDONLY);
    if (dst_fd < 0) {
        bayer_close(src_name, src_fd);
        return -1;
    }

    /* hint that we'll read from file sequentially */
    posix_fadvise(src_fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(dst_fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    /* compare file contents */
    int rc = _dcmp_compare_2_files(src_name, dst_name, src_fd, dst_fd,
        0, 0, buff_size);

    /* close files */
    bayer_close(dst_name, dst_fd);
    bayer_close(src_name, src_fd);

    return rc;
}

/* compare entries from src into dst */
static void dcmp_strmap_compare(bayer_flist dst_list,
                                strmap* dst_map,
                                bayer_flist src_list,
                                strmap* src_map,
                                uint64_t *common_file,
                                uint64_t *common_type,
                                uint64_t *common_content)
{
    /* initialize output values */
    *common_file    = 0;
    *common_type    = 0;
    *common_content = 0;

    /* iterate over each item in source map */
    strmap_node* node;
    strmap_foreach(src_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index and state of source file */
        char src_state;
        uint64_t src_index;
        int rc = dcmp_strmap_item_decode(src_map, key, &src_index, &src_state);
        assert(rc == 0);
        assert(src_state == DCMP_STATE_INIT);

        /* get index and state of destination file */
        char dst_state;
        uint64_t dst_index;
        rc = dcmp_strmap_item_decode(dst_map, key, &dst_index, &dst_state);
        if (rc) {
            /* skip uncommon files */
            continue;
        }
        assert(dst_state == DCMP_STATE_INIT);

        /* found a file names common to both source and destination */
        (*common_file)++;

        /* get modes of files */
        mode_t src_mode = (mode_t) bayer_flist_file_get_mode(src_list, src_index);
        mode_t dst_mode = (mode_t) bayer_flist_file_get_mode(dst_list, dst_index);

        /* check whether files are of the same time */
        if ((src_mode & S_IFMT) != (dst_mode & S_IFMT)) {
            /* file type is different */
            dcmp_strmap_item_update(src_map, key, DCMP_STATE_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMP_STATE_COMMON);
            continue;
        }

        /* file has the same type in both source and destination */
        (*common_type)++;

        /* TODO: add support for symlinks */

        /* for now, we can only compare regular files */
        if (! S_ISREG(dst_mode)) {
            /* not regular file, take them as common content */
            dcmp_strmap_item_update(src_map, key, DCMP_STATE_TYPE);
            dcmp_strmap_item_update(dst_map, key, DCMP_STATE_TYPE);
            (*common_content)++;
            continue;
        }

        /* check that file sizes are the same */
        uint64_t src_size = bayer_flist_file_get_size(src_list, src_index);
        uint64_t dst_size = bayer_flist_file_get_size(dst_list, dst_index);
        if (src_size != dst_size) {
            /* file type is different */
            dcmp_strmap_item_update(src_map, key, DCMP_STATE_TYPE);
            dcmp_strmap_item_update(dst_map, key, DCMP_STATE_TYPE);
            continue;
        }

        /* the sizes are the same, now compare file contents */
        const char* src_name = bayer_flist_file_get_name(src_list, src_index);
        const char* dst_name = bayer_flist_file_get_name(dst_list, dst_index);
        rc = dcmp_compare_2_files(src_name, dst_name, 1048576);
        if (rc == 1) {
            /* found a difference in file contents */
            dcmp_strmap_item_update(src_map, key, DCMP_STATE_SIZE);
            dcmp_strmap_item_update(dst_map, key, DCMP_STATE_SIZE);
            continue;
        } else if (rc < 0) {
            BAYER_ABORT(-1, "Failed to compare file %s and %s errno=%d (%s)",
                src_name, dst_name, errno, strerror(errno));
        }

        /* files have the same content */
        (*common_content)++;
    }
}

static int
dcmp_map_fn(bayer_flist flist, uint64_t index, int ranks, void *args)
{
    /* the args pointer is a pointer to the directory prefix to
     * be ignored in full path name */
    char* prefix = (char *)args;
    size_t prefix_len = strlen(prefix);

    /* get name of item */
    const char* name = bayer_flist_file_get_name(flist, index);

    /* identify a rank responsible for this item */
    const char* ptr = name + prefix_len;
    size_t ptr_len = strlen(ptr);
    uint32_t hash = jenkins_one_at_a_time_hash(ptr, ptr_len);
    int rank = (int) (hash % (uint32_t)ranks);
    return rank;
}

int main(int argc, char **argv)
{
    /* initialize MPI and bayer libraries */
    MPI_Init(&argc, &argv);
    bayer_init();

    /* get our rank and number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* TODO: allow user to specify file lists as input files */

    /* TODO: three levels of comparison:
     *   1) file names only
     *   2) stat info + items in #1
     *   3) file contents + items in #2 */

    int option_index = 0;
    static struct option long_options[] = {
        {"help",     0, 0, 'h'},
        {0, 0, 0, 0}
    };

    /* read in command line options */
    int usage = 0;
    int help  = 0;
    while (1) {
        int c = getopt_long(
            argc, argv, "h",
            long_options, &option_index
        );

        if (c == -1) {
            break;
        }

        switch (c) {
        case 'h':
        case '?':
            usage = 1;
            help  = 1;
            break;
        default:
            usage = 1;
            break;
        }
    }

    /* if help flag was thrown, don't bother checking usage */
    if (! help) {
        /* we should have two arguments left, source and dest paths */
        int numargs = argc - optind;
        if (numargs != 2) {
            if (rank == 0) {
                BAYER_LOG(BAYER_LOG_ERR, "You must specify a source and destination path.");
            }
            usage = 1;
        }
    }

    /* print usage and exit if necessary */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        bayer_finalize();
        MPI_Finalize();
        if (help) {
            return 0;
        }
        return 1;
    }

    /* parse the source path */
    const char* usrpath1 = argv[optind];
    bayer_param_path_set(usrpath1, &param1);

    /* parse the destination path */
    const char* usrpath2 = argv[optind + 1];
    bayer_param_path_set(usrpath2, &param2);

    /* allocate lists for source and destinations */
    bayer_flist flist1 = bayer_flist_new();
    bayer_flist flist2 = bayer_flist_new();

    /* walk source and destination paths */
    const char* path1 = param1.path;
    bayer_flist_walk_path(path1, 1, flist1);

    const char* path2 = param2.path;
    bayer_flist_walk_path(path2, 1, flist2);

    /* map files to ranks based on portion following prefix directory */
    bayer_flist flist3 = bayer_flist_remap(flist1, dcmp_map_fn, (void*)path1);
    bayer_flist flist4 = bayer_flist_remap(flist2, dcmp_map_fn, (void*)path2);

    /* map each file name to its index and its comparison state */
    strmap* map1 = dcmp_strmap_creat(flist3, path1);
    strmap* map2 = dcmp_strmap_creat(flist4, path2);

    /* compare files in map1 with those in map2 */
    uint64_t common, same_type, same_content;
    dcmp_strmap_compare(flist3, map1, flist4, map2, &common, &same_type, &same_content);

    /* count total number of common file names */
    uint64_t global_common;
    MPI_Allreduce(&common, &global_common, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    /* count total number of file names unique to map1 */
    uint64_t global_uncommon1;
    uint64_t uncommon1 = strmap_size(map1) - common;
    MPI_Allreduce(&uncommon1, &global_uncommon1, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    /* count total number of file names unique to map2 */
    uint64_t global_uncommon2;
    uint64_t uncommon2 = strmap_size(map2) - common;
    MPI_Allreduce(&uncommon2, &global_uncommon2, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    /* count total number of file names common to map1 and map2 with
     * same file types */
    uint64_t global_same_type;
    MPI_Allreduce(&same_type, &global_same_type, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    /* count total number of file names common to map1 and map2 with
     * same file types and same file contents */
    uint64_t global_same_content;
    MPI_Allreduce(&same_content, &global_same_content, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    /* print summary info */
    if (rank == 0) {
        printf("Common files: %llu\n", global_common);
        printf("Common files with same type: %llu\n", global_same_type);
        printf("Common files with different type: %llu\n",
               global_common - global_same_type);
        printf("Common files with same type & content: %llu\n",
               global_same_content);
        printf("Common files with same type but different content: %llu\n",
               global_same_type - global_same_content);
        printf("Only in %s: %llu\n", path1, global_uncommon1);
        printf("Only in %s: %llu\n", path2, global_uncommon2);
        fflush(stdout);
    }

    /* free maps of file names to comparison state info */
    strmap_delete(&map1);
    strmap_delete(&map2);

    /* free file lists */
    bayer_flist_free(&flist1);
    bayer_flist_free(&flist2);
    bayer_flist_free(&flist3);
    bayer_flist_free(&flist4);

    /* free source and dest params */
    bayer_param_path_free(&param1);
    bayer_param_path_free(&param2);

    /* shut down */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
