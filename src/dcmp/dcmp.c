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
    /* initial state */
    DCMP_STATE_INIT     = 'a',
    /* both have this file */
    DCMP_STATE_COMMON,
    /* both are the same type */
    DCMP_STATE_TYPE,
    /* both are regular file and have same size */
    DCMP_STATE_SIZE,
    /* both have the same data */
    DCMP_STATE_CONTENT,
    /* both are the same file */
    DCMP_STATE_SAME,
};

static void
dcmp_strmap_item_init(strmap* map, const char *key, uint64_t index)
{
    /* Should be long enough for 64 bit number and a flag */
    char val[22];
    int len = snprintf(val, sizeof(val), "%llu",
                       (unsigned long long) index);

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
    const char* val = strmap_get(map, key);
    char new_val[22];
    size_t position;

    assert(strlen(val) < sizeof(new_val) - 2);

    strcpy(new_val, val);
    position = strlen(new_val) - 1;
    new_val[position] = state;
    new_val[position + 1] = '\0';
    strmap_set(map, key, new_val);
}

static int
dcmp_strmap_item_decode(strmap* map, const char *key, uint64_t *index, char *state)
{
    const char* val = strmap_get(map, key);
    char buf[22];

    if (val == NULL) {
        return -1;
    }

    *state = val[strlen(val) - 1];
    strcpy(buf, val);
    buf[strlen(buf) - 1] = '\0';
    *index = strtoull(buf, NULL, 0);
    return 0;
}

static strmap* dcmp_strmap_creat(bayer_flist list, const char* prefix)
{
    strmap* map = strmap_new();

    /* determine length of prefix string */
    size_t prefix_len = strlen(prefix);

    uint64_t index = 0;
    uint64_t count = bayer_flist_size(list);
    while (index < count) {
        /* get full path of file name */
        const char* name = bayer_flist_file_get_name(list, index);

        /* ignore prefix portion of path */
        name += prefix_len;
        dcmp_strmap_item_init(map, name, index);
        /* go to next item in list */
        index++;
    }

    return map;
}

/* compare entries from src into dst */
static void dcmp_strmap_compare(bayer_flist dst_list,
                                strmap* dst_map,
                                bayer_flist src_list,
                                strmap* src_map,
                                uint64_t *common_file,
                                uint64_t *common_type)
{
    strmap_node* node;

    *common_file = 0;
    *common_type = 0;
    strmap_foreach(src_map, node) {
        char src_state;
        uint64_t src_index;
        char dst_state;
        uint64_t dst_index;
        const char* key = strmap_node_key(node);
        int rc;

        rc = dcmp_strmap_item_decode(src_map, key, &src_index, &src_state);
        assert(rc == 0);
        assert(src_state == DCMP_STATE_INIT);

        rc = dcmp_strmap_item_decode(dst_map, key, &dst_index, &dst_state);
        if (rc) {
            /* skip uncommon files */
            continue;
        }
        assert(dst_state == DCMP_STATE_INIT);

        (*common_file)++;

        mode_t src_mode = (mode_t) bayer_flist_file_get_mode(src_list, src_index);
        mode_t dst_mode = (mode_t) bayer_flist_file_get_mode(dst_list, dst_index);

        if ((src_mode & S_IFMT) != (dst_mode & S_IFMT)) {
            /* file type is different */
            dcmp_strmap_item_update(src_map, key, DCMP_STATE_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMP_STATE_COMMON);
            continue;
        }
        (*common_type)++;
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
            break;
        default:
            usage = 1;
            break;
        }
    }

    /* we should have two arguments left, source and dest paths */
    int numargs = argc - optind;
    if(numargs != 2) {
        BAYER_LOG(BAYER_LOG_ERR, "You must specify a source and destination path.");
        usage = 1;
    }

    /* print usage and exit if necessary */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        bayer_finalize();
        MPI_Finalize();
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

    strmap* map1 = dcmp_strmap_creat(flist3, path1);
    strmap* map2 = dcmp_strmap_creat(flist4, path2);

    uint64_t common = 0;
    uint64_t same_type = 0;
    dcmp_strmap_compare(flist3, map1, flist4, map2, &common, &same_type);

    uint64_t global_common;
    MPI_Allreduce(&common, &global_common, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    uint64_t global_uncommon1;
    uint64_t uncommon1 = strmap_size(map1) - common;
    MPI_Allreduce(&uncommon1, &global_uncommon1, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    uint64_t global_uncommon2;
    uint64_t uncommon2 = strmap_size(map2) - common;
    MPI_Allreduce(&uncommon2, &global_uncommon2, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    uint64_t global_same_type;
    MPI_Allreduce(&same_type, &global_same_type, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    if (rank == 0) {
        printf("Common files: %llu\n", global_common);
        printf("Common files with same type : %llu\n", global_same_type);
        printf("Common files with different type : %llu\n", global_common - global_same_type);
        printf("Only in %s: %llu\n", path1, global_uncommon1);
        printf("Only in %s: %llu\n", path2, global_uncommon2);
        fflush(stdout);
    }

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
