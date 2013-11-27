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

#include "bayer.h"

/* globals to hold user-input paths */
static bayer_param_path param1;
static bayer_param_path param2;

/* Print a usage message */
static void print_usage()
{
    printf("\n");
    printf("Usage: dcmp [options] source target\n");
    printf("\n");
    printf("Options:\n");
    printf("  -h, --help  - print usage\n");
    printf("\n");
    fflush(stdout);
}

#define NOT_MATCHED     "N"
#define MATCHED         "M"

static strmap* map_creat(bayer_flist list, const char* prefix)
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

        /* add item to map */
        strmap_set(map, name, NOT_MATCHED);

        /* go to next item in list */
        index++;
    }

    return map;
}

/* match entries from src into dst */
static uint64_t map_match(strmap* dst, strmap* src)
{
    uint64_t matched = 0;
    strmap_node* node;
    strmap_foreach(src, node) {
        const char* key = strmap_node_key(node);
        const char* val = strmap_node_value(node);

        /* Skip matched files */
        if (strcmp(val, MATCHED) == 0) {
            continue;
        }

        const char* dst_val = strmap_get(dst, key);
        if (dst_val != NULL) {
            strmap_set(src, key, MATCHED);
            strmap_set(dst, key, MATCHED);
            matched++;
        }
    }
    return matched;
}

int map_name(bayer_flist flist, uint64_t index, int ranks, void *args)
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
    int c;

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
            print_usage(argv);
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
    bayer_flist_walk_path(path1, 0, flist1);

    const char* path2 = param2.path;
    bayer_flist_walk_path(path2, 0, flist2);

    /* map files to ranks based on portion following prefix directory */
    bayer_flist flist3 = bayer_flist_remap(flist1, map_name, (void*)path1);
    bayer_flist flist4 = bayer_flist_remap(flist2, map_name, (void*)path2);

    strmap* map1 = map_creat(flist3, path1);
    strmap* map2 = map_creat(flist4, path2);

    uint64_t matched = map_match(map1, map2);

    uint64_t global_matched;
    MPI_Allreduce(&matched, &global_matched, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    uint64_t global_unmatched1;
    uint64_t unmatched1 = strmap_size(map1) - matched;
    MPI_Allreduce(&unmatched1, &global_unmatched1, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    uint64_t global_unmatched2;
    uint64_t unmatched2 = strmap_size(map2) - matched;
    MPI_Allreduce(&unmatched2, &global_unmatched2, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);

    if (rank == 0) {
        printf("Common files: %llu\n", global_matched);
        printf("Only in %s: %llu\n", path1, global_unmatched1);
        printf("Only in %s: %llu\n", path2, global_unmatched2);
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
