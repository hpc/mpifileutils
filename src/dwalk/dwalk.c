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

#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h> /* asctime / localtime */

#include <stdarg.h> /* variable length args */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <assert.h>
#include <inttypes.h>
#include <string.h>

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"
#include "mfu_flist.h"

// getpwent getgrent to read user and group entries

/* TODO: change globals to struct */
static int walk_stat = 1;
static int dir_perm = 0;

/* keep stats during walk */
uint64_t total_dirs    = 0;
uint64_t total_files   = 0;
uint64_t total_links   = 0;
uint64_t total_unknown = 0;
uint64_t total_bytes   = 0;

static void print_summary(mfu_flist flist)
{
    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* step through and print data */
    uint64_t idx = 0;
    uint64_t max = mfu_flist_size(flist);
    while (idx < max) {
        if (mfu_flist_have_detail(flist)) {
            /* get mode */
            mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

            /* set file type */
            if (S_ISDIR(mode)) {
                total_dirs++;
            }
            else if (S_ISREG(mode)) {
                total_files++;
            }
            else if (S_ISLNK(mode)) {
                total_links++;
            }
            else {
                /* unknown file type */
                total_unknown++;
            }

            uint64_t size = mfu_flist_file_get_size(flist, idx);
            total_bytes += size;
        }
        else {
            /* get type */
            mfu_filetype type = mfu_flist_file_get_type(flist, idx);

            if (type == MFU_TYPE_DIR) {
                total_dirs++;
            }
            else if (type == MFU_TYPE_FILE) {
                total_files++;
            }
            else if (type == MFU_TYPE_LINK) {
                total_links++;
            }
            else {
                /* unknown file type */
                total_unknown++;
            }
        }

        /* go to next file */
        idx++;
    }

    /* get total directories, files, links, and bytes */
    uint64_t all_dirs, all_files, all_links, all_unknown, all_bytes;
    uint64_t all_count = mfu_flist_global_size(flist);
    MPI_Allreduce(&total_dirs,    &all_dirs,    1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_files,   &all_files,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_links,   &all_links,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_unknown, &all_unknown, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_bytes,   &all_bytes,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* convert total size to units */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && rank == 0) {
        printf("Items: %llu\n", (unsigned long long) all_count);
        printf("  Directories: %llu\n", (unsigned long long) all_dirs);
        printf("  Files: %llu\n", (unsigned long long) all_files);
        printf("  Links: %llu\n", (unsigned long long) all_links);
        /* printf("  Unknown: %lu\n", (unsigned long long) all_unknown); */

        if (mfu_flist_have_detail(flist)) {
            double agg_size_tmp;
            const char* agg_size_units;
            mfu_format_bytes(all_bytes, &agg_size_tmp, &agg_size_units);

            uint64_t size_per_file = 0.0;
            if (all_files > 0) {
                size_per_file = (uint64_t)((double)all_bytes / (double)all_files);
            }
            double size_per_file_tmp;
            const char* size_per_file_units;
            mfu_format_bytes(size_per_file, &size_per_file_tmp, &size_per_file_units);

            printf("Data: %.3lf %s (%.3lf %s per file)\n", agg_size_tmp, agg_size_units, size_per_file_tmp, size_per_file_units);
        }
    }

    return;
}

struct distribute_item {
    uint64_t value;
    uint64_t ranks;
};

#define MAX_DISTRIBUTE_SEPARATORS 128

struct distribute_option {
    int separator_number;
    uint64_t separators[MAX_DISTRIBUTE_SEPARATORS];
};

/*
 * Search the right position to insert the value
 * If the value exists already, do nothing
 * Otherwise, locate the right position, and move the array forward to
 * save the value.
 */
static int distribute_item_add(struct distribute_item *items,
                               uint64_t size, uint64_t *count,
                               uint64_t value, uint64_t ranks)
{
    uint64_t low = 0;
    uint64_t high;
    uint64_t middle;
    uint64_t pos;
    uint64_t c;

    c = *count;
    if (size < c)
        return -1;

    if (c == 0) {
        items[0].value = value;
        items[0].ranks = ranks;
        *count = 1;
        return 0;
    }

    high = c - 1;
    while (low < high)
    {
        middle = (high - low) / 2 + low;
        if (items[middle].value == value)
            return 0;
        /* In the left half */
        else if (items[middle].value < value)
            low = middle + 1;
        /* In the right half */
        else
            high = middle;
    }
    assert(low == high);
    if (items[low].value == value)
        return 0;

    if (size < c + 1)
        return -1;

    if (items[low].value < value)
        pos = low + 1;
    else
        pos = low;

    if (pos < c)
        memmove(&items[low + 1], &items[low], sizeof(*items) * (c - pos));

    items[pos].value = value;
    items[pos].ranks = ranks;
    *count = c + 1;
    return 0;
}

static int distribution_gather(struct distribute_option *option,
                               struct distribute_item *items, uint64_t *count,
                               uint64_t groups, int rank, int ranks)
{
    int *counts;
    int allcount;
    int i;
    uint64_t j;
    int disp = 0;
    int mycount = (int)*count;
    struct distribute_item *recvbuf = NULL;
    int* disps = NULL;
    MPI_Datatype item_type;
    uint64_t number;

    MPI_Type_contiguous(2, MPI_UINT64_T, &item_type);
    MPI_Type_commit(&item_type);

    /* Determine total number of children across all ranks */
    MPI_Allreduce(&mycount, &allcount, 1, MPI_UINT64_T, MPI_SUM,
                  MPI_COMM_WORLD);
    if (rank == 0) {
        recvbuf = MFU_MALLOC((uint64_t)allcount * sizeof(*items));
        counts = MFU_MALLOC((uint64_t)ranks * sizeof(int));
        disps = MFU_MALLOC((uint64_t)ranks * sizeof(int));
    }

    /* Tell rank 0 where the data is coming from */
    MPI_Gather(&mycount, 1, MPI_INT, counts, 1, MPI_INT, 0,
               MPI_COMM_WORLD);

    /* Compute displacements and total bytes */
    if (rank == 0) {
        for (i = 0; i < ranks; i++) {
            disps[i] = disp;
            disp += counts[i];
        }
    }

    /* Gather data to rank 0 */
    MPI_Gatherv(items, (int)mycount, item_type, recvbuf, counts, disps, item_type,
                0, MPI_COMM_WORLD);

    /* Summarize */
    if (rank != 0) {
        MPI_Type_free(&item_type);
        return 0;
    }

    for (i = mycount; i < allcount; i++) {
        distribute_item_add(items, groups, count, recvbuf[i].value,
                            recvbuf[i].ranks);
    }

    assert(*count == groups);

    /* Print results */
    if (option->separator_number == 0) {
        printf("Size\tNumber\n");
        for (i = 0; i < (int)groups; i++) {
            printf("%"PRIu64"\t%"PRIu64"\n",
                   items[i].value, items[i].ranks);
        }
    } else {
        printf("Range\tNumber\n");
        j = 0;
        for (i = 0; i <= option->separator_number; i++) {
            printf("[");
            if (i == 0)
                printf("0");
            else
                printf("%"PRIu64, option->separators[i - 1] + 1);

            printf("~");

            if (i == option->separator_number) {
                if (groups > 0 && j < groups) {
                    assert(items[j].value == option->separators[i - 1] + 1);
                    number = items[j].ranks;
                    j++;
                } else {
                    number = 0;
                }
                printf("MAX]\t%"PRIu64"\n", number);
            } else {
                if (groups > 0 && j < groups &&
                    items[j].value == option->separators[i]) {
                    number = items[j].ranks;
                    j++;
                } else {
                    number = 0;
                }
                printf("%"PRIu64"]\t%"PRIu64"\n",
                       option->separators[i], number);
            }
        }
    }
    mfu_free(&disps);
    mfu_free(&counts);
    MPI_Type_free(&item_type);
    return 0;
}

static uint64_t distribute_get_value(struct distribute_option *option,
                                     mfu_flist flist, uint64_t idx)
{
    uint64_t file_size;
    uint64_t separator = 0;
    int i;

    file_size = mfu_flist_file_get_size(flist, idx);
    if (option->separator_number == 0)
        return file_size;

    for (i = 0; i < option->separator_number; i++) {
        separator = option->separators[i];
        if (file_size <= separator) {
            return separator;
        }
    }
    /* Return the biggest separator + 1 */
    return separator + 1;
}

/* 1 for any kind of distribution field */
#define DISTRIBUTE_KEY_SIZE 1
static int print_flist_distribution(struct distribute_option *option,
                                    mfu_flist* pflist,
                                    int rank, int ranks)
{
    mfu_flist flist = *pflist;
    MPI_Datatype key;
    MPI_Datatype keysat;
    uint64_t output_bytes;
    uint64_t* group_id;
    uint64_t* group_ranks;
    uint64_t* group_rank;
    uint64_t checking_files;
    size_t list_bytes;
    uint64_t* list;
    uint64_t* ptr;
    uint64_t i;
    uint64_t index;
    uint64_t groups;
    struct distribute_item *items = NULL;
    uint64_t item_count = 0;
    uint64_t value;

    MPI_Type_contiguous(DISTRIBUTE_KEY_SIZE, MPI_UINT64_T, &key);
    MPI_Type_commit(&key);

    /* KEY + 1 for index in the filst */
    MPI_Type_contiguous(DISTRIBUTE_KEY_SIZE + 1, MPI_UINT64_T, &keysat);
    MPI_Type_commit(&keysat);

    checking_files = mfu_flist_size(flist);
    output_bytes = checking_files * sizeof(uint64_t);
    group_id = (uint64_t *) MFU_MALLOC(output_bytes);
    group_ranks = (uint64_t *) MFU_MALLOC(output_bytes);
    group_rank = (uint64_t *) MFU_MALLOC(output_bytes);

    list_bytes = checking_files * (DISTRIBUTE_KEY_SIZE + 1) * sizeof(uint64_t);
    list = (uint64_t *) MFU_MALLOC(list_bytes);

    /* Initialize the list */
    ptr = list;
    for (i = 0; i < checking_files; i++) {
        ptr[0] = distribute_get_value(option, flist, i);
        ptr[DISTRIBUTE_KEY_SIZE] = i; /* Index in flist */
        ptr += DISTRIBUTE_KEY_SIZE + 1;
    }

    /* Assign group ids and compute group sizes */
    DTCMP_Rankv((int)checking_files, list, &groups, group_id,
                group_ranks, group_rank, key, keysat, DTCMP_OP_UINT64T_ASCEND,
                DTCMP_FLAG_NONE, MPI_COMM_WORLD);

    if (groups > 0) {
        items = (struct distribute_item *)MFU_MALLOC(sizeof(*items) *
                                                       groups);
    }

    group_rank = (uint64_t *) MFU_MALLOC(groups);
    for (i = 0; i < checking_files; i++) {
        /* Get index into flist for this item */
        index = *(list + i * (DISTRIBUTE_KEY_SIZE + 1) +
                  DISTRIBUTE_KEY_SIZE);
        value = distribute_get_value(option, flist, index);
        distribute_item_add(items, groups, &item_count, value,
                            group_ranks[i]);
    }

    distribution_gather(option, items, &item_count, groups, rank, ranks);

    if (items)
        mfu_free(&items);
    mfu_free(&list);
    mfu_free(&group_rank);
    mfu_free(&group_ranks);
    mfu_free(&group_id);
    MPI_Type_free(&keysat);
    MPI_Type_free(&key);
    return 0;
}

/*
 * Search the right position to insert the separator
 * If the separator exists already, return failure
 * Otherwise, locate the right position, and move the array forward to
 * save the separator.
 */
static int distribute_separator_add(struct distribute_option *option,
                                    uint64_t separator)
{
    int low = 0;
    int high;
    int middle;
    int pos;
    int count;

    count = option->separator_number;
    option->separator_number++;
    if (option->separator_number > MAX_DISTRIBUTE_SEPARATORS) {
        printf("Too many seperators");
        return -1;
    }
    
    if (count == 0) {
        option->separators[0] = separator;
        return 0;
    }

    high = count - 1;
    while (low < high)
    {
        middle = (high - low) / 2 + low;
        if (option->separators[middle] == separator)
            return -1;
        /* In the left half */
        else if (option->separators[middle] < separator)
            low = middle + 1;
        /* In the right half */
        else
            high = middle;
    }
    assert(low == high);
    if (option->separators[low] == separator)
        return -1;

    if (option->separators[low] < separator)
        pos = low + 1;
    else
        pos = low;

    if (pos < count)
        memmove(&option->separators[low + 1], &option->separators[low],
                sizeof(*option->separators) * (uint64_t)(count - pos));

    option->separators[pos] = separator;
    return 0;
}

static int distribution_parse(struct distribute_option *option,
                              const char *string)
{
    char *ptr;
    char *next;
    unsigned long long separator;
    char *str;
    int status = 0;

    if (strncmp(string, "size", strlen("size")) != 0)
        return -1;

    option->separator_number = 0;
    if (strlen(string) == strlen("size"))
        return 0;

    if (string[strlen("size")] != ':')
        return -1;

    str = MFU_STRDUP(string);
    /* Parse separators */
    ptr = str + strlen("size:");
    next = ptr;
    while (ptr && ptr < str + strlen(string)) {
        next = strchr(ptr, ',');
        if (next != NULL) {
            *next = '\0';
            next++;
        }

        if (mfu_abtoull(ptr, &separator) != MFU_SUCCESS) {
            printf("Invalid seperator \"%s\"\n", ptr);
            status = -1;
            goto out;
        }

        if (distribute_separator_add(option, separator)) {
            printf("Duplicated seperator \"%llu\"\n", separator);
            status = -1;
            goto out;
        }

        ptr = next;
    }

out:
    mfu_free(&str);
    return status;
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dwalk [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file>                      - read list from file\n");
    printf("  -o, --output <file>                     - write processed list to file\n");
    printf("  -l, --lite                              - walk file system without stat\n");
    printf("  -s, --sort <fields>                     - sort output by comma-delimited fields\n");
    printf("  -d, --distribution <field>:<separators> - print distribution by field\n");
    printf("  -p, --print                             - print files to screen\n");
    printf("  -v, --verbose                           - verbose output\n");
    printf("  -h, --help                              - print usage\n");
    printf("\n");
    printf("Fields: name,user,group,uid,gid,atime,mtime,ctime,size\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    int i;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* TODO: extend options
     *   - allow user to cache scan result in file
     *   - allow user to load cached scan as input
     *
     *   - allow user to filter by user, group, or filename using keyword or regex
     *   - allow user to specify time window
     *   - allow user to specify file sizes
     *
     *   - allow user to sort by different fields
     *   - allow user to group output (sum all bytes, group by user) */

    char* inputname  = NULL;
    char* outputname = NULL;
    char* sortfields = NULL;
    char* distribution = NULL;
    int walk = 0;
    int print = 0;
    struct distribute_option option;

    int option_index = 0;
    static struct option long_options[] = {
        {"distribution", 1, 0, 'd'},
        {"input",        1, 0, 'i'},
        {"output",       1, 0, 'o'},
        {"lite",         0, 0, 'l'},
        {"sort",         1, 0, 's'},
        {"print",        0, 0, 'p'},
        {"help",         0, 0, 'h'},
        {"verbose",      0, 0, 'v'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "d:i:o:ls:phv",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'd':
                distribution = MFU_STRDUP(optarg);
                break;
            case 'i':
                inputname = MFU_STRDUP(optarg);
                break;
            case 'o':
                outputname = MFU_STRDUP(optarg);
                break;
            case 'l':
                walk_stat = 0;
                break;
            case 's':
                sortfields = MFU_STRDUP(optarg);
                break;
            case 'p':
                print = 1;
                break;
            case 'h':
                usage = 1;
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if (rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* paths to walk come after the options */
    int numpaths = 0;
    mfu_param_path* paths = NULL;
    if (optind < argc) {
        /* got a path to walk */
        walk = 1;

        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        char** p = &argv[optind];
        mfu_param_path_set_all((uint64_t)numpaths, (const char**)p, paths);
        optind += numpaths;

        /* don't allow user to specify input file with walk */
        if (inputname != NULL) {
            usage = 1;
        }
    }
    else {
        /* if we're not walking, we must be reading,
         * and for that we need a file */
        if (inputname == NULL) {
            usage = 1;
        }
    }

    /* if user is trying to sort, verify the sort fields are valid */
    if (sortfields != NULL) {
        int maxfields;
        int nfields = 0;
        char* sortfields_copy = MFU_STRDUP(sortfields);
        if (walk_stat) {
            maxfields = 7;
            char* token = strtok(sortfields_copy, ",");
            while (token != NULL) {
                if (strcmp(token,  "name")  != 0 &&
                        strcmp(token, "-name")  != 0 &&
                        strcmp(token,  "user")  != 0 &&
                        strcmp(token, "-user")  != 0 &&
                        strcmp(token,  "group") != 0 &&
                        strcmp(token, "-group") != 0 &&
                        strcmp(token,  "uid")   != 0 &&
                        strcmp(token, "-uid")   != 0 &&
                        strcmp(token,  "gid")   != 0 &&
                        strcmp(token, "-gid")   != 0 &&
                        strcmp(token,  "atime") != 0 &&
                        strcmp(token, "-atime") != 0 &&
                        strcmp(token,  "mtime") != 0 &&
                        strcmp(token, "-mtime") != 0 &&
                        strcmp(token,  "ctime") != 0 &&
                        strcmp(token, "-ctime") != 0 &&
                        strcmp(token,  "size")  != 0 &&
                        strcmp(token, "-size")  != 0) {
                    /* invalid token */
                    if (rank == 0) {
                        printf("Invalid sort field: %s\n", token);
                    }
                    usage = 1;
                }
                nfields++;
                token = strtok(NULL, ",");
            }
        }
        else {
            maxfields = 1;
            char* token = strtok(sortfields_copy, ",");
            while (token != NULL) {
                if (strcmp(token,  "name")  != 0 &&
                        strcmp(token, "-name")  != 0) {
                    /* invalid token */
                    if (rank == 0) {
                        printf("Invalid sort field: %s\n", token);
                    }
                    usage = 1;
                }
                nfields++;
                token = strtok(NULL, ",");
            }
        }
        if (nfields > maxfields) {
            printf("Exceeded maximum number of sort fields: %d\n", maxfields);
            usage = 1;
        }
        mfu_free(&sortfields_copy);
    }

    if (distribution != NULL) {
        if (distribution_parse(&option, distribution) != 0) {
            if (rank == 0) {
                printf("Invalid distribution argument: %s\n", distribution);
                usage = 1;
            }
        } else if (rank == 0 && option.separator_number != 0) {
            printf("Seperators: ");
            for (i = 0; i < option.separator_number; i++) {
                if (i != 0)
                    printf(", ");
                printf("%"PRIu64, option.separators[i]);
            }
            printf("\n");
        }
    }

    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        MPI_Finalize();
        return 0;
    }

    /* TODO: check stat fields fit within MPI types */
    // if (sizeof(st_uid) > uint64_t) error(); etc...

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    if (walk) {
        /* walk list of input paths */
        mfu_param_path_walk(numpaths, paths, walk_stat, flist, dir_perm);
    }
    else {
        /* read data from cache file */
        mfu_flist_read_cache(inputname, flist);
    }

    /* TODO: filter files */
    //filter_files(&flist);

    /* sort files */
    if (sortfields != NULL) {
        /* TODO: don't sort unless all_count > 0 */
        mfu_flist_sort(sortfields, &flist);
    }

    /* print details for individual files */
    if (print) {
        mfu_flist_print(flist);
    }

    /* print summary about all files */
    print_summary(flist);

    if (distribution != NULL) {
        print_flist_distribution(&option, &flist, rank, ranks);
    }

    /* write data to cache file */
    if (outputname != NULL) {
        mfu_flist_write_cache(outputname, flist);
    }

    /* free users, groups, and files objects */
    mfu_flist_free(&flist);

    /* free memory allocated for options */
    mfu_free(&sortfields);
    mfu_free(&outputname);
    mfu_free(&inputname);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
