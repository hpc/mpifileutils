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

/* keep stats during walk */
uint64_t total_dirs    = 0;
uint64_t total_files   = 0;
uint64_t total_links   = 0;
uint64_t total_unknown = 0;
uint64_t total_bytes   = 0;

#define MAX_DISTRIBUTE_SEPARATORS 128
struct distribute_option {
    int separator_number;
    uint64_t separators[MAX_DISTRIBUTE_SEPARATORS];
};

static int print_flist_distribution(struct distribute_option *option, mfu_flist* pflist, int rank)
{
    /* file list to use */
    mfu_flist flist = *pflist;

    /* get local size for each rank */
    uint64_t size = mfu_flist_size(flist);

    /* allocate a count for each bin, initialize the bin counts to 0
     * it is separator + 1 because the last bin is the last separator
     * to the DISTRIBUTE_MAX */
    int separators = option->separator_number;
    uint64_t* dist = (uint64_t*) MFU_MALLOC((separators + 1) * sizeof(uint64_t));

    /* initialize the bin counts to 0 */
    for (int i = 0; i <= separators; i++) {
        dist[i] = 0;
    }

    /* for each file, identify appropriate bin and increment its count */
    for (int i = 0; i < size; i++) {
         /* get the size of the file */
         uint64_t file_size = mfu_flist_file_get_size(flist, i);

         /* loop through the bins and find the one the file belongs to,
          * set last bin to -1, if a bin is not found while looping through the
          * list of file size separators, then it belongs in the last bin
          * so (last file size - MAX bin) */
         int max_bin_flag = -1;
         for (int j = 0; j < separators; j++) {
             if (file_size <= option->separators[j]) {
                 /* found the bin set bin index & increment its count */
                 dist[j]++;

                 /* a file for this bin was found so can't belong to
                  * last bin (so set the flag) & exit the loop */
                 max_bin_flag = 1;
                 break;
             }
         }

         /* if max_bin_flag is still -1 then the file belongs to the last bin */
         if (max_bin_flag < 0) {
             dist[separators]++;
         }
    }

    /* get the total sum across all of the bins */
    uint64_t* disttotal = (uint64_t*) MFU_MALLOC((separators + 1) * sizeof(uint64_t));
    MPI_Allreduce(dist, disttotal, (uint64_t)separators + 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* Print the file distribution */
    if (rank == 0) {
         /* number of files in a bin */
         uint64_t number;
         printf("Range\tNumber\n");
         for (int i = 0; i <= option->separator_number; i++) {
             printf("[");
             if (i == 0) {
                 printf("0");
             } else {
                 printf("%"PRIu64, option->separators[i - 1] + 1);
             }

             printf("~");

             if (i == option->separator_number) {
                 number = disttotal[i];
                 printf("MAX]\t%"PRIu64"\n", number);
             } else {
                 number = disttotal[i];
                 printf("%"PRIu64"]\t%"PRIu64"\n",
                 option->separators[i], number);
             }
         }
    }

    /* free the memory used to hold bin counts */
    mfu_free(&disttotal);
    mfu_free(&dist);

    return 0;
}

/*
 * Search the right position to insert the separator
 * If the separator exists already, return failure
 * Otherwise, locate the right position, and move the array forward to
 * save the separator.
 */
static int distribute_separator_add(struct distribute_option *option, uint64_t separator)
{
    int low = 0;
    int high;
    int middle;
    int pos;
    int count;

    count = option->separator_number;
    option->separator_number++;
    if (option->separator_number > MAX_DISTRIBUTE_SEPARATORS) {
        printf("Too many separators");
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

static int distribution_parse(struct distribute_option *option, const char *string)
{
    char *ptr;
    char *next;
    unsigned long long separator;
    char *str;
    int status = 0;

    if (strncmp(string, "size", strlen("size")) != 0) {
        return -1;
    }

    option->separator_number = 0;
    if (strlen(string) == strlen("size")) {
        return 0;
    }

    if (string[strlen("size")] != ':') {
        return -1;
    }

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
            printf("Invalid separator \"%s\"\n", ptr);
            status = -1;
            goto out;
        }

        if (distribute_separator_add(option, separator)) {
            printf("Duplicated separator \"%llu\"\n", separator);
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
    printf("  -i, --input <file>    - read list from file\n");
    printf("  -o, --output <file>   - write processed list to file in binary format\n");
    printf("  -t, --text            - use with -o; write processed list to file in ascii format\n");
    printf("  -l, --lite            - walk file system without stat\n");
    printf("  -s, --sort <fields>   - sort output by comma-delimited fields\n");
    printf("  -d, --distribution <field>:<separators>\n                        - print distribution by field\n");
    printf("  -p, --print           - print files to screen\n");
    printf("  -v, --verbose         - verbose output\n");
    printf("  -q, --quiet           - quiet output\n");
    printf("  -h, --help            - print usage\n");
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

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

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
    int walk                 = 0;
    int print                = 0;
    int text                 = 0;
    struct distribute_option option;

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",        1, 0, 'i'},
        {"output",       1, 0, 'o'},
        {"lite",         0, 0, 'l'},
        {"sort",         1, 0, 's'},
        {"distribution", 1, 0, 'd'},
        {"print",        0, 0, 'p'},
        {"verbose",      0, 0, 'v'},
        {"quiet",        0, 0, 'q'},
        {"help",         0, 0, 'h'},
        {"text",         0, 0, 't'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:o:ls:d:pvqht",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = MFU_STRDUP(optarg);
                break;
            case 'o':
                outputname = MFU_STRDUP(optarg);
                break;
            case 'l':
                /* don't stat each file on the walk */
                walk_opts->use_stat = 0;
                break;
            case 's':
                sortfields = MFU_STRDUP(optarg);
                break;
            case 'd':
                distribution = MFU_STRDUP(optarg);
                break;
            case 'p':
                print = 1;
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = 0;
                break;
            case 't':
                text = 1;
                break;
            case 'h':
                usage = 1;
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
        if (walk_opts->use_stat) {
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
            if (rank == 0) {
                printf("Exceeded maximum number of sort fields: %d\n", maxfields);
            }
            usage = 1;
        }
        mfu_free(&sortfields_copy);
    }

    if (distribution != NULL) {
        if (distribution_parse(&option, distribution) != 0) {
            if (rank == 0) {
                printf("Invalid distribution argument: %s\n", distribution);
            }
            usage = 1;
        } else if (rank == 0 && option.separator_number != 0) {
            printf("Separators: ");
            for (i = 0; i < option.separator_number; i++) {
                if (i != 0) {
                    printf(", ");
                }
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

    /* create an empty file list with default values */
    mfu_flist flist = mfu_flist_new();

    if (walk) {
        /* walk list of input paths */
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist);
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

    /* print summary statistics of flist */
    mfu_flist_print_summary(flist);

    /* print distribution if user specified this option */
    if (distribution != NULL) {
        print_flist_distribution(&option, &flist, rank);
    }

    /* write data to cache file */
    if (outputname != NULL) {
        if (!text) {
            mfu_flist_write_cache(outputname, flist);
        } else {
            mfu_flist_write_text(outputname, flist);
        }
    }

    /* free users, groups, and files objects */
    mfu_flist_free(&flist);

    /* free memory allocated for options */
    mfu_free(&distribution);
    mfu_free(&sortfields);
    mfu_free(&outputname);
    mfu_free(&inputname);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
