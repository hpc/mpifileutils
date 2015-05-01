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

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"

// getpwent getgrent to read user and group entries

/* TODO: change globals to struct */
static int verbose   = 0;
static int walk_stat = 1;

/* keep stats during walk */
uint64_t total_dirs    = 0;
uint64_t total_files   = 0;
uint64_t total_links   = 0;
uint64_t total_unknown = 0;
uint64_t total_bytes   = 0;

static void print_summary(bayer_flist flist)
{
    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* step through and print data */
    uint64_t idx = 0;
    uint64_t max = bayer_flist_size(flist);
    while (idx < max) {
        if (bayer_flist_have_detail(flist)) {
            /* get mode */
            mode_t mode = (mode_t) bayer_flist_file_get_mode(flist, idx);

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

            uint64_t size = bayer_flist_file_get_size(flist, idx);
            total_bytes += size;
        }
        else {
            /* get type */
            bayer_filetype type = bayer_flist_file_get_type(flist, idx);

            if (type == BAYER_TYPE_DIR) {
                total_dirs++;
            }
            else if (type == BAYER_TYPE_FILE) {
                total_files++;
            }
            else if (type == BAYER_TYPE_LINK) {
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
    uint64_t all_count = bayer_flist_global_size(flist);
    MPI_Allreduce(&total_dirs,    &all_dirs,    1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_files,   &all_files,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_links,   &all_links,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_unknown, &all_unknown, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_bytes,   &all_bytes,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* convert total size to units */
    if (verbose && rank == 0) {
        printf("Items: %llu\n", (unsigned long long) all_count);
        printf("  Directories: %llu\n", (unsigned long long) all_dirs);
        printf("  Files: %llu\n", (unsigned long long) all_files);
        printf("  Links: %llu\n", (unsigned long long) all_links);
        /* printf("  Unknown: %lu\n", (unsigned long long) all_unknown); */

        if (bayer_flist_have_detail(flist)) {
            double agg_size_tmp;
            const char* agg_size_units;
            bayer_format_bytes(all_bytes, &agg_size_tmp, &agg_size_units);

            uint64_t size_per_file = 0.0;
            if (all_files > 0) {
                size_per_file = (uint64_t)((double)all_bytes / (double)all_files);
            }
            double size_per_file_tmp;
            const char* size_per_file_units;
            bayer_format_bytes(size_per_file, &size_per_file_tmp, &size_per_file_units);

            printf("Data: %.3lf %s (%.3lf %s per file)\n", agg_size_tmp, agg_size_units, size_per_file_tmp, size_per_file_units);
        }
    }

    return;
}

static char type_str_unknown[] = "UNK";
static char type_str_dir[]     = "DIR";
static char type_str_file[]    = "REG";
static char type_str_link[]    = "LNK";

static void print_file(bayer_flist flist, uint64_t idx, int rank)
{
    /* get filename */
    const char* file = bayer_flist_file_get_name(flist, idx);

    if (bayer_flist_have_detail(flist)) {
        /* get mode */
        mode_t mode = (mode_t) bayer_flist_file_get_mode(flist, idx);

        uint32_t uid = (uint32_t) bayer_flist_file_get_uid(flist, idx);
        uint32_t gid = (uint32_t) bayer_flist_file_get_gid(flist, idx);
        uint64_t acc = bayer_flist_file_get_atime(flist, idx);
        uint64_t mod = bayer_flist_file_get_mtime(flist, idx);
        uint64_t cre = bayer_flist_file_get_ctime(flist, idx);
        uint64_t size = bayer_flist_file_get_size(flist, idx);
        const char* username  = bayer_flist_file_get_username(flist, idx);
        const char* groupname = bayer_flist_file_get_groupname(flist, idx);

        char access_s[30];
        char modify_s[30];
        char create_s[30];
        time_t access_t = (time_t) acc;
        time_t modify_t = (time_t) mod;
        time_t create_t = (time_t) cre;
        size_t access_rc = strftime(access_s, sizeof(access_s) - 1, "%FT%T", localtime(&access_t));
        size_t modify_rc = strftime(modify_s, sizeof(modify_s) - 1, "%FT%T", localtime(&modify_t));
        size_t create_rc = strftime(create_s, sizeof(create_s) - 1, "%FT%T", localtime(&create_t));
        if (access_rc == 0 || modify_rc == 0 || create_rc == 0) {
            /* error */
            access_s[0] = '\0';
            modify_s[0] = '\0';
            create_s[0] = '\0';
        }

        char mode_format[11];
        bayer_format_mode(mode, mode_format);

        printf("%s %s %s A%s M%s C%s %lu %s\n",
               mode_format, username, groupname,
               access_s, modify_s, create_s, (unsigned long)size, file
              );
#if 0
        printf("Mode=%lx(%s) UID=%d(%s) GUI=%d(%s) Access=%s Modify=%s Create=%s Size=%lu File=%s\n",
               (unsigned long)mode, mode_format, uid, username, gid, groupname,
               access_s, modify_s, create_s, (unsigned long)size, file
              );
#endif
    }
    else {
        /* get type */
        bayer_filetype type = bayer_flist_file_get_type(flist, idx);
        char* type_str = type_str_unknown;
        if (type == BAYER_TYPE_DIR) {
            type_str = type_str_dir;
        }
        else if (type == BAYER_TYPE_FILE) {
            type_str = type_str_file;
        }
        else if (type == BAYER_TYPE_LINK) {
            type_str = type_str_link;
        }

        printf("Type=%s File=%s\n",
               type_str, file
              );
    }
}

static void print_files(bayer_flist flist)
{
    /* number of items to print from start and end of list */
    uint64_t range = 10;

    /* allocate send and receive buffers */
    size_t pack_size = bayer_flist_file_pack_size(flist);
    size_t bufsize = 2 * range * pack_size;
    void* sendbuf = BAYER_MALLOC(bufsize);
    void* recvbuf = BAYER_MALLOC(bufsize);

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* identify the number of items we have, the total number,
     * and our offset in the global list */
    uint64_t count  = bayer_flist_size(flist);
    uint64_t total  = bayer_flist_global_size(flist);
    uint64_t offset = bayer_flist_global_offset(flist);

    /* count the number of items we'll send */
    int num = 0;
    uint64_t idx = 0;
    while (idx < count) {
        uint64_t global = offset + idx;
        if (global < range || (total - global) <= range) {
            num++;
        }
        idx++;
    }

    /* allocate arrays to store counts and displacements */
    int* counts = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));
    int* disps  = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));

    /* tell rank 0 where the data is coming from */
    int bytes = num * (int)pack_size;
    MPI_Gather(&bytes, 1, MPI_INT, counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* pack items into sendbuf */
    idx = 0;
    char* ptr = (char*) sendbuf;
    while (idx < count) {
        uint64_t global = offset + idx;
        if (global < range || (total - global) <= range) {
            ptr += bayer_flist_file_pack(ptr, flist, idx);
        }
        idx++;
    }

    /* compute displacements and total bytes */
    int recvbytes = 0;
    if (rank == 0) {
        int i;
        disps[0] = 0;
        recvbytes += counts[0];
        for (i = 1; i < ranks; i++) {
            disps[i] = disps[i - 1] + counts[i - 1];
            recvbytes += counts[i];
        }
    }

    /* gather data to rank 0 */
    MPI_Gatherv(sendbuf, bytes, MPI_BYTE, recvbuf, counts, disps, MPI_BYTE, 0, MPI_COMM_WORLD);

    /* create temporary list to unpack items into */
    bayer_flist tmplist = bayer_flist_subset(flist);

    /* unpack items into new list */
    if (rank == 0) {
        ptr = (char*) recvbuf;
        char* end = ptr + recvbytes;
        while (ptr < end) {
            bayer_flist_file_unpack(ptr, tmplist);
            ptr += pack_size;
        }
    }

    /* summarize list */
    bayer_flist_summarize(tmplist);

    /* print files */
    if (rank == 0) {
        printf("\n");
        uint64_t tmpidx = 0;
        uint64_t tmpsize = bayer_flist_size(tmplist);
        while (tmpidx < tmpsize) {
            print_file(tmplist, tmpidx, rank);
            tmpidx++;
            if (tmpidx == range) {
                printf("\n<snip>\n\n");
            }
        }
        printf("\n");
    }

    /* free our temporary list */
    bayer_flist_free(&tmplist);

    /* free memory */
    bayer_free(&disps);
    bayer_free(&counts);
    bayer_free(&sendbuf);
    bayer_free(&recvbuf);

    return;
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dwalk [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file>  - read list from file\n");
    printf("  -o, --output <file> - write processed list to file\n");
    printf("  -l, --lite          - walk file system without stat\n");
    printf("  -s, --sort <fields> - sort output by comma-delimited fields\n");
    printf("  -p, --print         - print files to screen\n");
    printf("  -v, --verbose       - verbose output\n");
    printf("  -h, --help          - print usage\n");
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
    bayer_init();

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
    int walk = 0;
    int print = 0;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",    1, 0, 'i'},
        {"output",   1, 0, 'o'},
        {"lite",     0, 0, 'l'},
        {"sort",     1, 0, 's'},
        {"print",    0, 0, 'p'},
        {"help",     0, 0, 'h'},
        {"verbose",  0, 0, 'v'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:o:ls:phv",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = BAYER_STRDUP(optarg);
                break;
            case 'o':
                outputname = BAYER_STRDUP(optarg);
                break;
            case 'l':
                walk_stat = 0;
                break;
            case 's':
                sortfields = BAYER_STRDUP(optarg);
                break;
            case 'p':
                print = 1;
                break;
            case 'h':
                usage = 1;
                break;
            case 'v':
                verbose = 1;
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
    bayer_param_path* paths = NULL;
    if (optind < argc) {
        /* got a path to walk */
        walk = 1;

        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (bayer_param_path*) BAYER_MALLOC((size_t)numpaths * sizeof(bayer_param_path));

        /* process each path */
        char** p = &argv[optind];
        bayer_param_path_set_all((uint64_t)numpaths, (const char**)p, paths);
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
        char* sortfields_copy = BAYER_STRDUP(sortfields);
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
        bayer_free(&sortfields_copy);
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
    bayer_flist flist = bayer_flist_new();

    if (walk) {
        /* walk list of input paths */
        bayer_param_path_walk(numpaths, paths, walk_stat, flist);
    }
    else {
        /* read data from cache file */
        double start_read = MPI_Wtime();
        bayer_flist_read_cache(inputname, flist);
        double end_read = MPI_Wtime();

        /* get total file count */

        /* report read count, time, and rate */
        if (verbose && rank == 0) {
            uint64_t all_count = bayer_flist_global_size(flist);
            double secs = end_read - start_read;
            double rate = 0.0;
            if (secs > 0.0) {
                rate = ((double)all_count) / secs;
            }
            printf("Read %lu files in %f seconds (%f files/sec)\n",
                   all_count, secs, rate
                  );
        }
    }

    /* TODO: filter files */
    //filter_files(&flist);

    /* sort files */
    if (sortfields != NULL) {
        /* TODO: don't sort unless all_count > 0 */

        double start_sort = MPI_Wtime();
        bayer_flist_sort(sortfields, &flist);
        double end_sort = MPI_Wtime();

        /* report sort count, time, and rate */
        if (verbose && rank == 0) {
            uint64_t all_count = bayer_flist_global_size(flist);
            double secs = end_sort - start_sort;
            double rate = 0.0;
            if (secs > 0.0) {
                rate = ((double)all_count) / secs;
            }
            printf("Sorted %lu files in %f seconds (%f files/sec)\n",
                   all_count, secs, rate
                  );
        }
    }

    /* print files */
    if (print) {
        print_files(flist);
    }

    print_summary(flist);

    /* write data to cache file */
    if (outputname != NULL) {
        /* report the filename we're writing to */
        if (verbose && rank == 0) {
            printf("Writing to output file: %s\n", outputname);
            fflush(stdout);
        }

        double start_write = MPI_Wtime();
        bayer_flist_write_cache(outputname, flist);
        double end_write = MPI_Wtime();

        /* report write count, time, and rate */
        if (verbose && rank == 0) {
            uint64_t all_count = bayer_flist_global_size(flist);
            double secs = end_write - start_write;
            double rate = 0.0;
            if (secs > 0.0) {
                rate = ((double)all_count) / secs;
            }
            printf("Wrote %lu files in %f seconds (%f files/sec)\n",
                   all_count, secs, rate
                  );
        }
    }

    /* free users, groups, and files objects */
    bayer_flist_free(&flist);

    /* free memory allocated for options */
    bayer_free(&sortfields);
    bayer_free(&outputname);
    bayer_free(&inputname);

    /* free the path parameters */
    bayer_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    bayer_free(&paths);

    /* shut down MPI */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
