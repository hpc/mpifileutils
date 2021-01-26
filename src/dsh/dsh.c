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

#include <string.h>
#include <regex.h>

#if 0
/* use readline for prompt processing */
#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>
#endif

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"
#include "strmap.h"

// getpwent getgrent to read user and group entries

/* TODO: change globals to struct */
static int verbose   = 0;

/* keep stats during walk */
uint64_t total_dirs    = 0;
uint64_t total_files   = 0;
uint64_t total_links   = 0;
uint64_t total_unknown = 0;
uint64_t total_bytes   = 0;

typedef enum {
    NULLFIELD = 0,
    FILENAME,
    USERNAME,
    GROUPNAME,
    USERID,
    GROUPID,
    ATIME,
    MTIME,
    CTIME,
    FILESIZE,
} sort_field;

/* routine for sorting strings in ascending order */
static int my_strcmp(const void* a, const void* b)
{
    return strcmp((const char*)a, (const char*)b);
}

static int my_strcmp_rev(const void* a, const void* b)
{
    return strcmp((const char*)b, (const char*)a);
}

static int sort_files_readdir(const char* sortfields, mfu_flist* pflist)
{
    /* get list from caller */
    mfu_flist flist = *pflist;

    /* create a new list as subset of original list */
    mfu_flist flist2 = mfu_flist_subset(flist);

    uint64_t incount = mfu_flist_size(flist);
    uint64_t chars   = mfu_flist_file_max_name(flist);

    /* create datatype for packed file list element */
    MPI_Datatype dt_sat;
    size_t bytes = mfu_flist_file_pack_size(flist);
    MPI_Type_contiguous((int)bytes, MPI_BYTE, &dt_sat);

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* build type for file path */
    MPI_Datatype dt_filepath;
    MPI_Type_contiguous((int)chars, MPI_CHAR, &dt_filepath);
    MPI_Type_commit(&dt_filepath);

    /* build comparison op for filenames */
    DTCMP_Op op_filepath;
    if (DTCMP_Op_create(dt_filepath, my_strcmp, &op_filepath) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create sorting operation for filepath");
    }

    /* build comparison op for filenames */
    DTCMP_Op op_filepath_rev;
    if (DTCMP_Op_create(dt_filepath, my_strcmp_rev, &op_filepath_rev) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create reverse sorting operation for filepath");
    }

    /* TODO: process sort fields */
    const int MAXFIELDS = 1;
    MPI_Datatype types[MAXFIELDS];
    DTCMP_Op ops[MAXFIELDS];
    sort_field fields[MAXFIELDS];
    size_t lengths[MAXFIELDS];
    int nfields = 0;
    for (nfields = 0; nfields < MAXFIELDS; nfields++) {
        types[nfields]   = MPI_DATATYPE_NULL;
        ops[nfields]     = DTCMP_OP_NULL;
    }
    nfields = 0;
    char* sortfields_copy = MFU_STRDUP(sortfields);
    char* token = strtok(sortfields_copy, ",");
    while (token != NULL) {
        int valid = 1;
        if (strcmp(token, "name") == 0) {
            types[nfields]   = dt_filepath;
            ops[nfields]     = op_filepath;
            fields[nfields]  = FILENAME;
            lengths[nfields] = chars;
        }
        else if (strcmp(token, "-name") == 0) {
            types[nfields]   = dt_filepath;
            ops[nfields]     = op_filepath_rev;
            fields[nfields]  = FILENAME;
            lengths[nfields] = chars;
        }
        else {
            /* invalid token */
            valid = 0;
            if (rank == 0) {
                printf("Invalid sort field: %s\n", token);
            }
        }
        if (valid) {
            nfields++;
        }
        if (nfields > MAXFIELDS) {
            /* TODO: print warning if we have too many fields */
            break;
        }
        token = strtok(NULL, ",");
    }
    mfu_free(&sortfields_copy);

    /* build key type */
    MPI_Datatype dt_key;
    if (DTCMP_Type_create_series(nfields, types, &dt_key) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create type for key");
    }

    /* create sort op */
    DTCMP_Op op_key;
    if (DTCMP_Op_create_series(nfields, ops, &op_key) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create sorting operation for key");
    }

    /* build keysat type */
    MPI_Datatype dt_keysat, keysat_types[2];
    keysat_types[0] = dt_key;
    keysat_types[1] = dt_sat;
    if (DTCMP_Type_create_series(2, keysat_types, &dt_keysat) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create type for keysat");
    }

    /* get extent of key type */
    MPI_Aint key_lb, key_extent;
    MPI_Type_get_extent(dt_key, &key_lb, &key_extent);

    /* get extent of keysat type */
    MPI_Aint keysat_lb, keysat_extent;
    MPI_Type_get_extent(dt_keysat, &keysat_lb, &keysat_extent);

    /* get extent of sat type */
    MPI_Aint sat_lb, sat_extent;
    MPI_Type_get_extent(dt_sat, &sat_lb, &sat_extent);

    /* compute size of sort element and allocate buffer */
    size_t sortbufsize = (size_t)keysat_extent * incount;
    void* sortbuf = MFU_MALLOC(sortbufsize);

    /* copy data into sort elements */
    uint64_t idx = 0;
    char* sortptr = (char*) sortbuf;
    while (idx < incount) {
        /* copy in access time */
        int i;
        for (i = 0; i < nfields; i++) {
            if (fields[i] == FILENAME) {
                const char* name = mfu_flist_file_get_name(flist, idx);
                strcpy(sortptr, name);
            }
            sortptr += lengths[i];
        }

        /* pack file element */
        sortptr += mfu_flist_file_pack(sortptr, flist, idx);

        idx++;
    }

    /* sort data */
    void* outsortbuf;
    int outsortcount;
    DTCMP_Handle handle;
    int sort_rc = DTCMP_Sortz(
                      sortbuf, (int)incount, &outsortbuf, &outsortcount,
                      dt_key, dt_keysat, op_key, DTCMP_FLAG_NONE,
                      MPI_COMM_WORLD, &handle
                  );
    if (sort_rc != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to sort data");
    }

    /* step through sorted data filenames */
    idx = 0;
    sortptr = (char*) outsortbuf;
    while (idx < (uint64_t)outsortcount) {
        sortptr += key_extent;
        sortptr += mfu_flist_file_unpack(sortptr, flist2);
        idx++;
    }

    /* build summary of new list */
    mfu_flist_summarize(flist2);

    /* free memory */
    DTCMP_Free(&handle);

    /* free ops */
    DTCMP_Op_free(&op_key);
    DTCMP_Op_free(&op_filepath_rev);
    DTCMP_Op_free(&op_filepath);

    /* free types */
    MPI_Type_free(&dt_keysat);
    MPI_Type_free(&dt_key);
    MPI_Type_free(&dt_filepath);

    /* free input buffer holding sort elements */
    mfu_free(&sortbuf);

    /* free the satellite type */
    MPI_Type_free(&dt_sat);

    /* return new list and free old one */
    *pflist = flist2;
    mfu_flist_free(&flist);

    return 0;
}

static uint32_t gettime(void)
{
    uint32_t t = 0;
    time_t now = time(NULL);
    if (now != (time_t)-1) {
        t = (uint32_t) now;
    }
    return t;
}

static void filter_files(mfu_flist* pflist)
{
    mfu_flist flist = *pflist;

    // for each file, if (now - atime) > 60d and (now - ctime) > 60d, add to list
    mfu_flist eligible = mfu_flist_subset(flist);

    static uint32_t limit = 60 * 24 * 3600; /* 60 days */
    uint32_t now = gettime();
    uint64_t idx = 0;
    uint64_t files = mfu_flist_size(flist);
    while (idx < files) {
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_FILE || type == MFU_TYPE_LINK) {
            /* we only purge files and links */
            uint32_t acc = (uint32_t) mfu_flist_file_get_atime(flist, idx);
            uint32_t cre = (uint32_t) mfu_flist_file_get_ctime(flist, idx);
            if ((now - acc) > limit && (now - cre) > limit) {
                /* only purge items that have not been
                 * accessed or changed in past limit seconds */
                mfu_flist_file_copy(flist, idx, eligible);
            }
        }
        idx++;
    }

    mfu_flist_summarize(eligible);

    mfu_flist_free(&flist);
    *pflist = eligible;
    return;
}

/* pick out all files that are contained within a given directory,
 * we include the directory itself if the inclusive flag is set */
static void filter_files_path(mfu_flist flist, mfu_path* path, int inclusive, mfu_flist* out_eligible, mfu_flist* out_leftover)
{
    /* the files that satisfy the filter are copied to eligible,
     * while others are copied to leftover */
    mfu_flist eligible = mfu_flist_subset(flist);
    mfu_flist leftover = mfu_flist_subset(flist);

    /* get the parent directory in string form to compare during
     * inclusive checks */
    const char* path_str = mfu_path_strdup(path);

    uint64_t idx = 0;
    uint64_t files = mfu_flist_size(flist);
    while (idx < files) {
        const char* filename = mfu_flist_file_get_name(flist, idx);
        mfu_path* fpath = mfu_path_from_str(filename);

        if (mfu_path_cmp(path, fpath) == MFU_PATH_DEST_CHILD) {
            mfu_flist_file_copy(flist, idx, eligible);
        } else if (inclusive && strcmp(path_str, filename) == 0) {
            /* also include path itself if inclusive flag is set */
            mfu_flist_file_copy(flist, idx, eligible);
        } else {
            /* this file does not match the filter */
            mfu_flist_file_copy(flist, idx, leftover);
        }

        mfu_path_delete(&fpath);
        idx++;
    }
    mfu_free(&path_str);

    mfu_flist_summarize(eligible);
    mfu_flist_summarize(leftover);

    *out_eligible = eligible;
    *out_leftover = leftover;

    return;
}

/* pick out all files that are contained within a given directory,
 * and whose first component after the given directory matches
 * the provided regex */
static void filter_files_regex(mfu_flist flist, mfu_path* path, const char* regex, mfu_flist* out_eligible, mfu_flist* out_leftover)
{
    /* compile the regex */
    regex_t re;
    int rc = regcomp(&re, regex, REG_NOSUB);
    if (rc != 0) {
        printf("Error compiling regex for %s\n", regex);
    }

    /* the files that satisfy the filter are copied to eligible,
     * while others are copied to leftover */
    mfu_flist eligible = mfu_flist_subset(flist);
    mfu_flist leftover = mfu_flist_subset(flist);

    /* get number of components in current path */
    int components = mfu_path_components(path);

    uint64_t idx = 0;
    uint64_t files = mfu_flist_size(flist);
    while (idx < files) {
        const char* filename = mfu_flist_file_get_name(flist, idx);
        mfu_path* fpath = mfu_path_from_str(filename);

        if (mfu_path_cmp(path, fpath) == MFU_PATH_DEST_CHILD) {
            /* first check that we have an item in the current path */
            /* get component of path immediately following path */
            mfu_path_slice(fpath, components, 1);
            const char* item = mfu_path_strdup(fpath);
            rc = regexec(&re, item, 0, NULL, 0);
            if (rc == 0) {
                /* got a match */
                mfu_flist_file_copy(flist, idx, eligible);
            } else {
                /* this file does not match the filter */
                mfu_flist_file_copy(flist, idx, leftover);
            }
            mfu_free(&item);
        } else {
            /* this file does not match the filter */
            mfu_flist_file_copy(flist, idx, leftover);
        }

        mfu_path_delete(&fpath);
        idx++;
    }

    mfu_flist_summarize(eligible);
    mfu_flist_summarize(leftover);

    *out_eligible = eligible;
    *out_leftover = leftover;

    /* free the regular expression */
    regfree(&re);

    return;
}

static void sum_child(mfu_flist flist, uint64_t idx, uint64_t* vals)
{
    /* increase our item count by one */
    vals[0] += 1;

    /* if item is a file, add its size */
    mfu_filetype type = mfu_flist_file_get_type(flist, idx);
//    if (type == MFU_TYPE_FILE) {
        uint64_t size = mfu_flist_file_get_size(flist, idx);
        vals[1] += size;
//    }

    return;
}

static uint64_t* decode_addr(const char* str)
{
    /* decode address of data structure from string */
    void* buf;
    sscanf(str, "%p", &buf);
    uint64_t* vals = (uint64_t*) buf;
    return vals;
}

/* gather data from procs to rank 0 */
static void print_sums(mfu_path* origpath, uint64_t count, uint64_t allmax, uint64_t maxcount, 
        uint64_t sum_bytes, uint64_t sum_count, MPI_Datatype dt, void* buf, int print_default)
{
    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* determine total number of children across all ranks */
    uint64_t allcount;
    MPI_Allreduce(&count, &allcount, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* compute max inode count across all procs */
    uint64_t allmaxcount;
    MPI_Allreduce(&maxcount, &allmaxcount, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* sum up bytes across all children */
    uint64_t allsum_bytes;
    MPI_Allreduce(&sum_bytes, &allsum_bytes, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* sum up inode count across all children */
    uint64_t allsum_count;
    MPI_Allreduce(&sum_count, &allsum_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* compute size of single element */
    size_t size = 2 * sizeof(uint64_t) + allmax;
    size_t bufsize = allcount * size;
    void* recvbuf = NULL;
    int* counts = NULL;
    int* disps = NULL;
    if (rank == 0) {
        recvbuf = MFU_MALLOC(bufsize);
        counts = (int*) MFU_MALLOC(ranks * sizeof(int));
        disps = (int*) MFU_MALLOC(ranks * sizeof(int));
    }

    int mycount = (int) count;
    MPI_Gather(&mycount, 1, MPI_INT, counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        int disp = 0;
        int r;
        for (r = 0; r < ranks; r++) {
            disps[r] = disp;
            disp += counts[r];
        }
    }

    MPI_Gatherv(buf, mycount, dt, recvbuf, counts, disps, dt, 0, MPI_COMM_WORLD);

    /* determine number of digits to display max inode count */
    uint64_t maxinodes = allsum_count;
    uint64_t span = 10;
    int digits = 1;
    while (maxinodes >= span) {
        span *= 10;
        digits++;
    }

    /* make sure inodes field is large enough
     * for column header "Items" */
    if (digits < 5) {
        digits = 5;
    }
    
    /* get the minimum value of print default and allcount to print 
     * So, if print default is less than allcount and greater than or 
     * equal zero print that, but if not, then print what is in the list */
    uint64_t print_count = allcount;
    if (print_default < print_count && print_default >= 0) {
        print_count = print_default;
    }

    /* print sorted data */
    if (rank == 0) {
        double agg_size_tmp;
        const char* agg_size_units;
        mfu_format_bytes(allsum_bytes, &agg_size_tmp, &agg_size_units);
    
        double allsum_tmp;
        const char* allsum_units;
        mfu_format_count(allsum_count, &allsum_tmp, &allsum_units);

        /* print header info */
        char* origpath_str = mfu_path_strdup(origpath);
        printf("--------------------------\n");
        printf("     Bytes    Items Path\n");
        printf("%6.2f %3s %6.2f %1s %s\n", agg_size_tmp, agg_size_units, allsum_tmp, allsum_units, origpath_str);
        printf("--------------------------\n");
        mfu_free(&origpath_str);

        uint64_t i;
        char* ptr = (char*) recvbuf;
        for (i = 0; i < print_count; i++) {
            uint64_t bytes = * (uint64_t*) ptr;
            ptr += sizeof(uint64_t);
    
            char* name = ptr;
            ptr += allmax;
    
            uint64_t count = * (uint64_t*) ptr;
            ptr += sizeof(uint64_t);
    
            double agg_size_tmp;
            const char* agg_size_units;
            mfu_format_bytes(bytes, &agg_size_tmp, &agg_size_units);
    
            double count_tmp;
            const char* count_units;
            mfu_format_count(count, &count_tmp, &count_units);
    
            printf("%6.2f %3s %6.2f %1s %s\n", agg_size_tmp, agg_size_units, count_tmp, count_units, name);
        }
        printf("\n(printed top %llu of a total of %llu lines)\n", (unsigned long long)print_count, 
                (unsigned long long)allcount);
        fflush(stdout);
    }

    mfu_free(&disps);
    mfu_free(&counts);
    mfu_free(&recvbuf);

    return;
}

/* here we sort all items by child name, execute scans to sum data
 * for a given name, and then finally resort and print values based
 * on those sums */
static void sort_scan_sort(mfu_path* origpath, uint64_t allmax, 
        uint64_t numchildren, strmap* children, int print_default)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* define datatype and comparison operation for sort and segmented scan */
    MPI_Datatype key;
    DTCMP_Op cmp;
    DTCMP_Str_create_ascend((int) allmax, &key, &cmp);

    /* define type for sort */
    MPI_Datatype keysat;
    MPI_Datatype types[5];
    types[0] = key;
    types[1] = MPI_UINT64_T;
    types[2] = MPI_UINT64_T;
    types[3] = MPI_UINT64_T;
    types[4] = MPI_UINT64_T;
    DTCMP_Type_create_series(5, types, &keysat);

    /* allocate memory for sort */
    size_t itemsize = allmax + 4 * sizeof(uint64_t);
    size_t bufsize = numchildren * itemsize;
    void* buf = MFU_MALLOC(bufsize);

    /* copy items into buffer */
    uint64_t idx = 0;
    char* ptr = (char*) buf;
    const strmap_node* elem;
    for (elem  = strmap_node_first(children);
         elem != NULL;
         elem  = strmap_node_next(elem))
    {
        const char* key = strmap_node_key(elem);
        const char* val = strmap_node_value(elem);

        /* decode address of data structure from string */
        uint64_t* vals = decode_addr(val);

        /* copy childname into buffer */
        strcpy(ptr, key);
        ptr += allmax;

        /* get pointer to count fields */
        uint64_t* bufvals = (uint64_t*) ptr;
        ptr += 4 * sizeof(uint64_t);

        /* copy return address into payload */
        bufvals[0] = rank;
        bufvals[1] = idx;

        /* copy values into buffer */
        bufvals[2] = vals[0];
        bufvals[3] = vals[1];

        /* increase our index */
        idx++;
    }

    /* sort data by child name, attach item and byte counts as satellite data */
    void* sortedbuf = MFU_MALLOC(bufsize);
    DTCMP_Sortv(
        buf, sortedbuf, numchildren,
        key, keysat, cmp, DTCMP_FLAG_NONE, MPI_COMM_WORLD
    );
    mfu_free(&buf);
    buf = sortedbuf;
    sortedbuf = NULL;

    MPI_Type_free(&keysat);

    /* prepare arrays for segmented scan operations */
    size_t strbuf_size = numchildren * allmax;
    void* strbuf = MFU_MALLOC(strbuf_size);
    uint64_t* rankval  = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* rankltr  = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* rankrtl  = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* countval = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* countltr = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* countrtl = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* bytesval = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* bytesltr = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* bytesrtl = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    char* ptrdst = (char*) strbuf;
    char* ptrsrc = (char*) buf;
    uint64_t i;
    for (i = 0; i < numchildren; i++) {
        /* copy child names into array of strings for segmented scan */
        strcpy(ptrdst, ptrsrc);
        ptrdst += allmax;
        ptrsrc += allmax;

        /* initialize input data arrays for segmented scan */
        uint64_t* bufvals = (uint64_t*) ptrsrc;
        rankval[i]  = 1;
        countval[i] = bufvals[2];
        bytesval[i] = bufvals[3];
        ptrsrc += 4 * sizeof(uint64_t);

        /* initialize scan output values */
        rankltr[i] = 0;
        rankrtl[i] = 0;
        countltr[i] = 0;
        countrtl[i] = 0;
        bytesltr[i] = 0;
        bytesrtl[i] = 0;
    }

    /* execute scan */
    DTCMP_Segmented_exscanv(
        numchildren, strbuf, key, cmp,
        rankval,  rankltr,  rankrtl, MPI_UINT64_T, MPI_SUM,
        DTCMP_FLAG_NONE, MPI_COMM_WORLD
    );
    DTCMP_Segmented_exscanv(
        numchildren, strbuf, key, cmp,
        countval, countltr, countrtl, MPI_UINT64_T, MPI_SUM,
        DTCMP_FLAG_NONE, MPI_COMM_WORLD
    );
    DTCMP_Segmented_exscanv(
        numchildren, strbuf, key, cmp,
        bytesval, bytesltr, bytesrtl, MPI_UINT64_T, MPI_SUM,
        DTCMP_FLAG_NONE, MPI_COMM_WORLD
    );

    /* total counts and bytes for each item */
    uint64_t report_count = 0;
    uint64_t* counttot = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    uint64_t* bytestot = (uint64_t*) MFU_MALLOC(numchildren * sizeof(uint64_t));
    for (i = 0; i < numchildren; i++) {
        counttot[i] = countval[i] + countltr[i] + countrtl[i];
        bytestot[i] = bytesval[i] + bytesltr[i] + bytesrtl[i];

        if (rankltr[i] == 0) {
            report_count++;
        }
    }

    /* create types and comparison operation for sorting final data */
    MPI_Datatype report_key, report_keysat;
    MPI_Datatype report_types[2];
    report_types[0] = MPI_UINT64_T;
    report_types[1] = key;
    DTCMP_Type_create_series(2, report_types, &report_key);

    report_types[0] = report_key;
    report_types[1] = MPI_UINT64_T;
    DTCMP_Type_create_series(2, report_types, &report_keysat);

    DTCMP_Op report_cmp;
    DTCMP_Op_create_series2(DTCMP_OP_UINT64T_DESCEND, cmp, &report_cmp);

    /* to report data, sort by size, then name, include item count */
    size_t report_size = 2 * sizeof(uint64_t) + allmax;
    size_t reportbuf_size = report_count * report_size;
    void* reportbuf = MFU_MALLOC(reportbuf_size);

    /* prepare buffer for sorting, and remember max count */
    uint64_t sum_bytes = 0;
    uint64_t sum_count = 0;
    uint64_t maxcount = 0;
    ptr = (char*) reportbuf;
    for (i = 0; i < numchildren; i++) {
        if (rankltr[i] == 0) {
            memcpy(ptr, &bytestot[i], sizeof(uint64_t));
            ptr += sizeof(uint64_t);

            char* name = strbuf + i * allmax;
            strcpy(ptr, name);
            ptr += allmax;

            memcpy(ptr, &counttot[i], sizeof(uint64_t));
            ptr += sizeof(uint64_t);

            sum_bytes += bytestot[i];
            sum_count += counttot[i];
            if (counttot[i] > maxcount) {
                maxcount = counttot[i];
            }
        }
    }

    /* sort data */
    void* sorted_reportbuf = MFU_MALLOC(reportbuf_size);
    DTCMP_Sortv(
        reportbuf, sorted_reportbuf, report_count,
        report_key, report_keysat, report_cmp, DTCMP_FLAG_NONE, MPI_COMM_WORLD
    );

    /* print sorted data */
    print_sums(origpath, report_count, allmax, maxcount, sum_bytes, 
            sum_count, report_keysat, sorted_reportbuf, print_default);

    mfu_free(&sorted_reportbuf);
    mfu_free(&reportbuf);

    DTCMP_Op_free(&report_cmp);
    MPI_Type_free(&report_keysat);
    MPI_Type_free(&report_key);

    /* free counts */
    mfu_free(&counttot);
    mfu_free(&bytestot);

    /* free memory */
    mfu_free(&bytesrtl);
    mfu_free(&bytesltr);
    mfu_free(&bytesval);
    mfu_free(&countrtl);
    mfu_free(&countltr);
    mfu_free(&countval);
    mfu_free(&rankrtl);
    mfu_free(&rankltr);
    mfu_free(&rankval);
    mfu_free(&strbuf);

    /* free memory */
    mfu_free(&buf);
    DTCMP_Op_free(&cmp);
    MPI_Type_free(&key);

    return;
}

/* given a list of files and a path, compute number of items and
 * bytes of each child item in path */
static void summarize_children(mfu_flist flist, mfu_path* path, int print_default)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int path_comps = mfu_path_components(path);

    /* map child name to data structure (encodes address of struct as string) */
    strmap* children = strmap_new();

    /* sum up number of items and bytes under each child item */
    uint64_t numchildren = 0;
    uint64_t maxname = 0;
    uint64_t idx = 0;
    uint64_t files = mfu_flist_size(flist);
    while (idx < files) {
        /* get full path to item and create path object */
        const char* filename = mfu_flist_file_get_name(flist, idx);
        mfu_path* fpath = mfu_path_from_str(filename);
               
        /* identify child under parent to which this item belongs */
        mfu_path* childname = mfu_path_sub(fpath, path_comps, 1);
        const char* childname_str = mfu_path_strdup(childname);

        /* keep track of largest childname we see */
        size_t len = strlen(childname_str) + 1;
        if (len > maxname) {
            maxname = (uint64_t) len;
        }

        /* add contribution for this child */
        const char* val = strmap_getf(children, "%s", childname_str);
        if (val == NULL) {
            /* new child, increment our count */
            numchildren++;

            /* allocate and initialize structure for tracking info on this child */
            uint64_t* vals = MFU_MALLOC(2 * sizeof(uint64_t));
            vals[0] = 0;
            vals[1] = 0;

            /* add in contribution for this child */
            sum_child(flist, idx, vals);

            /* encode address as string */
            char p[1024];
            sprintf(p, "%p", (void*) vals);

            /* store address of data structure for this child */
            strmap_set(children, childname_str, p);
        } else {
            /* add in contribution for this child */
            uint64_t* vals = decode_addr(val);
            sum_child(flist, idx, vals);
        }

        /* free objects we created */
        mfu_free(&childname_str);
        mfu_path_delete(&childname);
        mfu_path_delete(&fpath);

        /* process next item */
        idx++;
    }

    /* compute max name across all tasks */
    uint64_t allmax;
    MPI_Allreduce(&maxname, &allmax, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* if we have a non-empty name, print it */
    if (allmax > 0) {
        sort_scan_sort(path, allmax, numchildren, children, print_default);
    }

    /* free data structure allocated for each child */
    const strmap_node* elem;
    for (elem  = strmap_node_first(children);
         elem != NULL;
         elem  = strmap_node_next(elem))
    {
        const char* val = strmap_node_value(elem);
        uint64_t* vals = decode_addr(val);
        mfu_free(&vals);
    }

    /* delete the string map */
    strmap_delete(&children);

    return;
}

static int sort_files_stat(const char* sortfields, mfu_flist* pflist)
{
    /* get list from caller */
    mfu_flist flist = *pflist;

    /* create a new list as subset of original list */
    mfu_flist flist2 = mfu_flist_subset(flist);

    uint64_t incount     = mfu_flist_size(flist);
    uint64_t chars       = mfu_flist_file_max_name(flist);
    uint64_t chars_user  = mfu_flist_user_max_name(flist);
    uint64_t chars_group = mfu_flist_group_max_name(flist);

    /* create datatype for packed file list element */
    MPI_Datatype dt_sat;
    size_t bytes = mfu_flist_file_pack_size(flist);
    MPI_Type_contiguous((int)bytes, MPI_BYTE, &dt_sat);

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* build type for file path */
    MPI_Datatype dt_filepath, dt_user, dt_group;
    MPI_Type_contiguous((int)chars,       MPI_CHAR, &dt_filepath);
    MPI_Type_contiguous((int)chars_user,  MPI_CHAR, &dt_user);
    MPI_Type_contiguous((int)chars_group, MPI_CHAR, &dt_group);
    MPI_Type_commit(&dt_filepath);
    MPI_Type_commit(&dt_user);
    MPI_Type_commit(&dt_group);

    /* build comparison op for filenames */
    DTCMP_Op op_filepath, op_user, op_group;
    if (DTCMP_Op_create(dt_filepath, my_strcmp, &op_filepath) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create sorting operation for filepath");
    }
    if (DTCMP_Op_create(dt_user, my_strcmp, &op_user) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create sorting operation for username");
    }
    if (DTCMP_Op_create(dt_group, my_strcmp, &op_group) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create sorting operation for groupname");
    }

    /* build comparison op for filenames */
    DTCMP_Op op_filepath_rev, op_user_rev, op_group_rev;
    if (DTCMP_Op_create(dt_filepath, my_strcmp_rev, &op_filepath_rev) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create reverse sorting operation for groupname");
    }
    if (DTCMP_Op_create(dt_user, my_strcmp_rev, &op_user_rev) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create reverse sorting operation for groupname");
    }
    if (DTCMP_Op_create(dt_group, my_strcmp_rev, &op_group_rev) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create reverse sorting operation for groupname");
    }

    /* TODO: process sort fields */
    const int MAXFIELDS = 7;
    MPI_Datatype types[MAXFIELDS];
    DTCMP_Op ops[MAXFIELDS];
    sort_field fields[MAXFIELDS];
    size_t lengths[MAXFIELDS];
    int nfields = 0;
    for (nfields = 0; nfields < MAXFIELDS; nfields++) {
        types[nfields]   = MPI_DATATYPE_NULL;
        ops[nfields]     = DTCMP_OP_NULL;
    }
    nfields = 0;
    char* sortfields_copy = MFU_STRDUP(sortfields);
    char* token = strtok(sortfields_copy, ",");
    while (token != NULL) {
        int valid = 1;
        if (strcmp(token, "name") == 0) {
            types[nfields]   = dt_filepath;
            ops[nfields]     = op_filepath;
            fields[nfields]  = FILENAME;
            lengths[nfields] = chars;
        }
        else if (strcmp(token, "-name") == 0) {
            types[nfields]   = dt_filepath;
            ops[nfields]     = op_filepath_rev;
            fields[nfields]  = FILENAME;
            lengths[nfields] = chars;
        }
        else if (strcmp(token, "user") == 0) {
            types[nfields]   = dt_user;
            ops[nfields]     = op_user;
            fields[nfields]  = USERNAME;
            lengths[nfields] = chars_user;
        }
        else if (strcmp(token, "-user") == 0) {
            types[nfields]   = dt_user;
            ops[nfields]     = op_user_rev;
            fields[nfields]  = USERNAME;
            lengths[nfields] = chars_user;
        }
        else if (strcmp(token, "group") == 0) {
            types[nfields]   = dt_group;
            ops[nfields]     = op_group;
            fields[nfields]  = GROUPNAME;
            lengths[nfields] = chars_group;
        }
        else if (strcmp(token, "-group") == 0) {
            types[nfields]   = dt_group;
            ops[nfields]     = op_group_rev;
            fields[nfields]  = GROUPNAME;
            lengths[nfields] = chars_group;
        }
        else if (strcmp(token, "uid") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
            fields[nfields]  = USERID;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "-uid") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
            fields[nfields]  = USERID;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "gid") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
            fields[nfields]  = GROUPID;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "-gid") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
            fields[nfields]  = GROUPID;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "atime") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
            fields[nfields]  = ATIME;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "-atime") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
            fields[nfields]  = ATIME;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "mtime") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
            fields[nfields]  = MTIME;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "-mtime") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
            fields[nfields]  = MTIME;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "ctime") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
            fields[nfields]  = CTIME;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "-ctime") == 0) {
            types[nfields]   = MPI_UINT32_T;
            ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
            fields[nfields]  = CTIME;
            lengths[nfields] = 4;
        }
        else if (strcmp(token, "size") == 0) {
            types[nfields]   = MPI_UINT64_T;
            ops[nfields]     = DTCMP_OP_UINT64T_ASCEND;
            fields[nfields]  = FILESIZE;
            lengths[nfields] = 8;
        }
        else if (strcmp(token, "-size") == 0) {
            types[nfields]   = MPI_UINT64_T;
            ops[nfields]     = DTCMP_OP_UINT64T_DESCEND;
            fields[nfields]  = FILESIZE;
            lengths[nfields] = 8;
        }
        else {
            /* invalid token */
            valid = 0;
            if (rank == 0) {
                printf("Invalid sort field: %s\n", token);
            }
        }
        if (valid) {
            nfields++;
        }
        if (nfields > MAXFIELDS) {
            /* TODO: print warning if we have too many fields */
            break;
        }
        token = strtok(NULL, ",");
    }
    mfu_free(&sortfields_copy);

    /* build key type */
    MPI_Datatype dt_key;
    if (DTCMP_Type_create_series(nfields, types, &dt_key) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create key type");
    }

    /* create op to sort by access time, then filename */
    DTCMP_Op op_key;
    if (DTCMP_Op_create_series(nfields, ops, &op_key) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create sorting operation for key");
    }

    /* build keysat type */
    MPI_Datatype dt_keysat, keysat_types[2];
    keysat_types[0] = dt_key;
    keysat_types[1] = dt_sat;
    if (DTCMP_Type_create_series(2, keysat_types, &dt_keysat) != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to create keysat type");
    }

    /* get extent of key type */
    MPI_Aint key_lb, key_extent;
    MPI_Type_get_extent(dt_key, &key_lb, &key_extent);

    /* get extent of keysat type */
    MPI_Aint keysat_lb, keysat_extent;
    MPI_Type_get_extent(dt_keysat, &keysat_lb, &keysat_extent);

    /* get extent of sat type */
    MPI_Aint stat_lb, stat_extent;
    MPI_Type_get_extent(dt_sat, &stat_lb, &stat_extent);

    /* compute size of sort element and allocate buffer */
    size_t sortbufsize = (size_t)keysat_extent * incount;
    void* sortbuf = MFU_MALLOC(sortbufsize);

    /* copy data into sort elements */
    uint64_t idx = 0;
    char* sortptr = (char*) sortbuf;
    while (idx < incount) {
        /* copy in access time */
        int i;
        for (i = 0; i < nfields; i++) {
            if (fields[i] == FILENAME) {
                const char* name = mfu_flist_file_get_name(flist, idx);
                strcpy(sortptr, name);
            }
            else if (fields[i] == USERNAME) {
                const char* name = mfu_flist_file_get_username(flist, idx);
                strcpy(sortptr, name);
            }
            else if (fields[i] == GROUPNAME) {
                const char* name = mfu_flist_file_get_groupname(flist, idx);
                strcpy(sortptr, name);
            }
            else if (fields[i] == USERID) {
                uint32_t val32 = (uint32_t) mfu_flist_file_get_uid(flist, idx);
                memcpy(sortptr, &val32, 4);
            }
            else if (fields[i] == GROUPID) {
                uint32_t val32 = (uint32_t) mfu_flist_file_get_gid(flist, idx);
                memcpy(sortptr, &val32, 4);
            }
            else if (fields[i] == ATIME) {
                uint32_t val32 = (uint32_t) mfu_flist_file_get_atime(flist, idx);
                memcpy(sortptr, &val32, 4);
            }
            else if (fields[i] == MTIME) {
                uint32_t val32 = (uint32_t) mfu_flist_file_get_mtime(flist, idx);
                memcpy(sortptr, &val32, 4);
            }
            else if (fields[i] == CTIME) {
                uint32_t val32 = (uint32_t) mfu_flist_file_get_ctime(flist, idx);
                memcpy(sortptr, &val32, 4);
            }
            else if (fields[i] == FILESIZE) {
                uint64_t val64 = mfu_flist_file_get_size(flist, idx);
                memcpy(sortptr, &val64, 8);
            }

            sortptr += lengths[i];
        }

        /* pack file element */
        sortptr += mfu_flist_file_pack(sortptr, flist, idx);

        idx++;
    }

    /* sort data */
    void* outsortbuf;
    int outsortcount;
    DTCMP_Handle handle;
    int sort_rc = DTCMP_Sortz(
                      sortbuf, (int)incount, &outsortbuf, &outsortcount,
                      dt_key, dt_keysat, op_key, DTCMP_FLAG_NONE,
                      MPI_COMM_WORLD, &handle
                  );
    if (sort_rc != DTCMP_SUCCESS) {
        MFU_ABORT(1, "Failed to sort data");
    }

    /* step through sorted data filenames */
    idx = 0;
    sortptr = (char*) outsortbuf;
    while (idx < (uint64_t)outsortcount) {
        sortptr += key_extent;
        sortptr += mfu_flist_file_unpack(sortptr, flist2);
        idx++;
    }

    /* build summary of new list */
    mfu_flist_summarize(flist2);

    /* free memory */
    DTCMP_Free(&handle);

    /* free ops */
    DTCMP_Op_free(&op_key);
    DTCMP_Op_free(&op_group_rev);
    DTCMP_Op_free(&op_user_rev);
    DTCMP_Op_free(&op_filepath_rev);
    DTCMP_Op_free(&op_group);
    DTCMP_Op_free(&op_user);
    DTCMP_Op_free(&op_filepath);

    /* free types */
    MPI_Type_free(&dt_keysat);
    MPI_Type_free(&dt_key);
    MPI_Type_free(&dt_group);
    MPI_Type_free(&dt_user);
    MPI_Type_free(&dt_filepath);

    /* free input buffer holding sort elements */
    mfu_free(&sortbuf);

    /* free the satellite type */
    MPI_Type_free(&dt_sat);

    /* return new list and free old one */
    *pflist = flist2;
    mfu_flist_free(&flist);

    return 0;
}

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
    if (verbose && rank == 0) {
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

static char mode_format[11];
static void prepare_mode_format(mode_t mode)
{
    if (S_ISDIR(mode)) {
        mode_format[0] = 'd';
    }
    else if (S_ISLNK(mode)) {
        mode_format[0] = 'l';
    }
    else {
        mode_format[0] = '-';
    }

    if (S_IRUSR & mode) {
        mode_format[1] = 'r';
    }
    else {
        mode_format[1] = '-';
    }

    if (S_IWUSR & mode) {
        mode_format[2] = 'w';
    }
    else {
        mode_format[2] = '-';
    }

    if (S_IXUSR & mode) {
        mode_format[3] = 'x';
    }
    else {
        mode_format[3] = '-';
    }

    if (S_IRGRP & mode) {
        mode_format[4] = 'r';
    }
    else {
        mode_format[4] = '-';
    }

    if (S_IWGRP & mode) {
        mode_format[5] = 'w';
    }
    else {
        mode_format[5] = '-';
    }

    if (S_IXGRP & mode) {
        mode_format[6] = 'x';
    }
    else {
        mode_format[6] = '-';
    }

    if (S_IROTH & mode) {
        mode_format[7] = 'r';
    }
    else {
        mode_format[7] = '-';
    }

    if (S_IWOTH & mode) {
        mode_format[8] = 'w';
    }
    else {
        mode_format[8] = '-';
    }

    if (S_IXOTH & mode) {
        mode_format[9] = 'x';
    }
    else {
        mode_format[9] = '-';
    }

    mode_format[10] = '\0';

    return;
}

static char type_str_unknown[] = "UNK";
static char type_str_dir[]     = "DIR";
static char type_str_file[]    = "REG";
static char type_str_link[]    = "LNK";

static void print_file(mfu_flist flist, uint64_t idx, int rank)
{
    /* get filename */
    const char* file = mfu_flist_file_get_name(flist, idx);

    if (mfu_flist_have_detail(flist)) {
        /* get mode */
        mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

        uint32_t uid = (uint32_t) mfu_flist_file_get_uid(flist, idx);
        uint32_t gid = (uint32_t) mfu_flist_file_get_gid(flist, idx);
        uint64_t acc = mfu_flist_file_get_atime(flist, idx);
        uint64_t mod = mfu_flist_file_get_mtime(flist, idx);
        uint64_t cre = mfu_flist_file_get_ctime(flist, idx);
        uint64_t size = mfu_flist_file_get_size(flist, idx);
        const char* username  = mfu_flist_file_get_username(flist, idx);
        const char* groupname = mfu_flist_file_get_groupname(flist, idx);

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

        prepare_mode_format(mode);

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
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        char* type_str = type_str_unknown;
        if (type == MFU_TYPE_DIR) {
            type_str = type_str_dir;
        }
        else if (type == MFU_TYPE_FILE) {
            type_str = type_str_file;
        }
        else if (type == MFU_TYPE_LINK) {
            type_str = type_str_link;
        }

        printf("Type=%s File=%s\n",
               type_str, file
              );
    }
}

static void print_files(mfu_flist flist, mfu_path* path)
{
    /* number of items to print from start and end of list */
    uint64_t range = 10;

    /* allocate send and receive buffers */
    size_t pack_size = mfu_flist_file_pack_size(flist);
    size_t bufsize = 2 * range * pack_size;
    void* sendbuf = MFU_MALLOC(bufsize);
    void* recvbuf = MFU_MALLOC(bufsize);

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* identify the number of items we have, the total number,
     * and our offset in the global list */
    uint64_t count  = mfu_flist_size(flist);
    uint64_t total  = mfu_flist_global_size(flist);
    uint64_t offset = mfu_flist_global_offset(flist);

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
    int* counts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* disps  = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));

    /* tell rank 0 where the data is coming from */
    int bytes = num * (int)pack_size;
    MPI_Gather(&bytes, 1, MPI_INT, counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* pack items into sendbuf */
    idx = 0;
    char* ptr = (char*) sendbuf;
    while (idx < count) {
        uint64_t global = offset + idx;
        if (global < range || (total - global) <= range) {
            ptr += mfu_flist_file_pack(ptr, flist, idx);
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
    mfu_flist tmplist = mfu_flist_subset(flist);

    /* unpack items into new list */
    if (rank == 0) {
        ptr = (char*) recvbuf;
        char* end = ptr + recvbytes;
        while (ptr < end) {
            mfu_flist_file_unpack(ptr, tmplist);
            ptr += pack_size;
        }
    }

    /* summarize list */
    mfu_flist_summarize(tmplist);

    /* print files */
    if (rank == 0) {
        printf("\n");
        uint64_t tmpidx = 0;
        uint64_t tmpsize = mfu_flist_size(tmplist);
        while (tmpidx < tmpsize) {
            print_file(tmplist, tmpidx, rank);
            tmpidx++;
            if (tmpidx == range && tmpsize > 2 * range) {
                printf("\n<snip>\n\n");
            }
        }
        printf("\n");
    }

    /* free our temporary list */
    mfu_flist_free(&tmplist);

    /* free memory */
    mfu_free(&disps);
    mfu_free(&counts);
    mfu_free(&sendbuf);
    mfu_free(&recvbuf);

    return;
}

static int invalid_sortfields(char* sortfields, mfu_walk_opts_t* walk_opts)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* if user is trying to sort, verify the sort fields are valid */
    int invalid = 0;
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
                    invalid = 1;
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
                    invalid = 1;
                }
                nfields++;
                token = strtok(NULL, ",");
            }
        }
        if (nfields > maxfields) {
            printf("Exceeded maximum number of sort fields: %d\n", maxfields);
            invalid = 1;
        }
        mfu_free(&sortfields_copy);
    }

    return invalid;
}

/* given a string like foo*, convert to equivalent C-based regex,
 * start string with ^, replace each "*" with ".*", end string with $ */
static char* arg_to_regex(const char* arg)
{
    /* count number of bytes we need */
    size_t count = 2; /* for ^ and $ at ends of regex */
    char* str = (char*)arg;
    char* tok = strchr(str, '*');
    while (tok != NULL) {
      count += tok - str; /* copy text leading up to * */
      count += 2; /* replace each * with .* */
      str = tok + 1;
      tok = strchr(str, '*');
    }
    count += strlen(str);
    count += 1; /* trailing NULL */

    /* allocate memory for regex */
    char* regex = MFU_MALLOC(count);

    /* prepend ^ to match start of sequence */
    strcpy(regex, "^");

    /* replace each * with .* */
    str = (char*)arg;
    tok = strchr(str, '*');
    while (tok != NULL) {
      strncat(regex, str, tok - str);
      strcat(regex, ".*");
      str = tok + 1;
      tok = strchr(str, '*');
    }

    /* remaining text */
    strcat(regex, str);

    /* append $ to match end of sequence */
    strcat(regex, "$");

    return regex;
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dsh [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -f, --file <file>   - read list from file, and write processed list back to file\n");
    printf("  -i, --input <file>  - read list from file\n");
    printf("  -o, --output <file> - write processed list to file\n");
    printf("  -l, --lite          - walk file system without stat\n");
    printf("  -v, --verbose       - verbose output\n");
    printf("  -h, --help          - print usage\n");
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

    /* pointer to mfu_file src object */
    mfu_file_t* mfu_src_file = mfu_file_new();

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
    int walk = 0;
    int text = 0;
    /* set print default to 25 for now */
    int print_default = 100;

    int option_index = 0;
    static struct option long_options[] = {
        {"file",     1, 0, 'f'},
        {"input",    1, 0, 'i'},
        {"output",   1, 0, 'o'},
        {"lite",     0, 0, 'l'},
        {"help",     0, 0, 'h'},
        {"verbose",  0, 0, 'v'},
        {"text",     0, 0, 't'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "f:i:o:lhvt",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'f':
                /* use the same file for input and output */
                inputname  = MFU_STRDUP(optarg);
                outputname = MFU_STRDUP(optarg);
                break;
            case 'i':
                inputname = MFU_STRDUP(optarg);
                break;
            case 'o':
                outputname = MFU_STRDUP(optarg);
                break;
            case 'l':
                walk_opts->use_stat = 0;
                break;
            case 'h':
                usage = 1;
                break;
            case 'v':
                verbose = 1;
                break;
            case 't':
                text = 1;
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
        const char** argpaths = (const char**)(&argv[optind]);
        mfu_param_path_set_all(numpaths, argpaths, paths, mfu_src_file, true);

        /* advance to next set of options */
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

    /* print usage if we need to */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_file_delete(&mfu_src_file);
        MPI_Finalize();
        return 0;
    }

    /* TODO: check stat fields fit within MPI types */
    // if (sizeof(st_uid) > uint64_t) error(); etc...

    /* initialize our sorting library */
    DTCMP_Init();

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    /* get our list of files, either by walking or reading an
     * input file */
    if (walk) {
        /* walk list of input paths */
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist, mfu_src_file);
    }
    else {
        /* read list from file */
        mfu_flist_read_cache(inputname, flist);
    }

    /* start process from the root directory */
    mfu_path* path = mfu_path_from_str("/");
    mfu_path_reduce(path);

    /* start command loop */
    while (1) {
        int print = 0;
        char* sortfields = NULL;
        char* regex      = NULL;

// http://web.mit.edu/gnu/doc/html/rlman_2.html
// http://sunsite.ualberta.ca/Documentation/Gnu/readline-4.1/html_node/readline_45.html
        /* print prompt */
//        char* input = NULL;
        if (rank == 0) {
            char* path_str = mfu_path_strdup(path);
            printf("%s >>:\n", path_str);
            fflush(stdout);
//            input = readline(NULL);
            mfu_free(&path_str);
        }

        /* read command */
        int size = 0;
        char line[1024];
        if (rank == 0) {
#if 0
            strncpy(line, input, sizeof(line));
            size = strlen(input) + 1;
            add_history(input);
#endif
            char* val = fgets(line, sizeof(line), stdin);

            /* TODO: check that we got a whole line */

            /* if we read something successfully, compute its length */
            if (val != NULL) {
                size = strlen(val) + 1;
            }
        }

//        free(input);

        /* bcast data to all tasks, bcast length of 0 if nothing read */
        MPI_Bcast(&size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        if (size > 0) {
            MPI_Bcast(line, size, MPI_CHAR, 0, MPI_COMM_WORLD);
        }

        /* exit if command size is 0 */
        if (size == 0) {
            break;
        }

        /* TODO: tokenize command to make this less lame */
        /* parse command */
        if (strncmp(line, "exit", 4) == 0) {
            /* break the loop on exit */
            break;
        } else if (strncmp(line, "pwd", 3) == 0) {
            /* just print the current directory */
            if (rank == 0) {
                char* path_str = mfu_path_strdup(path);
                printf("%s\n", path_str);
                fflush(stdout);
                mfu_free(&path_str);
            }
        } else if (strncmp(line, "cd", 2) == 0) {
            char subpath[1024];
            int scan_rc = sscanf(line, "cd %s\n", subpath);
            if (scan_rc == 1) {
                mfu_path* subp = mfu_path_from_str(subpath);
                if (mfu_path_is_absolute(subp)) {
                    /* we got an absolute path, reset our current path */
                    mfu_path_delete(&path);
                    path = mfu_path_from_str(subpath);
                } else {
                    /* got a relative path, tack it on to existing path */
                    mfu_path_append(path, subp);
                }
                mfu_path_delete(&subp);
                mfu_path_reduce(path);
            } else if (rank == 0) {
                printf("Invalid 'cd' command\n");
                fflush(stdout);
            }
        } else if (strncmp(line, "ls", 2) == 0) {
            /* print list to screen */
            print = 1;

            /* TODO: process other filter fields */

            /* process sort fields */
            char ls_args[1024];
            int scan_rc = sscanf(line, "ls %s\n", ls_args);
            if (scan_rc == 1) {
                regex = arg_to_regex(ls_args);
#if 0
                sortfields = MFU_STRDUP(ls_args);
                if (invalid_sortfields(sortfields, walk_opts)) {
                    /* disable printing and sorting */
                    mfu_free(&sortfields);
                    print = 0;

                    /* print error message */
                    if (rank == 0) {
                        printf("Invalid 'ls' command\n");
                        printf("ls [fields...] -- sort output by comma-delimited fields\n");
                        printf("Fields: name,user,group,uid,gid,atime,mtime,ctime,size\n");
                        fflush(stdout);
                    }
                }
#endif
            }
        } else if (strncmp(line, "rm", 2) == 0) {
            char subpath[1024];
            int scan_rc = sscanf(line, "rm %s\n", subpath);
            if (scan_rc == 1) {
                /* determine path to remove */
                mfu_path* remove_path = NULL;
                mfu_path* subp = mfu_path_from_str(subpath);
                if (mfu_path_is_absolute(subp)) {
                    /* we got an absolute path, reset our current path */
                    remove_path = mfu_path_from_str(subpath);
                } else {
                    /* got a relative path, tack it on to existing path */
                    remove_path = mfu_path_dup(path);
                    mfu_path_append(remove_path, subp);
                }
                mfu_path_reduce(remove_path);

                /* filter files by path and remove them, include the path (directory)
                 * as an item to be removed */
                int inclusive = 1;
                mfu_flist filtered, leftover;
                filter_files_path(flist, remove_path, inclusive, &filtered, &leftover);
                mfu_flist_unlink(filtered, false, mfu_src_file);
                mfu_flist_free(&filtered);

                /* to update our list after removing files above, set flist
                 * to just the remaining set of files */
                mfu_flist_free(&flist);
                flist = leftover;

                mfu_path_delete(&subp);
                mfu_path_delete(&remove_path);
            } else if (rank == 0) {
                printf("Invalid 'rm' command\n");
                fflush(stdout);
            }
        } else if (strncmp(line, "ls", 2) == 0) {
        } else {
            if (rank == 0) {
                printf("Invalid command\n");
                printf("Commands: pwd, cd, ls, rm, exit\n");
                fflush(stdout);
            }
        }
        
        if (print) {
            /* filter files by path, exclude the path itself from the list,
             * just list its contents */
            int inclusive = 0;
            mfu_flist filtered, leftover;
            filter_files_path(flist, path, inclusive, &filtered, &leftover);

            /* filter items out by regex if we have one */
            if (regex != NULL) {
                mfu_flist filtered2, leftover2;
                filter_files_regex(filtered, path, regex, &filtered2, &leftover2);
                mfu_flist_free(&filtered);
                mfu_flist_free(&leftover2);
                filtered = filtered2;
            }

            summarize_children(filtered, path, print_default);

            /* free the list */
            mfu_flist_free(&filtered);
            mfu_flist_free(&leftover);
        }

        mfu_free(&regex);
        mfu_free(&sortfields);
    }

    /* free our path */
    mfu_path_delete(&path);

    /* write data to cache file */
    if (outputname != NULL) {
        if (!text) {
            mfu_flist_write_cache(outputname, flist);
        } else {
            mfu_flist_write_text(outputname, flist);
        }
    }

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* free the mfu_file object */
    mfu_file_delete(&mfu_src_file);

    /* free users, groups, and files objects */
    mfu_flist_free(&flist);

    /* free memory allocated for options */
    mfu_free(&outputname);
    mfu_free(&inputname);

    /* shut down the sorting library */
    DTCMP_Finalize();

    /* free the path parameters */
    for (i = 0; i < numpaths; i++) {
        mfu_param_path_free(&paths[i]);
    }
    mfu_free(&paths);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
