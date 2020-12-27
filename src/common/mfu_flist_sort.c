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
#include "mfu.h"

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

static mfu_flist sort_files_readdir(const char* sortfields, mfu_flist flist)
{
    uint64_t incount = mfu_flist_size(flist);
    uint64_t chars   = mfu_flist_file_max_name(flist);

    /* create a new list as subset of original list */
    mfu_flist flist2 = mfu_flist_subset(flist);

    /* bail out if there is nothing to sort, the problem is that
     * we end up with chars==0 in that case, and we can't create
     * a valid comparison op for 0-length strings */
    if (chars == 0) {
        mfu_flist_summarize(flist2);
        return flist2;
    }

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
                MFU_LOG(MFU_LOG_ERR, "Invalid sort field: %s\n", token);
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

    /* return new list */
    return flist2;
}

static mfu_flist sort_files_stat(const char* sortfields, mfu_flist flist)
{
    uint64_t incount     = mfu_flist_size(flist);
    uint64_t chars       = mfu_flist_file_max_name(flist);
    uint64_t chars_user  = mfu_flist_user_max_name(flist);
    uint64_t chars_group = mfu_flist_group_max_name(flist);

    /* create a new list as subset of original list */
    mfu_flist flist2 = mfu_flist_subset(flist);

    /* bail out if there is nothing to sort, the problem is that
     * we end up with chars==0 in that case, and we can't create
     * a valid comparison op for 0-length strings */
    if (chars == 0 || chars_user == 0 || chars_group == 0) {
        mfu_flist_summarize(flist2);
        return flist2;
    }

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
                MFU_LOG(MFU_LOG_ERR, "Invalid sort field: %s\n", token);
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

    /* return new list */
    return flist2;
}

/* sort flist by specified fields, given as common-delimitted list
 * precede field name with '-' character to reverse sort order:
 *   name,user,group,uid,gid,atime,mtime,ctime,size
 * For example to sort by size in descending order, followed by name
 *   char fields[] = "size,-name"; */
mfu_flist mfu_flist_sort(const char* sortfields, mfu_flist flist)
{
    if (sortfields == NULL) {
        MFU_ABORT(1, "mfu_flist_sort called with invalid sortfields");
    }

    /* start timer */
    double start_sort = MPI_Wtime();

    /* sort list */
    mfu_flist flist2 = MFU_FLIST_NULL;
    if (mfu_flist_have_detail(flist)) {
        flist2 = sort_files_stat(sortfields, flist);
    }
    else {
        flist2 = sort_files_readdir(sortfields, flist);
    }

    /* end timer */
    double end_sort = MPI_Wtime();

    /* report sort count, time, and rate */
    if (mfu_rank == 0) {
        uint64_t all_count = mfu_flist_global_size(flist);
        double secs = end_sort - start_sort;
        double rate = 0.0;
        if (secs > 0.0) {
            rate = ((double)all_count) / secs;
        }
        MFU_LOG(MFU_LOG_INFO, "Sorted %lu items in %.3lf seconds (%.3lf items/sec)",
            all_count, secs, rate
        );
    }

    /* wait for summary to be printed */
    MPI_Barrier(MPI_COMM_WORLD);

    return flist2;
}
