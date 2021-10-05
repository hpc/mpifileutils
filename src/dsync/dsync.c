/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   Written by Adam Moody <moody20@llnl.gov>.
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
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <mpi.h>
#include <libcircle.h>
#include <linux/limits.h>
#include <libgen.h>
#include <errno.h>
#include <dtcmp.h>
#include <inttypes.h>
#define _XOPEN_SOURCE 600
#include <fcntl.h>
#include <string.h>

/* for bool type, true/false macros */
#include <stdbool.h>
#include <assert.h>

#include "mfu.h"
#include "strmap.h"
#include "list.h"

#include "mfu_errors.h"

/* for daos */
#ifdef DAOS_SUPPORT
#include "mfu_daos.h"
#endif

/* Print a usage message */
static void print_usage(void)
{
    printf("\n");
    printf("Usage: dsync [options] source target\n");
    printf("\n");
#ifdef DAOS_SUPPORT
    printf("DAOS paths can be specified as:\n");
    printf("       daos://<pool>/<cont>[/<path>] | <UNS path>\n");
#endif
    printf("Options:\n");
    printf("      --dryrun            - show differences, but do not synchronize files\n");
    printf("  -b  --batch-files <N>   - batch files into groups of N during copy\n");
    printf("      --bufsize <SIZE>    - IO buffer size in bytes (default " MFU_BUFFER_SIZE_STR ")\n");
    printf("      --chunksize <SIZE>  - minimum work size per task in bytes (default " MFU_CHUNK_SIZE_STR ")\n");
    printf("  -X, --xattrs <OPT>      - copy xattrs (none, all, non-lustre, libattr)\n");
#ifdef DAOS_SUPPORT
    printf("      --daos-api          - DAOS API in {DFS, DAOS} (default uses DFS for POSIX containers)\n");
#endif
    printf("  -c, --contents          - read and compare file contents rather than compare size and mtime\n");
    printf("  -D, --delete            - delete extraneous files from target\n");
    printf("  -L, --dereference       - copy original files instead of links\n");
    printf("  -P, --no-dereference    - don't follow links in source\n"); 
    printf("  -s, --direct            - open files with O_DIRECT\n");
    printf("      --link-dest <DIR>   - hardlink to files in DIR when unchanged\n");
    printf("  -S, --sparse            - create sparse files when possible\n");
    printf("      --progress <N>      - print progress every N seconds\n");
    printf("  -v, --verbose           - verbose output\n");
    printf("  -q, --quiet             - quiet output\n");
    printf("  -h, --help              - print usage\n");
    printf("\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    fflush(stdout);
}

typedef enum _dsync_state {
    /* initial state */
    DCMPS_INIT = 'A',

    /* have common data/metadata */
    DCMPS_COMMON,

    /* have common data/metadata, not valid for DCMPF_EXIST */
    DCMPS_DIFFER,

     /*
      * This file only exist in src directory.
      * Only valid for DCMPF_EXIST.
      */
    DCMPS_ONLY_SRC,

     /*
      * This file only exist in dest directory.
      * Only valid for DCMPF_EXIST.
      * Not used yet,
      * because we don't want to waste a loop in dsync_strmap_compare()
      */
    DCMPS_ONLY_DEST,

    DCMPS_MAX,
} dsync_state;

typedef enum _dsync_field {
    DCMPF_EXIST = 0, /* both have this file */
    DCMPF_TYPE,      /* both are the same type */
    DCMPF_SIZE,      /* both are regular file and have same size */
    DCMPF_UID,       /* both have the same UID */
    DCMPF_GID,       /* both have the same GID */
    DCMPF_ATIME,     /* both have the same atime */
    DCMPF_MTIME,     /* both have the same mtime */
    DCMPF_CTIME,     /* both have the same ctime */
    DCMPF_PERM,      /* both have the same permission */
    DCMPF_ACL,       /* both have the same ACLs */
    DCMPF_CONTENT,   /* both have the same data */
    DCMPF_MAX,
} dsync_field;

#define DCMPF_EXIST_DEPEND   (1 << DCMPF_EXIST)
#define DCMPF_TYPE_DEPEND    (DCMPF_EXIST_DEPEND | (1 << DCMPF_TYPE))
#define DCMPF_SIZE_DEPEND    (DCMPF_TYPE_DEPEND | (1 << DCMPF_SIZE))
#define DCMPF_UID_DEPEND     (DCMPF_EXIST_DEPEND | (1 << DCMPF_UID))
#define DCMPF_GID_DEPEND     (DCMPF_EXIST_DEPEND | (1 << DCMPF_GID))
#define DCMPF_ATIME_DEPEND   (DCMPF_EXIST_DEPEND | (1 << DCMPF_ATIME))
#define DCMPF_MTIME_DEPEND   (DCMPF_EXIST_DEPEND | (1 << DCMPF_MTIME))
#define DCMPF_CTIME_DEPEND   (DCMPF_EXIST_DEPEND | (1 << DCMPF_CTIME))
#define DCMPF_PERM_DEPEND    (DCMPF_EXIST_DEPEND | (1 << DCMPF_PERM))
#define DCMPF_ACL_DEPEND     (DCMPF_EXIST_DEPEND | (1 << DCMPF_ACL))
#define DCMPF_CONTENT_DEPEND (DCMPF_SIZE_DEPEND | (1 << DCMPF_CONTENT))

uint64_t dsync_field_depend[] = {
    [DCMPF_EXIST]   = DCMPF_EXIST_DEPEND,
    [DCMPF_TYPE]    = DCMPF_TYPE_DEPEND,
    [DCMPF_SIZE]    = DCMPF_SIZE_DEPEND,
    [DCMPF_UID]     = DCMPF_UID_DEPEND,
    [DCMPF_GID]     = DCMPF_GID_DEPEND,
    [DCMPF_ATIME]   = DCMPF_ATIME_DEPEND,
    [DCMPF_MTIME]   = DCMPF_MTIME_DEPEND,
    [DCMPF_CTIME]   = DCMPF_CTIME_DEPEND,
    [DCMPF_PERM]    = DCMPF_PERM_DEPEND,
    [DCMPF_ACL]     = DCMPF_ACL_DEPEND,
    [DCMPF_CONTENT] = DCMPF_CONTENT_DEPEND,
};

struct dsync_expression {
    dsync_field field;              /* the concerned field */
    dsync_state state;              /* expected state of the field */
    struct list_head linkage;      /* linkage to struct dsync_conjunction */
};

struct dsync_conjunction {
    struct list_head linkage;      /* linkage to struct dsync_disjunction */
    struct list_head expressions;  /* list of logical conjunction */
    mfu_flist src_matched_list;    /* matched src items in this conjunction */
    mfu_flist dst_matched_list;    /* matched dst items in this conjunction */
};

struct dsync_disjunction {
    struct list_head linkage;      /* linkage to struct dsync_output */
    struct list_head conjunctions; /* list of logical conjunction */
    unsigned count;		   /* logical conjunctions count */
};

struct dsync_output {
    char* file_name;               /* output file name */
    struct list_head linkage;      /* linkage to struct dsync_options */
    struct dsync_disjunction *disjunction; /* logical disjunction rules */
};

struct dsync_options {
    struct list_head outputs;      /* list of outputs */
    int contents;                  /* check file contents rather than size and mtime */
    int dry_run;                   /* dry run */
    int verbose;
    int quiet;
    int debug;                     /* check result after get result */
    int delete;                    /* delete extraneous files from destination dirs */
    char* link_dest;               /* link dest dir */
    int need_compare[DCMPF_MAX];   /* fields that need to be compared  */
};

struct dsync_options options = {
    .outputs      = LIST_HEAD_INIT(options.outputs),
    .contents     = 0,
    .dry_run      = 0,
    .verbose      = 0,
    .quiet        = 0,
    .debug        = 0,
    .delete       = 0,
    .link_dest    = NULL,
    .need_compare = {0,}
};

/* From tail to head */
const char *dsync_default_outputs[] = {
    "EXIST=COMMON@CONTENT=DIFFER",
    "EXIST=COMMON@CONTENT=COMMON",
    "EXIST=COMMON@TYPE=DIFFER",
    "EXIST=COMMON@TYPE=COMMON",
    "EXIST=DIFFER",
    "EXIST=COMMON",
    "EXIST=COMMON@UID=DIFFER",
    "EXIST=COMMON@GID=DIFFER",
    "EXIST=COMMON@PERM=DIFFER",
    "EXIST=COMMON@ATIME=DIFFER",
    "EXIST=COMMON@MTIME=DIFFER",
    NULL,
};

static const char* dsync_field_to_string(dsync_field field, int simple)
{
    assert(field < DCMPF_MAX);
    switch (field) {
    case DCMPF_EXIST:
        if (simple) {
            return "EXIST";
        } else {
            return "existence";
        }
        break;
    case DCMPF_TYPE:
        if (simple) {
            return "TYPE";
        } else {
            return "type";
        }
        break;
    case DCMPF_SIZE:
        if (simple) {
            return "SIZE";
        } else {
            return "size";
        }
        break;
    case DCMPF_UID:
        if (simple) {
            return "UID";
        } else {
            return "user ID";
        }
        break;
    case DCMPF_GID:
        if (simple) {
            return "GID";
        } else {
            return "group ID";
        }
        break;
    case DCMPF_ATIME:
        if (simple) {
            return "ATIME";
        } else {
            return "access time";
        }
        break;
    case DCMPF_MTIME:
        if (simple) {
            return "MTIME";
        } else {
            return "modification time";
        }
        break;
    case DCMPF_CTIME:
        if (simple) {
            return "CTIME";
        } else {
            return "change time";
        }
        break;
    case DCMPF_PERM:
        if (simple) {
            return "PERM";
        } else {
            return "permission";
        }
        break;
    case DCMPF_ACL:
        if (simple) {
            return "ACL";
        } else {
            return "Access Control Lists";
        }
        break;
    case DCMPF_CONTENT:
        if (simple) {
            return "CONTENT";
        } else {
            return "content";
        }
        break;
    case DCMPF_MAX:
    default:
        return NULL;
        break;
    }
    return NULL;
}

static int dsync_field_from_string(const char* string, dsync_field *field)
{
    dsync_field i;
    for (i = 0; i < DCMPF_MAX; i ++) {
        if (strcmp(dsync_field_to_string(i, 1), string) == 0) {
            *field = i;
            return 0;
        }
    }
    return -ENOENT;
}

static const char* dsync_state_to_string(dsync_state state, int simple)
{
    switch (state) {
    case DCMPS_INIT:
        if (simple) {
            return "INIT";
        } else {
            return "initial";
        }
        break;
    case DCMPS_COMMON:
        if (simple) {
            return "COMMON";
        } else {
            return "the same";
        }
        break;
    case DCMPS_DIFFER:
        if (simple) {
            return "DIFFER";
        } else {
            return "different";
        }
        break;
    case DCMPS_ONLY_SRC:
        if (simple) {
            return "ONLY_SRC";
        } else {
            return "exist only in source directory";
        }
        break;
    case DCMPS_ONLY_DEST:
        if (simple) {
            return "ONLY_DEST";
        } else {
            return "exist only in destination directory";
        }
        break;
    case DCMPS_MAX:
    default:
        return NULL;
        break;
    }
    return NULL;
}

static int dsync_state_from_string(const char* string, dsync_state *state)
{
    dsync_state i;
    for (i = DCMPS_INIT; i < DCMPS_MAX; i ++) {
        if (strcmp(dsync_state_to_string(i, 1), string) == 0) {
            *state = i;
            return 0;
        }
    }
    return -ENOENT;
}

/* given a filename as the key, encode an index followed
 * by the init state */
static void dsync_strmap_item_init(
    strmap* map,
    const char *key,
    uint64_t item_index)
{
    /* Should be long enough for 64 bit number and DCMPF_MAX */
    char val[21 + DCMPF_MAX];
    int i;

    /* encode the index */
    int len = snprintf(val, sizeof(val), "%llu",
                       (unsigned long long) item_index);

    /* encode the state (state characters and trailing NUL) */
    assert((size_t)len + DCMPF_MAX + 1 <= (sizeof(val)));
    size_t position = strlen(val);
    for (i = 0; i < DCMPF_MAX; i++) {
        val[position] = DCMPS_INIT;
        position++;
    }
    val[position] = '\0';

    /* add item to map */
    strmap_set(map, key, val);
}

static void dsync_strmap_item_update(
    strmap* map,
    const char *key,
    dsync_field field,
    dsync_state state)
{
    /* Should be long enough for 64 bit number and DCMPF_MAX */
    char new_val[21 + DCMPF_MAX];

    /* lookup item from map */
    const char* val = strmap_get(map, key);

    /* copy existing index over */
    assert(field < DCMPF_MAX);
    assert(strlen(val) + 1 <= sizeof(new_val));
    strcpy(new_val, val);

    /* set new state value */
    size_t position = strlen(new_val) - DCMPF_MAX;
    new_val[position + field] = state;

    /* reinsert item in map */
    strmap_set(map, key, new_val);
}

static int dsync_strmap_item_index(
    strmap* map,
    const char *key,
    uint64_t *item_index)
{
    /* Should be long enough for 64 bit number and DCMPF_MAX */
    char new_val[21 + DCMPF_MAX];

    /* lookup item from map */
    const char* val = strmap_get(map, key);
    if (val == NULL) {
        return -1;
    }

    /* extract index */
    assert(strlen(val) + 1 <= sizeof(new_val));
    strcpy(new_val, val);
    new_val[strlen(new_val) - DCMPF_MAX] = '\0';
    *item_index = strtoull(new_val, NULL, 0);

    return 0;
}

static int dsync_strmap_item_state(
    strmap* map,
    const char *key,
    dsync_field field,
    dsync_state *state)
{
    /* lookup item from map */
    const char* val = strmap_get(map, key);
    if (val == NULL) {
        return -1;
    }

    /* extract state */
    assert(strlen(val) > DCMPF_MAX);
    assert(field < DCMPF_MAX);
    size_t position = strlen(val) - DCMPF_MAX;
    *state = val[position + field];

    return 0;
}

/* map each file name to its index in the file list and initialize
 * its state for comparison operation */
static strmap* dsync_strmap_creat(mfu_flist list, const char* prefix)
{
    /* create a new string map to map a file name to a string
     * encoding its index and state */
    strmap* map = strmap_new();

    /* determine length of prefix string */
    size_t prefix_len = strlen(prefix);

    /* iterate over each item in the file list */
    uint64_t i = 0;
    uint64_t count = mfu_flist_size(list);
    while (i < count) {
        /* get full path of file name */
        const char* name = mfu_flist_file_get_name(list, i);

        /* ignore prefix portion of path */
        name += prefix_len;

        /* create entry for this file */
        dsync_strmap_item_init(map, name, i);

        /* go to next item in list */
        i++;
    }

    return map;
}

#define dsync_compare_field(field_name, field)                                \
do {                                                                         \
    uint64_t src = mfu_flist_file_get_ ## field_name(src_list, src_index); \
    uint64_t dst = mfu_flist_file_get_ ## field_name(dst_list, dst_index); \
    if (src != dst) {                                                        \
        /* file type is different */                                         \
        dsync_strmap_item_update(src_map, key, field, DCMPS_DIFFER);          \
        dsync_strmap_item_update(dst_map, key, field, DCMPS_DIFFER);          \
        diff++;                                                              \
    } else {                                                                 \
        dsync_strmap_item_update(src_map, key, field, DCMPS_COMMON);          \
        dsync_strmap_item_update(dst_map, key, field, DCMPS_COMMON);          \
    }                                                                        \
} while(0)

static void dsync_compare_acl(
    const char *key,
    mfu_flist src_list,
    uint64_t src_index,
    mfu_flist dst_list,
    uint64_t dst_index,
    strmap* src_map,
    strmap* dst_map,
    int *diff)
{
    void *src_val, *dst_val;
    ssize_t src_size, dst_size;
    bool is_same = true;

#if DCOPY_USE_XATTRS
    src_val = mfu_flist_file_get_acl(src_list, src_index, &src_size,
                                     "system.posix_acl_access");
    dst_val = mfu_flist_file_get_acl(dst_list, dst_index, &dst_size,
                                     "system.posix_acl_access");

    if (src_size == dst_size) {
        if (src_size > 0) {
            if (memcmp(src_val, dst_val, src_size)) {
                is_same = false;
                goto out;
            }
        }
    } else {
        is_same = false;
        goto out;
    }

    mfu_filetype type = mfu_flist_file_get_type(src_list, src_index);
    if (type == MFU_TYPE_DIR) {
        mfu_free(&src_val);
        mfu_free(&dst_val);

        src_val = mfu_flist_file_get_acl(src_list, src_index, &src_size,
                                         "system.posix_acl_default");
        dst_val = mfu_flist_file_get_acl(dst_list, dst_index, &dst_size,
                                         "system.posix_acl_default");

        if (src_size == dst_size) {
            if (src_size > 0) {
                if (memcmp(src_val, dst_val, src_size)) {
                    is_same = false;
                    goto out;
                }
            }
        } else {
            is_same = false;
            goto out;
        }
    }

out:
    mfu_free(&src_val);
    mfu_free(&dst_val);

#endif
    if (is_same) {
        dsync_strmap_item_update(src_map, key, DCMPF_ACL, DCMPS_COMMON);
        dsync_strmap_item_update(dst_map, key, DCMPF_ACL, DCMPS_COMMON);
    } else {
        dsync_strmap_item_update(src_map, key, DCMPF_ACL, DCMPS_DIFFER);
        dsync_strmap_item_update(dst_map, key, DCMPF_ACL, DCMPS_DIFFER);
        (*diff)++;
    }
}

static int dsync_option_need_compare(dsync_field field)
{
    return options.need_compare[field];
}

/* Return -1 when error, return 0 when equal, return > 0 when diff */
static int dsync_compare_metadata(
    mfu_flist src_list,
    strmap* src_map,
    uint64_t src_index,
    mfu_flist dst_list,
    strmap* dst_map,
    uint64_t dst_index,
    const char* key)
{
    int diff = 0;

    if (dsync_option_need_compare(DCMPF_SIZE)) {
        mfu_filetype type = mfu_flist_file_get_type(src_list, src_index);
        if (type != MFU_TYPE_DIR) {
            dsync_compare_field(size, DCMPF_SIZE);
        } else {
            dsync_strmap_item_update(src_map, key, DCMPF_SIZE, DCMPS_COMMON);
            dsync_strmap_item_update(dst_map, key, DCMPF_SIZE, DCMPS_COMMON);
        }
    }
    if (dsync_option_need_compare(DCMPF_GID)) {
        dsync_compare_field(gid, DCMPF_GID);
    }
    if (dsync_option_need_compare(DCMPF_UID)) {
        dsync_compare_field(uid, DCMPF_UID);
    }
    if (dsync_option_need_compare(DCMPF_ATIME)) {
        dsync_compare_field(atime, DCMPF_ATIME);
    }
    if (dsync_option_need_compare(DCMPF_MTIME)) {
        dsync_compare_field(mtime, DCMPF_MTIME);
    }
    if (dsync_option_need_compare(DCMPF_CTIME)) {
        dsync_compare_field(ctime, DCMPF_CTIME);
    }
    if (dsync_option_need_compare(DCMPF_PERM)) {
        dsync_compare_field(perm, DCMPF_PERM);
    }
    if (dsync_option_need_compare(DCMPF_ACL)) {
        dsync_compare_acl(key, src_list,src_index,
                         dst_list, dst_index,
                         src_map, dst_map, &diff);
    }

    return diff;
}

/* variable to record total number of bytes to be compared to report progress and estimate time reamining */
static uint64_t dsync_total_count;

static void compare_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    uint64_t read_bytes  = vals[0];
    uint64_t write_bytes = vals[1];

    /* compute average delete rate */
    double read_rate  = 0.0;
    double write_rate = 0.0;
    if (secs > 0) {
        read_rate  = (double)read_bytes / secs;
        write_rate = (double)write_bytes / secs;
    }

    /* compute percentage of items removed */
    double percent = 0.0;
    if (dsync_total_count > 0) {
        percent = 100.0 * (double)read_bytes / (double)dsync_total_count;
    }

    /* compute estimated time remaining */
    double secs_remaining = -1.0;
    if (read_rate > 0.0) {
        secs_remaining = (double)(dsync_total_count - read_bytes) / read_rate;
    }

    /* convert bytes to units */
    double agg_read_size_tmp;
    const char* agg_read_size_units;
    mfu_format_bytes(read_bytes, &agg_read_size_tmp, &agg_read_size_units);

    /* convert bandwidth to units */
    double agg_read_rate_tmp;
    const char* agg_read_rate_units;
    mfu_format_bw(read_rate, &agg_read_rate_tmp, &agg_read_rate_units);

    /* convert bytes to units */
    double agg_write_size_tmp;
    const char* agg_write_size_units;
    mfu_format_bytes(write_bytes, &agg_write_size_tmp, &agg_write_size_units);

    /* convert bandwidth to units */
    double agg_write_rate_tmp;
    const char* agg_write_rate_units;
    mfu_format_bw(write_rate, &agg_write_rate_tmp, &agg_write_rate_units);

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Compared %.3lf %s (%.0f%%) in %.3lf secs (%.3lf %s) %.0f secs left ...",
            agg_read_size_tmp, agg_read_size_units, percent, secs, agg_read_rate_tmp, agg_read_rate_units, secs_remaining);
#if 0
        MFU_LOG(MFU_LOG_INFO, "Updated %.3lf %s in %.3lf secs (%.3lf %s) ...",
            agg_write_size_tmp, agg_write_size_units, secs, agg_write_rate_tmp, agg_write_rate_units);
#endif
    } else {
        MFU_LOG(MFU_LOG_INFO, "Compared %.3lf %s (%.0f%%) in %.3lf secs (%.3lf %s) done",
            agg_read_size_tmp, agg_read_size_units, percent, secs, agg_read_rate_tmp, agg_read_rate_units);
#if 0
        MFU_LOG(MFU_LOG_INFO, "Updated %.3lf %s in %.3lf secs (%.3lf %s) done",
            agg_write_size_tmp, agg_write_size_units, secs, agg_write_rate_tmp, agg_write_rate_units);
#endif
    }
}

/* given a list of source/destination files to compare, spread file
 * sections to processes to compare in parallel, fill
 * in comparison results in source and dest string maps */
static void dsync_strmap_compare_data_link_dest(
    mfu_flist src_compare_list,
    mfu_flist link_compare_list,
    mfu_flist link_same_list,
    mfu_copy_opts_t* copy_opts,
    uint64_t* count_bytes_read,
    uint64_t* count_bytes_written,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume we'll succeed */
    int rc = 0;

    /* count number of bytes to compare to print percent progress and estimated time remaining */
    uint64_t idx;
    uint64_t size = mfu_flist_size(src_compare_list);
    uint64_t bytes = 0;
    for (idx = 0; idx < size; idx++) {
        /* count bytes from regular files */
        mfu_filetype type = mfu_flist_file_get_type(src_compare_list, idx);
        if (type == MFU_TYPE_FILE) {
            bytes += mfu_flist_file_get_size(src_compare_list, idx);
        }
    }

    /* double to account for source and destination bytes */
    bytes *= 2;

    /* get total for print percent progress while creating */
    dsync_total_count = 0;
    MPI_Allreduce(&bytes, &dsync_total_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* get chunk size for copying files (just hard-coded for now) */
    uint64_t chunk_size = copy_opts->chunk_size;

    /* get the linked list of file chunks for the src and dest */
    mfu_file_chunk* src_head = mfu_file_chunk_list_alloc(src_compare_list, chunk_size);
    mfu_file_chunk* dst_head = mfu_file_chunk_list_alloc(link_compare_list, chunk_size);

    /* get a count of how many items are the chunk list */
    uint64_t list_count = mfu_file_chunk_list_size(src_head);

    /* allocate a flag for each element in chunk list,
     * will store 0 to mean data of this chunk is the same 1 if different
     * to be used as input to logical OR to determine state of entire file */
    int* vals = (int*) MFU_MALLOC(list_count * sizeof(int));

    /* whether we should overwrite bytes in destination file during compare */
    int overwrite = 0;

    /* start progress messages when comparing data */
    uint64_t count_bytes[2];
    count_bytes[0] = *count_bytes_read;
    count_bytes[1] = *count_bytes_written;
    mfu_progress* compare_prog = mfu_progress_start(mfu_progress_timeout, 2, MPI_COMM_WORLD, compare_progress_fn);

    /* compare bytes for each file section and set flag based on what we find */
    uint64_t i = 0;
    const mfu_file_chunk* src_p = src_head;
    const mfu_file_chunk* dst_p = dst_head;
    for (i = 0; i < list_count; i++) {
        /* get offset into file that we should compare (bytes) */
        off_t offset = (off_t)src_p->offset;

        /* get length of section that we should compare (bytes) */
        off_t length = (off_t)src_p->length;
        
        /* get length of file that we should compare (bytes) */
        off_t filesize = (off_t)src_p->file_size;
        
        /* compare the contents of the files */
        int compare_rc = mfu_compare_contents(src_p->name, dst_p->name, offset, length, filesize,
                overwrite, copy_opts, count_bytes_read, count_bytes_written, compare_prog,
                mfu_src_file, mfu_dst_file);
        if (compare_rc == -1) {
            /* we hit an error while reading */
            rc = -1;
            MFU_LOG(MFU_LOG_ERR,
              "Failed to open, lseek, or read %s and/or %s. Assuming contents are different.",
                 src_p->name, dst_p->name);

            /* set flag to consider files to be different,
             * could actually be the same, but we'll draw attention to them this way */
            compare_rc = 1;
        }

        /* record results of comparison */
        vals[i] = compare_rc;

        /* update pointers for src and dest in linked list */
        src_p = src_p->next;
        dst_p = dst_p->next;
    }

    /* finalize progress messages */
    count_bytes[0] = *count_bytes_read;
    count_bytes[1] = *count_bytes_written;
    mfu_progress_complete(count_bytes, &compare_prog);

    /* allocate a flag for each item in our file list */
    int* results = (int*) MFU_MALLOC(size * sizeof(int));

    /* execute logical OR over chunks for each file */
    mfu_file_chunk_list_lor(src_compare_list, src_head, vals, results);

    /* unpack contents of recv buffer & store results in strmap */
    for (i = 0; i < size; i++) {
        /* get comparison results for this item */
        int flag = results[i];

        /* set flag in strmap to record status of file */
        if (flag == 0) {
            /* if same, add to same list */
	    mfu_flist_file_copy(link_compare_list, i, link_same_list);
        }
    }

    /* free memory */
    mfu_free(&results);
    mfu_free(&vals);
    mfu_file_chunk_list_free(&src_head);
    mfu_file_chunk_list_free(&dst_head);

    /* determine whether any process hit an error,
     * input is either 0 or -1, so MIN will return -1 if any */
    int all_rc;
    MPI_Allreduce(&rc, &all_rc, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    rc = all_rc;
}

static int dsync_strmap_compare_data(
    mfu_flist src_compare_list,
    strmap* src_map,
    mfu_flist dst_compare_list,
    strmap* dst_map,
    mfu_flist src_list,
    mfu_flist src_cp_list,
    mfu_flist dst_same_list,
    mfu_flist dst_remove_list,
    size_t strlen_prefix,
    bool use_hardlinks,
    mfu_copy_opts_t* copy_opts,
    uint64_t* count_bytes_read,
    uint64_t* count_bytes_written,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume we'll succeed */
    int rc = 0;

    /* count number of bytes to compare to print percent progress and estimated time remaining */
    uint64_t idx;
    uint64_t size = mfu_flist_size(src_compare_list);
    uint64_t bytes = 0;
    for (idx = 0; idx < size; idx++) {
        /* count bytes from regular files */
        mfu_filetype type = mfu_flist_file_get_type(src_compare_list, idx);
        if (type == MFU_TYPE_FILE) {
            bytes += mfu_flist_file_get_size(src_compare_list, idx);
        }
    }

    /* double to account for source and destination bytes */
    bytes *= 2;

    /* get total for print percent progress while creating */
    dsync_total_count = 0;
    MPI_Allreduce(&bytes, &dsync_total_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* get chunk size for copying files (just hard-coded for now) */
    uint64_t chunk_size = copy_opts->chunk_size;

    /* get the linked list of file chunks for the src and dest */
    mfu_file_chunk* src_head = mfu_file_chunk_list_alloc(src_compare_list, chunk_size);
    mfu_file_chunk* dst_head = mfu_file_chunk_list_alloc(dst_compare_list, chunk_size);

    /* get a count of how many items are the chunk list */
    uint64_t list_count = mfu_file_chunk_list_size(src_head);

    /* allocate a flag for each element in chunk list,
     * will store 0 to mean data of this chunk is the same 1 if different
     * to be used as input to logical OR to determine state of entire file */
    int* vals = (int*) MFU_MALLOC(list_count * sizeof(int));

    /* whether we should overwrite bytes in destination file during compare */
    int overwrite = 1;
    if (options.dry_run || use_hardlinks) {
        overwrite = 0;
    }

    /* start progress messages when comparing data */
    uint64_t count_bytes[2];
    count_bytes[0] = *count_bytes_read;
    count_bytes[1] = *count_bytes_written;
    mfu_progress* compare_prog = mfu_progress_start(mfu_progress_timeout, 2, MPI_COMM_WORLD, compare_progress_fn);

    /* compare bytes for each file section and set flag based on what we find */
    uint64_t i = 0;
    const mfu_file_chunk* src_p = src_head;
    const mfu_file_chunk* dst_p = dst_head;
    for (i = 0; i < list_count; i++) {
        /* get offset into file that we should compare (bytes) */
        off_t offset = (off_t)src_p->offset;

        /* get length of section that we should compare (bytes) */
        off_t length = (off_t)src_p->length;

        /* get length of file that we should compare (bytes) */
        off_t filesize = (off_t)src_p->file_size;
        
        /* compare the contents of the files */
        int compare_rc = mfu_compare_contents(src_p->name, dst_p->name, offset, length, filesize,
                overwrite, copy_opts, count_bytes_read, count_bytes_written, compare_prog,
                mfu_src_file, mfu_dst_file);
        if (compare_rc == -1) {
            /* we hit an error while reading */
            rc = -1;
            MFU_LOG(MFU_LOG_ERR,
              "Failed to open, lseek, or read %s and/or %s. Assuming contents are different.",
                 src_p->name, dst_p->name);

            /* set flag to consider files to be different,
             * could actually be the same, but we'll draw attention to them this way */
            compare_rc = 1;
        }

        /* record results of comparison */
        vals[i] = compare_rc;

        /* update pointers for src and dest in linked list */
        src_p = src_p->next;
        dst_p = dst_p->next;
    }

    /* finalize progress messages */
    count_bytes[0] = *count_bytes_read;
    count_bytes[1] = *count_bytes_written;
    mfu_progress_complete(count_bytes, &compare_prog);

    /* allocate a flag for each item in our file list */
    int* results = (int*) MFU_MALLOC(size * sizeof(int));

    /* execute logical OR over chunks for each file */
    mfu_file_chunk_list_lor(src_compare_list, src_head, vals, results);

    /* unpack contents of recv buffer & store results in strmap */
    for (i = 0; i < size; i++) {
        /* lookup name of file based on id to send to strmap updata call */
        const char* name = mfu_flist_file_get_name(src_compare_list, i);

        /* ignore prefix portion of path to use as key */
        name += strlen_prefix;

        /* get comparison results for this item */
        int flag = results[i];

        /* set flag in strmap to record status of file */
        if (flag != 0) {
            /* update to say contents of the files were found to be different */
            dsync_strmap_item_update(src_map, name, DCMPF_CONTENT, DCMPS_DIFFER);
            dsync_strmap_item_update(dst_map, name, DCMPF_CONTENT, DCMPS_DIFFER);

            /* mark file to be deleted from destination, copied from source */
            if (use_hardlinks) {
                mfu_flist_file_copy(dst_compare_list, i, dst_remove_list);
                mfu_flist_file_copy(src_compare_list, i, src_cp_list);
            }

            /* Note: File does not need to be truncated for syncing because the size
             * of the dst and src will be the same. It is one of the checks in
             * dsync_strmap_compare */
        } else {
            /* update to say contents of the files were found to be the same */
            dsync_strmap_item_update(src_map, name, DCMPF_CONTENT, DCMPS_COMMON);
            dsync_strmap_item_update(dst_map, name, DCMPF_CONTENT, DCMPS_COMMON);

            /* record that destination file matches source */
            if (use_hardlinks) {
                mfu_flist_file_copy(dst_compare_list, i, dst_same_list);
            }
        }
    }

    /* free memory */
    mfu_free(&results);
    mfu_free(&vals);
    mfu_file_chunk_list_free(&src_head);
    mfu_file_chunk_list_free(&dst_head);

    /* determine whether any process hit an error,
     * input is either 0 or -1, so MIN will return -1 if any */
    int all_rc;
    MPI_Allreduce(&rc, &all_rc, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    rc = all_rc;

    return rc;
}

/* given the list of files in the destination, the original list of files
 * to be copied to the destination, the list of files in the destination
 * that are the same as the source, and the list of files in link-dest
 * that are the same as the source, generate a list of files that will
 * actually be copied, a list of files that will be hardlinked, and append
 * to the list of files in the destination that will be deleted */
static void dsync_generate_real_lists(
    size_t src_strlen_prefix,   /* length of prefix string to source directory */
    const mfu_param_path *link_path, /* param path for link-dest directory */
    mfu_flist dst_list,         /* list of files in destination */
    strmap *dst_map,            /* map each file in destination to its index in dst_list */
    mfu_flist src_cp_list,      /* list of files to be copied to destination */
    mfu_flist dst_same_list,    /* list of files in destination that are same as in source */
    mfu_flist link_same_list,   /* list of files in link-dest that are same as in source */
    mfu_flist src_real_cp_list, /* OUT - list of files to actually be copied */
    mfu_flist link_dst_list,    /* OUT - list of files to be hardlinked from destination */
    mfu_flist dst_remove_list,  /* OUT - add to list of files to be deleted (replaced) from destination */
    mfu_file_t* mfu_dst_file)   /* I/O filesystem functions to use for destination */
{
    uint64_t idx;

    /* create map of item name to index in its respective list */
    strmap* link_same_map = dsync_strmap_creat(link_same_list, link_path->path);

    /* walk list of files we need to copy from source to destination,
     * and split into set that must actually be copied and set that
     * will be hardlinked instead */
    uint64_t cp_size = mfu_flist_size(src_cp_list);
    for (idx = 0; idx < cp_size; idx++) {
        /* get file name of this item */
        const char* name = mfu_flist_file_get_name(src_cp_list, idx);

        /* ignore prefix portion of path to use as key */
        name += src_strlen_prefix;
    
        /* can't hardlink to directories or symlinks, we must actually copy them */
        mfu_filetype type = mfu_flist_file_get_type(src_cp_list, idx);
        if (type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            mfu_flist_file_copy(src_cp_list, idx, src_real_cp_list);
            continue;
        }

        /* if item is in copy list, check whether the version in link-dest
         * is the same, if so, we'll create a hardlink,
         * otherwise we need to make a fresh copy */
        uint64_t index;
        int rc = dsync_strmap_item_index(link_same_map, name, &index);
        if (rc >= 0) {
            /* file in link-dest is same as source,
             * create a hardlink in destination */
            mfu_flist_file_copy(link_same_list, index, link_dst_list);
        } else {
            /* we'll actually copy this file */
            mfu_flist_file_copy(src_cp_list, idx, src_real_cp_list);
        }
    }

    /* walk list of items in destination that are the same as the source,
     * if they are files and they are also the same as the corresponding item
     * in link-dest, remove them from destination and replace them with hardlinks */
    uint64_t ignore_size = mfu_flist_size(dst_same_list);
    for (idx = 0; idx < ignore_size; idx++) {
        /* get file name of this item */
        const char* name = mfu_flist_file_get_name(dst_same_list, idx);

        /* ignore prefix portion of path to use as key */
        name += src_strlen_prefix;

        /* can't hardlink to directories or symlinks, we must actually copy them */
        mfu_filetype type = mfu_flist_file_get_type(dst_same_list, idx);
        if (type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            continue;
        }

        /* if item in destination is the same as the source file
         * and if item in link-dest is also the same as the source file,
         * remove existing item at destination and replace with hardlink,
         * otherwise, do nothing */
        uint64_t index;
        int rc = dsync_strmap_item_index(link_same_map, name, &index);
        if (rc >= 0) {
            /* get index of item in destination list */
            uint64_t dst_index;
            rc = dsync_strmap_item_index(dst_map, name, &dst_index);
            assert(rc >= 0);

            /* get full path to destination and link-dest */
            const char* dst_name = mfu_flist_file_get_name(dst_list, dst_index);
            const char* link_dst_name = mfu_flist_file_get_name(link_same_list, index);

            /* skip if the target is already a link to link_dest */
            struct stat dst_st, link_dst_st;
            rc = mfu_file_lstat(dst_name, &dst_st, mfu_dst_file);
            if (!rc) {
                rc = mfu_file_lstat(link_dst_name, &link_dst_st, mfu_dst_file);
                if (!rc &&
                    dst_st.st_ino == link_dst_st.st_ino &&
                    dst_st.st_dev == link_dst_st.st_dev)
                {
                    continue;
                }
            }

            /* remove item from destination, and replace with a hardlink */
            mfu_flist_file_copy(link_same_list, index, link_dst_list);
            mfu_flist_file_copy(dst_list, dst_index, dst_remove_list);
        }
    }

    /* finalize the lists we just added items to */
    mfu_flist_summarize(src_real_cp_list);
    mfu_flist_summarize(link_dst_list);
    mfu_flist_summarize(dst_remove_list);

    /* free the map */
    strmap_delete(&link_same_map);
}

/* given a list of source/destination files to compare, spread file
 * sections to processes to compare in parallel, fill
 * in comparison results in source and dest string maps */
static void dsync_strmap_compare_lite_link_dest(
    mfu_flist src_compare_list,
    mfu_flist link_compare_list,
    mfu_flist link_same_list)
{
    /* get size of source and destination compare lists */
    uint64_t size = mfu_flist_size(src_compare_list);

    /* check size and mtime of each item */
    uint64_t idx;
    for (idx = 0; idx < size; idx++) {
        /* get file sizes */
        uint64_t src_size = mfu_flist_file_get_size(src_compare_list, idx);
        uint64_t dst_size = mfu_flist_file_get_size(link_compare_list, idx);

        /* get mtime seconds and nsecs */
        uint64_t src_mtime      = mfu_flist_file_get_mtime(src_compare_list, idx);
        uint64_t src_mtime_nsec = mfu_flist_file_get_mtime_nsec(src_compare_list, idx);
        uint64_t dst_mtime      = mfu_flist_file_get_mtime(link_compare_list, idx);
        uint64_t dst_mtime_nsec = mfu_flist_file_get_mtime_nsec(link_compare_list, idx);

        /* if size and mtime are the same, we assume the file contents are same */
        if ((src_size == dst_size) &&
            (src_mtime == dst_mtime) && (src_mtime_nsec == dst_mtime_nsec))
        {
            mfu_flist_file_copy(link_compare_list, idx, link_same_list);
        }
    }
}

/* given a list of source/destination files to compare, spread file
 * sections to processes to compare in parallel, fill
 * in comparison results in source and dest string maps */
static int dsync_strmap_compare_lite(
    mfu_flist src_compare_list,
    mfu_flist src_cp_list,
    mfu_flist dst_same_list,
    strmap* src_map,
    mfu_flist dst_compare_list,
    mfu_flist dst_remove_list,
    strmap* dst_map,
    size_t strlen_prefix,
    bool use_hardlinks)
{
    /* assume we'll succeed */
    int rc = 0;

    /* get size of source and destination compare lists */
    uint64_t size = mfu_flist_size(src_compare_list);

    /* check size and mtime of each item */
    uint64_t idx;
    for (idx = 0; idx < size; idx++) {
        /* lookup name of file based on id to send to strmap updata call */
        const char* name = mfu_flist_file_get_name(src_compare_list, idx);

        /* ignore prefix portion of path to use as key */
        name += strlen_prefix;

        /* get file sizes */
        uint64_t src_size = mfu_flist_file_get_size(src_compare_list, idx);
        uint64_t dst_size = mfu_flist_file_get_size(dst_compare_list, idx);

        /* get mtime seconds and nsecs */
        uint64_t src_mtime      = mfu_flist_file_get_mtime(src_compare_list, idx);
        uint64_t src_mtime_nsec = mfu_flist_file_get_mtime_nsec(src_compare_list, idx);
        uint64_t dst_mtime      = mfu_flist_file_get_mtime(dst_compare_list, idx);
        uint64_t dst_mtime_nsec = mfu_flist_file_get_mtime_nsec(dst_compare_list, idx);

        /* if size or mtime is different, we assume the file contents are different */
        if ((src_size != dst_size) ||
            (src_mtime != dst_mtime) || (src_mtime_nsec != dst_mtime_nsec))
        {
            /* update to say contents of the files were found to be different */
            dsync_strmap_item_update(src_map, name, DCMPF_CONTENT, DCMPS_DIFFER);
            dsync_strmap_item_update(dst_map, name, DCMPF_CONTENT, DCMPS_DIFFER);

            /* mark file to be deleted from destination, copied from source */
            if (!options.dry_run || use_hardlinks) {
                mfu_flist_file_copy(dst_compare_list, idx, dst_remove_list);
                mfu_flist_file_copy(src_compare_list, idx, src_cp_list);
            }
        } else {
            /* update to say contents of the files were found to be the same */
            dsync_strmap_item_update(src_map, name, DCMPF_CONTENT, DCMPS_COMMON);
            dsync_strmap_item_update(dst_map, name, DCMPF_CONTENT, DCMPS_COMMON);

            /* record that detination file matches source */
            if (use_hardlinks) {
                mfu_flist_file_copy(dst_compare_list, idx, dst_same_list);
            }
        }
    }

    return rc;
}

static void print_comparison_stats(
    mfu_flist src_list,
    double start_compare,
    double end_compare,
    time_t *time_started,
    time_t *time_ended,
    uint64_t num_files,
    uint64_t bytes_read,
    uint64_t bytes_written)
{
    /* get total number of bytes across all processes */
    uint64_t total_bytes_read, total_bytes_written;
    MPI_Allreduce(&bytes_read,    &total_bytes_read,    1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&bytes_written, &total_bytes_written, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* if the verbose option is set print the timing data
     * report compare count, time, and rate */
    if (mfu_rank == 0) {
       /* get the amount of time the compare function took */
       double time_diff = end_compare - start_compare;

       /* calculate byte and file rate */
       double file_rate  = 0.0;
       double read_rate  = 0.0;
       double write_rate = 0.0;
       if (time_diff > 0.0) {
           file_rate  = ((double)num_files) / time_diff;
           read_rate  = ((double)total_bytes_read)    / time_diff;
           write_rate = ((double)total_bytes_written) / time_diff;
       }

       /* convert uint64 to strings for printing to user */
       char starttime_str[256];
       char endtime_str[256];

       struct tm* localstart = localtime(time_started);
       struct tm cp_localstart = *localstart;
       struct tm* localend = localtime(time_ended);
       struct tm cp_localend = *localend;

       strftime(starttime_str, 256, "%b-%d-%Y, %H:%M:%S", &cp_localstart);
       strftime(endtime_str, 256, "%b-%d-%Y, %H:%M:%S", &cp_localend);

       /* convert read size to units */
       double read_size_tmp;
       const char* read_size_units;
       mfu_format_bytes(total_bytes_read, &read_size_tmp, &read_size_units);

       /* convert write size to units */
       double write_size_tmp;
       const char* write_size_units;
       mfu_format_bytes(total_bytes_written, &write_size_tmp, &write_size_units);

       /* convert read bandwidth to units */
       double read_rate_tmp;
       const char* read_rate_units;
       mfu_format_bw(read_rate, &read_rate_tmp, &read_rate_units);

       /* convert write bandwidth to units */
       double write_rate_tmp;
       const char* write_rate_units;
       mfu_format_bw(write_rate, &write_rate_tmp, &write_rate_units);

       MFU_LOG(MFU_LOG_INFO, "Started   : %s", starttime_str);
       MFU_LOG(MFU_LOG_INFO, "Completed : %s", endtime_str);
       MFU_LOG(MFU_LOG_INFO, "Seconds   : %.3lf", time_diff);
       MFU_LOG(MFU_LOG_INFO, "Items     : %" PRId64, num_files);
       MFU_LOG(MFU_LOG_INFO, "Item Rate : %lu items in %f seconds (%f items/sec)",
            num_files, time_diff, file_rate);
       if (options.contents) {
           MFU_LOG(MFU_LOG_INFO, "Bytes read   : %.3lf %s (%" PRId64 " bytes)",
                read_size_tmp, read_size_units, total_bytes_read);
           MFU_LOG(MFU_LOG_INFO, "Bytes written: %.3lf %s (%" PRId64 " bytes)",
                write_size_tmp, write_size_units, total_bytes_written);
           MFU_LOG(MFU_LOG_INFO, "Read Rate    : %.3lf %s (%" PRId64 " bytes in %.3lf seconds)",
                read_rate_tmp, read_rate_units, total_bytes_read, time_diff);
           MFU_LOG(MFU_LOG_INFO, "Write Rate   : %.3lf %s (%" PRId64 " bytes in %.3lf seconds)",
            write_rate_tmp, write_rate_units, total_bytes_written, time_diff);
       }
    }
}

/* loop on the dest map to check for files only in the dst list
 * and copy to a remove_list for the --sync option */
static void dsync_only_dst(strmap* src_map,
    strmap* dst_map, mfu_flist dst_list, mfu_flist dst_remove_list)
{
    /* iterate over each item in dest map */
    const strmap_node* node;
    strmap_foreach(dst_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of destination file */
        uint64_t dst_index;
        int ret = dsync_strmap_item_index(dst_map, key, &dst_index);
        assert(ret == 0);

        /* get index of source file */
        uint64_t src_index;
        ret = dsync_strmap_item_index(src_map, key, &src_index);
        if (ret) {
            /* This file only exist in dest */
            mfu_flist_file_copy(dst_list, dst_index, dst_remove_list);
        }
    }
}

static int dsync_sync_files(
    strmap* src_map,
    strmap* dst_map,
    const mfu_param_path* src_path,
    const mfu_param_path* dest_path,
    const mfu_param_path* link_path,
    mfu_flist dst_list,
    mfu_flist dst_remove_list,
    mfu_flist link_dst_list,
    mfu_flist src_cp_list,
    mfu_copy_opts_t* copy_opts,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume we'll succeed */
    int rc = 0;
    int tmp_rc;

    /* get our rank and number of ranks */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Parse the source and destination paths. */
    int valid, copy_into_dir;
    mfu_param_path_check_copy(1, src_path, dest_path, mfu_src_file, mfu_dst_file,
                              copy_opts->no_dereference, &valid, &copy_into_dir);
    if (!valid) {
        /* TODO we may want to pass a special error to the caller to
         * "exit" instead of continuing. */
        return -1;
    }

    /* record copy_into_dir flag result from check_copy into
     * mfu copy options structure */
    copy_opts->copy_into_dir = copy_into_dir;

    /* get files that are only in the destination directory */
    if (options.delete) {
        dsync_only_dst(src_map, dst_map, dst_list, dst_remove_list);
    }

    /* summarize dst remove list and remove files */
    mfu_flist_summarize(dst_remove_list);

    /* delete files from destination if needed */
    uint64_t remove_size = mfu_flist_global_size(dst_remove_list);
    if (remove_size > 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Deleting items from destination");
        }
        mfu_flist_unlink(dst_remove_list, 0, mfu_dst_file);
    }

    /* summarize the src copy list for files
     * that need to be copied into dest directory */
    mfu_flist_summarize(src_cp_list);

    /* copy files from source to destination if needed */
    uint64_t copy_size = mfu_flist_global_size(src_cp_list);
    if (copy_size > 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Copying items to destination");
        }
        tmp_rc = mfu_flist_copy(src_cp_list, 1, src_path, dest_path, copy_opts,
                                mfu_src_file, mfu_dst_file);
        if (tmp_rc < 0) {
            rc = -1;
        }
    }

    if (link_path) {
        /* summarize the link dst list for files 
         * that need to be copied into dest directory */ 
        mfu_flist_summarize(link_dst_list); 

        /* link files from link dest to destination if needed */ 
        uint64_t link_size = mfu_flist_global_size(link_dst_list);
        if (link_size > 0) {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Linking items in destination");
            }
            tmp_rc = mfu_flist_hardlink(link_dst_list, link_path, dest_path,
                                        copy_opts, mfu_src_file, mfu_dst_file);
            if (tmp_rc < 0) {
                rc = -1;
            }
        }
    }

    return rc;
}

/* compare entries from src to items in link-dest */
static int dsync_strmap_compare_link_dest(
    mfu_flist src_list,
    strmap* src_map,
    mfu_flist link_list,
    strmap* link_map,
    mfu_flist link_same_list,
    mfu_copy_opts_t* copy_opts,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume we'll succeed */
    int rc = 0;
    int tmp_rc;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);

    /* create lists to track files whose content must be checked */
    mfu_flist src_compare_list = mfu_flist_subset(src_list);
    mfu_flist link_compare_list = mfu_flist_subset(link_list);

    /* iterate over each item in source map */
    const strmap_node* node;
    strmap_foreach(src_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of source file */
        uint64_t src_index;
        tmp_rc = dsync_strmap_item_index(src_map, key, &src_index);
        assert(tmp_rc == 0);

        /* get index of destination file */
        uint64_t dst_index;
        tmp_rc = dsync_strmap_item_index(link_map, key, &dst_index);
        if (tmp_rc) {
            /* skip uncommon files, all other states are DCMPS_INIT */
            continue;
        }

        tmp_rc = dsync_compare_metadata(src_list, src_map, src_index,
             link_list, link_map, dst_index,
             key);
        assert(tmp_rc >= 0);

        /* Skip if no need to compare type.
         * All the following comparison depends on type. */
        if (!dsync_option_need_compare(DCMPF_TYPE)) {
            continue;
        }

        /* get modes of files */
        mode_t src_mode = (mode_t) mfu_flist_file_get_mode(src_list,
            src_index);
        mode_t dst_mode = (mode_t) mfu_flist_file_get_mode(link_list,
            dst_index);

        /* check whether files are of the same type */
        if ((src_mode & S_IFMT) != (dst_mode & S_IFMT)) {
            continue;
        }

        /* Skip if no need to compare content. */
        if (!dsync_option_need_compare(DCMPF_CONTENT)) {
            continue;
        }

        /* for now, we can only compare content of regular files */
        /* TODO: add support for symlinks */
        if (! S_ISREG(dst_mode)) {
            continue;
        }

        /* check whether the file sizes are the same */
        dsync_state state;
        tmp_rc = dsync_strmap_item_state(src_map, key, DCMPF_SIZE, &state);
        assert(tmp_rc == 0);
        if (state == DCMPS_DIFFER) {
            continue;
        }

        /* If we get to this point, we need to open files and compare
         * file contents.  We'll first identify all such files so that
         * we can do this comparison in parallel more effectively.  For
         * now copy these files to the list of files we need to compare. */

        /* make a copy of the src and dest files where the data needs
         * to be compared and store in src & dest compare lists */
        mfu_flist_file_copy(src_list, src_index, src_compare_list);
        mfu_flist_file_copy(link_list, dst_index, link_compare_list);
    }

    /* summarize lists of files for which we need to compare data contents */
    mfu_flist_summarize(src_compare_list);
    mfu_flist_summarize(link_compare_list);

    /* initalize total_bytes_read to zero */
    uint64_t total_files         = 0;
    uint64_t total_bytes_read    = 0;
    uint64_t total_bytes_written = 0;

    /* compare the contents of the files if we have anything in the compare list */
    uint64_t cmp_global_size = mfu_flist_global_size(src_compare_list);
    if (cmp_global_size > 0) {
        total_files = cmp_global_size;
        if (options.contents) {
            /* comparing contents could take a while */
            if (rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Comparing file contents of %llu items with link dest", total_files);
            }

            /* compare file contents byte-by-byte, overwrites destination
             * file in place if found to be different during comparison */
            dsync_strmap_compare_data_link_dest(src_compare_list,
                link_compare_list, link_same_list, copy_opts,
                &total_bytes_read, &total_bytes_written,
                mfu_src_file, mfu_dst_file
            );
        } else {
            /* comparing contents could take a while */
            if (rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Comparing file sizes and modification times of %llu items with link dest", total_files);
            }

            /* assume contents are different if size or mtime are different,
             * adds files to remove and copy lists if different */
            dsync_strmap_compare_lite_link_dest(src_compare_list,
                link_compare_list, link_same_list
            );
        }
    }

    /* complete the list of same items */
    mfu_flist_summarize(link_same_list);

    /* wait for all procs to finish before stopping timer */
    MPI_Barrier(MPI_COMM_WORLD);

    /* determine whether any process hit an error,
     * input is either 0 or -1, so MIN will return -1 if any */
    int all_rc;
    MPI_Allreduce(&rc, &all_rc, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    rc = all_rc;

    return rc;
}

/* compare entries from src into dst */
static int dsync_strmap_compare(
    mfu_flist src_list,
    strmap* src_map,
    mfu_flist dst_list,
    strmap* dst_map,
    mfu_flist link_list,
    strmap* link_map,
    size_t strlen_prefix,
    mfu_copy_opts_t* copy_opts,
    const mfu_param_path* src_path,
    const mfu_param_path* dest_path,
    const mfu_param_path* link_path,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume we'll succeed */
    int rc = 0;
    int tmp_rc;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);

    time_t   time_started;
    time_t   time_ended;

    double start_compare = MPI_Wtime();
    time(&time_started);

    /* create lists to track files whose content must be checked */
    mfu_flist src_compare_list = mfu_flist_subset(src_list);
    mfu_flist dst_compare_list = mfu_flist_subset(dst_list);

    /* list to track files to be copied from source */
    mfu_flist src_cp_list     = mfu_flist_subset(src_list);

    /* list to track files to be deleted from destination */
    mfu_flist dst_remove_list = mfu_flist_subset(dst_list);

    /* list to track files that are the same in destination and source directories */
    mfu_flist dst_same_list = MFU_FLIST_NULL;

    /* list to track files tha are the same in link-dest and source directories */
    mfu_flist link_same_list = MFU_FLIST_NULL;

    /* list to track files to be linked (hard-link) rather than copied to destination */
    mfu_flist link_dst_list = MFU_FLIST_NULL;

    /* list to track files that must be copied to destination, after accounting for hardlinks */
    mfu_flist src_real_cp_list = MFU_FLIST_NULL;

    /* allocate lists to manage links */
    if (link_path != NULL) {
        dst_same_list    = mfu_flist_subset(src_list);
        link_same_list   = mfu_flist_subset(link_list);
        link_dst_list    = mfu_flist_subset(link_list);
        src_real_cp_list = mfu_flist_subset(src_list);
    }

    /* use a map as a list to record source and destination indices
     * for entries that need a refresh on metadata */
    strmap* metadata_refresh = strmap_new();

    /* iterate over each item in source map */
    const strmap_node* node;
    strmap_foreach(src_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of source file */
        uint64_t src_index;
        tmp_rc = dsync_strmap_item_index(src_map, key, &src_index);
        assert(tmp_rc == 0);

        /* get index of destination file */
        uint64_t dst_index;
        tmp_rc = dsync_strmap_item_index(dst_map, key, &dst_index);
        if (tmp_rc) {
            /* item only exists in the source */
            dsync_strmap_item_update(src_map, key, DCMPF_EXIST, DCMPS_ONLY_SRC);

            /* add items only in src directory into src copy list,
             * will be later copied into dst dir */
            if (!options.dry_run) {
                mfu_flist_file_copy(src_list, src_index, src_cp_list);
            }

            /* skip uncommon files, all other states are DCMPS_INIT */
            continue;
        }


        /* item exists in both source and destination,
         * so update our state to record that fact */
        dsync_strmap_item_update(src_map, key, DCMPF_EXIST, DCMPS_COMMON);
        dsync_strmap_item_update(dst_map, key, DCMPF_EXIST, DCMPS_COMMON);

        tmp_rc = dsync_compare_metadata(src_list, src_map, src_index,
             dst_list, dst_map, dst_index,
             key);
        assert(tmp_rc >= 0);

        /* add any item that is in both source and destination to meta
         * refresh list, only include those that have different metadata. */
        dsync_state uid_state, gid_state, perm_state, atime_state, mtime_state;
        tmp_rc = dsync_strmap_item_state(src_map, key, DCMPF_UID, &uid_state);
        assert(tmp_rc == 0);
        tmp_rc = dsync_strmap_item_state(src_map, key, DCMPF_GID, &gid_state);
        assert(tmp_rc == 0);
        tmp_rc = dsync_strmap_item_state(src_map, key, DCMPF_PERM, &perm_state);
        assert(tmp_rc == 0);
        tmp_rc = dsync_strmap_item_state(src_map, key, DCMPF_ATIME, &atime_state);
        assert(tmp_rc == 0);
        tmp_rc = dsync_strmap_item_state(src_map, key, DCMPF_MTIME, &mtime_state);
        assert(tmp_rc == 0);
        if ((uid_state == DCMPS_DIFFER) || (gid_state == DCMPS_DIFFER) ||
            (perm_state == DCMPS_DIFFER) || (atime_state == DCMPS_DIFFER) ||
            (mtime_state == DCMPS_DIFFER)) {
            strmap_setf(metadata_refresh, "%llu=%llu", src_index, dst_index);
        }

        /* Skip if no need to compare type.
         * All the following comparison depends on type. */
        if (!dsync_option_need_compare(DCMPF_TYPE)) {
            continue;
        }

        /* get modes of files */
        mode_t src_mode = (mode_t) mfu_flist_file_get_mode(src_list,
            src_index);
        mode_t dst_mode = (mode_t) mfu_flist_file_get_mode(dst_list,
            dst_index);

        /* check whether files are of the same type */
        if ((src_mode & S_IFMT) != (dst_mode & S_IFMT)) {
            /* file type is different, no need to go any futher */
            dsync_strmap_item_update(src_map, key, DCMPF_TYPE, DCMPS_DIFFER);
            dsync_strmap_item_update(dst_map, key, DCMPF_TYPE, DCMPS_DIFFER);

            /* if the types are different we need to make sure we delete the
             * file of the same name in the dst dir, and copy the type in
             * the src dir to the dst directory */
            if (!options.dry_run) {
                mfu_flist_file_copy(src_list, src_index, src_cp_list);
                mfu_flist_file_copy(dst_list, dst_index, dst_remove_list);
            }

            if (!dsync_option_need_compare(DCMPF_CONTENT)) {
                continue;
            }

            /* take them as differ content */
            dsync_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dsync_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            continue;
        }

        /* record that items have same type in source and destination */
        dsync_strmap_item_update(src_map, key, DCMPF_TYPE, DCMPS_COMMON);
        dsync_strmap_item_update(dst_map, key, DCMPF_TYPE, DCMPS_COMMON);

        /* Skip if no need to compare content. */
        if (!dsync_option_need_compare(DCMPF_CONTENT)) {
            continue;
        }

        /* for now, we can only compare content of regular files */
        /* TODO: add support for symlinks */
        if (! S_ISREG(dst_mode)) {
            /* not regular file, take them as common content */
            dsync_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            dsync_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            continue;
        }

        /* first check whether file sizes match */
        dsync_state state;
        tmp_rc = dsync_strmap_item_state(src_map, key, DCMPF_SIZE, &state);
        assert(tmp_rc == 0);
        if (state == DCMPS_DIFFER) {
            /* file size is different, their contents should be different */
            dsync_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dsync_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);

            /* if the file sizes are different then we need to remove the file in
             * the dst directory, and replace it with the one in the src directory */
            if (!options.dry_run) {
                mfu_flist_file_copy(src_list, src_index, src_cp_list);
                mfu_flist_file_copy(dst_list, dst_index, dst_remove_list);
            }

            continue;
        }

        /* If we get to this point, we need to open files and compare
         * file contents.  We'll first identify all such files so that
         * we can do this comparison in parallel more effectively.  For
         * now copy these files to the list of files we need to compare. */

        /* make a copy of the src and dest files where the data needs
         * to be compared and store in src & dest compare lists */
        mfu_flist_file_copy(src_list, src_index, src_compare_list);
        mfu_flist_file_copy(dst_list, dst_index, dst_compare_list);
    }

    /* summarize lists of files for which we need to compare data contents */
    mfu_flist_summarize(src_compare_list);
    mfu_flist_summarize(dst_compare_list);

    /* initalize total_bytes_read to zero */
    uint64_t total_files         = 0;
    uint64_t total_bytes_read    = 0;
    uint64_t total_bytes_written = 0;

    /* compare the contents of the files if we have anything in the compare list */
    uint64_t cmp_global_size = mfu_flist_global_size(src_compare_list);
    if (cmp_global_size > 0) {
        total_files = cmp_global_size;

        /* do not overwrite files, just compare if hardlinks enabled */
        bool use_hardlinks = (link_path != NULL);

        if (options.contents) {
            /* comparing contents could take a while */
            if (rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Comparing file contents of %llu items", total_files);
            }

            /* compare file contents byte-by-byte, overwrites destination
             * file in place if found to be different during comparison
             * and hardlinks are not enabled */
            tmp_rc = dsync_strmap_compare_data(src_compare_list, src_map,
                dst_compare_list, dst_map, src_list, src_cp_list, dst_same_list,
                dst_remove_list, strlen_prefix, use_hardlinks, copy_opts,
                &total_bytes_read, &total_bytes_written,
                mfu_src_file, mfu_dst_file
            );
            if (tmp_rc < 0) {
                rc = -1;
            }
        } else {
            /* comparing contents could take a while */
            if (rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Comparing file sizes and modification times of %llu items", total_files);
            }

            /* assume contents are different if size or mtime are different,
             * adds files to remove and copy lists if different */
            tmp_rc = dsync_strmap_compare_lite(src_compare_list, src_cp_list, dst_same_list,
                src_map, dst_compare_list, dst_remove_list, dst_map,
                strlen_prefix, use_hardlinks
            );
            if (tmp_rc < 0) {
                rc = -1;
            }
        }
    }

    /* wait for all procs to finish before stopping timer */
    MPI_Barrier(MPI_COMM_WORLD);

    if (link_path != NULL) {
        /* compare files in source and link-dest and create list of items
         * that are the same */
        rc = dsync_strmap_compare_link_dest(src_list, src_map,
            link_list, link_map, link_same_list, copy_opts,
            mfu_src_file, mfu_dst_file);

        /* of the items to be copied, some may be actual copies,
         * and some may be hardlinks, we'll break the copy list
         * into a real_cp list and a link_dst list so finalize it */
        mfu_flist_summarize(src_cp_list);

        /* identify set of items that must really be copied and those
         * which can be hardlinked, including existing files in destination
         * that can be removed and hardlinked */
        dsync_generate_real_lists(strlen_prefix, link_path, dst_list, dst_map,
            src_cp_list, dst_same_list, link_same_list,
            src_real_cp_list, link_dst_list, dst_remove_list, mfu_dst_file);
    }

    /* wait for all procs to finish before stopping timer */
    MPI_Barrier(MPI_COMM_WORLD);

    double end_compare = MPI_Wtime();
    time(&time_ended);

    /* print time, bytes read, and bandwidth */
    if (mfu_debug_level >= MFU_LOG_VERBOSE) {
        print_comparison_stats(src_list, start_compare, end_compare,
            &time_started, &time_ended, total_files, total_bytes_read, total_bytes_written
        );
    }

    /* remove the files from the destination list that are not
     * in the src list. Then, we copy the files that are only
     * in the src list into the destination list. */

    if (!options.dry_run) {
        /* pick the original copy list or the filtered one depending on
         * whether files are to be hardlinked in destination */
        mfu_flist cp_list = src_cp_list;
        if (link_path != NULL) {
            cp_list = src_real_cp_list;
        }

        /* sync the files that are in the source and destination directories */
        tmp_rc = dsync_sync_files(src_map, dst_map,
            src_path, dest_path, link_path, dst_list, dst_remove_list,
            link_dst_list, cp_list, copy_opts, mfu_src_file, mfu_dst_file);
        if (tmp_rc < 0) {
            rc = -1;
        }

        /* NOTE: this will set metadata on any files that were deleted from
         * the destination and copied fresh from the source a second time,
         * which is not efficient, but should still be correct */

        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Updating timestamps on newly copied files");
        }

        /* update metadata on files */
        strmap_foreach(metadata_refresh, node) {
            /* extract source and destination indices */
            unsigned long long src_i, dst_i;
            const char* key = strmap_node_key(node);
            const char* val = strmap_node_value(node);
            sscanf(key, "%llu", &src_i);
            sscanf(val, "%llu", &dst_i);
            uint64_t src_index = (uint64_t) src_i;
            uint64_t dst_index = (uint64_t) dst_i;

            /* copy metadata values from source to destination, if needed */
            tmp_rc = mfu_flist_file_sync_meta(src_list, src_index, dst_list,
                                              dst_index, mfu_dst_file);
            if (tmp_rc < 0) {
                rc = -1;
            }
        }
    }

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Completed updating timestamps");
    }

    /* done with our list of files for refreshing metadata */
    strmap_delete(&metadata_refresh);

    /* free lists used for removing and copying files */
    mfu_flist_free(&dst_remove_list);
    mfu_flist_free(&src_cp_list);

    /* free the compare flists */
    mfu_flist_free(&dst_compare_list);
    mfu_flist_free(&src_compare_list);

    /* free lists used for hardlinks */
    if (link_path) {
        mfu_flist_free(&dst_same_list);
        mfu_flist_free(&link_same_list);
        mfu_flist_free(&link_dst_list);
        mfu_flist_free(&src_real_cp_list);
    }

    /* determine whether any process hit an error,
     * input is either 0 or -1, so MIN will return -1 if any */
    int all_rc;
    MPI_Allreduce(&rc, &all_rc, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    rc = all_rc;

    return rc;
}

/* loop on the src map to check the results */
static void dsync_strmap_check_src(strmap* src_map,
                                  strmap* dst_map)
{
    assert(dsync_option_need_compare(DCMPF_EXIST));
    /* iterate over each item in source map */
    const strmap_node* node;
    strmap_foreach(src_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);
        int only_src = 0;

        /* get index of source file */
        uint64_t src_index;
        int ret = dsync_strmap_item_index(src_map, key, &src_index);
        assert(ret == 0);

        /* get index of destination file */
        uint64_t dst_index;
        ret = dsync_strmap_item_index(dst_map, key, &dst_index);
        if (ret) {
            only_src = 1;
        }

        /* First check exist state */
        dsync_state src_exist_state;
        ret = dsync_strmap_item_state(src_map, key, DCMPF_EXIST,
            &src_exist_state);
        assert(ret == 0);

        dsync_state dst_exist_state;
        ret = dsync_strmap_item_state(dst_map, key, DCMPF_EXIST,
            &dst_exist_state);
        if (only_src) {
            assert(ret);
        } else {
            assert(ret == 0);
        }

        if (only_src) {
            /* This file never checked for dest */
            assert(src_exist_state == DCMPS_ONLY_SRC);
        } else {
            assert(src_exist_state == dst_exist_state);
            assert(dst_exist_state == DCMPS_COMMON);
        }

        dsync_field field;
        for (field = 0; field < DCMPF_MAX; field++) {
            if (field == DCMPF_EXIST) {
                continue;
            }

            /* get state of src and dest */
            dsync_state src_state;
            ret = dsync_strmap_item_state(src_map, key, field, &src_state);
            assert(ret == 0);

            dsync_state dst_state;
            ret = dsync_strmap_item_state(dst_map, key, field, &dst_state);
            if (only_src) {
                assert(ret);
            } else {
                assert(ret == 0);
            }

            if (only_src) {
                /* all states are not checked */
                assert(src_state == DCMPS_INIT);
            } else {
                /* all stats of source and dest are the same */
                assert(src_state == dst_state);
                /* all states are either common, differ or skiped */
                if (dsync_option_need_compare(field)) {
                    assert(src_state == DCMPS_COMMON || src_state == DCMPS_DIFFER);
                } else {
                    // XXXX
                    if (src_state != DCMPS_INIT) {
                        printf("XXX %s wrong state %s\n", dsync_field_to_string(field, 1), dsync_state_to_string(src_state, 1));
                    }
                    assert(src_state == DCMPS_INIT);
                }
            }
        }
    }
}

/* loop on the dest map to check the results */
static void dsync_strmap_check_dst(strmap* src_map,
    strmap* dst_map)
{
    assert(dsync_option_need_compare(DCMPF_EXIST));

    /* iterate over each item in dest map */
    const strmap_node* node;
    strmap_foreach(dst_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);
        int only_dest = 0;

        /* get index of destination file */
        uint64_t dst_index;
        int ret = dsync_strmap_item_index(dst_map, key, &dst_index);
        assert(ret == 0);

        /* get index of source file */
        uint64_t src_index;
        ret = dsync_strmap_item_index(src_map, key, &src_index);
        if (ret) {
            /* This file only exist in dest */
            only_dest = 1;
        }

        /* First check exist state */
        dsync_state src_exist_state;
        ret = dsync_strmap_item_state(src_map, key, DCMPF_EXIST,
            &src_exist_state);
        if (only_dest) {
            assert(ret);
        } else {
            assert(ret == 0);
        }

        dsync_state dst_exist_state;
        ret = dsync_strmap_item_state(dst_map, key, DCMPF_EXIST,
            &dst_exist_state);
        assert(ret == 0);

        if (only_dest) {
            /* This file never checked for dest */
            assert(dst_exist_state == DCMPS_INIT);
        } else {
            assert(src_exist_state == dst_exist_state);
            assert(dst_exist_state == DCMPS_COMMON ||
                dst_exist_state == DCMPS_ONLY_SRC);
        }

        dsync_field field;
        for (field = 0; field < DCMPF_MAX; field++) {
            if (field == DCMPF_EXIST) {
                continue;
            }

            /* get state of src and dest */
            dsync_state src_state;
            ret = dsync_strmap_item_state(src_map, key, field,
                &src_state);
            if (only_dest) {
                assert(ret);
            } else {
                assert(ret == 0);
            }

            dsync_state dst_state;
            ret = dsync_strmap_item_state(dst_map, key, field,
                &dst_state);
            assert(ret == 0);

            if (only_dest) {
                /* all states are not checked */
                assert(dst_state == DCMPS_INIT);
            } else {
                assert(src_state == dst_state);
                /* all states are either common, differ or skiped */
                assert(src_state == DCMPS_COMMON ||
                    src_state == DCMPS_DIFFER ||
                    src_state == DCMPS_INIT);
            }

            if (only_dest || dst_exist_state == DCMPS_ONLY_SRC) {
                /* This file never checked for dest */
                assert(dst_state == DCMPS_INIT);
            } else {
                /* all stats of source and dest are the same */
                assert(src_state == dst_state);
                /* all states are either common, differ or skiped */
                if (dsync_option_need_compare(field)) {
                    assert(src_state == DCMPS_COMMON ||
                    src_state == DCMPS_DIFFER);
                } else {
                    assert(src_state == DCMPS_INIT);
                }
            }
        }
    }
}

/* check the result maps are valid */
static void dsync_strmap_check(
    strmap* src_map,
    strmap* dst_map)
{
    dsync_strmap_check_src(src_map, dst_map);
    dsync_strmap_check_dst(src_map, dst_map);
}

static int dsync_map_fn(
    mfu_flist flist,
    uint64_t idx,
    int ranks,
    void *args)
{
    /* the args pointer is a pointer to the directory prefix to
     * be ignored in full path name */
    char* prefix = (char *)args;
    size_t prefix_len = strlen(prefix);

    /* get name of item */
    const char* name = mfu_flist_file_get_name(flist, idx);

    /* identify a rank responsible for this item */
    const char* ptr = name + prefix_len;
    size_t ptr_len = strlen(ptr);
    uint32_t hash = mfu_hash_jenkins(ptr, ptr_len);
    int rank = (int) (hash % (uint32_t)ranks);
    return rank;
}

static struct dsync_expression* dsync_expression_alloc(void)
{
    struct dsync_expression *expression;

    expression = (struct dsync_expression*)
        MFU_MALLOC(sizeof(struct dsync_expression));
    INIT_LIST_HEAD(&expression->linkage);

    return expression;
}

static void dsync_expression_free(struct dsync_expression *expression)
{
    assert(list_empty(&expression->linkage));
    mfu_free(&expression);
}

static void dsync_expression_print(
    struct dsync_expression *expression,
    int simple)
{
    if (simple) {
        printf("(%s = %s)", dsync_field_to_string(expression->field, 1),
            dsync_state_to_string(expression->state, 1));
    } else {
        /* Special output for DCMPF_EXIST */
        if (expression->field == DCMPF_EXIST) {
            assert(expression->state == DCMPS_ONLY_SRC ||
                   expression->state == DCMPS_ONLY_DEST ||
                   expression->state == DCMPS_DIFFER ||
                   expression->state == DCMPS_COMMON);
            switch (expression->state) {
            case DCMPS_ONLY_SRC:
                printf("exist only in source directory");
                break;
            case DCMPS_ONLY_DEST:
                printf("exist only in destination directory");
                break;
            case DCMPS_COMMON:
                printf("exist in both directories");
                break;
            case DCMPS_DIFFER:
                printf("exist only in one directory");
                break;
            /* To avoid compiler warnings be exhaustive
             * and include all possible expression states */
            case DCMPS_INIT: //fall through
            case  DCMPS_MAX: //fall through
            default:
                assert(0);
            }
        } else {
            assert(expression->state == DCMPS_DIFFER ||
                   expression->state == DCMPS_COMMON);
            printf("have %s %s", dsync_state_to_string(expression->state, 0),
                   dsync_field_to_string(expression->field, 0));
            if (expression->state == DCMPS_DIFFER) {
                /* Make sure plurality is valid */
                printf("s");
            }
        }
    }
}

static int dsync_expression_match(
    struct dsync_expression *expression,
    strmap* map,
    const char* key)
{
    int ret;
    dsync_state state;
    dsync_state exist_state;

    ret = dsync_strmap_item_state(map, key, DCMPF_EXIST, &exist_state);
    assert(ret == 0);
    if (exist_state == DCMPS_ONLY_SRC) {
        /*
         * Map is source and file only exist in source.
         * All fields are invalid execpt DCMPF_EXIST.
         */
        if (expression->field == DCMPF_EXIST &&
            (expression->state == DCMPS_ONLY_SRC ||
             expression->state == DCMPS_DIFFER)) {
            return 1;
        }
        return 0;
    } else if (exist_state == DCMPS_INIT) {
        /*
         * Map is dest and file only exist in dest.
         * All fields are invalid execpt DCMPF_EXIST.
         * DCMPS_INIT sate of DCMPF_EXIST in dest is
         * considered as DCMPS_ONLY_DEST.
         */
        if (expression->field == DCMPF_EXIST &&
            (expression->state == DCMPS_ONLY_DEST ||
             expression->state == DCMPS_DIFFER)) {
            return 1;
        }
        return 0;
    } else {
        assert(exist_state == DCMPS_COMMON);
        if (expression->field == DCMPF_EXIST) {
            if (expression->state == DCMPS_COMMON) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    assert(exist_state == DCMPS_COMMON);
    assert(expression->field != DCMPF_EXIST);

    ret = dsync_strmap_item_state(map, key, expression->field, &state);
    assert(ret == 0);
    /* All fields should have been compared. */
    assert(state == DCMPS_COMMON || state == DCMPS_DIFFER);
    if (expression->state == state) {
        return 1;
    }

    return 0;
}

static struct dsync_conjunction* dsync_conjunction_alloc(void)
{
    struct dsync_conjunction *conjunction;

    conjunction = (struct dsync_conjunction*)
        MFU_MALLOC(sizeof(struct dsync_conjunction));
    INIT_LIST_HEAD(&conjunction->linkage);
    INIT_LIST_HEAD(&conjunction->expressions);
    conjunction->src_matched_list = mfu_flist_new();
    conjunction->dst_matched_list = mfu_flist_new();

    return conjunction;
}

static void dsync_conjunction_add_expression(
    struct dsync_conjunction* conjunction,
    struct dsync_expression* expression)
{
    assert(list_empty(&expression->linkage));
    list_add_tail(&expression->linkage, &conjunction->expressions);
}

static void dsync_conjunction_free(struct dsync_conjunction *conjunction)
{
    struct dsync_expression* expression;
    struct dsync_expression* n;

    assert(list_empty(&conjunction->linkage));
    list_for_each_entry_safe(expression,
                             n,
                             &conjunction->expressions,
                             linkage) {
        list_del_init(&expression->linkage);
        dsync_expression_free(expression);
    }
    assert(list_empty(&conjunction->expressions));
    mfu_flist_free(&conjunction->src_matched_list);
    mfu_flist_free(&conjunction->dst_matched_list);
    mfu_free(&conjunction);
}

static void dsync_conjunction_print(
    struct dsync_conjunction *conjunction,
    int simple)
{
    struct dsync_expression* expression;

    if (simple) {
        printf("(");
    }
    list_for_each_entry(expression,
                        &conjunction->expressions,
                        linkage) {
        dsync_expression_print(expression, simple);
        if (expression->linkage.next != &conjunction->expressions) {
            if (simple) {
                printf("&&");
            } else {
                printf(" and ");
            }
        }
    }
    if (simple) {
        printf(")");
    }
}

/* if matched return 1, else return 0 */
static int dsync_conjunction_match(
    struct dsync_conjunction *conjunction,
    strmap* map,
    const char* key)
{
    struct dsync_expression* expression;
    int matched;

    list_for_each_entry(expression,
                        &conjunction->expressions,
                        linkage) {
        matched = dsync_expression_match(expression, map, key);
        if (!matched) {
            return 0;
        }
    }
    return 1;
}

static struct dsync_disjunction* dsync_disjunction_alloc(void)
{
    struct dsync_disjunction *disjunction;

    disjunction = (struct dsync_disjunction*)
        MFU_MALLOC(sizeof(struct dsync_disjunction));
    INIT_LIST_HEAD(&disjunction->linkage);
    INIT_LIST_HEAD(&disjunction->conjunctions);
    disjunction->count = 0;

    return disjunction;
}

static void dsync_disjunction_add_conjunction(
    struct dsync_disjunction* disjunction,
    struct dsync_conjunction* conjunction)
{
    assert(list_empty(&conjunction->linkage));
    list_add_tail(&conjunction->linkage, &disjunction->conjunctions);
    disjunction->count++;
}

static void dsync_disjunction_free(struct dsync_disjunction* disjunction)
{
    struct dsync_conjunction *conjunction;
    struct dsync_conjunction *n;

    assert(list_empty(&disjunction->linkage));
    list_for_each_entry_safe(conjunction,
                             n,
                             &disjunction->conjunctions,
                             linkage) {
        list_del_init(&conjunction->linkage);
        dsync_conjunction_free(conjunction);
    }
    assert(list_empty(&disjunction->conjunctions));
    mfu_free(&disjunction);
}

static void dsync_disjunction_print(
    struct dsync_disjunction* disjunction,
    int simple,
    int indent)
{
    struct dsync_conjunction *conjunction;

    list_for_each_entry(conjunction,
                        &disjunction->conjunctions,
                        linkage) {
        dsync_conjunction_print(conjunction, simple);

        printf(": [%lu/%lu]", mfu_flist_global_size(conjunction->src_matched_list),
                              mfu_flist_global_size(conjunction->dst_matched_list));
        if (conjunction->linkage.next != &disjunction->conjunctions) {

            if (simple) {
                printf("||");
            } else {
                printf(", or\n");
                int i;
                for (i = 0; i < indent; i++) {
                    printf(" ");
                }
            }
        }
    }
}

/* if matched return 1, else return 0 */
static int dsync_disjunction_match(
    struct dsync_disjunction* disjunction,
    strmap* map,
    const char* key,
    int is_src)
{
    struct dsync_conjunction *conjunction;
    int matched;

    list_for_each_entry(conjunction,
                        &disjunction->conjunctions,
                        linkage) {
        matched = dsync_conjunction_match(conjunction, map, key);
        if (matched) {
            if (is_src)
                mfu_flist_increase(&conjunction->src_matched_list);
            else
                mfu_flist_increase(&conjunction->dst_matched_list);
            return 1;
        }
    }
    return 0;
}

static struct dsync_output* dsync_output_alloc(void)
{
    struct dsync_output* output;

    output = (struct dsync_output*) MFU_MALLOC(sizeof(struct dsync_output));
    output->file_name = NULL;
    INIT_LIST_HEAD(&output->linkage);
    output->disjunction = NULL;

    return output;
}

static void dsync_output_init_disjunction(
    struct dsync_output* output,
    struct dsync_disjunction* disjunction)
{
    assert(output->disjunction == NULL);
    output->disjunction = disjunction;
}

static void dsync_output_free(struct dsync_output* output)
{
    assert(list_empty(&output->linkage));
    dsync_disjunction_free(output->disjunction);
    output->disjunction = NULL;
    if (output->file_name != NULL) {
        mfu_free(&output->file_name);
    }
    mfu_free(&output);
}

static void dsync_option_fini(void)
{
    struct dsync_output* output;
    struct dsync_output* n;

    list_for_each_entry_safe(output,
                             n,
                             &options.outputs,
                             linkage) {
        list_del_init(&output->linkage);
        dsync_output_free(output);
    }
    assert(list_empty(&options.outputs));

    mfu_free(&options.link_dest);
}

static void dsync_option_add_output(struct dsync_output *output, int add_at_head)
{
    assert(list_empty(&output->linkage));
    if (add_at_head) {
        list_add(&output->linkage, &options.outputs);
    } else {
        list_add_tail(&output->linkage, &options.outputs);
    }
}

static void dsync_option_add_comparison(dsync_field field)
{
    uint64_t depend = dsync_field_depend[field];
    uint64_t i;
    for (i = 0; i < DCMPF_MAX; i++) {
        if ((depend & ((uint64_t)1 << i)) != (uint64_t)0) {
            options.need_compare[i] = 1;
        }
    }
}

static int dsync_output_flist_match(
    struct dsync_output *output,
    strmap* map,
    mfu_flist flist,
    mfu_flist new_flist,
    mfu_flist *matched_flist,
    int is_src)
{
    const strmap_node* node;
    struct dsync_conjunction *conjunction;

    /* iterate over each item in map */
    strmap_foreach(map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of file */
        uint64_t idx;
        int ret = dsync_strmap_item_index(map, key, &idx);
        assert(ret == 0);

        if (dsync_disjunction_match(output->disjunction, map, key, is_src)) {
            mfu_flist_increase(matched_flist);
            mfu_flist_file_copy(flist, idx, new_flist);
        }
    }

    list_for_each_entry(conjunction,
                        &output->disjunction->conjunctions,
                        linkage) {
        if (is_src) {
            mfu_flist_summarize(conjunction->src_matched_list);
        } else {
            mfu_flist_summarize(conjunction->dst_matched_list);
        }
    }

    return 0;
}

#define DCMP_OUTPUT_PREFIX "Files which "

static int dsync_output_write(
    struct dsync_output *output,
    mfu_flist src_flist,
    strmap* src_map,
    mfu_flist dst_flist,
    strmap* dst_map)
{
    int ret = 0;
    mfu_flist new_flist = mfu_flist_subset(src_flist);

    /* find matched file in source map */
    mfu_flist src_matched = mfu_flist_new();
    ret = dsync_output_flist_match(output, src_map, src_flist,
                                  new_flist, &src_matched, 1);
    assert(ret == 0);

    /* find matched file in dest map */
    mfu_flist dst_matched = mfu_flist_new();
    ret = dsync_output_flist_match(output, dst_map, dst_flist,
                                  new_flist, &dst_matched, 0);
    assert(ret == 0);

    mfu_flist_summarize(new_flist);
    mfu_flist_summarize(src_matched);
    mfu_flist_summarize(dst_matched);
    if (output->file_name != NULL) {
        mfu_flist_write_cache(output->file_name, new_flist);
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == 0) {
        printf(DCMP_OUTPUT_PREFIX);
        dsync_disjunction_print(output->disjunction, 0,
                               strlen(DCMP_OUTPUT_PREFIX));

        if (output->disjunction->count > 1)
            printf(", total number: %lu/%lu",
                   mfu_flist_global_size(src_matched),
                   mfu_flist_global_size(dst_matched));

        if (output->file_name != NULL) {
            printf(", dumped to \"%s\"",
                   output->file_name);
        }
        printf("\n");
    }
    mfu_flist_free(&new_flist);
    mfu_flist_free(&src_matched);
    mfu_flist_free(&dst_matched);

    return 0;
}

static int dsync_outputs_write(
    mfu_flist src_list,
    strmap* src_map,
    mfu_flist dst_list,
    strmap* dst_map)
{
    struct dsync_output* output;
    int ret = 0;

    list_for_each_entry(output,
                        &options.outputs,
                        linkage) {
        ret = dsync_output_write(output, src_list, src_map, dst_list, dst_map);
        if (ret) {
            fprintf(stderr,
                "failed to output to file \"%s\"\n",
                output->file_name);
            break;
        }
    }
    return ret;
}

#define DCMP_PATH_DELIMITER        ":"
#define DCMP_DISJUNCTION_DELIMITER ","
#define DCMP_CONJUNCTION_DELIMITER "@"
#define DCMP_EXPRESSION_DELIMITER  "="

static int dsync_expression_parse(
    struct dsync_conjunction* conjunction,
    const char* expression_string)
{
    char* tmp = MFU_STRDUP(expression_string);
    char* field_string;
    char* state_string;
    int ret = 0;
    struct dsync_expression* expression;

    expression = dsync_expression_alloc();

    state_string = tmp;
    field_string = strsep(&state_string, DCMP_EXPRESSION_DELIMITER);
    if (!*field_string || state_string == NULL || !*state_string) {
        fprintf(stderr,
            "expression %s illegal, field \"%s\", state \"%s\"\n",
            expression_string, field_string, state_string);
        ret = -EINVAL;
        goto out;
    }

    ret = dsync_field_from_string(field_string, &expression->field);
    if (ret) {
        fprintf(stderr,
            "field \"%s\" illegal\n",
            field_string);
        ret = -EINVAL;
        goto out;
    }

    ret = dsync_state_from_string(state_string, &expression->state);
    if (ret || expression->state == DCMPS_INIT) {
        fprintf(stderr,
            "state \"%s\" illegal\n",
            state_string);
        ret = -EINVAL;
        goto out;
    }

    if ((expression->state == DCMPS_ONLY_SRC ||
         expression->state == DCMPS_ONLY_DEST) &&
        (expression->field != DCMPF_EXIST)) {
        fprintf(stderr,
            "ONLY_SRC or ONLY_DEST is only valid for EXIST\n");
        ret = -EINVAL;
        goto out;
    }

    dsync_conjunction_add_expression(conjunction, expression);

    /* Add comparison we need for this expression */
    dsync_option_add_comparison(expression->field);
out:
    if (ret) {
        dsync_expression_free(expression);
    }
    mfu_free(&tmp);
    return ret;
}

static int dsync_conjunction_parse(
    struct dsync_disjunction* disjunction,
    const char* conjunction_string)
{
    int ret = 0;
    char* tmp = MFU_STRDUP(conjunction_string);
    char* expression;
    char* next;
    struct dsync_conjunction* conjunction;

    conjunction = dsync_conjunction_alloc();

    next = tmp;
    while ((expression = strsep(&next, DCMP_CONJUNCTION_DELIMITER))) {
        if (!*expression) {
            /* empty */
            continue;
        }

        ret = dsync_expression_parse(conjunction, expression);
        if (ret) {
            fprintf(stderr,
                "failed to parse expression \"%s\"\n", expression);
            goto out;
        }
    }

    dsync_disjunction_add_conjunction(disjunction, conjunction);
out:
    if (ret) {
        dsync_conjunction_free(conjunction);
    }
    mfu_free(&tmp);
    return ret;
}

static int dsync_disjunction_parse(
    struct dsync_output *output,
    const char *disjunction_string)
{
    int ret = 0;
    char* tmp = MFU_STRDUP(disjunction_string);
    char* conjunction = NULL;
    char* next;
    struct dsync_disjunction* disjunction;

    disjunction = dsync_disjunction_alloc();

    next = tmp;
    while ((conjunction = strsep(&next, DCMP_DISJUNCTION_DELIMITER))) {
        if (!*conjunction) {
            /* empty */
            continue;
        }

        ret = dsync_conjunction_parse(disjunction, conjunction);
        if (ret) {
            fprintf(stderr,
                "failed to parse conjunction \"%s\"\n", conjunction);
            goto out;
        }
    }

    dsync_output_init_disjunction(output, disjunction);
out:
    if (ret) {
        dsync_disjunction_free(disjunction);
    }
    mfu_free(&tmp);
    return ret;
}

static int dsync_option_output_parse(const char *option, int add_at_head)
{
    char* tmp = MFU_STRDUP(option);
    char* disjunction;
    char* file_name;
    int ret = 0;
    struct dsync_output* output;

    output = dsync_output_alloc();

    file_name = tmp;
    disjunction = strsep(&file_name, DCMP_PATH_DELIMITER);
    if (!*disjunction) {
        fprintf(stderr,
            "output string illegal, disjunction \"%s\", file name \"%s\"\n",
            disjunction, file_name);
        ret = -EINVAL;
        goto out;
    }

    ret = dsync_disjunction_parse(output, disjunction);
    if (ret) {
        goto out;
    }

    if (file_name != NULL && *file_name) {
        output->file_name = MFU_STRDUP(file_name);
    }
    dsync_option_add_output(output, add_at_head);
out:
    if (ret) {
        dsync_output_free(output);
    }
    mfu_free(&tmp);
    return ret;
}

/* link_dest doesn't support dirs cross filesystems */
static int dsync_validate_link_dest(const char *link_dest, const char *dest, mfu_file_t* mfu_file)
{
    struct stat dest_st, link_dest_st;
    int rc;

    errno = 0;
    rc = mfu_file_lstat(dest, &dest_st, mfu_file);
    if (rc < 0) {
        /* if destpath need to create, check the pdir */
        if (errno == ENOENT) {
            char *dest_path = MFU_STRDUP(dest);
            dirname(dest_path);

            rc = mfu_file_lstat(dest_path, &dest_st, mfu_file);
            mfu_free(&dest_path);
        }

        if (rc < 0) {
            return rc;
        }
    }

    rc = mfu_file_lstat(link_dest, &link_dest_st, mfu_file);
    if (rc < 0) {
        return rc;
    }

    if (dest_st.st_dev == link_dest_st.st_dev) {
        return 0;
    } else {
        return -EINVAL;
    }
}

#ifdef DAOS_SUPPORT
/* Setup DAOS for dsync */
static int dsync_daos_setup(
    int rank,
    daos_args_t* daos_args,
    char** argpaths,
    int numpaths,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* For error handling */
    bool daos_do_cleanup = false;
    bool daos_do_exit = false;

    /* Always allow the destination to exist. */
    daos_args->allow_exist_dst_cont = true;

    /* Set up DAOS arguments, containers, dfs, etc. */
    int daos_rc = daos_setup(rank, argpaths, numpaths, daos_args, mfu_src_file, mfu_dst_file);
    if (daos_rc != 0) {
        return 1;
    }

    /* DAOS does not support hard links */
    if (options.link_dest != NULL &&
            (mfu_src_file->type == DAOS || mfu_src_file->type == DFS ||
             mfu_dst_file->type == DAOS || mfu_dst_file->type == DFS)) {
        MFU_LOG(MFU_LOG_ERR, "DAOS does not support --link-dest.");
        return 1;
    }

    /* Not yet supported */
    if (options.delete && (mfu_src_file->type == DAOS || mfu_dst_file->type == DAOS)) {
        MFU_LOG(MFU_LOG_ERR, "DAOS API does not support --delete.");
        return 1;
    }

    return 0;
}

/* Perform a DAOS object sync */
static int dsync_daos(
    int rank,
    daos_args_t* daos_args,
    mfu_copy_opts_t* copy_opts,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* always compare contents */
    /* TODO DAOS figure out a reliable "lite" comparison */
    options.contents = 1;

    mfu_flist flist = mfu_flist_new();

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Gathering source objects");
    }
    int rc = mfu_daos_flist_walk(daos_args, daos_args->src_coh,
                                 &daos_args->src_epc, flist);
    if (rc != 0) {
        rc = 1;
        goto dsync_daos_out;
    }

    if (mfu_flist_global_size(flist) == 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "ERROR: No objects found in source container.");
        }
        rc = 1;
        goto dsync_daos_out;
    }

    /* Collectively copy all objects */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Copying source objects");
    }
    bool compare_dst = options.contents;
    bool write_dst = !options.dry_run;
    int tmp_rc = mfu_daos_flist_sync(daos_args, flist, compare_dst, write_dst);
    if (tmp_rc != 0) {
        rc = 1;
    }

dsync_daos_out:
    mfu_flist_free(&flist);
    return rc;
}
#endif

int main(int argc, char **argv)
{
    int rc = 0;

    /* initialize MPI and mfu libraries */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* pointer to mfu_file src and dest objects */
    mfu_file_t* mfu_src_file = mfu_file_new();
    mfu_file_t* mfu_dst_file = mfu_file_new();

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* pointer to mfu_copy opts */
    mfu_copy_opts_t* copy_opts = mfu_copy_opts_new();

    /* TODO: allow user to specify file lists as input files */

    /* TODO: three levels of comparison:
     *   1) file names only
     *   2) stat info + items in #1
     *   3) file contents + items in #2 */

    /* walk by default because there is no input file option */
    int walk = 1;

    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    /* By default, sync option will preserve all attributes. */
    copy_opts->preserve = true;

    /* flag to check for sync option */
    copy_opts->do_sync = 1;

#ifdef DAOS_SUPPORT
    /* DAOS vars */ 
    daos_args_t* daos_args = daos_args_new();    
#endif

    int option_index = 0;
    static struct option long_options[] = {
        {"dryrun",         0, 0, 'n'},
        {"batch-files",    1, 0, 'b'},
        {"bufsize",        1, 0, 'B'},
        {"chunksize",      1, 0, 'k'},
        {"xattrs",         1, 0, 'X'},
        {"daos-prefix",    1, 0, 'Y'},
        {"daos-api",       1, 0, 'y'},
        {"contents",       0, 0, 'c'},
        {"delete",         0, 0, 'D'},
        {"dereference",    0, 0, 'L'},
        {"no-dereference", 0, 0, 'P'},
        {"direct",         0, 0, 's'},
        {"output",         1, 0, 'o'}, // undocumented
        {"debug",          0, 0, 'd'}, // undocumented
        {"link-dest",      1, 0, 'l'},
        {"sparse",         0, 0, 'S'},
        {"progress",       1, 0, 'R'},
        {"verbose",        0, 0, 'v'},
        {"quiet",          0, 0, 'q'},
        {"help",           0, 0, 'h'},
        {0, 0, 0, 0}
    };
    int ret = 0;
    int i;

    /* read in command line options */
    int usage = 0;
    int help  = 0;
    unsigned long long bytes = 0;

    /* Don't delete dst files by default */
    options.delete = 0;

    while (1) {
        int c = getopt_long(
            argc, argv, "b:cDso:LPSvqhX:",
            long_options, &option_index
        );

        if (c == -1) {
            break;
        }

        switch (c) {
        case 'b':
            copy_opts->batch_files = atoi(optarg);
            break;
        case 'B':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR,
                            "Failed to parse block size: '%s'", optarg);
                }
                usage = 1;
            } else {
                copy_opts->buf_size = (size_t)bytes;
            }
            break;
        case 'k':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR,
                            "Failed to parse chunk size: '%s'", optarg);
                }
                usage = 1;
            } else {
                copy_opts->chunk_size = bytes;
            }
            break;
        case 'X':
            copy_opts->copy_xattrs = parse_copy_xattrs_option(optarg);
            if (copy_opts->copy_xattrs == XATTR_COPY_INVAL) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Unrecognized option '%s' for --xattrs", optarg);
                }
                usage = 1;
            }
            break;
#ifdef DAOS_SUPPORT
        case 'Y':
            daos_args->dfs_prefix = MFU_STRDUP(optarg);
            break;
        case 'y':
            if (daos_parse_api_str(optarg, &daos_args->api) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to parse --daos-api");
                usage = 1;
            }
            break;
#endif
        case 'c':
            options.contents++;
            break;
        case 'n':
            options.dry_run++;
            break;
        case 'D':
            options.delete = 1;
            break;
        case 'L':
            /* turn on dereference.
             * turn off no_dereference */
            copy_opts->dereference = 1;
            walk_opts->dereference = 1;
            copy_opts->no_dereference = 0;
        case 'P':
            /* turn on no_dereference.
             * turn off dereference */
            copy_opts->no_dereference = 1;
            copy_opts->dereference = 0;
            walk_opts->dereference = 0;
            break;
        case 's':
            copy_opts->direct = true;
            if(rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Using O_DIRECT");
            }
            break;
        case 'l':
            options.link_dest = MFU_STRDUP(optarg);
            break;
        case 'o':
            ret = dsync_option_output_parse(optarg, 0);
            if (ret) {
                usage = 1;
            }
            break;
        case 'd':
            options.debug++;
            break;
        case 'S':
            copy_opts->sparse = 1;
            break;
        case 'R':
            mfu_progress_timeout = atoi(optarg);
            break;
        case 'v':
            options.verbose++;
            mfu_debug_level = MFU_LOG_VERBOSE;
            break;
        case 'q':
            options.quiet++;
            mfu_debug_level = MFU_LOG_NONE;
            /* since process won't be printed in quiet anyway,
             * disable the algorithm to save some overhead */
            mfu_progress_timeout = 0;
            break;
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

    /* check that we got a valid progress value */
    if (mfu_progress_timeout < 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Seconds in --progress must be non-negative: %d invalid", mfu_progress_timeout);
        }
        usage = 1;
    }

    /* Generate default output */
    if (list_empty(&options.outputs)) {
        /*
         * If -o option is not given,
         * we want to add default output,
         * in case there is no output at all.
         */
        for (i = 0; ; i++) {
            if (dsync_default_outputs[i] == NULL) {
                break;
            }
            dsync_option_output_parse(dsync_default_outputs[i], 1);
            assert(ret == 0);
        }
    }

    /* we should have two arguments left, source and dest paths */
    int numargs = argc - optind;

    /* if help flag was thrown, don't bother checking usage */
    if (numargs != 2 && !help) {
        MFU_LOG(MFU_LOG_ERR,
            "You must specify a source and destination path.");
        usage = 1;
    }

    /* print usage and exit if necessary */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        rc = 1;
        goto dsync_common_cleanup;
    }

    /* pointer to path arguments */
    char** argpaths = &argv[optind];

#ifdef DAOS_SUPPORT
    int daos_rc = dsync_daos_setup(rank, daos_args, argpaths, numargs, mfu_src_file, mfu_dst_file);
    if (daos_rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Detected one or more DAOS errors: "MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
        rc = 1;
        goto dsync_common_cleanup;
    }

    /* Handle the DAOS API case */
    if (mfu_src_file->type == DAOS || mfu_dst_file->type == DAOS) {
        daos_rc = dsync_daos(rank, daos_args, copy_opts, mfu_src_file, mfu_dst_file);
        if (daos_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Detected one or more DAOS errors: "MFU_ERRF, MFU_ERRP(-MFU_ERR_DAOS));
            rc = 1;
        }
        goto dsync_common_cleanup;
    }
#endif

    /* allocate space for each path */
    mfu_param_path* paths = (mfu_param_path*) MFU_MALLOC((size_t)numargs * sizeof(mfu_param_path));

    /* first item is source and second is dest */
    mfu_param_path* srcpath  = &paths[0];
    mfu_param_path* destpath = &paths[1];

    /* process each path */
    mfu_param_path_set((const char*)argpaths[0], srcpath,  mfu_src_file, true);
    mfu_param_path_set((const char*)argpaths[1], destpath, mfu_dst_file, false);

    /* advance to next set of options */
    optind += numargs;

    /* create an empty file list */
    mfu_flist flist_tmp_src = mfu_flist_new();
    mfu_flist flist_tmp_dst = mfu_flist_new();

    mfu_param_path* linkpath = NULL;
    mfu_flist flist_tmp_link = MFU_FLIST_NULL;     
    if (options.link_dest != NULL) {
        /* user wants to use hardlinks,
         * check whether we can given destination and link-dest dirs */
        int validate_rc;
        if (rank == 0) {
            validate_rc = dsync_validate_link_dest(options.link_dest, destpath->path, mfu_dst_file);
        }
        MPI_Bcast(&validate_rc, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (validate_rc < 0) {
            /* can't use hardlinks from dest to link-dest,
             * so disable link_dest and fallback to full copies */
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Invalid link_dest: [%s], ret:%d. Disabled link_dest",
                        options.link_dest, rc);
            }
            mfu_free(&options.link_dest);
        } else {
            /* we can use hardlinks, so set up variables for it */
            linkpath = (mfu_param_path*) MFU_MALLOC(sizeof(mfu_param_path));
            mfu_param_path_set(options.link_dest, linkpath, mfu_dst_file, true);
            flist_tmp_link = mfu_flist_new();
        }
    }

    /* walk source path */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Walking source path");
    }
    mfu_flist_walk_param_paths(1, srcpath, walk_opts, flist_tmp_src, mfu_src_file);

    /* check that we actually got something so that we don't delete
     * an entire target directory because of a typo on the source dir */
    if (mfu_flist_global_size(flist_tmp_src) == 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "ERROR: No items found at source: `%s'", srcpath->orig);
        }
        mfu_flist_free(&flist_tmp_src);
        mfu_flist_free(&flist_tmp_dst);
        if (options.link_dest != NULL) {
            mfu_flist_free(&flist_tmp_link);
            mfu_param_path_free(linkpath);
            mfu_free(&linkpath);
        }
        rc = 1;
        goto dsync_common_cleanup;
    }

    /* walk destinaton path.
     * We never dereference the destination */
    int tmp_dereference = walk_opts->dereference;
    walk_opts->dereference = 0;
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Walking destination path");
    }
    mfu_flist_walk_param_paths(1, destpath, walk_opts, flist_tmp_dst, mfu_dst_file);

    /* walk link-dest path if we have one */
    if (options.link_dest != NULL) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Walking link-dest path");
        }
        mfu_flist_walk_param_paths(1, linkpath, walk_opts, flist_tmp_link, mfu_dst_file);
    }

    /* reset the dereference flag */
    walk_opts->dereference = tmp_dereference;

    /* store src and dest path strings */
    const char* path_src = srcpath->path;
    const char* path_dst = destpath->path;

    char* path_link = NULL;
    if (options.link_dest != NULL) {
        path_link = linkpath->path;
    }

    /* map files to ranks based on portion following prefix directory */
    mfu_flist flist_src = mfu_flist_remap(flist_tmp_src, (mfu_flist_map_fn)dsync_map_fn, (const void*)path_src);
    mfu_flist flist_dst = mfu_flist_remap(flist_tmp_dst, (mfu_flist_map_fn)dsync_map_fn, (const void*)path_dst);

    mfu_flist flist_link = MFU_FLIST_NULL;
    if (options.link_dest != NULL) {
        flist_link = mfu_flist_remap(flist_tmp_link, (mfu_flist_map_fn)dsync_map_fn, (const void*)path_link);
    }

    /* free original file lists */
    mfu_flist_free(&flist_tmp_src);
    mfu_flist_free(&flist_tmp_dst);
    if (options.link_dest != NULL) {
        mfu_flist_free(&flist_tmp_link);
    }

    /* map each file name to its index and its comparison state */
    strmap* map_src = dsync_strmap_creat(flist_src, path_src);
    strmap* map_dst = dsync_strmap_creat(flist_dst, path_dst);
    strmap* map_link = NULL;
    if (options.link_dest != NULL) {
        map_link = dsync_strmap_creat(flist_link, path_link);
    }

    /* compare files in map_src with those in map_dst */
    int tmp_rc = dsync_strmap_compare(flist_src, map_src, flist_dst, map_dst, flist_link, map_link,
        strlen(path_src), copy_opts, srcpath, destpath, linkpath, mfu_src_file, mfu_dst_file);
    if (tmp_rc < 0) {
        rc = 1;
    }

    /* free maps of file names to comparison state info */
    strmap_delete(&map_src);
    strmap_delete(&map_dst);
    if (options.link_dest != NULL) {
        strmap_delete(&map_link);
    }

    /* free file lists */
    mfu_flist_free(&flist_src);
    mfu_flist_free(&flist_dst);
    if (options.link_dest != NULL) {
        mfu_flist_free(&flist_link);
    }

    /* free param path for link-dest if we have one */
    if (options.link_dest != NULL) {
        mfu_param_path_free(linkpath);
        mfu_free(&linkpath);
    }

    /* free all param paths */
    mfu_param_path_free_all(numargs, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* Common cleanup for all APIs and early exit conditions */
dsync_common_cleanup:
    dsync_option_fini();

    /* free the copy options structure */
    mfu_copy_opts_delete(&copy_opts);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

#ifdef DAOS_SUPPORT
    /* Cleanup DAOS-related variables, etc. */
    daos_cleanup(daos_args, mfu_src_file, mfu_dst_file);
#endif

    /* delete file objects */
    mfu_file_delete(&mfu_src_file);
    mfu_file_delete(&mfu_dst_file);

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Completed sync");
    }

    /* shut down */
    mfu_finalize();
    MPI_Finalize();

    return rc;
}
