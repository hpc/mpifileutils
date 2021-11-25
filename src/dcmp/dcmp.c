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

/* for daos */
#ifdef DAOS_SUPPORT
#include "mfu_daos.h"
#endif

/* Print a usage message */
static void print_usage(void)
{
    printf("\n");
    printf("Usage: dcmp [options] source target\n");
    printf("\n");
#ifdef DAOS_SUPPORT
    printf("DAOS paths can be specified as:\n");
    printf("       daos://<pool>/<cont>[/<path>] | <UNS path>\n");
#endif
    printf("Options:\n");
    printf("  -o, --output <EXPR:FILE>  - write list of entries matching EXPR to FILE\n");
    printf("  -t, --text                - change output option to write in text format\n");
    printf("  -b, --base                - enable base checks and normal output with --output\n");
    printf("      --bufsize <SIZE>      - IO buffer size in bytes (default " MFU_BUFFER_SIZE_STR ")\n");
    printf("      --chunksize <SIZE>    - minimum work size per task in bytes (default " MFU_CHUNK_SIZE_STR ")\n");
#ifdef DAOS_SUPPORT
    printf("      --daos-api            - DAOS API in {DFS, DAOS} (default uses DFS for POSIX containers)\n");
#endif
    printf("  -s, --direct              - open files with O_DIRECT\n");
    printf("      --progress <N>        - print progress every N seconds\n");
    printf("  -v, --verbose             - verbose output\n");
    printf("  -q, --quiet               - quiet output\n");
    printf("  -l, --lite                - only compares file modification time and size\n");
    //printf("  -d, --debug               - run in debug mode\n");
    printf("  -h, --help                - print usage\n");
    printf("\n");
    printf("EXPR consists of one or more FIELD=STATE conditions, separated with '@' for AND or ',' for OR.\n");
    printf("AND operators bind with higher precedence than OR.\n");
    printf("\n");
    printf("Fields: EXIST,TYPE,SIZE,UID,GID,ATIME,MTIME,CTIME,PERM,ACL,CONTENT\n");
    printf("States: DIFFER,COMMON\n");
    printf("Additional States for EXIST: ONLY_SRC,ONLY_DEST\n");
    printf("\n");
    printf("Example expressions:\n");
    printf("- Entry exists in both source and target and type differs between the two\n");
    printf("  EXIST=COMMON@TYPE=DIFFER\n");
    printf("\n");
    printf("- Entry exists only in source, or types differ, or permissions differ, or mtimes differ\n");
    printf("  EXIST=ONLY_SRC,TYPE=DIFFER,PERM=DIFFER,MTIME=DIFFER\n");
    printf("\n");
    printf("By default, dcmp checks the following expressions and prints results to stdout:\n");
    printf("  EXIST=COMMON\n");
    printf("  EXIST=DIFFER\n");
    printf("  EXIST=COMMON@TYPE=COMMON\n");
    printf("  EXIST=COMMON@TYPE=DIFFER\n");
    printf("  EXIST=COMMON@CONTENT=COMMON\n");
    printf("  EXIST=COMMON@CONTENT=DIFFER\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
}

typedef enum _dcmp_state {
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
      * because we don't want to waste a loop in dcmp_strmap_compare()
      */
    DCMPS_ONLY_DEST,

    DCMPS_MAX,
} dcmp_state;

typedef enum _dcmp_field {
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
} dcmp_field;

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

uint64_t dcmp_field_depend[] = {
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

struct dcmp_expression {
    dcmp_field field;              /* the concerned field */
    dcmp_state state;              /* expected state of the field */
    struct list_head linkage;      /* linkage to struct dcmp_conjunction */
};

struct dcmp_conjunction {
    struct list_head linkage;      /* linkage to struct dcmp_disjunction */
    struct list_head expressions;  /* list of logical conjunction */
    mfu_flist src_matched_list;    /* matched src items in this conjunction */
    mfu_flist dst_matched_list;    /* matched dst items in this conjunction */
};

struct dcmp_disjunction {
    struct list_head linkage;      /* linkage to struct dcmp_output */
    struct list_head conjunctions; /* list of logical conjunction */
    unsigned count;                /* logical conjunctions count */
};

struct dcmp_output {
    char* file_name;               /* output file name */
    struct list_head linkage;      /* linkage to struct dcmp_options */
    struct dcmp_disjunction *disjunction; /* logical disjunction rules */
};

struct dcmp_options {
    struct list_head outputs;      /* list of outputs */
    int verbose;
    int quiet;
    int lite;
    int format;                    /* output data format, 0 for text, 1 for raw */
    int base;                      /* whether to do base check */
    int debug;                     /* check result after get result */
    int need_compare[DCMPF_MAX];   /* fields that need to be compared  */
};

struct dcmp_options options = {
    .outputs      = LIST_HEAD_INIT(options.outputs),
    .verbose      = 0,
    .quiet        = 0,
    .lite         = 0,
    .format       = 1,
    .base         = 0,
    .debug        = 0,
    .need_compare = {0,}
};

/* From tail to head */
const char *dcmp_default_outputs[] = {
    "EXIST=COMMON@CONTENT=DIFFER",
    "EXIST=COMMON@CONTENT=COMMON",
    "EXIST=COMMON@TYPE=DIFFER",
    "EXIST=COMMON@TYPE=COMMON",
    "EXIST=DIFFER",
    "EXIST=COMMON",
    NULL,
};

static const char* dcmp_field_to_string(dcmp_field field, int simple)
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

static int dcmp_field_from_string(const char* string, dcmp_field *field)
{
    dcmp_field i;
    for (i = 0; i < DCMPF_MAX; i ++) {
        if (strcmp(dcmp_field_to_string(i, 1), string) == 0) {
            *field = i;
            return 0;
        }
    }
    return -ENOENT;
}

static const char* dcmp_state_to_string(dcmp_state state, int simple)
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

static int dcmp_state_from_string(const char* string, dcmp_state *state)
{
    dcmp_state i;
    for (i = DCMPS_INIT; i < DCMPS_MAX; i ++) {
        if (strcmp(dcmp_state_to_string(i, 1), string) == 0) {
            *state = i;
            return 0;
        }
    }
    return -ENOENT;
}

/* given a filename as the key, encode an index followed
 * by the init state */
static void dcmp_strmap_item_init(
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

static void dcmp_strmap_item_update(
    strmap* map,
    const char *key,
    dcmp_field field,
    dcmp_state state)
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

static int dcmp_strmap_item_index(
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

static int dcmp_strmap_item_state(
    strmap* map,
    const char *key,
    dcmp_field field,
    dcmp_state *state)
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
static strmap* dcmp_strmap_creat(mfu_flist list, const char* prefix)
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
        dcmp_strmap_item_init(map, name, i);

        /* go to next item in list */
        i++;
    }

    return map;
}

static void dcmp_compare_acl(
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
        dcmp_strmap_item_update(src_map, key, DCMPF_ACL, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_ACL, DCMPS_COMMON);
    } else {
        dcmp_strmap_item_update(src_map, key, DCMPF_ACL, DCMPS_DIFFER);
        dcmp_strmap_item_update(dst_map, key, DCMPF_ACL, DCMPS_DIFFER);
        (*diff)++;
    }
}

static int dcmp_option_need_compare(dcmp_field field)
{
    return options.need_compare[field];
}

/* Return -1 when error, return 0 when equal, return > 0 when diff */
static int dcmp_compare_metadata(
    mfu_flist src_list,
    strmap* src_map,
    uint64_t src_index,
    mfu_flist dst_list,
    strmap* dst_map,
    uint64_t dst_index,
    const char* key)
{
    int diff = 0;

    if (dcmp_option_need_compare(DCMPF_SIZE)) {
        mfu_filetype type = mfu_flist_file_get_type(src_list, src_index);
        if (type != MFU_TYPE_DIR) {
            uint64_t src = mfu_flist_file_get_size(src_list, src_index);
            uint64_t dst = mfu_flist_file_get_size(dst_list, dst_index);
            if (src != dst) {
                /* file size is different */
                dcmp_strmap_item_update(src_map, key, DCMPF_SIZE, DCMPS_DIFFER);
                dcmp_strmap_item_update(dst_map, key, DCMPF_SIZE, DCMPS_DIFFER);
                diff++;
             } else {
                dcmp_strmap_item_update(src_map, key, DCMPF_SIZE, DCMPS_COMMON);
                dcmp_strmap_item_update(dst_map, key, DCMPF_SIZE, DCMPS_COMMON);
             }
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_SIZE, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_SIZE, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(DCMPF_GID)) {
        uint64_t src = mfu_flist_file_get_gid(src_list, src_index);
        uint64_t dst = mfu_flist_file_get_gid(dst_list, dst_index);
        if (src != dst) {
            /* file gid is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_GID, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_GID, DCMPS_DIFFER);
             diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_GID, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_GID, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(DCMPF_UID)) {
        uint64_t src = mfu_flist_file_get_uid(src_list, src_index);
        uint64_t dst = mfu_flist_file_get_uid(dst_list, dst_index);
        if (src != dst) {
            /* file uid is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_UID, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_UID, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_UID, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_UID, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(DCMPF_ATIME)) {
        uint64_t src_atime      = mfu_flist_file_get_atime(src_list, src_index);
        uint64_t src_atime_nsec = mfu_flist_file_get_atime_nsec(src_list, src_index);
        uint64_t dst_atime      = mfu_flist_file_get_atime(dst_list, dst_index);
        uint64_t dst_atime_nsec = mfu_flist_file_get_atime_nsec(dst_list, dst_index);
        if ((src_atime != dst_atime) || (src_atime_nsec != dst_atime_nsec)) {
            /* file atime is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_ATIME, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_ATIME, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_ATIME, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_ATIME, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(DCMPF_MTIME)) {
        uint64_t src_mtime      = mfu_flist_file_get_mtime(src_list, src_index);
        uint64_t src_mtime_nsec = mfu_flist_file_get_mtime_nsec(src_list, src_index);
        uint64_t dst_mtime      = mfu_flist_file_get_mtime(dst_list, dst_index);
        uint64_t dst_mtime_nsec = mfu_flist_file_get_mtime_nsec(dst_list, dst_index);
        if ((src_mtime != dst_mtime) || (src_mtime_nsec != dst_mtime_nsec)) {
            /* file mtime is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_MTIME, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_MTIME, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_MTIME, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_MTIME, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(DCMPF_CTIME)) {
        uint64_t src_ctime      = mfu_flist_file_get_ctime(src_list, src_index);
        uint64_t src_ctime_nsec = mfu_flist_file_get_ctime_nsec(src_list, src_index);
        uint64_t dst_ctime      = mfu_flist_file_get_ctime(dst_list, dst_index);
        uint64_t dst_ctime_nsec = mfu_flist_file_get_ctime_nsec(dst_list, dst_index);
        if ((src_ctime != dst_ctime) || (src_ctime_nsec != dst_ctime_nsec)) {
            /* file ctime is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_CTIME, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CTIME, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_CTIME, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CTIME, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(DCMPF_PERM)) {
        uint64_t src = mfu_flist_file_get_perm(src_list, src_index);
        uint64_t dst = mfu_flist_file_get_perm(dst_list, dst_index);
        if (src != dst) {
            /* file perm is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_PERM, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_PERM, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_PERM, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_PERM, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(DCMPF_ACL)) {
        dcmp_compare_acl(key, src_list,src_index,
                         dst_list, dst_index,
                         src_map, dst_map, &diff);
    }

    return diff;
}

/* use Allreduce to get the total number of bytes read if
 * data was compared */
static uint64_t get_total_bytes_read(mfu_flist src_compare_list) {

    /* get counter for flist id & byte_count */
    uint64_t idx;
    uint64_t byte_count = 0;

    /* get size of flist */
    uint64_t size = mfu_flist_size(src_compare_list);

    /* count up the number of bytes in src list
     * multiply by two in order to include number
     * of bytes read in dst list as well */
    for (idx = 0; idx < size; idx++) {
        byte_count += mfu_flist_file_get_size(src_compare_list, idx) * 2;
    }

    /* buffer for total byte count for Allreduce */
    uint64_t total_bytes_read;

    /* get total number of bytes across all processes */
    MPI_Allreduce(&byte_count, &total_bytes_read, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* return the toal number of bytes */
    return total_bytes_read;
}

/* variable to hold total bytes to be compared for computing progress and estimated time remaining */
static uint64_t compare_total_count;

static void compare_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    uint64_t bytes = vals[0];

    /* compute average delete rate */
    double byte_rate  = 0.0;
    if (secs > 0) {
        byte_rate  = (double)bytes / secs;
    }

    /* compute percentage of items removed */
    double percent = 0.0;
    if (compare_total_count > 0) {
        percent = 100.0 * (double)bytes / (double)compare_total_count;
    }

    /* estimate seconds remaining */
    double secs_remaining = -1.0;
    if (byte_rate > 0.0) {
        secs_remaining = (double)(compare_total_count - bytes) / byte_rate;
    }

    /* convert bytes to units */
    double agg_size_tmp;
    const char* agg_size_units;
    mfu_format_bytes(bytes, &agg_size_tmp, &agg_size_units);

    /* convert bandwidth to units */
    double agg_rate_tmp;
    const char* agg_rate_units;
    mfu_format_bw(byte_rate, &agg_rate_tmp, &agg_rate_units);

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Compared %.3lf %s (%.0f%%) in %.3lf secs (%.3lf %s) %.0f secs left ...",
            agg_size_tmp, agg_size_units, percent, secs, agg_rate_tmp, agg_rate_units, secs_remaining);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Compared %.3lf %s (%.0f%%) in %.3lf secs (%.3lf %s) done",
            agg_size_tmp, agg_size_units, percent, secs, agg_rate_tmp, agg_rate_units);
    }
}

/* given a list of source/destination files to compare, spread file
 * sections to processes to compare in parallel, fill
 * in comparison results in source and dest string maps */
static int dcmp_strmap_compare_data(
    mfu_flist src_compare_list,
    strmap* src_map,
    mfu_flist dst_compare_list,
    strmap* dst_map,
    size_t strlen_prefix,
    mfu_copy_opts_t* copy_opts,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume we'll succeed */
    int rc = 0;

    /* let user know what we're doing */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
         MFU_LOG(MFU_LOG_INFO, "Comparing file contents");
    }

    /* first, count number of bytes in our part of the source list,
     * and double it to count for bytes to read in destination */
    uint64_t idx;
    uint64_t size = mfu_flist_size(src_compare_list);
    uint64_t bytes = 0;
    for (idx = 0; idx < size; idx++) {
        /* count regular files and symlinks */
        mfu_filetype type = mfu_flist_file_get_type(src_compare_list, idx);
        if (type == MFU_TYPE_FILE) {
            bytes += mfu_flist_file_get_size(src_compare_list, idx);
        }
    }
    bytes *= 2;

    /* get total for print percent progress while creating */
    compare_total_count = 0;
    MPI_Allreduce(&bytes, &compare_total_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* get chunk size for copying files */
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

    /* start progress messages when comparing data */
    mfu_progress* prg = mfu_progress_start(mfu_progress_timeout, 2, MPI_COMM_WORLD, compare_progress_fn);

    /* compare bytes for each file section and set flag based on what we find */
    uint64_t i = 0;
    const mfu_file_chunk* src_p = src_head;
    const mfu_file_chunk* dst_p = dst_head;
    uint64_t bytes_read    = 0;
    uint64_t bytes_written = 0;
    for (i = 0; i < list_count; i++) {
        /* get offset into file that we should compare (bytes) */
        off_t offset = (off_t)src_p->offset;

        /* get length of section that we should compare (bytes) */
        off_t length = (off_t)src_p->length;

        /* get size of file that we should compare (bytes) */
        off_t filesize = (off_t)src_p->file_size;

        /* compare the contents of the files */
        int overwrite = 0;
        int compare_rc = mfu_compare_contents(src_p->name, dst_p->name, offset, length, filesize,
                overwrite, copy_opts, &bytes_read, &bytes_written, prg, mfu_src_file, mfu_dst_file);
        if (compare_rc == -1) {
            /* we hit an error while reading */
            rc = -1;
            MFU_LOG(MFU_LOG_ERR,
              "Failed to open, lseek, or read %s and/or %s. Assuming contents are different.",
                 src_p->name, dst_p->name);

            /* consider files to be different,
             * they could be the same, but we'll draw attention to them this way */
            compare_rc = 1;
        }

        /* record results of comparison */
        vals[i] = compare_rc;

        /* update pointers for src and dest in linked list */
        src_p = src_p->next;
        dst_p = dst_p->next;
    }

    /* finalize progress messages */
    uint64_t count_bytes[2];
    count_bytes[0] = bytes_read;
    count_bytes[1] = bytes_written;
    mfu_progress_complete(count_bytes, &prg);

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
            dcmp_strmap_item_update(src_map, name, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, name, DCMPF_CONTENT, DCMPS_DIFFER);

        } else {
            /* update to say contents of the files were found to be the same */
            dcmp_strmap_item_update(src_map, name, DCMPF_CONTENT, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, name, DCMPF_CONTENT, DCMPS_COMMON);
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

static void time_strmap_compare(mfu_flist src_list, double start_compare,
                                double end_compare, time_t *time_started,
                                time_t *time_ended, uint64_t total_bytes_read) {

    /* if the verbose option is set print the timing data
        report compare count, time, and rate */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
       /* find out how many files were compared */
       uint64_t all_count = mfu_flist_global_size(src_list);

       /* get the amount of time the compare function took */
       double time_diff = end_compare - start_compare;

       /* calculate byte and file rate */
       double file_rate = 0.0;
       double byte_rate = 0.0;
       if (time_diff > 0.0) {
           file_rate = ((double)all_count) / time_diff;
           byte_rate = ((double)total_bytes_read) / time_diff;
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

       /* convert size to units */
       double size_tmp;
       const char* size_units;
       mfu_format_bytes(total_bytes_read, &size_tmp, &size_units);

       /* convert bandwidth to units */
       double total_bytes_tmp;
       const char* rate_units;
       mfu_format_bw(byte_rate, &total_bytes_tmp, &rate_units);

       MFU_LOG(MFU_LOG_INFO, "Started   : %s", starttime_str);
       MFU_LOG(MFU_LOG_INFO, "Completed : %s", endtime_str);
       MFU_LOG(MFU_LOG_INFO, "Seconds   : %.3lf", time_diff);
       MFU_LOG(MFU_LOG_INFO, "Items     : %" PRId64, all_count);
       MFU_LOG(MFU_LOG_INFO, "Item Rate : %lu items in %f seconds (%f items/sec)",
            all_count, time_diff, file_rate);
       MFU_LOG(MFU_LOG_INFO, "Bytes read: %.3lf %s (%" PRId64 " bytes)",
            size_tmp, size_units, total_bytes_read);
       MFU_LOG(MFU_LOG_INFO, "Byte Rate : %.3lf %s (%.3" PRId64 " bytes in %.3lf seconds)",
            total_bytes_tmp, rate_units, total_bytes_read, time_diff);
    }
}

/* compare entries from src into dst */
static int dcmp_strmap_compare(
    mfu_flist src_list,
    strmap* src_map,
    mfu_flist dst_list,
    strmap* dst_map,
    size_t strlen_prefix,
    mfu_copy_opts_t* copy_opts,
    const mfu_param_path* src_path,
    const mfu_param_path* dest_path,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume we'll succeed */
    int rc = 0;
    int tmp_rc;

    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);

    /* let user know what we're doing */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
         MFU_LOG(MFU_LOG_INFO, "Comparing items");
    }

    time_t   time_started;
    time_t   time_ended;

    double start_compare = MPI_Wtime();
    time(&time_started);

    /* create compare_lists */
    mfu_flist src_compare_list = mfu_flist_subset(src_list);
    mfu_flist dst_compare_list = mfu_flist_subset(dst_list);

    /* get mtime seconds and nsecs to check modification times of src & dst */
    uint64_t src_mtime;
    uint64_t src_mtime_nsec;
    uint64_t dst_mtime;
    uint64_t dst_mtime_nsec;

    /* iterate over each item in source map */
    const strmap_node* node;
    strmap_foreach(src_map, node) {

        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of source file */
        uint64_t src_index;
        tmp_rc = dcmp_strmap_item_index(src_map, key, &src_index);
        assert(tmp_rc == 0);

        /* get index of destination file */
        uint64_t dst_index;
        tmp_rc = dcmp_strmap_item_index(dst_map, key, &dst_index);

        /* get mtime seconds and nsecs to check modification times of src & dst */
        src_mtime      = mfu_flist_file_get_mtime(src_list, src_index);
        src_mtime_nsec = mfu_flist_file_get_mtime_nsec(src_list, src_index);
        dst_mtime      = mfu_flist_file_get_mtime(dst_list, dst_index);
        dst_mtime_nsec = mfu_flist_file_get_mtime_nsec(dst_list, dst_index);

        if (tmp_rc) {
            dcmp_strmap_item_update(src_map, key, DCMPF_EXIST, DCMPS_ONLY_SRC);

            /* skip uncommon files, all other states are DCMPS_INIT */
            continue;
        }

        dcmp_strmap_item_update(src_map, key, DCMPF_EXIST, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_EXIST, DCMPS_COMMON);

        /* get modes of files */
        mode_t src_mode = (mode_t) mfu_flist_file_get_mode(src_list,
            src_index);
        mode_t dst_mode = (mode_t) mfu_flist_file_get_mode(dst_list,
            dst_index);

        tmp_rc = dcmp_compare_metadata(src_list, src_map, src_index,
             dst_list, dst_map, dst_index,
             key);

        assert(tmp_rc >= 0);

        if (!dcmp_option_need_compare(DCMPF_TYPE)) {
            /*
             * Skip if no need to compare type.
             * All the following comparison depends on type.
             */
            continue;
        }

        /* check whether files are of the same type */
        if ((src_mode & S_IFMT) != (dst_mode & S_IFMT)) {
            /* file type is different, no need to go any futher */
            dcmp_strmap_item_update(src_map, key, DCMPF_TYPE, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_TYPE, DCMPS_DIFFER);

            if (!dcmp_option_need_compare(DCMPF_CONTENT)) {
                continue;
            }

            /* take them as differ content */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            continue;
        }

        dcmp_strmap_item_update(src_map, key, DCMPF_TYPE, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_TYPE, DCMPS_COMMON);

        if (!dcmp_option_need_compare(DCMPF_CONTENT)) {
            /* Skip if no need to compare content. */
            continue;
        }

        /* for now, we can only compare content of regular files */
        /* TODO: add support for symlinks */
        if (! S_ISREG(dst_mode)) {
            /* not regular file, take them as common content */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            continue;
        }

        dcmp_state state;
        tmp_rc = dcmp_strmap_item_state(src_map, key, DCMPF_SIZE, &state);
        assert(tmp_rc == 0);
        if (state == DCMPS_DIFFER) {
            /* file size is different, their contents should be different */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            continue;
        }

        if (options.lite) {
            if ((src_mtime != dst_mtime) || (src_mtime_nsec != dst_mtime_nsec)) {
                /* modification times are different, assume content is different.
                 * I don't think we can assume contents are different if the
                 * lite option is not on. Because files can have different
                 * modification times, but still have the same content. */
                dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
                dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            } else {
                dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_COMMON);
                dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            }
            continue;
        }

        /* If we get to this point, we need to open files and compare
         * file contents.  We'll first identify all such files so that
         * we can do this comparison in parallel more effectively.  For
         * now copy these files to the list of files we need to compare. */
        mfu_flist_file_copy(src_list, src_index, src_compare_list);
        mfu_flist_file_copy(dst_list, dst_index, dst_compare_list);
    }

    /* summarize lists of files for which we need to compare data contents */
    mfu_flist_summarize(src_compare_list);
    mfu_flist_summarize(dst_compare_list);

    uint64_t cmp_global_size = 0;
    if (!options.lite) {
        /* compare the contents of the files if we have anything in the compare list */
        cmp_global_size = mfu_flist_global_size(src_compare_list);
        if (cmp_global_size > 0) {
            tmp_rc = dcmp_strmap_compare_data(src_compare_list, src_map, dst_compare_list,
                    dst_map, strlen_prefix, copy_opts, mfu_src_file, mfu_dst_file);
            if (tmp_rc < 0) {
                /* got a read error, signal that back to caller */
                rc = -1;
            }
        }
    }

    /* wait for all procs to finish before stopping timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double end_compare = MPI_Wtime();
    time(&time_ended);

    /* initalize total_bytes_read to zero */
    uint64_t total_bytes_read = 0;

    /* get total bytes read (if any) */
    if (cmp_global_size > 0) {
        total_bytes_read = get_total_bytes_read(src_compare_list);
    }

    time_strmap_compare(src_list, start_compare, end_compare, &time_started,
                        &time_ended, total_bytes_read);

    /* free the compare flists */
    mfu_flist_free(&dst_compare_list);
    mfu_flist_free(&src_compare_list);

    return rc;
}

/* loop on the src map to check the results */
static void dcmp_strmap_check_src(strmap* src_map,
                                  strmap* dst_map)
{
    assert(dcmp_option_need_compare(DCMPF_EXIST));
    /* iterate over each item in source map */
    const strmap_node* node;
    strmap_foreach(src_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);
        int only_src = 0;

        /* get index of source file */
        uint64_t src_index;
        int ret = dcmp_strmap_item_index(src_map, key, &src_index);
        assert(ret == 0);

        /* get index of destination file */
        uint64_t dst_index;
        ret = dcmp_strmap_item_index(dst_map, key, &dst_index);
        if (ret) {
            only_src = 1;
        }

        /* First check exist state */
        dcmp_state src_exist_state;
        ret = dcmp_strmap_item_state(src_map, key, DCMPF_EXIST,
            &src_exist_state);
        assert(ret == 0);

        dcmp_state dst_exist_state;
        ret = dcmp_strmap_item_state(dst_map, key, DCMPF_EXIST,
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

        dcmp_field field;
        for (field = 0; field < DCMPF_MAX; field++) {
            if (field == DCMPF_EXIST) {
                continue;
            }

            /* get state of src and dest */
            dcmp_state src_state;
            ret = dcmp_strmap_item_state(src_map, key, field, &src_state);
            assert(ret == 0);

            dcmp_state dst_state;
            ret = dcmp_strmap_item_state(dst_map, key, field, &dst_state);
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
                /* all states are either common, differ or skipped */
                if (dcmp_option_need_compare(field)) {
                    assert(src_state == DCMPS_COMMON || src_state == DCMPS_DIFFER);
                } else {
                    // XXXX
                    if (src_state != DCMPS_INIT) {
                        MFU_LOG(MFU_LOG_ERR, "XXX %s wrong state %s\n",
                                dcmp_field_to_string(field, 1), dcmp_state_to_string(src_state, 1));
                    }
                    assert(src_state == DCMPS_INIT);
                }
            }
        }
    }
}

/* loop on the dest map to check the results */
static void dcmp_strmap_check_dst(strmap* src_map,
    strmap* dst_map)
{
    assert(dcmp_option_need_compare(DCMPF_EXIST));

    /* iterate over each item in dest map */
    const strmap_node* node;
    strmap_foreach(dst_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);
        int only_dest = 0;

        /* get index of destination file */
        uint64_t dst_index;
        int ret = dcmp_strmap_item_index(dst_map, key, &dst_index);
        assert(ret == 0);

        /* get index of source file */
        uint64_t src_index;
        ret = dcmp_strmap_item_index(src_map, key, &src_index);
        if (ret) {
            /* This file only exist in dest */
            only_dest = 1;
        }

        /* First check exist state */
        dcmp_state src_exist_state;
        ret = dcmp_strmap_item_state(src_map, key, DCMPF_EXIST,
            &src_exist_state);
        if (only_dest) {
            assert(ret);
        } else {
            assert(ret == 0);
        }

        dcmp_state dst_exist_state;
        ret = dcmp_strmap_item_state(dst_map, key, DCMPF_EXIST,
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

        dcmp_field field;
        for (field = 0; field < DCMPF_MAX; field++) {
            if (field == DCMPF_EXIST) {
                continue;
            }

            /* get state of src and dest */
            dcmp_state src_state;
            ret = dcmp_strmap_item_state(src_map, key, field,
                &src_state);
            if (only_dest) {
                assert(ret);
            } else {
                assert(ret == 0);
            }

            dcmp_state dst_state;
            ret = dcmp_strmap_item_state(dst_map, key, field,
                &dst_state);
            assert(ret == 0);

            if (only_dest) {
                /* all states are not checked */
                assert(dst_state == DCMPS_INIT);
            } else {
                assert(src_state == dst_state);
                /* all states are either common, differ or skipped */
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
                /* all states are either common, differ or skipped */
                if (dcmp_option_need_compare(field)) {
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
static void dcmp_strmap_check(
    strmap* src_map,
    strmap* dst_map)
{
    dcmp_strmap_check_src(src_map, dst_map);
    dcmp_strmap_check_dst(src_map, dst_map);
}

static int dcmp_map_fn(
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

static struct dcmp_expression* dcmp_expression_alloc(void)
{
    struct dcmp_expression *expression;

    expression = (struct dcmp_expression*)
        MFU_MALLOC(sizeof(struct dcmp_expression));
    INIT_LIST_HEAD(&expression->linkage);

    return expression;
}

static void dcmp_expression_free(struct dcmp_expression *expression)
{
    assert(list_empty(&expression->linkage));
    mfu_free(&expression);
}

static void dcmp_expression_print(
    struct dcmp_expression *expression,
    int simple)
{
    if (simple) {
        printf("(%s = %s)", dcmp_field_to_string(expression->field, 1),
            dcmp_state_to_string(expression->state, 1));
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
            printf("have %s %s", dcmp_state_to_string(expression->state, 0),
                   dcmp_field_to_string(expression->field, 0));
            if (expression->state == DCMPS_DIFFER) {
                /* Make sure plurality is valid */
                printf("s");
            }
        }
    }
}

static int dcmp_expression_match(
    struct dcmp_expression *expression,
    strmap* map,
    const char* key)
{
    int ret;
    dcmp_state state;
    dcmp_state exist_state;

    ret = dcmp_strmap_item_state(map, key, DCMPF_EXIST, &exist_state);
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

    ret = dcmp_strmap_item_state(map, key, expression->field, &state);
    assert(ret == 0);

     /* All fields should have been compared. */
    assert(state == DCMPS_COMMON || state == DCMPS_DIFFER);
    if (expression->state == state) {
        return 1;
    }

    return 0;
}

static struct dcmp_conjunction* dcmp_conjunction_alloc(void)
{
    struct dcmp_conjunction *conjunction;

    conjunction = (struct dcmp_conjunction*)
        MFU_MALLOC(sizeof(struct dcmp_conjunction));
    INIT_LIST_HEAD(&conjunction->linkage);
    INIT_LIST_HEAD(&conjunction->expressions);
    conjunction->src_matched_list = mfu_flist_new();
    conjunction->dst_matched_list = mfu_flist_new();

    return conjunction;
}

static void dcmp_conjunction_add_expression(
    struct dcmp_conjunction* conjunction,
    struct dcmp_expression* expression)
{
    assert(list_empty(&expression->linkage));
    list_add_tail(&expression->linkage, &conjunction->expressions);
}

static void dcmp_conjunction_free(struct dcmp_conjunction *conjunction)
{
    struct dcmp_expression* expression;
    struct dcmp_expression* n;

    assert(list_empty(&conjunction->linkage));
    list_for_each_entry_safe(expression,
                             n,
                             &conjunction->expressions,
                             linkage) {
        list_del_init(&expression->linkage);
        dcmp_expression_free(expression);
    }
    assert(list_empty(&conjunction->expressions));
    mfu_flist_free(&conjunction->src_matched_list);
    mfu_flist_free(&conjunction->dst_matched_list);
    mfu_free(&conjunction);
}

static void dcmp_conjunction_print(
    struct dcmp_conjunction *conjunction,
    int simple)
{
    struct dcmp_expression* expression;

    if (simple) {
        printf("(");
    }
    list_for_each_entry(expression,
                        &conjunction->expressions,
                        linkage) {
        dcmp_expression_print(expression, simple);
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
static int dcmp_conjunction_match(
    struct dcmp_conjunction *conjunction,
    strmap* map,
    const char* key)
{
    struct dcmp_expression* expression;
    int matched;

    list_for_each_entry(expression,
                        &conjunction->expressions,
                        linkage) {
        matched = dcmp_expression_match(expression, map, key);
        if (!matched) {
            return 0;
        }
    }
    return 1;
}

static struct dcmp_disjunction* dcmp_disjunction_alloc(void)
{
    struct dcmp_disjunction *disjunction;

    disjunction = (struct dcmp_disjunction*)
        MFU_MALLOC(sizeof(struct dcmp_disjunction));
    INIT_LIST_HEAD(&disjunction->linkage);
    INIT_LIST_HEAD(&disjunction->conjunctions);
    disjunction->count = 0;

    return disjunction;
}

static void dcmp_disjunction_add_conjunction(
    struct dcmp_disjunction* disjunction,
    struct dcmp_conjunction* conjunction)
{
    assert(list_empty(&conjunction->linkage));
    list_add_tail(&conjunction->linkage, &disjunction->conjunctions);
    disjunction->count++;
}

static void dcmp_disjunction_free(struct dcmp_disjunction* disjunction)
{
    struct dcmp_conjunction *conjunction;
    struct dcmp_conjunction *n;

    assert(list_empty(&disjunction->linkage));
    list_for_each_entry_safe(conjunction,
                             n,
                             &disjunction->conjunctions,
                             linkage) {
        list_del_init(&conjunction->linkage);
        dcmp_conjunction_free(conjunction);
    }
    assert(list_empty(&disjunction->conjunctions));
    mfu_free(&disjunction);
}

static void dcmp_disjunction_print(
    struct dcmp_disjunction* disjunction,
    int simple,
    int indent)
{
    struct dcmp_conjunction *conjunction;

    list_for_each_entry(conjunction,
                        &disjunction->conjunctions,
                        linkage) {
        dcmp_conjunction_print(conjunction, simple);

        int size_src_matched = mfu_flist_global_size(conjunction->src_matched_list);
        int size_dst_matched = mfu_flist_global_size(conjunction->dst_matched_list);

        /* if src and dst don't match src and dest numbers need to
         * be reported separately */
        if (size_src_matched == size_dst_matched) {
            printf(": %lu (Src: %lu Dest: %lu)", size_src_matched,
                   size_src_matched, size_dst_matched);
        } else {
            printf(": N/A (Src: %lu Dest: %lu)", size_src_matched,
                   size_dst_matched);
        }

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
static int dcmp_disjunction_match(
    struct dcmp_disjunction* disjunction,
    strmap* map,
    const char* key,
    int is_src)
{
    struct dcmp_conjunction *conjunction;
    int matched;

    list_for_each_entry(conjunction,
                        &disjunction->conjunctions,
                        linkage) {
        matched = dcmp_conjunction_match(conjunction, map, key);
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

static struct dcmp_output* dcmp_output_alloc(void)
{
    struct dcmp_output* output;

    output = (struct dcmp_output*) MFU_MALLOC(sizeof(struct dcmp_output));
    output->file_name = NULL;
    INIT_LIST_HEAD(&output->linkage);
    output->disjunction = NULL;

    return output;
}

static void dcmp_output_init_disjunction(
    struct dcmp_output* output,
    struct dcmp_disjunction* disjunction)
{
    assert(output->disjunction == NULL);
    output->disjunction = disjunction;
}

static void dcmp_output_free(struct dcmp_output* output)
{
    assert(list_empty(&output->linkage));
    if (output->disjunction != NULL) {
        dcmp_disjunction_free(output->disjunction);
        output->disjunction = NULL;
    }
    if (output->file_name != NULL) {
        mfu_free(&output->file_name);
    }
    mfu_free(&output);
}

static void dcmp_option_fini(void)
{
    struct dcmp_output* output;
    struct dcmp_output* n;

    list_for_each_entry_safe(output,
                             n,
                             &options.outputs,
                             linkage) {
        list_del_init(&output->linkage);
        dcmp_output_free(output);
    }
    assert(list_empty(&options.outputs));
}

static void dcmp_option_add_output(struct dcmp_output *output, int add_at_head)
{
    assert(list_empty(&output->linkage));
    if (add_at_head) {
        list_add(&output->linkage, &options.outputs);
    } else {
        list_add_tail(&output->linkage, &options.outputs);
    }
}

static void dcmp_option_add_comparison(dcmp_field field)
{
    uint64_t depend = dcmp_field_depend[field];
    uint64_t i;
    for (i = 0; i < DCMPF_MAX; i++) {
        if ((depend & ((uint64_t)1 << i)) != (uint64_t)0) {
            options.need_compare[i] = 1;
        }
    }
}

static int dcmp_output_flist_match(
    struct dcmp_output *output,
    strmap* map,
    mfu_flist flist,
    mfu_flist new_flist,
    mfu_flist *matched_flist,
    int is_src)
{
    const strmap_node* node;
    struct dcmp_conjunction *conjunction;

    /* iterate over each item in map */
    strmap_foreach(map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of file */
        uint64_t idx;
        int ret = dcmp_strmap_item_index(map, key, &idx);
        assert(ret == 0);

        if (dcmp_disjunction_match(output->disjunction, map, key, is_src)) {
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

#define DCMP_OUTPUT_PREFIX "Number of items that "

static int dcmp_output_write(
    struct dcmp_output *output,
    mfu_flist src_flist,
    strmap* src_map,
    mfu_flist dst_flist,
    strmap* dst_map)
{
    int ret = 0;
    mfu_flist new_flist = mfu_flist_subset(src_flist);

    /* find matched file in source map */
    mfu_flist src_matched = mfu_flist_new();
    ret = dcmp_output_flist_match(output, src_map, src_flist,
                                  new_flist, &src_matched, 1);
    assert(ret == 0);

    /* find matched file in dest map */
    mfu_flist dst_matched = mfu_flist_new();
    ret = dcmp_output_flist_match(output, dst_map, dst_flist,
                                  new_flist, &dst_matched, 0);
    assert(ret == 0);

    mfu_flist_summarize(new_flist);
    mfu_flist_summarize(src_matched);
    mfu_flist_summarize(dst_matched);
    if (output->file_name != NULL) {
        if (options.format) {
            mfu_flist_write_cache(output->file_name, new_flist);
        } else {
            mfu_flist_write_text(output->file_name, new_flist);
        }
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == 0) {
        printf(DCMP_OUTPUT_PREFIX);
        dcmp_disjunction_print(output->disjunction, 0,
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

static int dcmp_outputs_write(
    mfu_flist src_list,
    strmap* src_map,
    mfu_flist dst_list,
    strmap* dst_map)
{
    struct dcmp_output* output;
    int ret = 0;

    list_for_each_entry(output,
                        &options.outputs,
                        linkage) {
        ret = dcmp_output_write(output, src_list, src_map, dst_list, dst_map);
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

static int dcmp_expression_parse(
    struct dcmp_conjunction* conjunction,
    const char* expression_string)
{
    char* tmp = MFU_STRDUP(expression_string);
    char* field_string;
    char* state_string;
    int ret = 0;
    struct dcmp_expression* expression;

    expression = dcmp_expression_alloc();

    state_string = tmp;
    field_string = strsep(&state_string, DCMP_EXPRESSION_DELIMITER);
    if (!*field_string || state_string == NULL || !*state_string) {
        fprintf(stderr,
            "expression %s illegal, field \"%s\", state \"%s\"\n",
            expression_string, field_string, state_string);
        ret = -EINVAL;
        goto out;
    }

    ret = dcmp_field_from_string(field_string, &expression->field);
    if (ret) {
        fprintf(stderr,
            "field \"%s\" illegal\n",
            field_string);
        ret = -EINVAL;
        goto out;
    }

    ret = dcmp_state_from_string(state_string, &expression->state);
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

    dcmp_conjunction_add_expression(conjunction, expression);

    /* Add comparison we need for this expression */
    dcmp_option_add_comparison(expression->field);
out:
    if (ret) {
        dcmp_expression_free(expression);
    }
    mfu_free(&tmp);
    return ret;
}

static int dcmp_conjunction_parse(
    struct dcmp_disjunction* disjunction,
    const char* conjunction_string)
{
    int ret = 0;
    char* tmp = MFU_STRDUP(conjunction_string);
    char* expression;
    char* next;
    struct dcmp_conjunction* conjunction;

    conjunction = dcmp_conjunction_alloc();

    next = tmp;
    while ((expression = strsep(&next, DCMP_CONJUNCTION_DELIMITER))) {
        if (!*expression) {
            /* empty */
            continue;
        }

        ret = dcmp_expression_parse(conjunction, expression);
        if (ret) {
            fprintf(stderr,
                "failed to parse expression \"%s\"\n", expression);
            goto out;
        }
    }

    dcmp_disjunction_add_conjunction(disjunction, conjunction);
out:
    if (ret) {
        dcmp_conjunction_free(conjunction);
    }
    mfu_free(&tmp);
    return ret;
}

static int dcmp_disjunction_parse(
    struct dcmp_output *output,
    const char *disjunction_string)
{
    int ret = 0;
    char* tmp = MFU_STRDUP(disjunction_string);
    char* conjunction = NULL;
    char* next;
    struct dcmp_disjunction* disjunction;

    disjunction = dcmp_disjunction_alloc();

    next = tmp;
    while ((conjunction = strsep(&next, DCMP_DISJUNCTION_DELIMITER))) {
        if (!*conjunction) {
            /* empty */
            continue;
        }

        ret = dcmp_conjunction_parse(disjunction, conjunction);
        if (ret) {
            fprintf(stderr,
                "failed to parse conjunction \"%s\"\n", conjunction);
            goto out;
        }
    }

    dcmp_output_init_disjunction(output, disjunction);
out:
    if (ret) {
        dcmp_disjunction_free(disjunction);
    }
    mfu_free(&tmp);
    return ret;
}

static int dcmp_option_output_parse(const char *option, int add_at_head)
{
    char* tmp = MFU_STRDUP(option);
    char* disjunction;
    char* file_name;
    int ret = 0;
    struct dcmp_output* output;

    output = dcmp_output_alloc();

    file_name = tmp;
    disjunction = strsep(&file_name, DCMP_PATH_DELIMITER);
    if (!*disjunction) {
        fprintf(stderr,
            "output string illegal, disjunction \"%s\", file name \"%s\"\n",
            disjunction, file_name);
        ret = -EINVAL;
        goto out;
    }

    ret = dcmp_disjunction_parse(output, disjunction);
    if (ret) {
        goto out;
    }

    if (file_name != NULL && *file_name) {
        output->file_name = MFU_STRDUP(file_name);
    }
    dcmp_option_add_output(output, add_at_head);
out:
    if (ret) {
        dcmp_output_free(output);
    }
    mfu_free(&tmp);
    return ret;
}

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

    /* allocate structure to define walk options */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* allocate structure to define copy (comparision) options */
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
    mfu_debug_level = MFU_LOG_VERBOSE;

#ifdef DAOS_SUPPORT
    /* DAOS vars */ 
    daos_args_t* daos_args = daos_args_new();    
#endif

    int option_index = 0;
    static struct option long_options[] = {
        {"output",        1, 0, 'o'},
        {"text",          0, 0, 't'},
        {"base",          0, 0, 'b'},
        {"bufsize",       1, 0, 'B'},
        {"chunksize",     1, 0, 'k'},
        {"daos-prefix",   1, 0, 'X'},
        {"daos-api",      1, 0, 'x'},
        {"direct",        0, 0, 's'},
        {"progress",      1, 0, 'R'},
        {"verbose",       0, 0, 'v'},
        {"quiet",         0, 0, 'q'},
        {"lite",          0, 0, 'l'},
        {"debug",         0, 0, 'd'},
        {"help",          0, 0, 'h'},
        {0, 0, 0, 0}
    };
    int ret = 0;
    int i;

    /* read in command line options */
    int usage = 0;
    int help  = 0;
    unsigned long long bytes = 0;
    while (1) {
        int c = getopt_long(
            argc, argv, "o:tbsvqldh",
            long_options, &option_index
        );

        if (c == -1) {
            break;
        }

        switch (c) {
        case 'o':
            ret = dcmp_option_output_parse(optarg, 0);
            if (ret) {
                usage = 1;
            }
            break;
        case 't':
            options.format = 0;
            break;
        case 'b':
            options.base++;
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
        case 's':
            copy_opts->direct = true;
            if(rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Using O_DIRECT");
            }
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
        case 'l':
            options.lite++;
            break;
        case 'd':
            options.debug++;
            break;
#ifdef DAOS_SUPPORT
        case 'X':
            daos_args->dfs_prefix = MFU_STRDUP(optarg);
            break;
        case 'x':
            if (daos_parse_api_str(optarg, &daos_args->api) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to parse --daos-api");
                usage = 1;
            }
            break;
#endif
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
    if (options.base || list_empty(&options.outputs)) {
        /*
         * If -o option is not given,
         * we want to add default output,
         * in case there is no output at all.
         */
        for (i = 0; ; i++) {
            if (dcmp_default_outputs[i] == NULL) {
                break;
            }
            ret = dcmp_option_output_parse(dcmp_default_outputs[i], 1);
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
        dcmp_option_fini();
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* allocate space for each path */
    mfu_param_path* paths = (mfu_param_path*) MFU_MALLOC((size_t)numargs * sizeof(mfu_param_path));

    /* pointer to path arguments */
    char** argpaths = (&argv[optind]);

#ifdef DAOS_SUPPORT
    /* For error handling */
    bool daos_do_cleanup = false;
    bool daos_do_exit = false;
    
    /* Set up DAOS arguments, containers, dfs, etc. */
    int daos_rc = daos_setup(rank, argpaths, numargs, daos_args, mfu_src_file, mfu_dst_file);
    if (daos_rc != 0) {
        daos_do_exit = true;
    }

    /* Not yet supported */
    if (mfu_src_file->type == DAOS || mfu_dst_file->type == DAOS) {
        MFU_LOG(MFU_LOG_ERR, "dcmp only supports DAOS POSIX containers with the DFS API.");
        daos_do_cleanup = true;
        daos_do_exit = true;
    }

    if (daos_do_cleanup) {
        daos_cleanup(daos_args, mfu_src_file, mfu_dst_file);
    }
    if (daos_do_exit) {
        dcmp_option_fini();
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }
#endif

    /* process each path */
    mfu_param_path_set_all(numargs, (const char**)argpaths, paths, mfu_src_file, true);

    /* advance to next set of options */
    optind += numargs;

    /* first item is source and second is dest */
    const mfu_param_path* srcpath  = &paths[0];
    const mfu_param_path* destpath = &paths[1];

    /* create an empty file list */
    mfu_flist flist1 = mfu_flist_new();
    mfu_flist flist2 = mfu_flist_new();

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Walking source path");
    }
    mfu_flist_walk_param_paths(1,  srcpath, walk_opts, flist1, mfu_src_file);

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Walking destination path");
    }
    mfu_flist_walk_param_paths(1, destpath, walk_opts, flist2, mfu_dst_file);

    /* store src and dest path strings */
    const char* path1 = srcpath->path;
    const char* path2 = destpath->path;

    /* map files to ranks based on portion following prefix directory */
    mfu_flist flist3 = mfu_flist_remap(flist1, (mfu_flist_map_fn)dcmp_map_fn, (const void*)path1);
    mfu_flist flist4 = mfu_flist_remap(flist2, (mfu_flist_map_fn)dcmp_map_fn, (const void*)path2);

    /* map each file name to its index and its comparison state */
    strmap* map1 = dcmp_strmap_creat(flist3, path1);
    strmap* map2 = dcmp_strmap_creat(flist4, path2);

    /* compare files in map1 with those in map2 */
    int tmp_rc = dcmp_strmap_compare(flist3, map1, flist4, map2, strlen(path1), copy_opts, srcpath, destpath,
                                     mfu_src_file, mfu_dst_file);
    if (tmp_rc < 0) {
        /* hit a read error on at least one file */
        rc = 1;
    }

    /* check the results are valid */
    if (options.debug) {
        dcmp_strmap_check(map1, map2);
    }

    /* write data to cache files and print summary */
    dcmp_outputs_write(flist3, map1, flist4, map2);

    /* free maps of file names to comparison state info */
    strmap_delete(&map1);
    strmap_delete(&map2);

    /* free file lists */
    mfu_flist_free(&flist1);
    mfu_flist_free(&flist2);
    mfu_flist_free(&flist3);
    mfu_flist_free(&flist4);

    /* free all param paths */
    mfu_param_path_free_all(numargs, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    dcmp_option_fini();

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

    /* shut down */
    mfu_finalize();
    MPI_Finalize();

    return rc;
}
