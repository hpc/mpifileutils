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
#include "list.h"

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
    printf("  -o, --output field0=state0@field1=state1,field2=state2:file "
    	   "- write list to file\n");
    printf("\n");
    fflush(stdout);
}

typedef enum _dcmp_state {
    /* initial state */
    DCMPS_INIT   = 'A',
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
      * becuase we don't want to waste a loop in dcmp_strmap_compare()
      */
    DCMPS_ONLY_DEST,
    DCMPS_MAX,
} dcmp_state;

typedef enum _dcmp_field {
    DCMPF_EXIST = 0,    /* both have this file */
    DCMPF_TYPE,         /* both are the same type */
    DCMPF_SIZE,         /* both are regular file and have same size */
    DCMPF_UID,          /* both have the same UID */
    DCMPF_GID,          /* both have the same GID */
    DCMPF_ATIME,        /* both have the same atime */
    DCMPF_MTIME,        /* both have the same mtime */
    DCMPF_CTIME,        /* both have the same ctime */
    DCMPF_CONTENT,      /* both have the same data */
//    DCMPF_SAME,         /* both are the same file */
    DCMPF_MAX,
} dcmp_field;

static const char* dcmp_field_to_string(dcmp_field field)
{
    assert(field >= 0);
    assert(field < DCMPF_MAX);
    switch (field) {
    case DCMPF_EXIST:
        return "EXIST";
        break;
    case DCMPF_TYPE:
        return "TYPE";
        break;
    case DCMPF_SIZE:
        return "SIZE";
        break;
    case DCMPF_UID:
        return "UID";
        break;
    case DCMPF_GID:
        return "GID";
        break;
    case DCMPF_ATIME:
        return "ATIME";
        break;
    case DCMPF_MTIME:
        return "MTIME";
        break;
    case DCMPF_CTIME:
        return "CTIME";
        break;
    case DCMPF_CONTENT:
        return "CONTENT";
        break;
//    case DCMPF_SAME:
//        return "SAME";
//        break;
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
        if (strcmp(dcmp_field_to_string(i), string) == 0) {
            *field = i;
            return 0;
        }
    }
    return -ENOENT;
}
static const char* dcmp_state_to_string(dcmp_state state)
{
    switch (state) {
    case DCMPS_INIT:
        return "INIT";
        break;
    case DCMPS_COMMON:
        return "COMMON";
        break;
    case DCMPS_DIFFER:
        return "DIFFER";
        break;
    case DCMPS_ONLY_SRC:
        return "ONLY_SRC";
        break;
    case DCMPS_ONLY_DEST:
        return "ONLY_DEST";
        break;
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
        if (strcmp(dcmp_state_to_string(i), string) == 0) {
            *state = i;
            return 0;
        }
    }
    return -ENOENT;
}

/* given a filename as the key, encode an index followed
 * by the init state */
static void
dcmp_strmap_item_init(strmap* map, const char *key, uint64_t index)
{
    /* Should be long enough for 64 bit number and DCMPF_MAX */
    char val[21 + DCMPF_MAX];
    int i;

    /* encode the index */
    int len = snprintf(val, sizeof(val), "%llu",
                       (unsigned long long) index);

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

static void
dcmp_strmap_item_update(strmap* map, const char *key,
    dcmp_field field, dcmp_state state)
{
    /* Should be long enough for 64 bit number and DCMPF_MAX */
    char new_val[21 + DCMPF_MAX];

    /* lookup item from map */
    const char* val = strmap_get(map, key);

    assert(field >= 0 && field < DCMPF_MAX);
    assert(strlen(val) + 1 <= sizeof(new_val));
    /* copy existing index over */
    strcpy(new_val, val);

    /* set new state value */
    size_t position = strlen(new_val) - DCMPF_MAX;
    new_val[position + field] = state;

    /* reinsert item in map */
    strmap_set(map, key, new_val);
}


static int
dcmp_strmap_item_index(strmap* map, const char *key, uint64_t *index)
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
    *index = strtoull(new_val, NULL, 0);

    return 0;
}

static int
dcmp_strmap_item_state(strmap* map, const char *key, dcmp_field field,
    dcmp_state *state)
{
    /* lookup item from map */
    const char* val = strmap_get(map, key);
    if (val == NULL) {
        return -1;
    }

    /* extract state */
    assert(strlen(val) > DCMPF_MAX);
    assert(field >= 0 && field < DCMPF_MAX);
    size_t position = strlen(val) - DCMPF_MAX;
    *state = val[position + field];

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
int _dcmp_compare_data(const char* src_name, const char* dst_name,
    int src_fd, int dst_fd, off_t offset, size_t size, size_t buff_size)
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
int dcmp_compare_data(const char* src_name, const char* dst_name,
    size_t buff_size)
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
    int rc = _dcmp_compare_data(src_name, dst_name, src_fd, dst_fd,
        0, 0, buff_size);

    /* close files */
    bayer_close(dst_name, dst_fd);
    bayer_close(src_name, src_fd);

    return rc;
}

#define dcmp_compare_field(field_name, field)                                \
do {                                                                         \
    uint64_t src = bayer_flist_file_get_ ## field_name(src_list, src_index); \
    uint64_t dst = bayer_flist_file_get_ ## field_name(dst_list, dst_index); \
    if (src != dst) {                                                        \
        /* file type is different */                                         \
        dcmp_strmap_item_update(src_map, key, field, DCMPS_DIFFER);          \
        dcmp_strmap_item_update(dst_map, key, field, DCMPS_DIFFER);          \
        diff++;                                                              \
    } else {                                                                 \
        dcmp_strmap_item_update(src_map, key, field, DCMPS_COMMON);          \
        dcmp_strmap_item_update(dst_map, key, field, DCMPS_COMMON);          \
    }                                                                        \
} while(0)

/* Return -1 when error, return 0 when equal, return > 0 when diff */
int dcmp_compare_metadata(bayer_flist dst_list,
     strmap* dst_map,
     uint64_t dst_index,
     bayer_flist src_list,
     strmap* src_map,
     uint64_t src_index,
     const char* key)
{
    int diff = 0;

    dcmp_compare_field(size, DCMPF_SIZE);
    dcmp_compare_field(gid, DCMPF_GID);
    dcmp_compare_field(uid, DCMPF_UID);
    dcmp_compare_field(atime, DCMPF_ATIME);
    dcmp_compare_field(mtime, DCMPF_MTIME);
    dcmp_compare_field(ctime, DCMPF_CTIME);

    return diff;
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

        /* get index of source file */
        uint64_t src_index;
        int rc = dcmp_strmap_item_index(src_map, key, &src_index);
        assert(rc == 0);

        /* get index of destination file */
        uint64_t dst_index;
        rc = dcmp_strmap_item_index(dst_map, key, &dst_index);
        if (rc) {
            dcmp_strmap_item_update(src_map, key, DCMPF_EXIST, DCMPS_ONLY_SRC);
            /* skip uncommon files, all other states are DCMPS_INIT */
            continue;
        }

        dcmp_strmap_item_update(src_map, key, DCMPF_EXIST, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_EXIST, DCMPS_COMMON);
        /* found a file names common to both source and destination */
        (*common_file)++;

        /* get modes of files */
        mode_t src_mode = (mode_t) bayer_flist_file_get_mode(src_list,
            src_index);
        mode_t dst_mode = (mode_t) bayer_flist_file_get_mode(dst_list,
            dst_index);

        rc = dcmp_compare_metadata(dst_list, dst_map, dst_index,
            src_list, src_map, src_index, key);
        assert(rc >= 0);

        /* check whether files are of the same type */
        if ((src_mode & S_IFMT) != (dst_mode & S_IFMT)) {
            /* file type is different, no need to go any futher */
            dcmp_strmap_item_update(src_map, key, DCMPF_TYPE, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_TYPE, DCMPS_DIFFER);
            /* take them as differ content */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            continue;
        }

        dcmp_strmap_item_update(src_map, key, DCMPF_TYPE, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_TYPE, DCMPS_COMMON);
        /* file has the same type in both source and destination */
        (*common_type)++;

        /* for now, we can only compare contente of regular files */
        /* TODO: add support for symlinks */
        if (! S_ISREG(dst_mode)) {
            /* not regular file, take them as common content */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            (*common_content)++;
            continue;
        }

        dcmp_state state;
        rc = dcmp_strmap_item_state(src_map, key, DCMPF_SIZE, &state);
        assert(rc == 0);
        if (state == DCMPS_DIFFER) {
            /* file size is different, their contents should be different */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            continue;
        }

        /* the sizes are the same, now compare file contents */
        const char* src_name = bayer_flist_file_get_name(src_list, src_index);
        const char* dst_name = bayer_flist_file_get_name(dst_list, dst_index);
        rc = dcmp_compare_data(src_name, dst_name, 1048576);
        if (rc == 1) {
            /* found a difference in file contents */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            continue;
        } else if (rc < 0) {
            BAYER_ABORT(-1, "Failed to compare file %s and %s errno=%d (%s)",
                src_name, dst_name, errno, strerror(errno));
        }

        dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_COMMON);
        /* files have the same content */
        (*common_content)++;
    }
}

/* loop on the src map to check the results */
static void dcmp_strmap_check_src(strmap* dst_map,
    strmap* src_map)
{
    /* iterate over each item in source map */
    strmap_node* node;
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
            ret = dcmp_strmap_item_state(src_map, key, field,
                &src_state);
            assert(ret == 0);

            dcmp_state dst_state;
            ret = dcmp_strmap_item_state(dst_map, key, field,
                &dst_state);
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
                /* all states are either common or differ */
                if (!(src_state == DCMPS_COMMON ||
                    src_state == DCMPS_DIFFER)) {
                    printf("key %s, field %d, src_state %c\n", key, field,
                        src_state);
                }
                assert(src_state == DCMPS_COMMON ||
                    src_state == DCMPS_DIFFER);
            }
        }
    }
}

/* loop on the dest map to check the results */
static void dcmp_strmap_check_dst(strmap* dst_map,
    strmap* src_map)
{
    /* iterate over each item in dest map */
    strmap_node* node;
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
                /* all states are either common or differ */
                assert(src_state == DCMPS_COMMON ||
                    src_state == DCMPS_DIFFER);
            }

            if (only_dest || dst_exist_state == DCMPS_ONLY_SRC) {
                /* This file never checked for dest */
                assert(dst_state == DCMPS_INIT);
            } else {
                /* all stats of source and dest are the same */
                assert(src_state == dst_state);
                /* all states are either common or differ */
                assert(src_state == DCMPS_COMMON ||
                    src_state == DCMPS_DIFFER);
            }
        }
    }
}

/* check the result maps are valid */
static void dcmp_strmap_check(strmap* dst_map,
    strmap* src_map)
{
    dcmp_strmap_check_src(dst_map, src_map);
    dcmp_strmap_check_dst(dst_map, src_map);
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
    uint32_t hash = bayer_hash_jenkins(ptr, ptr_len);
    int rank = (int) (hash % (uint32_t)ranks);
    return rank;
}

struct dcmp_expression {
    dcmp_field field;              /* the concerned field */
    dcmp_state state;              /* expected state of the field */
    struct list_head linkage;      /* linkage to struct dcmp_conjunction */
};

struct dcmp_conjunction {
    struct list_head linkage;      /* linkage to struct dcmp_disjunction */
    struct list_head expressions; /* list of logical conjunction */
};

struct dcmp_disjunction {
    struct list_head linkage;      /* linkage to struct dcmp_output */
    struct list_head conjunctions; /* list of logical conjunction */
};

struct dcmp_output {
    char* file_name;               /* output file name */
    struct list_head linkage;      /* linkage to struct dcmp_options */
    struct dcmp_disjunction *disjunction; /* logical disjunction rules */
};

struct dcmp_options {
    struct list_head outputs;      /* list of outputs */
    int verbose;
};

struct dcmp_options options = {
    .outputs = LIST_HEAD_INIT(options.outputs),
    .verbose  = 0,
};

static struct dcmp_expression* dcmp_expression_alloc()
{
    struct dcmp_expression *expression;

    expression = (struct dcmp_expression*)
        BAYER_MALLOC(sizeof(struct dcmp_expression));
    INIT_LIST_HEAD(&expression->linkage);

    return expression;
}

static void dcmp_expression_free(struct dcmp_expression *expression)
{
    assert(list_empty(&expression->linkage));
    bayer_free(&expression);
}

static void dcmp_expression_dump(struct dcmp_expression *expression)
{
    printf("(%s = %s)", dcmp_field_to_string(expression->field),
        dcmp_state_to_string(expression->state));
}

static int dcmp_expression_match(struct dcmp_expression *expression,
    strmap* map, const char* key)
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

static struct dcmp_conjunction* dcmp_conjunction_alloc()
{
    struct dcmp_conjunction *conjunction;

    conjunction = (struct dcmp_conjunction*)
        BAYER_MALLOC(sizeof(struct dcmp_conjunction));
    INIT_LIST_HEAD(&conjunction->linkage);
    INIT_LIST_HEAD(&conjunction->expressions);

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
    bayer_free(&conjunction);
}

static void dcmp_conjunction_dump(struct dcmp_conjunction *conjunction)
{
    struct dcmp_expression* expression;

    printf("(");
    list_for_each_entry(expression,
                        &conjunction->expressions,
                        linkage) {
        dcmp_expression_dump(expression);
        if (expression->linkage.next != &conjunction->expressions) {
            printf("&&");
        }
    }
    printf(")");
}

/* if matched return 1, else return 0 */
static int dcmp_conjunction_match(struct dcmp_conjunction *conjunction,
    strmap* map, const char* key)
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

static struct dcmp_disjunction* dcmp_disjunction_alloc()
{
    struct dcmp_disjunction *disjunction;

    disjunction = (struct dcmp_disjunction*)
        BAYER_MALLOC(sizeof(struct dcmp_disjunction));
    INIT_LIST_HEAD(&disjunction->linkage);
    INIT_LIST_HEAD(&disjunction->conjunctions);

    return disjunction;
}

static void dcmp_disjunction_add_conjunction(
    struct dcmp_disjunction* disjunction,
    struct dcmp_conjunction* conjunction)
{
    assert(list_empty(&conjunction->linkage));
    list_add_tail(&conjunction->linkage, &disjunction->conjunctions);
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
    bayer_free(&disjunction);
}

static void dcmp_disjunction_dump(struct dcmp_disjunction* disjunction)
{
    struct dcmp_conjunction *conjunction;

    list_for_each_entry(conjunction,
                        &disjunction->conjunctions,
                        linkage) {
        dcmp_conjunction_dump(conjunction);
        if (conjunction->linkage.next != &disjunction->conjunctions) {
            printf("||");
        }
    }
}

/* if matched return 1, else return 0 */
static int dcmp_disjunction_match(struct dcmp_disjunction* disjunction,
    strmap* map, const char* key)
{
    struct dcmp_conjunction *conjunction;
    int matched;

    list_for_each_entry(conjunction,
                        &disjunction->conjunctions,
                        linkage) {
        matched = dcmp_conjunction_match(conjunction, map, key);
        if (matched) {
            return 1;
        }
    }
    return 0;
}

static struct dcmp_output* dcmp_output_alloc()
{
    struct dcmp_output* output;

    output = (struct dcmp_output*) BAYER_MALLOC(sizeof(struct dcmp_output));
    output->file_name = NULL;
    INIT_LIST_HEAD(&output->linkage);
    output->disjunction = NULL;

    return output;
}

static void dcmp_output_init_disjunction(struct dcmp_output* output,
    struct dcmp_disjunction* disjunction)
{
    assert(output->disjunction == NULL);
    output->disjunction = disjunction;
}

static void dcmp_output_free(struct dcmp_output* output)
{
    assert(list_empty(&output->linkage));
    dcmp_disjunction_free(output->disjunction);
    output->disjunction = NULL;
    bayer_free(&output->file_name);
    bayer_free(&output);
}

static void dcmp_option_fini()
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

static void dcmp_option_add_output(struct dcmp_output *output)
{
    assert(list_empty(&output->linkage));
    list_add_tail(&output->linkage, &options.outputs);
}

static void dcmp_option_dump_outputs()
{
    struct dcmp_output* output;

    list_for_each_entry(output,
                        &options.outputs,
                        linkage) {
        printf("Print files matched rule ");
        dcmp_disjunction_dump(output->disjunction);
        printf(" to file %s\n", output->file_name);
    }
}

static int dcmp_output_flist_match(
    struct dcmp_output *output,
    strmap* map,
    bayer_flist flist,
    bayer_flist new_flist)
{
    strmap_node* node;
    /* iterate over each item in map */
    strmap_foreach(map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of file */
        uint64_t index;
        int ret = dcmp_strmap_item_index(map, key, &index);
        assert(ret == 0);

        if (dcmp_disjunction_match(output->disjunction, map, key)) {
            bayer_flist_file_copy(flist, index, new_flist);
        }
    }
    return 0;
}

static int dcmp_output_write(
    struct dcmp_output *output,
    bayer_flist dst_flist,
    strmap* dst_map,
    bayer_flist src_flist,
    strmap* src_map)
{
    int ret = 0;
    bayer_flist new_flist = bayer_flist_subset(src_flist);

    /* find matched file in source map */
    ret = dcmp_output_flist_match(output, src_map, src_flist, new_flist);
    assert(ret == 0);

    /* find matched file in dest map */
    ret = dcmp_output_flist_match(output, dst_map, dst_flist, new_flist);
    assert(ret == 0);

    bayer_flist_summarize(new_flist);
    bayer_flist_write_cache(output->file_name, new_flist);
    bayer_flist_free(&new_flist);

    return 0;
}

static int dcmp_outputs_write(
    bayer_flist dst_list,
    strmap* dst_map,
    bayer_flist src_list,
    strmap* src_map)
{
    struct dcmp_output* output;
    int ret = 0;

    list_for_each_entry(output,
                        &options.outputs,
                        linkage) {
        ret = dcmp_output_write(output, dst_list, dst_map, src_list, src_map);
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

static int dcmp_expression_parse(struct dcmp_conjunction* conjunction,
    const char* expression_string)
{
    char* tmp = BAYER_STRDUP(expression_string);
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
out:
    if (ret) {
        dcmp_expression_free(expression);
    }
    bayer_free(&tmp);
    return ret;
}

static int dcmp_conjunction_parse(struct dcmp_disjunction* disjunction,
    const char* conjunction_string)
{
    int ret = 0;
    char* tmp = BAYER_STRDUP(conjunction_string);
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
    bayer_free(&tmp);
    return ret;
}

static int dcmp_disjunction_parse(struct dcmp_output *output,
    const char *disjunction_string)
{
    int ret = 0;
    char* tmp = BAYER_STRDUP(disjunction_string);
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
    bayer_free(&tmp);
    return ret;
}

static int dcmp_option_output_parse(const char *option)
{
    char* tmp = BAYER_STRDUP(option);
    char* disjunction;
    char* file_name;
    int ret = 0;
    struct dcmp_output* output;

    output = dcmp_output_alloc();

    file_name = tmp;
    disjunction = strsep(&file_name, DCMP_PATH_DELIMITER);
    if (!*disjunction || file_name == NULL || !*file_name) {
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

    output->file_name = BAYER_STRDUP(file_name);
    dcmp_option_add_output(output);
out:
    if (ret) {
        dcmp_output_free(output);
    }
    bayer_free(&tmp);
    return ret;
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
        {"verbose",  0, 0, 'v'},
        {"help",     0, 0, 'h'},
        {"output",   1, 0, 'o'},
        {0, 0, 0, 0}
    };
    int ret = 0;

    /* read in command line options */
    int usage = 0;
    int help  = 0;
    while (1) {
        int c = getopt_long(
            argc, argv, "ho:v",
            long_options, &option_index
        );

        if (c == -1) {
            break;
        }

        switch (c) {
        case 'v':
            options.verbose ++;
            break;
        case 'o':
            ret = dcmp_option_output_parse(optarg);
            if (ret) {
                usage = 1;
            }
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

    /* if help flag was thrown, don't bother checking usage */
    if (! help) {
        /* we should have two arguments left, source and dest paths */
        int numargs = argc - optind;
        if (numargs != 2) {
            BAYER_LOG(BAYER_LOG_ERR,
                "You must specify a source and destination path.");
            usage = 1;
        }
    }

    /* print usage and exit if necessary */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        dcmp_option_fini();
        bayer_finalize();
        MPI_Finalize();
        return 1;
    }

    if (options.verbose) {
        dcmp_option_dump_outputs();
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
    dcmp_strmap_compare(flist3, map1, flist4, map2, &common, &same_type,
        &same_content);

    /* check the results are valid */
    dcmp_strmap_check(map1, map2);

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
    MPI_Allreduce(&same_content, &global_same_content, 1, MPI_UINT64_T,
        MPI_SUM, MPI_COMM_WORLD);

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

    /* write data to cache files */
    dcmp_outputs_write(flist3, map1, flist4, map2);

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

    dcmp_option_fini();
    /* shut down */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
