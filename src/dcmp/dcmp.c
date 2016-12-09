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

/* for bool type, true/false macros */
#include <stdbool.h>
#include <assert.h>

#include "bayer.h"
#include "list.h"
#include "dtcmp.h"

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
    printf("  -v, --verbose\n");
    printf("  -b, --base  - do base comparison\n");
    printf("  -o, --output field0=state0@field1=state1,field2=state2:file "
    	   "- write list to file\n");
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
      * becuase we don't want to waste a loop in dcmp_strmap_compare()
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
    [DCMPF_CONTENT] = DCMPF_CONTENT_DEPEND,
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
    uint64_t index)
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
    uint64_t *index)
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

/* Seeks to specified offset in source and destination files, reads in
 * size bytes from each file, compares result.  Return -1 on read error,
 * 0 when equal, 1 when there is a difference */
int _dcmp_compare_data(
    const char* src_name,
    const char* dst_name,
    int src_fd,
    int dst_fd,
    off_t offset,
    off_t size,
    size_t buff_size)
{
    /* assume we'll find that file contents are the same */
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
        if (src_read == 0) {
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
int dcmp_compare_data(
    const char* src_name,
    const char* dst_name,
    off_t offset,
    off_t length,
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
    posix_fadvise(src_fd, offset, length, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(dst_fd, offset, length, POSIX_FADV_SEQUENTIAL);

    /* compare file contents */
    int rc = _dcmp_compare_data(src_name, dst_name, src_fd, dst_fd,
        offset, length, buff_size);

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
int dcmp_compare_metadata(
    bayer_flist src_list,
    strmap* src_map,
    uint64_t src_index,
    bayer_flist dst_list,
    strmap* dst_map,
    uint64_t dst_index,
    const char* key)
{
    int diff = 0;

    if (dcmp_option_need_compare(DCMPF_SIZE)) {
        dcmp_compare_field(size, DCMPF_SIZE);
    }
    if (dcmp_option_need_compare(DCMPF_GID)) {
        dcmp_compare_field(gid, DCMPF_GID);
    }
    if (dcmp_option_need_compare(DCMPF_UID)) {
        dcmp_compare_field(uid, DCMPF_UID);
    }
    if (dcmp_option_need_compare(DCMPF_ATIME)) {
        dcmp_compare_field(atime, DCMPF_ATIME);
    }
    if (dcmp_option_need_compare(DCMPF_MTIME)) {
        dcmp_compare_field(mtime, DCMPF_MTIME);
    }
    if (dcmp_option_need_compare(DCMPF_CTIME)) {
        dcmp_compare_field(ctime, DCMPF_CTIME);
    }

    return diff;
}

static void dcmp_strmap_compare_data(
    bayer_flist src_compare_list,
    strmap* src_map,
    bayer_flist dst_compare_list,
    strmap* dst_map,
    size_t strlen_prefix)
{
    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double start_compare_data = MPI_Wtime();

    /* get the largest filename */
    uint64_t max_name = bayer_flist_file_max_name(src_compare_list);

    /* bail out if max_name is empty */
    if (max_name == 0) {
        return;
    }

    /* get chunk size for copying files (just hard-coded for now) */
    uint64_t chunk_size = 1024 * 1024;

    /* get the linked list of file chunks for the src and dest */
    bayer_file_chunk* src_head = bayer_file_chunk_list_alloc(src_compare_list, chunk_size);
    bayer_file_chunk* dst_head = bayer_file_chunk_list_alloc(dst_compare_list, chunk_size);

    /* get pointers to march through linked lists */
    bayer_file_chunk* src_p;
    bayer_file_chunk* dst_p;

    /* get a count of how many items are the compare list and total
     * number of bytes we'll read */
    int list_count = 0;
    uint64_t byte_count = 0;
    src_p = src_head;
    while (src_p != NULL) {
        list_count++;
        byte_count += (uint64_t) src_p->length * 2;
        src_p = src_p->next;
    }

    /* keys are the filename, so only bytes that belong to 
     * the same file will be compared via a flag in the segmented scan */
    char* keys = (char*) BAYER_MALLOC(list_count * max_name);

    /* vals pointer allocation for input to segmented scan, so 
     * dcmp_compare_data will return a 1 or 0 for each set of bytes */
    int* vals = (int*) BAYER_MALLOC(list_count * sizeof(int));

    /* ltr pointer for the output of the left-to-right-segmented scan,
     * rtl is for right-to-left scan, which we don't use */
    int* ltr  = (int*) BAYER_MALLOC(list_count * sizeof(int));
    int* rtl  = (int*) BAYER_MALLOC(list_count * sizeof(int)); 

    /* compare bytes for each file section and set flag based on what we find */
    int i = 0;
    char* name_ptr = keys;
    src_p = src_head;
    dst_p = dst_head;
    while (src_p != NULL) {
        /* get offset into file that we should compare (bytes) */
        off_t offset = src_p->offset;

        /* get length of section that we should compare (bytes) */
        off_t length = src_p->length;
        
        /* compare the contents of the files */
        int rc = dcmp_compare_data(src_p->name, dst_p->name, offset, length, 1048576);

        /* now record results of compare_data for sending to segmented scan */
        strncpy(name_ptr, src_p->name, max_name);
        vals[i] = rc;

        /* initialize our output values (have to do this because of exscan) */
        ltr[i] = 0;
        rtl[i] = 0;

        /* move to the start of the next filename */
        name_ptr += max_name;
        i++;

        /* update pointers for src and dest in linked list */
        src_p = src_p->next;
        dst_p = dst_p->next;
    }

    /* create type and comparison operation for file names */
    MPI_Datatype keytype;
    DTCMP_Op keyop;
    DTCMP_Str_create_ascend((int)max_name, &keytype, &keyop);

    /* execute segmented scan of comparison flags across file names */
    DTCMP_Segmented_exscan(list_count, keys, keytype, vals, ltr, rtl, MPI_INT, keyop, DTCMP_FLAG_NONE, MPI_LOR, MPI_COMM_WORLD);
    for (i = 0; i < list_count; i++) {
      /* turn segmented exscan into scan by or'ing in our input */
      ltr[i] |= vals[i];
    }
     
    /* we're done with the MPI type and operation, free them */
    MPI_Type_free(&keytype);
    DTCMP_Op_free(&keyop);

    /* get number of ranks */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* allocate arrays for alltoall -- one for sending, and one for receiving */
    int* sendcounts = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));
    int* recvcounts = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));
    int* recvdisps  = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));
    int* senddisps  = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));

    /* initialize send & receive arrays */
    for (int i = 0; i < ranks; i++) {
        sendcounts[i] = 0;
    }

    /* amount of bytes we are sending (it is doubled because we are 
     * sending the index & the flag) */
    size_t sendbytes = list_count * 2 * sizeof(uint64_t); 

    /* allocate space for send buffer */
    uint64_t* sendbuf = (uint64_t*) BAYER_MALLOC(sendbytes);

    /* Iterate over the list of files. For each file a process needs to report on,
     * we increment the counter correspoinding to the "owner" of the file. After
     * going through all files, we then have a count of the number of files we 
     * will report for each rank */
    i = 0;
    int disp = 0;
    src_p = src_head;
    while (src_p != NULL) {
        /* if we checked the last byte of the file, we need to send scan result to owner */
        if (src_p->offset + src_p->length >= src_p->file_size) {
            /* get a count of items that will be sent
             * to the rank owner */
            sendcounts[src_p->rank_of_owner] += 2;

            /* record index and ltr value in send buffer */
            sendbuf[disp    ] = src_p->index_of_owner;
            sendbuf[disp + 1] = (uint64_t)ltr[i];
            
            /* advance to next value in buffer*/
            disp += 2;
        }

        /* advance in struct list and ltr array */
        i++;
        src_p = src_p->next;
    }

    /* compute send buffer displacements */
    senddisps[0] = 0;
    for (i = 1; i < ranks; i++) {
        senddisps[i] = senddisps[i - 1] + sendcounts[i - 1];
    }

    /* alltoall to let every process know a count of how much it will be receiving */
    MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT, MPI_COMM_WORLD);

    /* calculate displacements for recv buffer for alltoallv */
    int recv_total = recvcounts[0];
    recvdisps[0] = 0;
    for (i = 1; i < ranks; i++) {
        recv_total += recvcounts[i];
        recvdisps[i] = recvdisps[i - 1] + recvcounts[i - 1];
    }

    /* allocate buffer to recv bytes into based on recvounts */
    uint64_t* recvbuf = (uint64_t*) BAYER_MALLOC(recv_total * sizeof(uint64_t));

    /* send the bytes to the correct rank that owns the file */
    MPI_Alltoallv(
        sendbuf, sendcounts, senddisps, MPI_UINT64_T,
        recvbuf, recvcounts, recvdisps, MPI_UINT64_T, MPI_COMM_WORLD
    );

    /* unpack contents of recv buffer  & store results in strmap */
    disp = 0;
    while (disp < recv_total) {
        /* local store of idx & flag values for each file */
        uint64_t idx  = recvbuf[disp];
        uint64_t flag = recvbuf[disp + 1];

        /* lookup name of file based on id to send to strmap updata call */
        char* name = bayer_flist_file_get_name(src_compare_list, idx);

        /* ignore prefix portion of path to use as key */
        name += strlen_prefix;

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

        /* go to next id & flag */
        disp += 2;
    }

    /* get total number of bytes across all processes */
    uint64_t total_bytes;
    MPI_Allreduce(&byte_count, &total_bytes, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* wait for all procs to finish before stopping the timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double end_compare_data = MPI_Wtime();

    /* report compared data count, time, and rate */
    if (bayer_debug_level >= BAYER_LOG_VERBOSE && bayer_rank == 0) {
        uint64_t all_count = bayer_flist_global_size(src_compare_list);
        double time_diff = end_compare_data - start_compare_data;
        double file_rate = 0.0;
        double byte_rate = 0.0;
        if (time_diff > 0.0) {
            file_rate = ((double)all_count) / time_diff;
            byte_rate = ((double)total_bytes) / time_diff;
        }

        /* convert size to units */
        double size_tmp;
        const char* size_units;
        bayer_format_bytes(total_bytes, &size_tmp, &size_units);

        /* convert bandwidth to units */
        double total_bytes_tmp;
        const char* rate_units;
        bayer_format_bw(byte_rate, &total_bytes_tmp, &rate_units);

        printf("Compared data of %lu items in %f seconds (%f items/sec) and (%.3lf %s ) \n", 
                all_count, time_diff, file_rate, total_bytes_tmp, rate_units);
        printf("Total bytes read: %.3lf %s\n", size_tmp, size_units);
    }

    /* free memory */
    bayer_free(&keys);
    bayer_free(&rtl);
    bayer_free(&ltr);
    bayer_free(&vals);
    bayer_free(&sendcounts);
    bayer_free(&recvcounts);
    bayer_free(&recvdisps);
    bayer_free(&senddisps);
    bayer_free(&recvbuf);
    bayer_free(&sendbuf);
    bayer_file_chunk_list_free(&src_head);
    bayer_file_chunk_list_free(&dst_head);

    return;
}

/* compare entries from src into dst */
static void dcmp_strmap_compare(bayer_flist src_list,
                                strmap* src_map,
                                bayer_flist dst_list,
                                strmap* dst_map,
                                size_t strlen_prefix)
{
    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double start_compare = MPI_Wtime();

    /* create compare_lists */
    bayer_flist src_compare_list = bayer_flist_subset(src_list);
    bayer_flist dst_compare_list = bayer_flist_subset(dst_list);

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

        /* get modes of files */
        mode_t src_mode = (mode_t) bayer_flist_file_get_mode(src_list,
            src_index);
        mode_t dst_mode = (mode_t) bayer_flist_file_get_mode(dst_list,
            dst_index);

        rc = dcmp_compare_metadata(src_list, src_map, src_index,
             dst_list, dst_map, dst_index,
             key);
        assert(rc >= 0);

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

        /* for now, we can only compare contente of regular files */
        /* TODO: add support for symlinks */
        if (! S_ISREG(dst_mode)) {
            /* not regular file, take them as common content */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_COMMON);
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

        /* If we get to this point, we need to open files and compare
         * file contents.  We'll first identify all such files so that
         * we can do this comparison in parallel more effectively.  For
         * now copy these files to the list of files we need to compare. */

        /* make a copy of the src and dest files where the data needs
         * to be compared and store in src & dest compare lists */
        bayer_flist_file_copy(src_list, src_index, src_compare_list);
        bayer_flist_file_copy(dst_list, dst_index, dst_compare_list);
    }
    
    /* summarize lists of files for which we need to compare data contents */
    bayer_flist_summarize(src_compare_list);
    bayer_flist_summarize(dst_compare_list);

    /* compare the contents of the files if we have anything in the compare list */
    uint64_t global_size = bayer_flist_global_size(src_compare_list);
    if (global_size > 0) {
        dcmp_strmap_compare_data(src_compare_list, src_map, dst_compare_list, dst_map, strlen_prefix);
    }

    /* free the compare flists */
    bayer_flist_free(&dst_compare_list);
    bayer_flist_free(&src_compare_list); 

    /* wait for all procs to finish before stopping timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double end_compare = MPI_Wtime();

    /* if the verbose option is set print the timing data */
    /* report compare count, time, and rate */
    if (bayer_debug_level >= BAYER_LOG_VERBOSE && bayer_rank == 0) {
       uint64_t all_count = bayer_flist_global_size(src_list);
       double time_diff = end_compare - start_compare;
       double rate = 0.0;
       if (time_diff > 0.0) {
           rate = ((double)all_count) / time_diff;
       }
       printf("Compared %lu items in %f seconds (%f items/sec)\n", all_count, time_diff, rate);
    }

    return;
}

/* loop on the src map to check the results */
static void dcmp_strmap_check_src(strmap* src_map,
                                  strmap* dst_map)
{
    assert(dcmp_option_need_compare(DCMPF_EXIST));
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
                /* all states are either common, differ or skiped */
                if (dcmp_option_need_compare(field)) {
                    assert(src_state == DCMPS_COMMON ||
                        src_state == DCMPS_DIFFER);
                } else {
                    // XXXX
                    if (src_state != DCMPS_INIT)
                        printf("XXX %s wrong state %s\n", dcmp_field_to_string(field, 1), dcmp_state_to_string(src_state, 1));
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
    bayer_flist flist,
    uint64_t index,
    int ranks,
    void *args)
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
    int base;                      /* whether to do base check */
    int debug;                     /* check result after get result */
    int need_compare[DCMPF_MAX];   /* fields that need to be compared  */
};

struct dcmp_options options = {
    .outputs      = LIST_HEAD_INIT(options.outputs),
    .verbose      = 0,
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
    const char* key)
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
    dcmp_disjunction_free(output->disjunction);
    output->disjunction = NULL;
    if (output->file_name != NULL) {
        bayer_free(&output->file_name);
    }
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
    int i;
    for (i = 0; i < DCMPF_MAX; i++) {
        if ((depend & (1 << i)) != 0) {
            options.need_compare[i] = 1;
        }
    }
}

int dcmp_option_need_compare(dcmp_field field)
{
    return options.need_compare[field];
}

static int dcmp_output_flist_match(
    struct dcmp_output *output,
    strmap* map,
    bayer_flist flist,
    bayer_flist new_flist,
    uint64_t *number)
{
    strmap_node* node;

    *number = 0;
    /* iterate over each item in map */
    strmap_foreach(map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of file */
        uint64_t index;
        int ret = dcmp_strmap_item_index(map, key, &index);
        assert(ret == 0);

        if (dcmp_disjunction_match(output->disjunction, map, key)) {
            (*number)++;
            bayer_flist_file_copy(flist, index, new_flist);
        }
    }
    return 0;
}

#define DCMP_OUTPUT_PREFIX "Files which "

static int dcmp_output_write(
    struct dcmp_output *output,
    bayer_flist src_flist,
    strmap* src_map,
    bayer_flist dst_flist,
    strmap* dst_map)
{
    int ret = 0;
    bayer_flist new_flist = bayer_flist_subset(src_flist);

    /* find matched file in source map */
    uint64_t src_matched = 0;
    ret = dcmp_output_flist_match(output, src_map, src_flist,
                                  new_flist, &src_matched);
    assert(ret == 0);

    /* find matched file in dest map */
    uint64_t dst_matched = 0;
    ret = dcmp_output_flist_match(output, dst_map, dst_flist,
                                  new_flist, &dst_matched);
    assert(ret == 0);

    bayer_flist_summarize(new_flist);
    if (output->file_name != NULL) {
        bayer_flist_write_cache(output->file_name, new_flist);
    }

    uint64_t src_matched_total;
    MPI_Allreduce(&src_matched, &src_matched_total, 1,
                  MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    uint64_t dst_matched_total;
    MPI_Allreduce(&dst_matched, &dst_matched_total, 1,
                  MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == 0) {
        printf(DCMP_OUTPUT_PREFIX);
        dcmp_disjunction_print(output->disjunction, 0,
                               strlen(DCMP_OUTPUT_PREFIX));
        printf(", number: %llu/%llu",
               src_matched_total,
               dst_matched_total);
        if (output->file_name != NULL) {
            printf(", dumped to \"%s\"",
                   output->file_name);
        }
        printf("\n");
    }
    bayer_flist_free(&new_flist);

    return 0;
}

static int dcmp_outputs_write(
    bayer_flist src_list,
    strmap* src_map,
    bayer_flist dst_list,
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

    /* Add comparison we need for this expression */
    dcmp_option_add_comparison(expression->field);
out:
    if (ret) {
        dcmp_expression_free(expression);
    }
    bayer_free(&tmp);
    return ret;
}

static int dcmp_conjunction_parse(
    struct dcmp_disjunction* disjunction,
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

static int dcmp_disjunction_parse(
    struct dcmp_output *output,
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

static int dcmp_option_output_parse(const char *option, int add_at_head)
{
    char* tmp = BAYER_STRDUP(option);
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
        output->file_name = BAYER_STRDUP(file_name);
    }
    dcmp_option_add_output(output, add_at_head);
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
        {"base",     0, 0, 'b'},
        {"debug",    0, 0, 'd'},
        {"help",     0, 0, 'h'},
        {"output",   1, 0, 'o'},
        {"verbose",  0, 0, 'v'},
        {0, 0, 0, 0}
    };
    int ret = 0;
    int i;

    /* read in command line options */
    int usage = 0;
    int help  = 0;
    while (1) {
        int c = getopt_long(
            argc, argv, "bdho:v",
            long_options, &option_index
        );

        if (c == -1) {
            break;
        }

        switch (c) {
        case 'b':
            options.base ++;
            break;
        case 'd':
            options.debug ++;
            break;
        case 'o':
            ret = dcmp_option_output_parse(optarg, 0);
            if (ret) {
                usage = 1;
            }
            break;
        case 'v':
            options.verbose ++;
            bayer_debug_level = BAYER_LOG_VERBOSE;
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
            dcmp_option_output_parse(dcmp_default_outputs[i], 1);
            assert(ret == 0);
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
    bayer_flist_walk_path(path1, 1, 0, flist1);

    const char* path2 = param2.path;
    bayer_flist_walk_path(path2, 1, 0, flist2);

    /* map files to ranks based on portion following prefix directory */
    bayer_flist flist3 = bayer_flist_remap(flist1, dcmp_map_fn, (void*)path1);
    bayer_flist flist4 = bayer_flist_remap(flist2, dcmp_map_fn, (void*)path2);

    /* map each file name to its index and its comparison state */
    strmap* map1 = dcmp_strmap_creat(flist3, path1);
    strmap* map2 = dcmp_strmap_creat(flist4, path2);

    /* compare files in map1 with those in map2 */
    dcmp_strmap_compare(flist3, map1, flist4, map2, strlen(path1));

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
