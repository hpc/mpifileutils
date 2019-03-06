/* Implements functions to read/write an flist from/to a file */

#define _GNU_SOURCE
#include <dirent.h>
#include <fcntl.h>
#include <sys/syscall.h>

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
#include <regex.h>

/* These headers are needed to query the Lustre MDS for stat
 * information.  This information may be incomplete, but it
 * is faster than a normal stat, which requires communication
 * with the MDS plus every OST a file is striped across. */
//#define LUSTRE_STAT
#ifdef LUSTRE_STAT
#include <sys/ioctl.h>
#include <lustre/lustre_user.h>
#endif /* LUSTRE_STAT */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "dtcmp.h"
#include "mfu.h"
#include "mfu_flist_internal.h"

static void mfu_pack_io_uint32(char** pptr, uint32_t value)
{
    /* convert from host to network order */
    uint32_t* ptr = *(uint32_t**)pptr;
    *ptr = mfu_hton32(value);
    *pptr += 4;
}

static void mfu_unpack_io_uint32(const char** pptr, uint32_t* value)
{
    /* convert from network to host order */
    const uint32_t* ptr = *(const uint32_t**)pptr;
    *value = mfu_ntoh32(*ptr);
    *pptr += 4;
}

static void mfu_pack_io_uint64(char** pptr, uint64_t value)
{
    /* convert from host to network order */
    uint64_t* ptr = *(uint64_t**)pptr;
    *ptr = mfu_hton64(value);
    *pptr += 8;
}

static void mfu_unpack_io_uint64(const char** pptr, uint64_t* value)
{
    /* convert from network to host order */
    const uint64_t* ptr = *(const uint64_t**)pptr;
    *value = mfu_ntoh64(*ptr);
    *pptr += 8;
}

static size_t buft_pack_size(const buf_t* items)
{
    size_t elem_size = items->chars + sizeof(uint64_t);
    size_t pack_size = items->count * elem_size;
    return pack_size;
}

static void buft_pack(void* buf, const buf_t* items)
{
    char* dptr = (char*)buf;
    const char* sptr = (const char*)items->buf;

    uint64_t i;
    for (i = 0; i < items->count; i++) {
        /* copy path from source to destination buffer */
        strncpy(dptr, sptr, items->chars);
        sptr += items->chars;
        dptr += items->chars;

        /* pack uint64 into destionation */
        mfu_pack_io_uint64(&dptr, *(const uint64_t*)sptr);
        sptr += sizeof(uint64_t);
    }
    return;
}

static void buft_unpack(const void* buf, buf_t* items)
{
    const char* buf_ptr = (const char*)buf;
    char* buft_ptr = (char*)items->buf;

    uint64_t i;
    for (i = 0; i < items->count; i++) {
        /* copy path from buffer to buft */
        strncpy(buft_ptr, buf_ptr, items->chars);
        buft_ptr += items->chars;
        buf_ptr  += items->chars;

        /* pack uint64 from buf into buft */
        mfu_unpack_io_uint64(&buf_ptr, (uint64_t*)buft_ptr);
        buft_ptr += sizeof(uint64_t);
    }
    return;
}

/* create a datatype to hold file name and stat info */
static void create_stattype(int detail, int chars, MPI_Datatype* dt_stat)
{
    /* build type for file path */
    MPI_Datatype dt_filepath;
    MPI_Type_contiguous(chars, MPI_CHAR, &dt_filepath);

    /* build keysat type */
    int fields;
    MPI_Datatype types[11];
    if (detail) {
        fields = 11;
        types[0]  = dt_filepath;  /* file name */
        types[1]  = MPI_UINT64_T; /* mode */
        types[2]  = MPI_UINT64_T; /* uid */
        types[3]  = MPI_UINT64_T; /* gid */
        types[4]  = MPI_UINT64_T; /* atime secs */
        types[5]  = MPI_UINT64_T; /* atime nsecs */
        types[6]  = MPI_UINT64_T; /* mtime secs */
        types[7]  = MPI_UINT64_T; /* mtime nsecs */
        types[8]  = MPI_UINT64_T; /* ctime secs */
        types[9]  = MPI_UINT64_T; /* ctime nsecs */
        types[10] = MPI_UINT64_T; /* size */
    }
    else {
        fields = 2;
        types[0] = dt_filepath;  /* file name */
        types[1] = MPI_UINT32_T; /* file type */
    }
    DTCMP_Type_create_series(fields, types, dt_stat);

    MPI_Type_free(&dt_filepath);
    return;
}

static size_t list_elem_encode_size(const elem_t* elem)
{
    size_t reclen = strlen(elem->file); /* filename */
    reclen += 2; /* | + type letter */
    reclen += 1; /* trailing newline */
    return reclen;
}

static size_t list_elem_encode(void* buf, const elem_t* elem)
{
    char* ptr = (char*) buf;

    size_t len = strlen(elem->file);
    strncpy(ptr, elem->file, len);
    ptr += len;

    *ptr = '|';
    ptr++;

    mfu_filetype type = elem->type;
    if (type == MFU_TYPE_FILE) {
        *ptr = 'F';
    }
    else if (type == MFU_TYPE_DIR) {
        *ptr = 'D';
    }
    else if (type == MFU_TYPE_LINK) {
        *ptr = 'L';
    }
    else {
        *ptr = 'U';
    }
    ptr++;

    *ptr = '\n';

    size_t reclen = len + 3;
    return reclen;
}

/* given a buffer, decode element and store values in elem */
static void list_elem_decode(char* buf, elem_t* elem)
{
    /* get name and advance pointer */
    const char* file = strtok(buf, "|");

    /* copy path */
    elem->file = MFU_STRDUP(file);

    /* set depth */
    elem->depth = mfu_flist_compute_depth(file);

    elem->detail = 0;

    const char* type = strtok(NULL, "|");
    if (type == NULL) {
        elem->type = MFU_TYPE_UNKNOWN;
        return;
    }

    char c = type[0];
    if (c == 'F') {
        elem->type = MFU_TYPE_FILE;
    }
    else if (c == 'D') {
        elem->type = MFU_TYPE_DIR;
    }
    else if (c == 'L') {
        elem->type = MFU_TYPE_LINK;
    }
    else {
        elem->type = MFU_TYPE_UNKNOWN;
    }

    return;
}

/* create a datatype to hold file name and stat info */
/* return number of bytes needed to pack element */
static size_t list_elem_pack_size(int detail, uint64_t chars, const elem_t* elem)
{
    size_t size;
    if (detail) {
        size = chars + 0 * 4 + 10 * 8;
    }
    else {
        size = chars + 1 * 4;
    }
    return size;
}

/* pack element into buffer and return number of bytes written */
static size_t list_elem_pack(void* buf, int detail, uint64_t chars, const elem_t* elem)
{
    /* set pointer to start of buffer */
    char* start = (char*) buf;
    char* ptr = start;

    /* copy in file name */
    char* file = elem->file;
    strcpy(ptr, file);
    ptr += chars;

    if (detail) {
        mfu_pack_io_uint64(&ptr, elem->mode);
        mfu_pack_io_uint64(&ptr, elem->uid);
        mfu_pack_io_uint64(&ptr, elem->gid);
        mfu_pack_io_uint64(&ptr, elem->atime);
        mfu_pack_io_uint64(&ptr, elem->atime_nsec);
        mfu_pack_io_uint64(&ptr, elem->mtime);
        mfu_pack_io_uint64(&ptr, elem->mtime_nsec);
        mfu_pack_io_uint64(&ptr, elem->ctime);
        mfu_pack_io_uint64(&ptr, elem->ctime_nsec);
        mfu_pack_io_uint64(&ptr, elem->size);
    }
    else {
        /* just have the file type */
        mfu_pack_io_uint32(&ptr, elem->type);
    }

    size_t bytes = (size_t)(ptr - start);
    return bytes;
}

/* unpack element from buffer and return number of bytes read */
static size_t list_elem_unpack(const void* buf, int detail, uint64_t chars, elem_t* elem)
{
    const char* start = (const char*) buf;
    const char* ptr = start;

    /* get name and advance pointer */
    const char* file = ptr;
    ptr += chars;

    /* copy path */
    elem->file = MFU_STRDUP(file);

    /* set depth */
    elem->depth = mfu_flist_compute_depth(file);

    elem->detail = detail;

    if (detail) {
        /* extract fields */
        mfu_unpack_io_uint64(&ptr, &elem->mode);
        mfu_unpack_io_uint64(&ptr, &elem->uid);
        mfu_unpack_io_uint64(&ptr, &elem->gid);
        mfu_unpack_io_uint64(&ptr, &elem->atime);
        mfu_unpack_io_uint64(&ptr, &elem->atime_nsec);
        mfu_unpack_io_uint64(&ptr, &elem->mtime);
        mfu_unpack_io_uint64(&ptr, &elem->mtime_nsec);
        mfu_unpack_io_uint64(&ptr, &elem->ctime);
        mfu_unpack_io_uint64(&ptr, &elem->ctime_nsec);
        mfu_unpack_io_uint64(&ptr, &elem->size);

        /* use mode to set file type */
        elem->type = mfu_flist_mode_to_filetype((mode_t)elem->mode);
    }
    else {
        mfu_unpack_io_uint32(&ptr, &elem->type);
    }

    size_t bytes = (size_t)(ptr - start);
    return bytes;
}

/* insert a file given a pointer to packed data */
static void list_insert_decode(flist_t* flist, char* buf)
{
    /* create new element to record file path, file type, and stat info */
    elem_t* elem = (elem_t*) MFU_MALLOC(sizeof(elem_t));

    /* decode buffer and store values in element */
    list_elem_decode(buf, elem);

    /* append element to tail of linked list */
    mfu_flist_insert_elem(flist, elem);

    return;
}

/* insert a file given a pointer to packed data */
static size_t list_insert_ptr(flist_t* flist, char* ptr, int detail, uint64_t chars)
{
    /* create new element to record file path, file type, and stat info */
    elem_t* elem = (elem_t*) MFU_MALLOC(sizeof(elem_t));

    /* get name and advance pointer */
    size_t bytes = list_elem_unpack(ptr, detail, chars, elem);

    /* append element to tail of linked list */
    mfu_flist_insert_elem(flist, elem);

    return bytes;
}

/****************************************
 * Read file list from file
 ***************************************/

static uint64_t get_filesize(const char* name)
{
    uint64_t size = 0;
    struct stat sb;
    int rc = mfu_lstat(name, &sb);
    if (rc == 0) {
        size = (uint64_t) sb.st_size;
    }
    return size;
}

/* reads a file assuming variable length records stored one per line,
 * data encoded in ASCII, fields separated by '|' characters,
 * we divide the file into sections and each process is responsible
 * for records in its section, tricky part is to handle records
 * that spill from one section into the next */
static void read_cache_variable(
    const char* name,
    MPI_File fh,
    char* datarep,
    flist_t* flist)
{
    MPI_Status status;

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* assume data records start at byte 0,
     * need to update this code if this is not the case */
    MPI_Offset disp = 0;

    /* indicate that we just have file names */
    flist->detail = 0;

    /* get file size to determine how much each process should read,
     * just have rank 0 read this and bcast to everyone */
    uint64_t filesize;
    if (rank == 0) {
        filesize = get_filesize(name);
    }
    MPI_Bcast(&filesize, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* TODO: consider stripe width */

    /* compute number of chunks in file */
    uint64_t chunk_size = 1024 * 1024;
    uint64_t chunks = filesize / chunk_size;
    if (chunks * chunk_size < filesize) {
        chunks++;
    }

    /* compute chunk count for each process */
    uint64_t chunk_count = chunks / (uint64_t) ranks;
    uint64_t remainder = chunks - chunk_count * (uint64_t) ranks;
    if ((uint64_t) rank < remainder) {
        chunk_count++;
    }

    /* get our chunk offset */
    uint64_t chunk_offset;
    MPI_Exscan(&chunk_count, &chunk_offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        chunk_offset = 0;
    }

    /* in order to avoid blowing out memory, we read into a fixed-size
     * buffer and unpack */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = chunk_size;
    void* buf1 = MFU_MALLOC(bufsize);
    void* buf2 = MFU_MALLOC(bufsize);
    void* buf  = buf1;

    /* set file view to be sequence of characters past header */
    MPI_File_set_view(fh, disp, MPI_CHAR, MPI_CHAR, datarep, MPI_INFO_NULL);

    /* compute offset of first byte we'll read,
     * the set_view above means we should start our offset at 0 */
    MPI_Offset read_offset = (MPI_Offset)(chunk_offset * chunk_size);

    /* compute offset of last byte we need to read,
     * note we may actually read further if our last record spills
     * into next chunk */
    uint64_t last_offset = (uint64_t) disp + (chunk_offset + chunk_count) * chunk_size;
    if (last_offset > filesize) {
        last_offset = filesize;
    }

    /* read last character from chunk before our first,
     * if this char is not a newline, then last record in the
     * previous chunk spills into ours, in which case we need
     * to scan past first newline */

    /* assume we don't have to scan past first newline */
    int scan = 0;
    if (read_offset > 0) {
        /* read last byte in chunk before our first */
        MPI_Offset pos = read_offset - 1;
        MPI_File_read_at(fh, pos, buf, 1, MPI_CHAR, &status);

        /* if last character is not newline, we need to scan past
         * first new line in our chunk */
        char* ptr = (char*) buf;
        if (*ptr != '\n') {
            scan = 1;
        }
    }

    /* read data from file in chunks, decode records and insert in list */
    uint64_t bufoffset = 0; /* offset within buffer where we should read data */
    int done = 0;
    while (! done) {
        /* we're done if we there's nothing to read */
        if ((uint64_t) read_offset >= filesize) {
            break;
        }

        /* determine number to read, try to read a full buffer's worth,
         * but reduce this if that overruns the end of the file */
        int read_count = (int)(bufsize - bufoffset);
        uint64_t remaining = filesize - (uint64_t) read_offset;
        if (remaining < (uint64_t) read_count) {
            read_count = (int) remaining;
        }

        /* read in our chunk */
        char* bufstart = (char*) buf + bufoffset;
        MPI_File_read_at(fh, read_offset, bufstart, read_count, MPI_CHAR, &status);

        /* TODO: check number of items read in status, in case file
         * size changed somehow since we first read the file size */

        /* update read offset for next time */
        read_offset += (MPI_Offset) read_count;

        /* setup pointers to work with read buffer,
         * note that end points one char past last valid character */
        char* ptr = (char*) buf;
        char* end = ptr + bufoffset + read_count;

        /* scan past first newline (first part of block is a partial
         * record handled by another process) */
        if (scan) {
            /* advance to the next newline character */
            while (ptr != end && *ptr != '\n') {
                ptr++;
            }

            /* go one past newline character */
            if (ptr != end) {
                ptr++;
            }

            /* no need to do that again */
            scan = 0;
        }

        /* process records */
        char* start = ptr;
        while (start != end) {
            /* start points to beginning of a record, scan to
             * search for end of record, advance ptr past next
             * newline character or to end of buffer */
            while (ptr != end && *ptr != '\n') {
                ptr++;
            }

            /* process record if we hit a newline,
             * otherwise copy partial record to other buffer */
            if (ptr != end) {
                /* we must be on a newline,
                 * terminate record string with NUL */
                *ptr = '\0';

                /* process record */
                list_insert_decode(flist, start);

                /* go one past newline character */
                ptr++;
                start = ptr;

                /* compute position of last byte we read,
                 * stop if we have reached or exceeded the limit that
                 * need to read */
                uint64_t pos = ((uint64_t)(read_offset - read_count)) - bufoffset + (uint64_t)(ptr - (char*)buf);
                if (pos >= last_offset) {
                    done = 1;
                    break;
                }

                /* if newline was at end of buffer, reset offset into read buffer */
                if (ptr >= end) {
                    bufoffset = 0;
                }
            }
            else {
                /* hit end of buffer but not end of record,
                 * copy partial record to start of next buffer */

                /* swap buffers */
                if (buf == buf1) {
                    buf = buf2;
                }
                else {
                    buf = buf1;
                }

                /* copy remainder to next buffer */
                size_t len = (size_t)(ptr - start);
                memcpy(buf, start, len);
                bufoffset = (uint64_t) len;

                /* done with this buffer */
                break;
            }
        }
    }

    /* free buffer */
    mfu_free(&buf2);
    mfu_free(&buf1);
    buf = NULL;

    return;
}

/* file format:
 *   uint64_t timestamp when walk started
 *   uint64_t timestamp when walk ended
 *   uint64_t total number of users
 *   uint64_t max username length
 *   uint64_t total number of groups
 *   uint64_t max groupname length
 *   uint64_t total number of files
 *   uint64_t max filename length
 *   list of <username(str), userid(uint64_t)>
 *   list of <groupname(str), groupid(uint64_t)>
 *   list of <files(str)>
 *   */
static void read_cache_v3(
    const char* name,
    MPI_Offset* outdisp,
    MPI_File fh,
    char* datarep,
    uint64_t* outstart,
    uint64_t* outend,
    flist_t* flist)
{
    MPI_Status status;

    MPI_Offset disp = *outdisp;

    /* indicate that we have stat data */
    flist->detail = 1;

    /* pointer to users, groups, and file buffer data structure */
    buf_t* users  = &flist->users;
    buf_t* groups = &flist->groups;

    /* get our rank */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* rank 0 reads and broadcasts header */
    uint64_t header[8];
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_read_at(fh, 0, header, 8, MPI_UINT64_T, &status);
    }
    MPI_Bcast(header, 8, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    disp += 8 * 8; /* 8 consecutive uint64_t types in external32 */

    uint64_t all_count;
    *outstart        = header[0];
    *outend          = header[1];
    users->count     = header[2];
    users->chars     = header[3];
    groups->count    = header[4];
    groups->chars    = header[5];
    all_count        = header[6];
    uint64_t chars   = header[7];

    /* compute count for each process */
    uint64_t count = all_count / (uint64_t)ranks;
    uint64_t remainder = all_count - count * (uint64_t)ranks;
    if ((uint64_t)rank < remainder) {
        count++;
    }

    /* get our offset */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }

    /* read users, if any */
    if (users->count > 0 && users->chars > 0) {
        /* create type */
        mfu_flist_usrgrp_create_stridtype((int)users->chars,  &(users->dt));

        /* get extent */
        MPI_Aint lb_user, extent_user;
        MPI_Type_get_extent(users->dt, &lb_user, &extent_user);

        /* allocate memory to hold data */
        size_t bufsize_user = users->count * (size_t)extent_user;
        users->buf = (void*) MFU_MALLOC(bufsize_user);
        users->bufsize = bufsize_user;

        /* read data */
        MPI_File_set_view(fh, disp, users->dt, users->dt, datarep, MPI_INFO_NULL);
        if (rank == 0) {
            MPI_File_read_at(fh, 0, users->buf, (int)users->count, users->dt, &status);
        }
        MPI_Bcast(users->buf, (int)users->count, users->dt, 0, MPI_COMM_WORLD);
        disp += (MPI_Offset) bufsize_user;
    }

    /* read groups, if any */
    if (groups->count > 0 && groups->chars > 0) {
        /* create type */
        mfu_flist_usrgrp_create_stridtype((int)groups->chars, &(groups->dt));

        /* get extent */
        MPI_Aint lb_group, extent_group;
        MPI_Type_get_extent(groups->dt, &lb_group, &extent_group);

        /* allocate memory to hold data */
        size_t bufsize_group = groups->count * (size_t)extent_group;
        groups->buf = (void*) MFU_MALLOC(bufsize_group);
        groups->bufsize = bufsize_group;

        /* read data */
        MPI_File_set_view(fh, disp, groups->dt, groups->dt, datarep, MPI_INFO_NULL);
        if (rank == 0) {
            MPI_File_read_at(fh, 0, groups->buf, (int)groups->count, groups->dt, &status);
        }
        MPI_Bcast(groups->buf, (int)groups->count, groups->dt, 0, MPI_COMM_WORLD);
        disp += (MPI_Offset) bufsize_group;
    }

    /* read files, if any */
    if (all_count > 0 && chars > 0) {
        /* create types */
        MPI_Datatype dt;
        create_stattype(flist->detail, (int)chars, &dt);

        /* get extents */
        MPI_Aint lb_file, extent_file;
        MPI_Type_get_extent(dt, &lb_file, &extent_file);

        /* in order to avoid blowing out memory, we'll pack into a smaller
         * buffer and iteratively make many collective reads */

        /* allocate a buffer, ensure it's large enough to hold at least one
         * complete record */
        size_t bufsize = 1024 * 1024;
        if (bufsize < (size_t) extent_file) {
            bufsize = (size_t) extent_file;
        }
        void* buf = MFU_MALLOC(bufsize);

        /* compute number of items we can fit in each read iteration */
        uint64_t bufcount = (uint64_t)bufsize / (uint64_t)extent_file;

        /* determine number of iterations we need to read all items */
        uint64_t iters = count / bufcount;
        if (iters * bufcount < count) {
            iters++;
        }

        /* compute max iterations across all procs */
        uint64_t all_iters;
        MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

        /* set file view to be sequence of datatypes past header */
        MPI_File_set_view(fh, disp, dt, dt, datarep, MPI_INFO_NULL);

        /* compute byte offset to read our element */
        MPI_Offset read_offset = (MPI_Offset)offset;

        /* iterate with multiple reads until all records are read */
        uint64_t totalcount = 0;
        while (all_iters > 0) {
            /* determine number to read */
            int read_count = (int) bufcount;
            uint64_t remaining = count - totalcount;
            if (remaining < bufcount) {
                read_count = (int) remaining;
            }

            /* TODO: read_at_all w/ external32 is broken in ROMIO as of MPICH-3.2rc1 */

            /* issue a collective read */
            //MPI_File_read_at_all(fh, read_offset, buf, read_count, dt, &status);
            MPI_File_read_at(fh, read_offset, buf, read_count, dt, &status);

            /* update our offset with the number of items we just read */
            read_offset += (MPI_Offset)read_count;
            totalcount += (uint64_t) read_count;

            /* unpack data from buffer into list */
            char* ptr = (char*) buf;
            uint64_t packcount = 0;
            while (packcount < (uint64_t) read_count) {
                /* unpack item from buffer and advance pointer */
                list_insert_ptr(flist, ptr, 1, chars);
                ptr += extent_file;
                packcount++;
            }

            /* one less iteration */
            all_iters--;
        }

        /* free buffer */
        mfu_free(&buf);

        /* free off our datatype */
        MPI_Type_free(&dt);
    }

    /* create maps of users and groups */
    mfu_flist_usrgrp_create_map(&flist->users, flist->user_id2name);
    mfu_flist_usrgrp_create_map(&flist->groups, flist->group_id2name);

    *outdisp = disp;
    return;
}

/* file format:
 * all integer values stored in network byte order
 *
 *   uint64_t file version
 *   uint64_t total number of users
 *   uint64_t max username length
 *   uint64_t total number of groups
 *   uint64_t max groupname length
 *   uint64_t total number of files
 *   uint64_t max filename length
 *   list of <username(str), userid(uint64_t)>
 *   list of <groupname(str), groupid(uint64_t)>
 *   list of <files(str)>
 *   */
static void read_cache_v4(
    const char* name,
    MPI_Offset* outdisp,
    MPI_File fh,
    char* datarep,
    flist_t* flist)
{
    MPI_Status status;

    MPI_Offset disp = *outdisp;

    /* indicate that we have stat data */
    flist->detail = 1;

    /* pointer to users, groups, and file buffer data structure */
    buf_t* users  = &flist->users;
    buf_t* groups = &flist->groups;

    /* get our rank */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* rank 0 reads and broadcasts header */
    uint64_t header[6];
    int header_size = 6 * 8; /* 6 consecutive uint64_t */
    MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        uint64_t header_packed[6];
        MPI_File_read_at(fh, 0, header_packed, header_size, MPI_BYTE, &status);
        const char* ptr = (const char*) header_packed;
        mfu_unpack_io_uint64(&ptr, &header[0]);
        mfu_unpack_io_uint64(&ptr, &header[1]);
        mfu_unpack_io_uint64(&ptr, &header[2]);
        mfu_unpack_io_uint64(&ptr, &header[3]);
        mfu_unpack_io_uint64(&ptr, &header[4]);
        mfu_unpack_io_uint64(&ptr, &header[5]);
    }
    MPI_Bcast(header, 6, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    disp += header_size;

    uint64_t all_count;
    users->count     = header[0];
    users->chars     = header[1];
    groups->count    = header[2];
    groups->chars    = header[3];
    all_count        = header[4];
    uint64_t chars   = header[5];

    /* compute count for each process */
    uint64_t count = all_count / (uint64_t)ranks;
    uint64_t remainder = all_count - count * (uint64_t)ranks;
    if ((uint64_t)rank < remainder) {
        count++;
    }

    /* get our offset */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }

    /* read users, if any */
    if (users->count > 0 && users->chars > 0) {
        /* create type */
        mfu_flist_usrgrp_create_stridtype((int)users->chars,  &(users->dt));

        /* get extent */
        MPI_Aint lb_user, extent_user;
        MPI_Type_get_extent(users->dt, &lb_user, &extent_user);

        /* allocate memory to hold data */
        size_t bufsize_user = users->count * (size_t)extent_user;
        users->buf = (void*) MFU_MALLOC(bufsize_user);
        users->bufsize = bufsize_user;

        /* read data */
        MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);
        int user_buf_size = (int) buft_pack_size(users);
        if (rank == 0) {
            char* user_buf = (char*) MFU_MALLOC(user_buf_size);
            MPI_File_read_at(fh, 0, user_buf, user_buf_size, MPI_BYTE, &status);
            buft_unpack(user_buf, users);
            mfu_free(&user_buf);
        }
        MPI_Bcast(users->buf, (int)users->count, users->dt, 0, MPI_COMM_WORLD);
        disp += (MPI_Offset) user_buf_size;
    }

    /* read groups, if any */
    if (groups->count > 0 && groups->chars > 0) {
        /* create type */
        mfu_flist_usrgrp_create_stridtype((int)groups->chars, &(groups->dt));

        /* get extent */
        MPI_Aint lb_group, extent_group;
        MPI_Type_get_extent(groups->dt, &lb_group, &extent_group);

        /* allocate memory to hold data */
        size_t bufsize_group = groups->count * (size_t)extent_group;
        groups->buf = (void*) MFU_MALLOC(bufsize_group);
        groups->bufsize = bufsize_group;

        /* read data */
        MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);
        int group_buf_size = (int) buft_pack_size(groups);
        if (rank == 0) {
            char* group_buf = (char*) MFU_MALLOC(group_buf_size);
            MPI_File_read_at(fh, 0, group_buf, group_buf_size, MPI_BYTE, &status);
            buft_unpack(group_buf, groups);
            mfu_free(&group_buf);
        }
        MPI_Bcast(groups->buf, (int)groups->count, groups->dt, 0, MPI_COMM_WORLD);
        disp += (MPI_Offset) group_buf_size;
    }

    /* read files, if any */
    if (all_count > 0 && chars > 0) {
        /* get size of file element */
        size_t elem_size = list_elem_pack_size(flist->detail, (int)chars, NULL);

        /* in order to avoid blowing out memory, we'll pack into a smaller
         * buffer and iteratively make many collective reads */

        /* allocate a buffer, ensure it's large enough to hold at least one
         * complete record */
        size_t bufsize = 1024 * 1024;
        if (bufsize < elem_size) {
            bufsize = elem_size;
        }
        void* buf = MFU_MALLOC(bufsize);

        /* compute number of items we can fit in each read iteration */
        uint64_t bufcount = (uint64_t)bufsize / (uint64_t)elem_size;

        /* determine number of iterations we need to read all items */
        uint64_t iters = count / bufcount;
        if (iters * bufcount < count) {
            iters++;
        }

        /* compute max iterations across all procs */
        uint64_t all_iters;
        MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

        /* set file view to be sequence of datatypes past header */
        MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);

        /* compute byte offset to read our element */
        MPI_Offset read_offset = (MPI_Offset)offset * elem_size;

        /* iterate with multiple reads until all records are read */
        uint64_t totalcount = 0;
        while (all_iters > 0) {
            /* determine number to read */
            int read_count = (int) bufcount;
            uint64_t remaining = count - totalcount;
            if (remaining < bufcount) {
                read_count = (int) remaining;
            }

            /* TODO: read_at_all w/ external32 is broken in ROMIO as of MPICH-3.2rc1 */

            /* compute number of bytes to read */
            int read_size = read_count * (int)elem_size;

            /* issue a collective read */
            //MPI_File_read_at_all(fh, read_offset, buf, read_size, MPI_BYTE, &status);
            MPI_File_read_at(fh, read_offset, buf, read_size, MPI_BYTE, &status);

            /* update our offset with the number of items we just read */
            read_offset += (MPI_Offset)read_size;
            totalcount += (uint64_t) read_count;

            /* unpack data from buffer into list */
            char* ptr = (char*) buf;
            uint64_t packcount = 0;
            while (packcount < (uint64_t) read_count) {
                /* unpack item from buffer and advance pointer */
                list_insert_ptr(flist, ptr, 1, chars);
                ptr += elem_size;
                packcount++;
            }

            /* one less iteration */
            all_iters--;
        }

        /* free buffer */
        mfu_free(&buf);
    }

    /* create maps of users and groups */
    mfu_flist_usrgrp_create_map(&flist->users, flist->user_id2name);
    mfu_flist_usrgrp_create_map(&flist->groups, flist->group_id2name);

    *outdisp = disp;
    return;
}

void mfu_flist_read_cache(
    const char* name,
    mfu_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;

    /* start timer */
    double start_read = MPI_Wtime();

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* report the filename we're writing to */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Reading from input file: %s", name);
    }

    /* open file */
    int rc;
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    int amode = MPI_MODE_RDONLY;
    rc = MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, MPI_INFO_NULL, &fh);
    if (rc != MPI_SUCCESS) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file %s", name);
        }
        return;
    }

    /* set file view */
    MPI_Offset disp = 0;

    /* rank 0 reads and broadcasts version */
    uint64_t version;
    MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        /* read version from file */
        uint64_t version_packed;
        MPI_File_read_at(fh, 0, &version_packed, 8, MPI_BYTE, &status);

        /* convert version into host format */
        const char* ptr = (const char*) &version_packed;
        mfu_unpack_io_uint64(&ptr, &version);
    }
#if 0
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_read_at(fh, 0, &version, 1, MPI_UINT64_T, &status);
    }
#endif
    MPI_Bcast(&version, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    disp += 1 * 8; /* 9 consecutive uint64_t types in external32 */

    /* read data from file */
    if (version == 4) {
        read_cache_v4(name, &disp, fh, datarep, flist);
    } else if (version == 3) {
        /* need a couple of dummy params to record walk start and end times */
        uint64_t outstart = 0;
        uint64_t outend = 0;
        read_cache_v3(name, &disp, fh, datarep, &outstart, &outend, flist);
    }
    else {
        /* TODO: unknown file format */
        read_cache_variable(name, fh, datarep, flist);
    }

    /* close file */
    MPI_File_close(&fh);

    /* compute global summary */
    mfu_flist_summarize(bflist);

    /* end timer */
    double end_read = MPI_Wtime();

    /* report read count, time, and rate */
    if (mfu_rank == 0) {
        uint64_t all_count = mfu_flist_global_size(bflist);
        double time_diff = end_read - start_read;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        MFU_LOG(MFU_LOG_INFO, "Read %lu files in %f seconds (%f files/sec)",
               all_count, time_diff, rate
              );
    }

    /* wait for summary to be printed */
    MPI_Barrier(MPI_COMM_WORLD);

    return;
}

/****************************************
 * Write file list to file
 ***************************************/

/* file version
 * 1: version, start, end, files, file chars, list (file)
 * 2: version, start, end, files, file chars, list (file, type)
 * 3: version, start, end, files, users, user chars, groups, group chars,
 *    files, file chars, list (user, userid), list (group, groupid),
 *    list (stat) */

/* write each record in ASCII format, terminated with newlines */
static void write_cache_readdir_variable(
    const char* name,
    flist_t* flist)
{
    /* get our rank in job & number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* use mpi io hints to stripe across OSTs */
    MPI_Info info;
    MPI_Info_create(&info);

    /* walk the list to determine the number of bytes we'll write */
    uint64_t bytes = 0;
    uint64_t recmax = 0;
    const elem_t* current = flist->list_head;
    while (current != NULL) {
        /* <name>|<type={D,F,L}>\n */
        uint64_t reclen = (uint64_t) list_elem_encode_size(current);
        if (recmax < reclen) {
            recmax = reclen;
        }
        bytes += reclen;
        current = current->next;
    }

    /* compute byte offset for each task */
    uint64_t offset;
    MPI_Scan(&bytes, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    offset -= bytes;

    /* open file */
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
    int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;

    /* change number of ranks to string to pass to MPI_Info */
    char str_buf[12];
    sprintf(str_buf, "%d", ranks);

    /* no. of I/O devices for lustre striping is number of ranks */
    MPI_Info_set(info, "striping_factor", str_buf);

    MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, info, &fh);

    /* truncate file to 0 bytes */
    MPI_File_set_size(fh, 0);

    MPI_Offset disp = 0;
#if 0
    /* prepare header */
    uint64_t header[5];
    header[0] = 2;               /* file version */
    header[1] = walk_start;      /* time_t when file walk started */
    header[2] = walk_end;        /* time_t when file walk stopped */
    header[3] = all_count;       /* total number of stat entries */
    header[4] = (uint64_t)chars; /* number of chars in file name */

    /* write the header */
    MPI_Offset disp = 0;
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_write_at(fh, disp, header, 5, MPI_UINT64_T, &status);
    }
    disp += 5 * 8;
#endif

    /* in order to avoid blowing out memory, we'll pack into a smaller
     * buffer and iteratively make many collective writes */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = 1024 * 1024;
    if (bufsize < recmax) {
        bufsize = recmax;
    }
    void* buf = MFU_MALLOC(bufsize);

    /* set file view to be sequence of datatypes past header */
    MPI_File_set_view(fh, disp, MPI_CHAR, MPI_CHAR, datarep, MPI_INFO_NULL);

    /* compute byte offset to write our element,
     * set_view above means our offset here should start from 0 */
    MPI_Offset write_offset = (MPI_Offset)offset;

    /* iterate with multiple writes until all records are written */
    current = flist->list_head;
    while (current != NULL) {
        /* copy stat data into write buffer */
        char* ptr = (char*) buf;
        size_t packsize = 0;
        size_t recsize = list_elem_encode_size(current);
        while (current != NULL && (packsize + recsize) <= bufsize) {
            /* pack item into buffer and advance pointer */
            size_t encode_bytes = list_elem_encode(ptr, current);
            ptr += encode_bytes;
            packsize += encode_bytes;

            /* get pointer to next element and update our recsize */
            current = current->next;
            if (current != NULL) {
                recsize = list_elem_encode_size(current);
            }
        }

        /* write file info */
        int write_count = (int) packsize;
        MPI_File_write_at(fh, write_offset, buf, write_count, MPI_CHAR, &status);

        /* update our offset with the number of bytes we just wrote */
        write_offset += (MPI_Offset) packsize;
    }

    /* free write buffer */
    mfu_free(&buf);

    /* close file */
    MPI_File_close(&fh);
        
    /* free mpi info */
    MPI_Info_free(&info);

    return;
}

/* file format:
 *   uint64_t timestamp when walk started
 *   uint64_t timestamp when walk ended
 *   uint64_t total number of files
 *   uint64_t max filename length
 *   list of <filenames(str), filetype(uint32_t)> */

static void write_cache_readdir(
    const char* name,
    uint64_t walk_start,
    uint64_t walk_end,
    flist_t* flist)
{
    /* get our rank in job & number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* use mpi io hints to stripe across OSTs */
    MPI_Info info;
    MPI_Info_create(&info);

    /* get number of items in our list and total file count */
    uint64_t count     = flist->list_count;
    uint64_t all_count = flist->total_files;
    uint64_t offset    = flist->offset;

    /* find smallest length that fits max and consists of integer
     * number of 8 byte segments */
    int max = (int) flist->max_file_name;
    int chars = max / 8;
    if (chars * 8 < max) {
        chars++;
    }
    chars *= 8;

    /* build datatype to hold file info */
    MPI_Datatype dt;
    create_stattype(flist->detail, chars, &dt);

    /* get extent of stat type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* open file */
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
    int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;

    /* change number of ranks to string to pass to MPI_Info */
    char str_buf[12];
    sprintf(str_buf, "%d", ranks);

    /* no. of I/O devices for lustre striping is number of ranks */
    MPI_Info_set(info, "striping_factor", str_buf);

    MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, info, &fh);

    /* truncate file to 0 bytes */
    MPI_File_set_size(fh, 0);

    /* prepare header */
    uint64_t header[5];
    header[0] = 2;               /* file version */
    header[1] = walk_start;      /* time_t when file walk started */
    header[2] = walk_end;        /* time_t when file walk stopped */
    header[3] = all_count;       /* total number of stat entries */
    header[4] = (uint64_t)chars; /* number of chars in file name */

    /* write the header */
    MPI_Offset disp = 0;
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_write_at(fh, 0, header, 5, MPI_UINT64_T, &status);
    }
    disp += 5 * 8;

    /* in order to avoid blowing out memory, we'll pack into a smaller
     * buffer and iteratively make many collective writes */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = 1024 * 1024;
    if (bufsize < (size_t) extent) {
        bufsize = (size_t) extent;
    }
    void* buf = MFU_MALLOC(bufsize);

    /* compute number of items we can fit in each write iteration */
    uint64_t bufcount = (uint64_t)bufsize / (uint64_t)extent;

    /* determine number of iterations we need to write all items */
    uint64_t iters = count / bufcount;
    if (iters * bufcount < count) {
        iters++;
    }

    /* compute max iterations across all procs */
    uint64_t all_iters;
    MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* set file view to be sequence of datatypes past header */
    MPI_File_set_view(fh, disp, dt, dt, datarep, MPI_INFO_NULL);

    /* compute byte offset to write our element */
    MPI_Offset write_offset = (MPI_Offset)offset;

    /* iterate with multiple writes until all records are written */
    const elem_t* current = flist->list_head;
    while (all_iters > 0) {
        /* copy stat data into write buffer */
        char* ptr = (char*) buf;
        uint64_t packcount = 0;
        while (current != NULL && packcount < bufcount) {
            /* pack item into buffer and advance pointer */
            size_t pack_bytes = list_elem_pack(ptr, flist->detail, (uint64_t)chars, current);
            ptr += pack_bytes;
            packcount++;
            current = current->next;
        }

        /* collective write of file info */
        int write_count = (int) packcount;
        MPI_File_write_at_all(fh, write_offset, buf, write_count, dt, &status);

        /* update our offset with the number of bytes we just wrote */
        write_offset += (MPI_Offset)packcount;

        /* one less iteration */
        all_iters--;
    }

    /* free write buffer */
    mfu_free(&buf);

    /* close file */
    MPI_File_close(&fh);

    /* free the datatype */
    MPI_Type_free(&dt);

    /* free mpi info */
    MPI_Info_free(&info);

    return;
}

static void write_cache_stat_v3(
    const char* name,
    uint64_t walk_start,
    uint64_t walk_end,
    flist_t* flist)
{
    buf_t* users  = &flist->users;
    buf_t* groups = &flist->groups;

    /* get our rank in job & number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* use mpi io hints to stripe across OSTs */
    MPI_Info info;
    MPI_Info_create(&info);

    /* get number of items in our list and total file count */
    uint64_t count     = flist->list_count;
    uint64_t all_count = flist->total_files;
    uint64_t offset    = flist->offset;

    /* find smallest length that fits max and consists of integer
     * number of 8 byte segments */
    int max = (int) flist->max_file_name;
    int chars = max / 8;
    if (chars * 8 < max) {
        chars++;
    }
    chars *= 8;

    /* build datatype to hold file info */
    MPI_Datatype dt;
    create_stattype(flist->detail, chars, &dt);

    /* get extent of stat type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* open file */
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
    int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;

    /* change number of ranks to string to pass to MPI_Info */
    char str_buf[12];
    sprintf(str_buf, "%d", ranks);

    /* no. of I/O devices for lustre striping is number of ranks */
    MPI_Info_set(info, "striping_factor", str_buf);

    MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, info, &fh);

    /* truncate file to 0 bytes */
    MPI_File_set_size(fh, 0);

    /* prepare header */
    uint64_t header[9];
    header[0] = 3;               /* file version */
    header[1] = walk_start;      /* time_t when file walk started */
    header[2] = walk_end;        /* time_t when file walk stopped */
    header[3] = users->count;    /* number of user records */
    header[4] = users->chars;    /* number of chars in user name */
    header[5] = groups->count;   /* number of group records */
    header[6] = groups->chars;   /* number of chars in group name */
    header[7] = all_count;       /* total number of stat entries */
    header[8] = (uint64_t)chars; /* number of chars in file name */

    /* write the header */
    MPI_Offset disp = 0;
    MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_write_at(fh, 0, header, 9, MPI_UINT64_T, &status);
    }
    disp += 9 * 8;

    if (users->dt != MPI_DATATYPE_NULL) {
        /* get extent user */
        MPI_Aint lb_user, extent_user;
        MPI_Type_get_extent(users->dt, &lb_user, &extent_user);

        /* write out users */
        MPI_File_set_view(fh, disp, users->dt, users->dt, datarep, MPI_INFO_NULL);
        if (rank == 0) {
            int write_count = (int) users->count;
            MPI_File_write_at(fh, 0, users->buf, write_count, users->dt, &status);
        }
        disp += (MPI_Offset)users->count * extent_user;
    }

    if (groups->dt != MPI_DATATYPE_NULL) {
        /* get extent group */
        MPI_Aint lb_group, extent_group;
        MPI_Type_get_extent(groups->dt, &lb_group, &extent_group);

        /* write out groups */
        MPI_File_set_view(fh, disp, groups->dt, groups->dt, datarep, MPI_INFO_NULL);
        if (rank == 0) {
            int write_count = (int) groups->count;
            MPI_File_write_at(fh, 0, groups->buf, write_count, groups->dt, &status);
        }
        disp += (MPI_Offset)groups->count * extent_group;
    }

    /* in order to avoid blowing out memory, we'll pack into a smaller
     * buffer and iteratively make many collective writes */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = 1024 * 1024;
    if (bufsize < (size_t) extent) {
        bufsize = (size_t) extent;
    }
    void* buf = MFU_MALLOC(bufsize);

    /* compute number of items we can fit in each write iteration */
    uint64_t bufcount = (uint64_t)bufsize / (uint64_t)extent;

    /* determine number of iterations we need to write all items */
    uint64_t iters = count / bufcount;
    if (iters * bufcount < count) {
        iters++;
    }

    /* compute max iterations across all procs */
    uint64_t all_iters;
    MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* set file view to be sequence of datatypes past header */
    MPI_File_set_view(fh, disp, dt, dt, datarep, MPI_INFO_NULL);

    /* compute byte offset to write our element */
    MPI_Offset write_offset = (MPI_Offset)offset;

    /* iterate with multiple writes until all records are written */
    const elem_t* current = flist->list_head;
    while (all_iters > 0) {
        /* copy stat data into write buffer */
        char* ptr = (char*) buf;
        uint64_t packcount = 0;
        while (current != NULL && packcount < bufcount) {
            /* pack item into buffer and advance pointer */
            size_t pack_bytes = list_elem_pack(ptr, flist->detail, (uint64_t)chars, current);
            ptr += pack_bytes;
            packcount++;
            current = current->next;
        }

        /* collective write of file info */
        int write_count = (int) packcount;
        MPI_File_write_at_all(fh, write_offset, buf, write_count, dt, &status);

        /* update our offset with the number of bytes we just wrote */
        write_offset += (MPI_Offset)packcount;

        /* one less iteration */
        all_iters--;
    }

    /* free write buffer */
    mfu_free(&buf);

    /* close file */
    MPI_File_close(&fh);

    /* free the datatype */
    MPI_Type_free(&dt);

    /* free mpi info */
    MPI_Info_free(&info);

    return;
}

static void write_cache_stat_v4(
    const char* name,
    flist_t* flist)
{
    buf_t* users  = &flist->users;
    buf_t* groups = &flist->groups;

    /* get our rank in job & number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* use mpi io hints to stripe across OSTs */
    MPI_Info info;
    MPI_Info_create(&info);

    /* get number of items in our list and total file count */
    uint64_t count     = flist->list_count;
    uint64_t all_count = flist->total_files;
    uint64_t offset    = flist->offset;

    /* find smallest length that fits max and consists of integer
     * number of 8 byte segments */
    int max = (int) flist->max_file_name;
    int chars = max / 8;
    if (chars * 8 < max) {
        chars++;
    }
    chars *= 8;

    /* compute size of each element */
    size_t elem_size = list_elem_pack_size(flist->detail, chars, NULL);

    /* open file */
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
    int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;

    /* change number of ranks to string to pass to MPI_Info */
    char str_buf[12];
    sprintf(str_buf, "%d", ranks);

    /* no. of I/O devices for lustre striping is number of ranks */
    MPI_Info_set(info, "striping_factor", str_buf);

    MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, info, &fh);

    /* truncate file to 0 bytes */
    MPI_File_set_size(fh, 0);

    /* prepare header */
    uint64_t header[7];
    char* ptr = (char*) header;
    mfu_pack_io_uint64(&ptr, 4);               /* file version */
    mfu_pack_io_uint64(&ptr, users->count);    /* number of user records */
    mfu_pack_io_uint64(&ptr, users->chars);    /* number of chars in user name */
    mfu_pack_io_uint64(&ptr, groups->count);   /* number of group records */
    mfu_pack_io_uint64(&ptr, groups->chars);   /* number of chars in group name */
    mfu_pack_io_uint64(&ptr, all_count);       /* total number of stat entries */
    mfu_pack_io_uint64(&ptr, (uint64_t)chars); /* number of chars in file name */

    /* write the header */
    MPI_Offset disp = 0;
    int header_bytes = 7 * 8;
    MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);
    if (rank == 0) {
        MPI_File_write_at(fh, 0, header, header_bytes, MPI_BYTE, &status);
    }
    disp += header_bytes;

    if (users->dt != MPI_DATATYPE_NULL) {
        /* write out users */
        MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);
        int user_buf_size = (int) buft_pack_size(users);
        if (rank == 0) {
            char* user_buf = (char*) MFU_MALLOC(user_buf_size);
            buft_pack(user_buf, users);
            MPI_File_write_at(fh, 0, user_buf, user_buf_size, MPI_BYTE, &status);
            mfu_free(&user_buf);
        }
        disp += (MPI_Offset)user_buf_size;
    }

    if (groups->dt != MPI_DATATYPE_NULL) {
        /* write out groups */
        MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);
        int group_buf_size = (int) buft_pack_size(groups);
        if (rank == 0) {
            char* group_buf = (char*) MFU_MALLOC(group_buf_size);
            buft_pack(group_buf, groups);
            MPI_File_write_at(fh, 0, group_buf, group_buf_size, MPI_BYTE, &status);
            mfu_free(&group_buf);
        }
        disp += (MPI_Offset)group_buf_size;
    }

    /* in order to avoid blowing out memory, we'll pack into a smaller
     * buffer and iteratively make many collective writes */

    /* allocate a buffer, ensure it's large enough to hold at least one
     * complete record */
    size_t bufsize = 1024 * 1024;
    if (bufsize < elem_size) {
        bufsize = elem_size;
    }
    void* buf = MFU_MALLOC(bufsize);

    /* compute number of items we can fit in each write iteration */
    uint64_t bufcount = (uint64_t)bufsize / (uint64_t)elem_size;

    /* determine number of iterations we need to write all items */
    uint64_t iters = count / bufcount;
    if (iters * bufcount < count) {
        iters++;
    }

    /* compute max iterations across all procs */
    uint64_t all_iters;
    MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* set file view to be sequence of datatypes past header */
    MPI_File_set_view(fh, disp, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);

    /* compute byte offset to write our element */
    MPI_Offset write_offset = (MPI_Offset)offset * elem_size;

    /* iterate with multiple writes until all records are written */
    const elem_t* current = flist->list_head;
    while (all_iters > 0) {
        /* copy stat data into write buffer */
        ptr = (char*) buf;
        uint64_t packcount = 0;
        while (current != NULL && packcount < bufcount) {
            /* pack item into buffer and advance pointer */
            size_t pack_bytes = list_elem_pack(ptr, flist->detail, (uint64_t)chars, current);
            ptr += pack_bytes;
            packcount += (uint64_t)pack_bytes;
            current = current->next;
        }

        /* collective write of file info */
        int write_count = (int) packcount;
        MPI_File_write_at_all(fh, write_offset, buf, write_count, MPI_BYTE, &status);

        /* update our offset with the number of bytes we just wrote */
        write_offset += (MPI_Offset)packcount;

        /* one less iteration */
        all_iters--;
    }

    /* free write buffer */
    mfu_free(&buf);

    /* close file */
    MPI_File_close(&fh);

    /* free mpi info */
    MPI_Info_free(&info);

    return;
}

void mfu_flist_write_cache(
    const char* name,
    mfu_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;

    /* start timer */
    double start_write = MPI_Wtime();

    /* total list items */
    uint64_t all_count = mfu_flist_global_size(flist);

    /* report the filename we're writing to */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing to output file: %s", name);
    }

    if (all_count > 0) {
        if (flist->detail) {
            //write_cache_stat_v3(name, 0, 0, flist);
            write_cache_stat_v4(name, flist);
        }
        else {
            //write_cache_readdir(name, 0, 0, flist);
            write_cache_readdir_variable(name, flist);
        }
    }

    /* end timer */
    double end_write = MPI_Wtime();

    /* report write count, time, and rate */
    if (mfu_rank == 0) {
        double secs = end_write - start_write;
        double rate = 0.0;
        if (secs > 0.0) {
            rate = ((double)all_count) / secs;
        }
        MFU_LOG(MFU_LOG_INFO, "Wrote %lu files in %f seconds (%f files/sec)",
               all_count, secs, rate
              );
    }

    /* wait for summary to be printed */
    MPI_Barrier(MPI_COMM_WORLD);

    return;
}

/* TODO: move this somewhere or modify existing print_file */
/* print information about a file given the index and rank (used in print_files) */
static size_t print_file_text(mfu_flist flist, uint64_t idx, char* buffer, size_t bufsize)
{
    size_t numbytes = 0;

    /* store types as strings for print_file */
    char type_str_unknown[] = "UNK";
    char type_str_dir[]     = "DIR";
    char type_str_file[]    = "REG";
    char type_str_link[]    = "LNK";

    /* get filename */
    const char* file = mfu_flist_file_get_name(flist, idx);

    if (mfu_flist_have_detail(flist)) {
        /* get mode */
        mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

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
        size_t modify_rc = strftime(modify_s, sizeof(modify_s) - 1, "%b %e %Y %H:%M", localtime(&modify_t));
        size_t create_rc = strftime(create_s, sizeof(create_s) - 1, "%FT%T", localtime(&create_t));
        if (access_rc == 0 || modify_rc == 0 || create_rc == 0) {
            /* error */
            access_s[0] = '\0';
            modify_s[0] = '\0';
            create_s[0] = '\0';
        }

        char mode_format[11];
        mfu_format_mode(mode, mode_format);

        double size_tmp;
        const char* size_units;
        mfu_format_bytes(size, &size_tmp, &size_units);

        numbytes = snprintf(buffer, bufsize, "%s %s %s %7.3f %2s %s %s\n",
            mode_format, username, groupname,
            size_tmp, size_units, modify_s, file
        );
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

        numbytes = snprintf(buffer, bufsize, "Type=%s File=%s\n",
            type_str, file
        );
    }

    return numbytes;
}

void mfu_flist_write_text(
    const char* name,
    mfu_flist bflist)
{
    /* convert handle to flist_t */
    flist_t* flist = (flist_t*) bflist;

    /* get our rank and size of the communicator */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* start timer */
    double start_write = MPI_Wtime();

    /* total list items */
    uint64_t all_count = mfu_flist_global_size(flist);

    /* report the filename we're writing to */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing to output file: %s", name);
    }

    /* compute size of buffer needed to hold all data */
    size_t bufsize = 0;
    uint64_t idx;
    uint64_t size = mfu_flist_size(flist);
    for (idx = 0; idx < size; idx++) {
        size_t count = print_file_text(flist, idx, NULL, 0);
        bufsize += count + 1;
    }

    /* allocate a buffer big enough to hold all of the data */
    char* buf = (char*) MFU_MALLOC(bufsize);

    /* format data in buffer */
    char* ptr = buf;
    size_t total = 0;
    for (idx = 0; idx < size; idx++) {
        size_t count = print_file_text(flist, idx, ptr, bufsize - total);
        total += count;
        ptr += count;
    }

    /* if we block things up into 128MB chunks, how many iterations
     * to write everything? */
    uint64_t maxwrite = 128 * 1024 * 1024;
    uint64_t iters = (uint64_t)total / maxwrite;
    if (iters * maxwrite < (uint64_t)total) {
        iters++;
    }

    /* get max iterations across all procs */
    uint64_t all_iters;
    MPI_Allreduce(&iters, &all_iters, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* use mpi io hints to stripe across OSTs */
    MPI_Info info;
    MPI_Info_create(&info);

    /* change number of ranks to string to pass to MPI_Info */
    char str_buf[12];
    sprintf(str_buf, "%d", ranks);

    /* no. of I/O devices for lustre striping is number of ranks */
    MPI_Info_set(info, "striping_factor", str_buf);

    /* open file */
    MPI_Status status;
    MPI_File fh;
    char datarep[] = "external32";
    int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;

    MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, info, &fh);

    /* truncate file to 0 bytes */
    MPI_File_set_size(fh, 0);

    /* set file view to be sequence of datatypes past header */
    MPI_File_set_view(fh, 0, MPI_BYTE, MPI_BYTE, datarep, MPI_INFO_NULL);

    /* compute byte offset to write our element */
    uint64_t offset = 0;
    uint64_t bytes = (uint64_t) total;
    MPI_Exscan(&bytes, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Offset write_offset = (MPI_Offset)offset;

    ptr = buf;
    uint64_t written = 0;
    while (all_iters > 0) {
        /* compute number of bytes left to write */
        uint64_t remaining = (uint64_t)total - written;

        /* compute count we'll write in this iteration */
        int write_count = (int) maxwrite;
        if (remaining < maxwrite) {
            write_count = (int) remaining;
        }
    
        /* collective write of file data */
        MPI_File_write_at_all(fh, write_offset, ptr, write_count, MPI_BYTE, &status);

        /* update our offset into the file */
        write_offset += (MPI_Offset) write_count;

        /* update pointer into our buffer */
        ptr += write_count;

        /* update number of bytes written so far */
        written += (uint64_t) write_count;

        /* decrement our collective write loop counter */
        all_iters--;
    }

    /* close file */
    MPI_File_close(&fh);

    /* free mpi info */
    MPI_Info_free(&info);

    /* free buffer */
    mfu_free(&buf);

    /* end timer */
    double end_write = MPI_Wtime();

    /* report write count, time, and rate */
    if (mfu_rank == 0) {
        double secs = end_write - start_write;
        double rate = 0.0;
        if (secs > 0.0) {
            rate = ((double)all_count) / secs;
        }
        MFU_LOG(MFU_LOG_INFO, "Wrote %lu files in %f seconds (%f files/sec)",
               all_count, secs, rate
              );
    }

    return;
}
