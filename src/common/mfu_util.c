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

#include "mfu.h"
#include "mpi.h"
#include "dtcmp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <limits.h>

int mfu_initialized = 0;

/* set globals */
int mfu_rank = -1;

/* users may override these to change settings */
FILE* mfu_debug_stream = NULL;
mfu_loglevel mfu_debug_level = MFU_LOG_ERR;

/* initialize mfu library,
 * reference counting allows for multiple init/finalize pairs */
int mfu_init()
{
    if (mfu_initialized == 0) {
        /* set globals */
        MPI_Comm_rank(MPI_COMM_WORLD, &mfu_rank);
        mfu_debug_stream = stdout;

        DTCMP_Init();
        mfu_initialized++;
    }
    return MFU_SUCCESS;
}

/* finalize mfu library */
int mfu_finalize()
{
    if (mfu_initialized > 0) {
        DTCMP_Finalize();
        mfu_initialized--;
    }
    return MFU_SUCCESS;
}

/* print abort message and call MPI_Abort to kill run */
void mfu_abort(const char* file, int line, int rc, const char* fmt, ...)
{
    va_list argp;
    fprintf(stderr, "ABORT: rank X on HOST: ");
    va_start(argp, fmt);
    vfprintf(stderr, fmt, argp);
    va_end(argp);
    fprintf(stderr, " @ %s:%d\n", file, line);

    MPI_Abort(MPI_COMM_WORLD, rc);
}

/* if size > 0 allocates size bytes and returns pointer,
 * calls mfu_abort if malloc fails, returns NULL if size == 0 */
void* mfu_malloc(size_t size, const char* file, int line)
{
    /* only bother if size > 0 */
    if (size > 0) {
        /* try to allocate memory and check whether we succeeded */
        void* ptr = malloc(size);
        if (ptr == NULL) {
            /* allocate failed, abort */
            mfu_abort(file, line, 1, "Failed to allocate %llu bytes",
                        (unsigned long long) size
                       );
        }

        /* return the pointer */
        return ptr;
    }

    return NULL;
}

/* if size > 0, allocates size bytes aligned with specified alignment
 * and returns pointer, calls mfu_abort on failure,
 * returns NULL if size == 0 */
void* mfu_memalign(size_t size, size_t alignment, const char* file, int line)
{
    /* only bother if size > 0 */
    if (size > 0) {
        /* try to allocate memory and check whether we succeeded */
        void* ptr;
        int rc = posix_memalign(&ptr, alignment, size);
        if (rc != 0) {
            /* allocate failed, abort */
            mfu_abort(file, line, 1, "Failed to allocate %llu bytes posix_memalign rc=%d",
                        (unsigned long long) size, rc
                       );
        }

        /* return the pointer */
        return ptr;
    }

    return NULL;
}

/* if str != NULL, call strdup and return pointer, calls mfu_abort if strdup fails */
char* mfu_strdup(const char* str, const char* file, int line)
{
    if (str != NULL) {
        /* TODO: check that str length is below some max? */
        char* ptr = strdup(str);
        if (ptr == NULL) {
            /* allocate failed, abort */
            mfu_abort(file, line, 1, "Failed to allocate string");
        }

        return ptr;
    }
    return NULL;
}

char* mfu_strdupf(const char* file, int line, const char* format, ...)
{
  va_list args;
  char* str = NULL;

  /* check that we have a format string */
  if (format == NULL) {
    return NULL;
  }

  /* compute the size of the string we need to allocate */
  va_start(args, format);
  int size = vsnprintf(NULL, 0, format, args) + 1;
  va_end(args);

  /* allocate and print the string */
  if (size > 0) {
    str = (char*) mfu_malloc(size, file, line);

    va_start(args, format);
    vsnprintf(str, size, format, args);
    va_end(args);
  }

  return str;
}

/* free memory if pointer is not NULL, set pointer to NULL */
void mfu_free(void* p)
{
    /* verify that we got a valid pointer to a pointer */
    if (p != NULL) {
        /* free memory if there is any */
        void* ptr = *(void**)p;
        if (ptr != NULL) {
            free(ptr);
        }

        /* set caller's pointer to NULL */
        *(void**)p = NULL;
    }
}

void mfu_bcast_strdup(const char* send, char** recv, int root, MPI_Comm comm)
{
    /* get our rank in the communicator */
    int rank;
    MPI_Comm_rank(comm, &rank);

    /* check that caller gave us a pointer to a char pointer */
    if (recv == NULL) {
        MFU_ABORT(1, "Invalid recv pointer");
    }

    /* First, broadcast length of string. */
    int len = 0;

    if (rank == root) {
        if (send != NULL) {
            len = (int)(strlen(send) + 1);

            /* TODO: check that strlen fits within an int */
        }
    }

    if (MPI_SUCCESS != MPI_Bcast(&len, 1, MPI_INT, root, comm)) {
        MFU_ABORT(1, "Failed to broadcast length of string");
    }

    /* If the string is non-zero bytes, allocate space and bcast it. */
    if (len > 0) {
        /* allocate space to receive string */
        *recv = (char*) MFU_MALLOC((size_t)len);

        /* Broadcast the string. */
        if (rank == root) {
            strncpy(*recv, send, (size_t)len);
        }

        if (MPI_SUCCESS != MPI_Bcast(*recv, len, MPI_CHAR, root, comm)) {
            MFU_ABORT(1, "Failed to bcast string");
        }

    }
    else {
        /* Root passed in a NULL value, so set the output to NULL. */
        *recv = NULL;
    }

    return;
}

static void mfu_format_1000(
    double input,
    const char** units_list,
    int units_len,
    double* val,
    const char* units[])
{
    /* divide input by 1000 until it falls to less than 1000,
     * increment units each time */
    int idx = 0;
    while (input / 1000.0 > 1.0) {
        input /= 1000.0;
        idx++;
        if (idx == (units_len - 1)) {
            /* we've gone as high as we can go */
            break;
        }
    }

    /* set output paramaters */
    *val   = input;
    *units = units_list[idx];

    return;
}

/* given a number of items, return value converted to returned units */
#define NUM_UNITS_COUNT (7)
static const char* units_count[] = {"", "k", "m", "g", "t", "p", "e"};
void mfu_format_count(uint64_t count, double* val, const char** units)
{
    double count_d = (double) count;
    mfu_format_1000(count_d, units_count, NUM_UNITS_COUNT, val, units);
    return;
}

static void mfu_format_1024(
    double input,
    const char** units_list,
    int units_len,
    double* val,
    const char* units[])
{
    /* divide input by 1024 until it falls to less than 1024,
     * increment units each time */
    int idx = 0;
    while (input / 1024.0 > 1.0) {
        input /= 1024.0;
        idx++;
        if (idx == (units_len - 1)) {
            /* we've gone as high as we can go */
            break;
        }
    }

    /* for values like 1005mb (more than 1000 but less than 1024 */
    if (input > 1000.0 && idx < (units_len - 1)) {
        input /= 1024.0;
        idx++;
    }

    /* set output paramaters */
    *val   = input;
    *units = units_list[idx];

    return;
}

/* given a number of bytes, return value converted to returned units */
#define NUM_UNITS_BYTES (7)
static const char* units_bytes[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
void mfu_format_bytes(uint64_t bytes, double* val, const char** units)
{
    double bytes_d = (double) bytes;
    mfu_format_1024(bytes_d, units_bytes, NUM_UNITS_BYTES, val, units);
    return;
}

#define NUM_UNITS_BW (7)
static const char* units_bw[] = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s", "PB/s", "EB/s"};
void mfu_format_bw(double bw, double* val, const char** units)
{
    mfu_format_1024(bw, units_bw, NUM_UNITS_BW, val, units);
    return;
}

static unsigned long long kilo  =                1024ULL;
static unsigned long long mega  =             1048576ULL;
static unsigned long long giga  =          1073741824ULL;
static unsigned long long tera  =       1099511627776ULL;
static unsigned long long peta  =    1125899906842624ULL;
static unsigned long long exa   = 1152921504606846976ULL;

/* abtoull ==> ASCII bytes to unsigned long long
 * Converts string like "10mb" to unsigned long long integer value
 * of 10*1024*1024.  Input string should have leading number followed
 * by optional units.  The leading number can be a floating point
 * value (read by strtod).  The trailing units consist of one or two
 * letters which should be attached to the number with no space
 * in between.  The units may be upper or lower case, and the second
 * letter if it exists, must be 'b' or 'B' (short for bytes).
 *
 * Valid units: k,K,m,M,g,G,t,T,p,P,e,E
 *
 * Examples: 2kb, 1.5m, 200GB, 1.4T.
 *
 * Returns MFU_SUCCESS if conversion is successful,
 * and MFU_FAILURE otherwise.
 *
 * Returns converted value in val parameter.  This
 * parameter is only updated if successful. */
int mfu_abtoull(const char* str, unsigned long long* val)
{
    /* check that we have a string */
    if (str == NULL) {
        return MFU_FAILURE;
    }

    /* check that we have a value to write to */
    if (val == NULL) {
        return MFU_FAILURE;
    }

    /* pull the floating point portion of our byte string off */
    errno = 0;
    char* next = NULL;
    double num = strtod(str, &next);
    if (errno != 0) {
        /* conversion failed */
        return MFU_FAILURE;
    }
    if (str == next) {
        /* no conversion performed */
        return MFU_FAILURE;
    }

    /* now extract any units, e.g. KB MB GB, etc */
    unsigned long long units = 1;
    if (*next != '\0') {
        switch (*next) {
            case 'k':
            case 'K':
                units = kilo;
                break;
            case 'm':
            case 'M':
                units = mega;
                break;
            case 'g':
            case 'G':
                units = giga;
                break;
            case 't':
            case 'T':
                units = tera;
                break;
            case 'p':
            case 'P':
                units = peta;
                break;
            case 'e':
            case 'E':
                units = exa;
                break;
            default:
                /* unknown units symbol */
                return MFU_FAILURE;
        }

        next++;

        /* handle optional b or B character, e.g. in 10KB vs 10K */
        if (*next == 'b' || *next == 'B') {
            next++;
        }

        /* check that we've hit the end of the string */
        if (*next != 0) {
            return MFU_FAILURE;
        }
    }

    /* check that we got a positive value */
    if (num < 0) {
        return MFU_FAILURE;
    }

    /* TODO: double check this overflow calculation */
    /* multiply by our units and check for overflow */
    double units_d = (double) units;
    double val_d = num * units_d;
    double max_d = (double) ULLONG_MAX;
    if (val_d > max_d) {
        /* overflow */
        return MFU_FAILURE;
    }

    /* set return value */
    *val = (unsigned long long) val_d;

    return MFU_SUCCESS;
}

/* give a mode type, print permission bits in ls -l form,
 * e.g., -rwxr--r-- for a file of 755
 * buf must be at least 11 characters long */
void mfu_format_mode(mode_t mode, char* buf)
{
    if (S_ISDIR(mode)) {
        buf[0] = 'd';
    }
    else if (S_ISLNK(mode)) {
        buf[0] = 'l';
    }
    else {
        buf[0] = '-';
    }

    if (S_IRUSR & mode) {
        buf[1] = 'r';
    }
    else {
        buf[1] = '-';
    }

    if (S_IWUSR & mode) {
        buf[2] = 'w';
    }
    else {
        buf[2] = '-';
    }

    if (S_IXUSR & mode) {
        buf[3] = 'x';
    }
    else {
        buf[3] = '-';
    }

    if (S_IRGRP & mode) {
        buf[4] = 'r';
    }
    else {
        buf[4] = '-';
    }

    if (S_IWGRP & mode) {
        buf[5] = 'w';
    }
    else {
        buf[5] = '-';
    }

    if (S_IXGRP & mode) {
        buf[6] = 'x';
    }
    else {
        buf[6] = '-';
    }

    if (S_IROTH & mode) {
        buf[7] = 'r';
    }
    else {
        buf[7] = '-';
    }

    if (S_IWOTH & mode) {
        buf[8] = 'w';
    }
    else {
        buf[8] = '-';
    }

    if (S_IXOTH & mode) {
        buf[9] = 'x';
    }
    else {
        buf[9] = '-';
    }

    buf[10] = '\0';

    return;
}

void mfu_pack_uint32(char** pptr, uint32_t value)
{
    /* TODO: convert to network order */
    uint32_t* ptr = *(uint32_t**)pptr;
    *ptr = value;
    *pptr += 4;
}

void mfu_unpack_uint32(const char** pptr, uint32_t* value)
{
    /* TODO: convert to host order */
    const uint32_t* ptr = *(const uint32_t**)pptr;
    *value = *ptr;
    *pptr += 4;
}

void mfu_pack_uint64(char** pptr, uint64_t value)
{
    /* TODO: convert to network order */
    uint64_t* ptr = *(uint64_t**)pptr;
    *ptr = value;
    *pptr += 8;
}

void mfu_unpack_uint64(const char** pptr, uint64_t* value)
{
    /* TODO: convert to host order */
    const uint64_t* ptr = *(const uint64_t**)pptr;
    *value = *ptr;
    *pptr += 8;
}

/* Bob Jenkins one-at-a-time hash: http://en.wikipedia.org/wiki/Jenkins_hash_function */
uint32_t mfu_hash_jenkins(const char* key, size_t len)
{
    uint32_t hash, i;
    for (hash = i = 0; i < len; ++i) {
        hash += ((unsigned char) key[i]);
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}
