/* to get nsec fields in stat structure */
#define _GNU_SOURCE

#include "mfu.h"
#include "mpi.h"
#include "dtcmp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <limits.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef ULLONG_MAX
#define ULLONG_MAX (__LONG_LONG_MAX__ * 2UL + 1UL)
#endif

#ifdef LUSTRE_SUPPORT
#include <lustre/lustreapi.h>
#endif

int mfu_initialized = 0;

/* set globals */
int mfu_rank = -1;

/* users may override these to change settings */
FILE* mfu_debug_stream = NULL;
mfu_loglevel mfu_debug_level = MFU_LOG_ERR;

/* default progress message timeout in seconds */
int mfu_progress_timeout = 10;

/***** DAOS utility functions ******/
#ifdef DAOS_SUPPORT
bool daos_uuid_valid(const uuid_t uuid)
{
    return uuid && !uuid_is_null(uuid);
}

/* Distribute process 0's pool or container handle to others. */
void daos_bcast_handle(
  int rank,              /* root rank for broadcast */
  daos_handle_t* handle, /* handle value to be broadcasted */
  daos_handle_t* poh,    /* daos pool for global2local conversion of container handle */
  enum handleType type)  /* handle type: POOL_HANDLE or CONT_HANDLE */
{
    int rc;

    d_iov_t global;
    global.iov_buf     = NULL;
    global.iov_buf_len = 0;
    global.iov_len     = 0;

    /* Get the global handle size. */
    if (rank == 0) {
        if (type == POOL_HANDLE) {
            rc = daos_pool_local2global(*handle, &global);
        } else {
            rc = daos_cont_local2global(*handle, &global);
        }
        if (rc != 0) {
            MFU_ABORT(-1, "Failed to get global handle size");
        }
    }

    /* broadcast size of global handle */
    MPI_Bcast(&global.iov_buf_len, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* allocate memory to hold global handle value */
    global.iov_len = global.iov_buf_len;
    global.iov_buf = MFU_MALLOC(global.iov_buf_len);

    /* convert from local handle to global handle */
    if (rank == 0) {
       if (type == POOL_HANDLE) {
           rc = daos_pool_local2global(*handle, &global);
       } else {
           rc = daos_cont_local2global(*handle, &global);
       }
       if (rc != 0) {
           MFU_ABORT(-1, "Failed to create global handle");
       }
    }

    /* broadcast global handle value */
    MPI_Bcast(global.iov_buf, global.iov_buf_len, MPI_BYTE, 0, MPI_COMM_WORLD);

    /* convert global handle to local value */
    if (rank != 0) {
        if (type == POOL_HANDLE) {
            rc = daos_pool_global2local(global, handle);
        } else {
            rc = daos_cont_global2local(*poh, global, handle);
        }
        if (rc != 0) {
            MFU_ABORT(-1, "Failed to get local handle");
        }
    }

    /* free temporary buffer used to hold global handle value */
    mfu_free(&global.iov_buf);
}

void daos_connect(
  int rank,
  const char* svc,
  uuid_t pool_uuid,
  uuid_t cont_uuid,
  daos_handle_t* poh,
  daos_handle_t* coh)
{
    /* TODO: if src daos path and dst daos path are false 
     * skip connecting to daos pool */

    /* have rank 0 connect to the pool and container,
     * we'll then broadcast the handle ids from rank 0 to everyone else */
    if (rank == 0) {
        d_rank_list_t* svcl = daos_rank_list_parse(svc, ":");
        if (svcl == NULL) {
            MFU_ABORT(-1, "Failed to parse DAOS rank list: '%s'", svc);
        }

        /* Connect to DAOS pool */
        daos_pool_info_t pool_info;
        int rc = daos_pool_connect(pool_uuid, NULL, svcl, DAOS_PC_RW,
                               poh, &pool_info, NULL);
        if (rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to connect to pool");
        }
        d_rank_list_free(svcl);

        /* attempt to open the daos container */
        daos_cont_info_t co_info;
        rc = daos_cont_open(*poh, cont_uuid, DAOS_COO_RW, coh, &co_info, NULL);

        /* If NOEXIST we create it */
        if (rc != 0) {
            /* create the container */
            uuid_t cuuid;
            rc = dfs_cont_create(*poh, cuuid, NULL, NULL, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to create DFS container");
            }

            /* try to open it again */
            rc = daos_cont_open(*poh, cuuid, DAOS_COO_RW, coh, &co_info, NULL);
            if (rc != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to open DFS container");
            }
        }
    }

    /* broadcast pool and container handles from rank 0 */
    daos_bcast_handle(rank, poh, poh, POOL_HANDLE);
    daos_bcast_handle(rank, coh, poh, CONT_HANDLE);
}
#endif

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
            mfu_abort(file, line, 1, "Failed to allocate %llu bytes. Try using more nodes.",
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
            mfu_abort(file, line, 1, "Failed to allocate %llu bytes posix_memalign rc=%d. Try using more nodes.",
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
            mfu_abort(file, line, 1, "Failed to allocate string. Try using more nodes.");
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

void mfu_stat_get_atimes(const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_atime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_atimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_atim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_atime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_uatime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_atime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

void mfu_stat_get_mtimes (const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_mtime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_mtimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_mtim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_mtime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_umtime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_mtime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

void mfu_stat_get_ctimes (const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_ctime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_ctimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_ctim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_ctime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_uctime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_ctime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

void mfu_stat_set_atimes (struct stat* sb, uint64_t secs, uint64_t nsecs)
{
    sb->st_atime = (time_t) secs;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    sb->st_atimespec.tv_nsec = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    sb->st_atim.tv_nsec = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    sb->st_atime_n = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    sb->st_uatime = (time_t) (nsecs / 1000);
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    sb->st_atime_usec = (time_t) (nsecs / 1000);
#else
    // error?
#endif
}

void mfu_stat_set_mtimes (struct stat* sb, uint64_t secs, uint64_t nsecs)
{
    sb->st_mtime = (time_t) secs;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    sb->st_mtimespec.tv_nsec = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    sb->st_mtim.tv_nsec = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    sb->st_mtime_n = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    sb->st_umtime = (time_t) (nsecs / 1000);
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    sb->st_mtime_usec = (time_t) (nsecs / 1000);
#else
    // error?
#endif
}

void mfu_stat_set_ctimes (struct stat* sb, uint64_t secs, uint64_t nsecs)
{
    sb->st_ctime = (time_t) secs;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    sb->st_ctimespec.tv_nsec = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    sb->st_ctim.tv_nsec = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    sb->st_ctime_n = (time_t) nsecs;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    sb->st_uctime = (time_t) (nsecs / 1000);
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    sb->st_ctime_usec = (time_t) (nsecs / 1000);
#else
    // error?
#endif
}

/* compares contents of two files and optionally overwrite dest with source,
 * returns -1 on error, 0 if equal, 1 if different */
int mfu_compare_contents(
    const char* src_name,          /* IN  - path name to souce file */
    const char* dst_name,          /* IN  - path name to destination file */
    off_t offset,                  /* IN  - offset with file to start comparison */
    off_t length,                  /* IN  - number of bytes to be compared */
    size_t bufsize,                /* IN  - size of I/O buffer to be used during compare */
    int overwrite,                 /* IN  - whether to replace dest with source contents (1) or not (0) */
    uint64_t* count_bytes_read,    /* OUT - number of bytes read (src + dest) */
    uint64_t* count_bytes_written, /* OUT - number of bytes written to dest */
    mfu_progress* prg)             /* IN  - progress message structure */
{
    /* open source file */
    int src_fd = mfu_open(src_name, O_RDONLY);
    if (src_fd < 0) {
        /* log error if there is an open failure on the src side */
        MFU_LOG(MFU_LOG_ERR, "Failed to open `%s' (errno=%d %s)",
          src_name, errno, strerror(errno));
       return -1;
    }

    /* avoid opening file in write mode if we're only reading */
    int dst_flags = O_RDONLY;
    if (overwrite) {
        dst_flags = O_RDWR;
    }

    /* open destination file */
    int dst_fd = mfu_open(dst_name, dst_flags);
    if (dst_fd < 0) {
        /* log error if there is an open failure on the dst side */
        MFU_LOG(MFU_LOG_ERR, "Failed to open `%s' (errno=%d %s)",
          dst_name, errno, strerror(errno));
        mfu_close(src_name, src_fd);
        return -1;
    }

    /* hint that we'll read from file sequentially */
    posix_fadvise(src_fd, offset, length, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(dst_fd, offset, length, POSIX_FADV_SEQUENTIAL);

    /* assume we'll find that file contents are the same */
    int rc = 0;

    /* seek to offset in source file */
    if (mfu_lseek(src_name, src_fd, offset, SEEK_SET) == (off_t)-1) {
        /* log error if there is an lseek failure on the src side */
        MFU_LOG(MFU_LOG_ERR, "Failed to lseek `%s', offset: %lx (errno=%d %s)",
          src_name, (unsigned long)offset, errno, strerror(errno));
        mfu_close(dst_name, dst_fd);
        mfu_close(src_name, src_fd);
        return -1;
    }

    /* seek to offset in destination file */
    if (mfu_lseek(dst_name, dst_fd, offset, SEEK_SET) == (off_t)-1) {
        /* log error if there is an lseek failure on the dst side */
        MFU_LOG(MFU_LOG_ERR, "Failed to lseek `%s', offset: %lx (errno=%d %s)",
          dst_name, (unsigned long)offset, errno, strerror(errno));
        mfu_close(dst_name, dst_fd);
        mfu_close(src_name, src_fd);
        return -1;
    }

    /* allocate buffers to read file data */
    void* src_buf  = MFU_MALLOC(bufsize);
    void* dest_buf = MFU_MALLOC(bufsize);

    /* read and compare data from files */
    off_t total_bytes = 0;
    while (length == 0 || total_bytes < length) {
        /* track current position in file for error reporting and seeking */
        off_t pos = offset + total_bytes;

        /* whether we should copy the source bytes to the destination */
        int need_copy = 0;

        /* determine number of bytes to read in this iteration */
        size_t left_to_read = bufsize;
        if (length > 0) {
            if (length - total_bytes < (off_t)bufsize) {
                left_to_read = (size_t)(length - total_bytes);
            }
        }

        /* read data from source file */
        ssize_t src_read = mfu_read(src_name, src_fd, (ssize_t*)src_buf, left_to_read);
        if (src_read < 0) {
            /* hit a read error */
            MFU_LOG(MFU_LOG_ERR, "Failed to read `%s' at offset %llx (errno=%d %s)",
              src_name, (unsigned long long)pos, errno, strerror(errno));
            rc = -1;
            break;
        }

        /* tally up number of bytes read */
        *count_bytes_read += (uint64_t) src_read;

        /* read data from destination file */
        ssize_t dst_read = mfu_read(dst_name, dst_fd, (ssize_t*)dest_buf, left_to_read);
        if (dst_read < 0) {
            /* hit a read error */
            MFU_LOG(MFU_LOG_ERR, "Failed to read `%s' at offset %llx (errno=%d %s)",
              dst_name, (unsigned long long)pos, errno, strerror(errno));
            rc = -1;
            break;
        }

        /* tally up number of bytes read */
        *count_bytes_read += (uint64_t) dst_read;

        /* TODO: could be a non-error short read, we could just adjust number
         * of bytes we compare and update offset to shorter of the two values
         * numread = min(src_read, dst_read) */

        /* check that we got the same number of bytes from each */
        if (src_read != dst_read) {
            /* one read came up shorter than the other */
            rc = 1;
            if (! overwrite) {
                break;
            }
            need_copy = 1;
        }

        /* check for EOF */
        if (src_read == 0) {
            /* hit end of source file */
            break;
        }

        /* if have same size buffers, and read some data, let's check the contents */
        if (src_read == dst_read) {
            if (memcmp((ssize_t*)src_buf, (ssize_t*)dest_buf, (size_t)src_read) != 0) {
                /* memory contents are different */
                rc = 1;
                if (! overwrite) {
                    break;
                }
                need_copy = 1;
            }
        }

        /* if the bytes are different,
         * then copy the bytes from the source into the destination */
        if (overwrite && need_copy == 1) {
            /* seek back to position to write to in destination file */
            if (mfu_lseek(dst_name, dst_fd, pos, SEEK_SET) == (off_t)-1) {
                /* log error if there is an lseek failure on the dst side */
                MFU_LOG(MFU_LOG_ERR, "Failed to lseek `%s', offset: %llx (errno=%d %s)",
                  dst_name, (unsigned long long)pos, strerror(errno));
                rc = -1;
                break;
            }

            /* write data to destination file */
            size_t bytes_to_write = (size_t) src_read;
            ssize_t bytes_written = mfu_write(dst_name, dst_fd, src_buf, bytes_to_write);
            if (bytes_written < 0) {
                /* hit a write error */
                MFU_LOG(MFU_LOG_ERR, "Failed to write `%s' at offset %llx (errno=%d %s)",
                  dst_name, (unsigned long long)pos, strerror(errno));
                rc = -1;
                break;
            }

            /* tally up number of bytes written */
            *count_bytes_written += (uint64_t) bytes_written;
        }

        /* add bytes to our total */
        total_bytes += (long unsigned int)src_read;

        /* update number of bytes read and written for progress messages */
        uint64_t count_bytes[2];
        count_bytes[0] = *count_bytes_read;
        count_bytes[1] = *count_bytes_written;
        mfu_progress_update(count_bytes, prg);
    }

    /* free buffers */
    mfu_free(&dest_buf);
    mfu_free(&src_buf);

    /* close files */
    mfu_close(dst_name, dst_fd);
    mfu_close(src_name, src_fd);

    return rc;
}

/* uses the lustre api to obtain stripe count and stripe size of a file */
int mfu_stripe_get(const char *path, uint64_t *stripe_size, uint64_t *stripe_count)
{
#ifdef LUSTRE_SUPPORT
#if defined(HAVE_LLAPI_LAYOUT)
    /* obtain the llapi_layout for a file by path */
    struct llapi_layout *layout = llapi_layout_get_by_path(path, 0);

    /* if no llapi_layout is returned, then some problem occured */
    if (layout == NULL) {
        return ENOENT;
    }

    /* obtain stripe count and stripe size */
    llapi_layout_stripe_size_get(layout, stripe_size);
    llapi_layout_stripe_count_get(layout, stripe_count);

    /* free the alloced llapi_layout */
    llapi_layout_free(layout);

    return 0;
#elif defined(HAVE_LLAPI_FILE_GET_STRIPE)
    int rc;
    int lumsz = lov_user_md_size(LOV_MAX_STRIPE_COUNT, LOV_USER_MAGIC_V3);
    struct lov_user_md *lum = malloc(lumsz);
    if (lum == NULL)
        return ENOMEM;

    rc = llapi_file_get_stripe(path, lum);
    if (rc) {
        free(lum);
        return rc;
    }

    *stripe_size = lum->lmm_stripe_size;
    *stripe_count = lum->lmm_stripe_count;
    free(lum);

    return 0;
#else
    fprintf(stderr, "Unexpected Lustre version.\n");
    fflush(stderr);
    MPI_Abort(MPI_COMM_WORLD, 1);
#endif
#endif

    return 0;
}

/* create a striped lustre file at the path provided with the specified stripe size and count */
void mfu_stripe_set(const char *path, uint64_t stripe_size, int stripe_count)
{
#ifdef LUSTRE_SUPPORT
#if defined(HAVE_LLAPI_LAYOUT)
    /* create a new llapi_layout for file creation */
    struct llapi_layout *layout = llapi_layout_alloc();
    int fd = -1;

    if (stripe_count == -1) {
        /* stripe count of -1 means use all availabe devices */
        llapi_layout_stripe_count_set(layout, LLAPI_LAYOUT_WIDE);
    } else if (stripe_count == 0) {
        /* stripe count of 0 means use the lustre filesystem default */
        llapi_layout_stripe_count_set(layout, LLAPI_LAYOUT_DEFAULT);
    } else {
        /* use the number of stripes specified*/
        llapi_layout_stripe_count_set(layout, stripe_count);
    }

    /* specify the stripe size of each stripe */
    llapi_layout_stripe_size_set(layout, stripe_size);

    /* create the file */
    fd = llapi_layout_file_create(path, 0, 0644, layout);
    if (fd < 0) {
        fprintf(stderr, "cannot create %s: %s\n", path, strerror(errno));
        fflush(stderr);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    close(fd);

    /* free our alloced llapi_layout */
    llapi_layout_free(layout);
#elif defined(HAVE_LLAPI_FILE_CREATE)
    int rc = llapi_file_create(path, stripe_size, 0, stripe_count, LOV_PATTERN_RAID0);
    if (rc < 0) {
        fprintf(stderr, "cannot create %s: %s\n", path, strerror(-rc));
        fflush(stderr);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
#else
    fprintf(stderr, "Unexpected Lustre version.\n");
    fflush(stderr);
    MPI_Abort(MPI_COMM_WORLD, 1);
#endif
#endif
}

/* executes a logical AND operation on flag on all procs on comm,
 * returns 1 if all true and 0 otherwise */
int mfu_alltrue(int flag, MPI_Comm comm)
{
    /* check that all processes wrote successfully */
    int alltrue;
    MPI_Allreduce(&flag, &alltrue, 1, MPI_INT, MPI_LAND, comm);
    return alltrue;
}
