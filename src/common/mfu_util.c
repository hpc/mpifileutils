/* to get nsec fields in stat structure */
#define _GNU_SOURCE

#include "mfu.h"
#include "mpi.h"
#include "dtcmp.h"
#include "mfu_errors.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <limits.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <sys/vfs.h>

#ifndef ULLONG_MAX
#define ULLONG_MAX (__LONG_LONG_MAX__ * 2UL + 1UL)
#endif

#ifdef LUSTRE_SUPPORT
#include <lustre/lustreapi.h>
#include <lustre/lustre_user.h>
#endif

int mfu_initialized = 0;

/* set globals */
int mfu_rank = -1;

/* users may override these to change settings */
FILE* mfu_debug_stream = NULL;
mfu_loglevel mfu_debug_level = MFU_LOG_ERR;

/* default progress message timeout in seconds */
int mfu_progress_timeout = 10;

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

/* if size > 0 allocates size bytes and returns pointer,
 * calls mfu_abort if calloc fails, returns NULL if size == 0 */
void* mfu_calloc(size_t nelem, size_t elsize, const char* file, int line)
{
    /* only bother if size > 0 */
    if (nelem > 0 && elsize > 0) {
        /* try to allocate memory and check whether we succeeded */
        void* ptr = calloc(nelem, elsize);
        if (ptr == NULL) {
            /* allocate failed, abort */
            mfu_abort(file, line, 1, "Failed to allocate %llu * %llu bytes. Try using more nodes.",
                        (unsigned long long) nelem, (unsigned long long) elsize
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
static const char* units_count[] = {"", "k", "M", "G", "T", "P", "E"};
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
static const char* units_bytes[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};
void mfu_format_bytes(uint64_t bytes, double* val, const char** units)
{
    double bytes_d = (double) bytes;
    mfu_format_1024(bytes_d, units_bytes, NUM_UNITS_BYTES, val, units);
    return;
}

#define NUM_UNITS_BW (7)
static const char* units_bw[] = {"B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s", "PiB/s", "EiB/s"};
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
    const char* src_name,          /* IN  - path name to source file */
    const char* dst_name,          /* IN  - path name to destination file */
    off_t offset,                  /* IN  - offset with file to start comparison */
    off_t length,                  /* IN  - number of bytes to be compared */
    off_t file_size,               /* IN  - size of file */
    int overwrite,                 /* IN  - whether to replace dest with source contents (1) or not (0) */
    mfu_copy_opts_t* copy_opts,    /* IN  - options for data compare/copy step */
    uint64_t* count_bytes_read,    /* OUT - number of bytes read (src + dest) */
    uint64_t* count_bytes_written, /* OUT - number of bytes written to dest */
    mfu_progress* prg,             /* IN  - progress message structure */
    mfu_file_t* mfu_src_file,      /* IN  - I/O filesystem functions to use for source */
    mfu_file_t* mfu_dst_file)      /* IN  - I/O filesystem functions to use for destination */
{
    /* extract values from copy options */
    int direct = copy_opts->direct;
    size_t buf_size = copy_opts->buf_size;

    /* for O_DIRECT, check that length is a multiple of buf_size */
    if (direct &&                      /* using O_DIRECT */
        offset + length < file_size && /* not at end of file */
        length % buf_size != 0)        /* length not an integer multiple of block size */
    {
        MFU_ABORT(-1, "O_DIRECT requires chunk size to be integer multiple of block size %llu",
            buf_size);
    }

    /* open source as read only, with optional O_DIRECT */
    int src_flags = O_RDONLY;
    if (direct) {
        src_flags |= O_DIRECT;
    }

    /* open source file */
    int src_rc = mfu_file_open(src_name, src_flags, mfu_src_file);
    if (src_rc != 0) {
        /* log error if there is an open failure on the src side */
        MFU_LOG(MFU_LOG_ERR, "Failed to open source file `%s' (errno=%d %s)",
                src_name, errno, strerror(errno));
        return -1;
    }

    /* avoid opening file in write mode if we're only reading,
     * optionally enable O_DIRECT */
    int dst_flags = O_RDONLY;
    if (overwrite) {
        dst_flags = O_RDWR;
    }
    if (direct) {
        dst_flags |= O_DIRECT;
    }

    /* open destination file */
    int dst_rc = mfu_file_open(dst_name, dst_flags, mfu_dst_file);
    if (dst_rc != 0) {
        /* log error if there is an open failure on the dst side */
        MFU_LOG(MFU_LOG_ERR, "Failed to open destination file `%s' (errno=%d %s)",
                dst_name, errno, strerror(errno));
        mfu_file_close(src_name, mfu_src_file);
        return -1;
    }

    /* hint that we'll read from file sequentially */
    if (mfu_src_file->type != DAOS && mfu_src_file->type != DFS) {
        posix_fadvise(mfu_src_file->fd, offset, length, POSIX_FADV_SEQUENTIAL);
        posix_fadvise(mfu_src_file->fd, offset, length, POSIX_FADV_SEQUENTIAL);
    }

    /* assume we'll find that file contents are the same */
    int rc = 0;

    /* TODO: replace this with mfu_copy_opts->buf */
    /* allocate buffer to write files, aligned on 1MB boundaraies */
    size_t alignment = 1024*1024;
    void* src_buf = (char*) MFU_MEMALIGN(buf_size, alignment);
    void* dst_buf = (char*) MFU_MEMALIGN(buf_size, alignment);

    /* initialize our starting offset within the file */
    off_t off = offset;

    /* if we write with O_DIRECT, we may need to truncate file */
    int need_truncate = 0;

    /* read and compare data from files */
    off_t total_bytes = 0;
    while (total_bytes < length) {
        /* whether we should copy the source bytes to the destination */
        int need_copy = 0;

        /* determine number of bytes to read in this iteration */
        size_t left_to_read = buf_size;
        if (! direct) {
            off_t remainder = length - total_bytes;
            if (remainder < (off_t)buf_size) {
                left_to_read = (size_t) remainder;
            }
        }

        /* read data from source file */
        ssize_t src_read = mfu_file_pread(src_name, (ssize_t*)src_buf, left_to_read, off, mfu_src_file);

        /* If we're using O_DIRECT, deal with short reads.
         * Retry with same buffer and offset since those must
         * be aligned at block boundaries. */
        while (direct &&                     /* using O_DIRECT */
               src_read > 0 &&               /* read was not an error or eof */
               src_read < left_to_read &&    /* shorter than requested */
               (off + src_read) < file_size) /* not at end of file */
        {
            /* TODO: probably should retry a limited number of times then abort */
            src_read = mfu_file_pread(src_name, src_buf, left_to_read, off, mfu_src_file);
        }

        /* check for read error */
        if (src_read < 0) {
            /* hit a read error */
            MFU_LOG(MFU_LOG_ERR, "Failed to read `%s' at offset %llx (errno=%d %s)",
                src_name, (unsigned long long)off, errno, strerror(errno));
            rc = -1;
            break;
        }

        /* check for early EOF */
        if (src_read == 0) {
            /* if the source is shorter than expected, consider this to be an error */
            MFU_LOG(MFU_LOG_ERR, "Source `%s' is shorter %llx than expected (errno=%d %s)",
                src_name, (unsigned long long)off, errno, strerror(errno));
            rc = -1;
            break;
        }

        /* tally up number of bytes read */
        *count_bytes_read += (uint64_t) src_read;

        /* read data from destination file */
        ssize_t dst_read = mfu_file_pread(dst_name, (ssize_t*)dst_buf, left_to_read, off, mfu_dst_file);

        /* If we're using O_DIRECT, deal with short reads.
         * Retry with same buffer and offset since those must
         * be aligned at block boundaries. */
        while (direct &&                     /* using O_DIRECT */
               dst_read > 0 &&               /* read was not an error or eof */
               dst_read < left_to_read &&    /* shorter than requested */
               (off + dst_read) < file_size) /* not at end of file */
        {
            /* TODO: probably should retry a limited number of times then abort */
            dst_read = mfu_file_pread(dst_name, dst_buf, left_to_read, off, mfu_dst_file);
        }

        /* check for read error */
        if (dst_read < 0) {
            /* hit a read error */
            MFU_LOG(MFU_LOG_ERR, "Failed to read `%s' at offset %llx (errno=%d %s)",
                dst_name, (unsigned long long)off, errno, strerror(errno));
            rc = -1;
            break;
        }

        /* check for early EOF */
        if (dst_read == 0) {
            /* destination is shorter than expected, consider this to be an error */
            MFU_LOG(MFU_LOG_ERR, "Destination `%s' is shorter than expected %llx (errno=%d %s)",
                dst_name, (unsigned long long)off, errno, strerror(errno));
            rc = -1;
            break;
        }

        /* tally up number of bytes read */
        *count_bytes_read += (uint64_t) dst_read;

        /* we could have a non-error short read, so adjust number
         * of bytes we compare and update offset to shorter of the two values
         * numread = min(src_read, dst_read) */
        ssize_t min_read = src_read;
        if (dst_read < min_read) {
            min_read = dst_read;
        }

        /* if have same size buffers, and read some data, let's check the contents */
        if (memcmp((ssize_t*)src_buf, (ssize_t*)dst_buf, (size_t)min_read) != 0) {
            /* memory contents are different */
            rc = 1;
            if (! overwrite) {
                break;
            }
            need_copy = 1;
        }

        /* if the bytes are different,
         * then copy the bytes from the source into the destination */
        if (overwrite && need_copy) {
            /* compute number of bytes to write */
            size_t bytes_to_write = (size_t) min_read;
            if (direct) {
                /* O_DIRECT requires particular write sizes,
                 * ok to write beyond end of file so long as
                 * we truncate in cleanup step */
                size_t remainder = buf_size - (size_t) min_read;
                if (remainder > 0) {
                    /* zero out the end of the buffer for security,
                     * don't want to leave data from another file at end of
                     * current file if we fail before truncating */
                    char* bufzero = ((char*)src_buf + min_read);
                    memset(bufzero, 0, remainder);

                    /* remember that we might need to truncate,
                     * because we may write past the end of the file */
                    need_truncate = 1;
                }

                /* assumes buf_size is magic size for O_DIRECT */
                bytes_to_write = buf_size;
            }

            /* we loop to account for short writes */
            ssize_t n = 0;
            while (n < bytes_to_write) {
                /* write data to destination file */
                ssize_t bytes_written = mfu_file_pwrite(dst_name, ((char*)src_buf) + n, bytes_to_write - n, off + n,
                                                   mfu_dst_file);

                /* check for write error */
                if (bytes_written < 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to write `%s' at offset %llx (errno=%d %s)", 
                        dst_name, (unsigned long long)off + n, strerror(errno));
                    rc = -1;
                    break;
                }

                /* So long as we're not using O_DIRECT, we can handle short writes
                 * by advancing by the number of bytes written.  For O_DIRECT, we
                 * need to keep buffer, file offset, and amount to write aligned
                 * on block boundaries, so just retry the entire operation. */
                if (!direct || bytes_written == bytes_to_write) {
                    /* advance index by number of bytes written */
                    n += bytes_written;

                    /* tally up number of bytes written */
                    *count_bytes_written += (uint64_t) bytes_written;
                }
            }
        }

        /* add bytes to our total */
        off += min_read;
        total_bytes += (long unsigned int)min_read;

        /* update number of bytes read and written for progress messages */
        uint64_t count_bytes[2];
        count_bytes[0] = *count_bytes_read;
        count_bytes[1] = *count_bytes_written;
        mfu_progress_update(count_bytes, prg);
    }

    /* truncate destination file if we might have written past the end */
    if (need_truncate) {
        off_t last_written = offset + length;
        off_t file_size_offt = (off_t) file_size;
        if (last_written >= file_size_offt) {
            /* Use ftruncate() here rather than truncate(), because grouplock
             * of Lustre would cause block to truncate() since the fd is different
             * from the out_fd. */
            if (mfu_file_ftruncate(mfu_dst_file, file_size_offt) < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to truncate destination file: %s (errno=%d %s)",
                    dst_name, errno, strerror(errno));
                rc = -1;
            }
        }
    }

    /* free buffers */
    mfu_free(&src_buf);
    mfu_free(&dst_buf);

    /* close files */
    mfu_file_close(dst_name, mfu_dst_file);
    mfu_file_close(src_name, mfu_src_file);

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

/* given a path, return true if on Lustre, false otherwise */
bool mfu_is_lustre(const char* path)
{
    bool is_lustre = false;

#ifdef LUSTRE_SUPPORT
    /* call statfs on the path and check the file system magic value */
    struct statfs buf;
    int rc = statfs(path, &buf);
    if (rc == 0) {
        if (buf.f_type == LL_SUPER_MAGIC) {
            is_lustre = true;
        }
    } else {
        MFU_LOG(MFU_LOG_ERR, "Failed to statfs path: `%s' (errno=%d %s)",
            path, errno, strerror(errno)
        );
    }
#endif /* LUSTRE_SUPPORT */

    return is_lustre;
}

/* executes a logical AND operation on flag on all procs on comm,
 * returns true if all true and false otherwise */
bool mfu_alltrue(bool flag, MPI_Comm comm)
{
    /* check that all processes wrote successfully */
    bool alltrue;
    MPI_Allreduce(&flag, &alltrue, 1, MPI_C_BOOL, MPI_LAND, comm);
    return alltrue;
}

/* given the rank of the calling process, the number of ranks,
 * and the number of items, compute starting offset and count
 * for the calling rank so as to evenly spread items across ranks */
void mfu_get_start_count(
    int rank,            /* rank of calling process */
    int ranks,           /* number of ranks */
    uint64_t num,        /* number of items */
    uint64_t* out_start, /* starting offset for calling rank */
    uint64_t* out_count) /* number of items for calling rank */
{
    /* divide items among ranks */
    uint64_t num_per_rank = num / ranks;
    uint64_t remainder = num - num_per_rank * ranks;

    /* compute starting entry and number of entries based on our rank */
    uint64_t start = 0;
    uint64_t count = 0;
    if (rank < remainder) {
        /* if we have a remainder, we given each rank from [0,remainder)
         * one extra item */
        count = num_per_rank + 1;
        start = rank * count;
    } else {
        /* all ranks from [remainder, ranks) are assigned num_per_rank items */
        count = num_per_rank;
        start = remainder * (count + 1) + (rank - remainder) * count;
    }

    /* restart starting offset and number of items to caller */
    *out_start = start;
    *out_count = count;

    return;
}
