#include "bayer.h"
#include "mpi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

int bayer_initialized = 0;

/* set globals */
int bayer_rank = -1;

/* users may override these to change settings */
FILE* bayer_debug_stream = NULL;
bayer_loglevel bayer_debug_level = BAYER_LOG_ERR;

/* initialize bayer library,
 * reference counting allows for multiple init/finalize pairs */
int bayer_init()
{
  if (bayer_initialized == 0) {
    /* set globals */
    MPI_Comm_rank(MPI_COMM_WORLD, &bayer_rank);
    bayer_debug_stream = stdout;

    DTCMP_Init();
    bayer_initialized++;
  }
  return BAYER_SUCCESS;
}

/* finalize bayer library */
int bayer_finalize()
{
  if (bayer_initialized > 0) {
    DTCMP_Finalize();
    bayer_initialized--;
  }
  return BAYER_SUCCESS;
}

/* print abort message and call MPI_Abort to kill run */
void bayer_abort(int rc, const char *fmt, ...)
{
  va_list argp;
  fprintf(stderr, "ABORT: rank X on HOST: ");
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");

  MPI_Abort(MPI_COMM_WORLD, rc);
}

/* if size > 0 allocates size bytes and returns pointer,
 * calls bayer_abort if malloc fails, returns NULL if size == 0 */
void* bayer_malloc(size_t size, const char* file, int line)
{
  /* only bother if size > 0 */
  if (size > 0) {
    /* try to allocate memory and check whether we succeeded */
    void* ptr = malloc(size);
    if (ptr == NULL) {
      /* allocate failed, abort */
      bayer_abort(1, "Failed to allocate %llu bytes @ %s:%d",
        (unsigned long long) size, file, line
      );
    }

    /* return the pointer */
    return ptr;
  }

  return NULL;
}

/* if size > 0, allocates size bytes aligned with specified alignment
 * and returns pointer, calls bayer_abort on failure,
 * returns NULL if size == 0 */
void* bayer_memalign(size_t size, size_t alignment, const char* file, int line)
{
  /* only bother if size > 0 */
  if (size > 0) {
    /* try to allocate memory and check whether we succeeded */
    void* ptr;
    int rc = posix_memalign(&ptr, alignment, size);
    if (rc != 0) {
      /* allocate failed, abort */
      bayer_abort(1, "Failed to allocate %llu bytes posix_memalign rc=%d @ %s:%d",
        (unsigned long long) size, rc, file, line
      );
    }

    /* return the pointer */
    return ptr;
  }

  return NULL;
}

/* if str != NULL, call strdup and return pointer, calls bayer_abort if strdup fails */
char* bayer_strdup(const char* str, const char* file, int line)
{
  if (str != NULL) {
    /* TODO: check that str length is below some max? */
    char* ptr = strdup(str);
    if (ptr == NULL) {
      /* allocate failed, abort */
      bayer_abort(1, "Failed to allocate string @ %s:%d",
        file, line
      );
    }

    return ptr;
  }
  return NULL;
}

/* free memory if pointer is not NULL, set pointer to NULL */
void bayer_free(void* p)
{
  /* verify that we got a valid pointer to a pointer */
  if (p != NULL) {
    /* free memory if there is any */
    void* ptr = *(void**)p;
    if (ptr != NULL ) {
      free(ptr);
    }

    /* set caller's pointer to NULL */
    *(void**)p = NULL;
  }
}

void bayer_bcast_strdup(const char* send, char** recv, int root, MPI_Comm comm)
{
    /* get our rank in the communicator */
    int rank;
    MPI_Comm_rank(comm, &rank);

    /* check that caller gave us a pointer to a char pointer */
    if(recv == NULL) {
        bayer_abort(1, "Invalid recv pointer @ %s:%d", __FILE__, __LINE__);
    }

    /* First, broadcast length of string. */
    int len = 0;

    if(rank == root) {
        if(send != NULL) {
            len = (int)(strlen(send) + 1);

            /* TODO: check that strlen fits within an int */
        }
    }

    if(MPI_SUCCESS != MPI_Bcast(&len, 1, MPI_INT, root, comm)) {
        bayer_abort(1, "Failed to broadcast length of string @ %s:%d",
            __FILE__, __LINE__
        );
    }

    /* If the string is non-zero bytes, allocate space and bcast it. */
    if(len > 0) {
        /* allocate space to receive string */
        *recv = (char*) BAYER_MALLOC((size_t)len);

        /* Broadcast the string. */
        if(rank == root) {
            strncpy(*recv, send, len);
        }

        if(MPI_SUCCESS != MPI_Bcast(*recv, len, MPI_CHAR, root, comm)) {
            bayer_abort(1, "Failed to bcast string @ %s:%d",
                __FILE__, __LINE__
            );
        }

    }
    else {
        /* Root passed in a NULL value, so set the output to NULL. */
        *recv = NULL;
    }

    return;
}

static void bayer_format_1024(
  double input,
  const char** units_list,
  int units_len,
  double* val,
  const char* units[])
{
    /* divide input by 1024 until it falls to less than 1024,
     * increment units each time */
    int idx = 0;
    while(input / 1024.0 > 1.0) {
        input /= 1024.0;
        idx++;
        if(idx == (units_len - 1)) {
            /* we've gone as high as we can go */
            break;
        }
    }

    /* set output paramaters */
    *val   = input;
    *units = units_list[idx];

    return;
}
/* given a number of bytes, return value converted to returned units */
#define NUM_UNITS_BYTES (7)
static const char* units_bytes[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
void bayer_format_bytes(uint64_t bytes, double* val, const char** units)
{
    double bytes_d = (double) bytes;
    bayer_format_1024(bytes_d, units_bytes, NUM_UNITS_BYTES, val, units);
    return;
}

#define NUM_UNITS_BW (6)
static const char* units_bw[] = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s", "PB/s"};
void bayer_format_bw(double bw, double* val, const char** units)
{
    bayer_format_1024(bw, units_bw, NUM_UNITS_BW, val, units);
    return;
}

size_t bayer_pack_uint64(void* buf, uint64_t val)
{
    uint64_t* ptr = (uint64_t*) buf;
    *ptr = val;
    return 8;
}

size_t bayer_unpack_uint64(void* buf, uint64_t* val)
{
    uint64_t* ptr = (uint64_t*) buf;
    *val = *ptr;
    return 8;
}

/* pack all fields as 64-bit values, except for times which we
 * pack as two 64-bit values */
size_t bayer_stat_pack_size()
{
    size_t size = 16 * 8;
    return size;
}

size_t bayer_stat_pack(void* buf, const struct stat* s)
{
    char* ptr = (char*) buf;

    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_dev);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_ino);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_mode);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_nlink);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_uid);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_gid);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_rdev);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_size);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_blksize);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_blocks);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_atime);
    ptr += bayer_pack_uint64(ptr, (uint64_t) 0);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_mtime);
    ptr += bayer_pack_uint64(ptr, (uint64_t) 0);
    ptr += bayer_pack_uint64(ptr, (uint64_t) s->st_ctime);
    ptr += bayer_pack_uint64(ptr, (uint64_t) 0);

    size_t bytes = (ptr - (char*)buf);
    return bytes;
}

size_t bayer_stat_unpack(const void* buf, struct stat* s)
{
    uint64_t val;
    char* ptr = (char*) buf;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_dev = (dev_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_ino = (ino_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_mode = (mode_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_nlink = (nlink_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_uid = (uid_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_gid = (gid_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_rdev = (dev_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_size = (off_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_blksize = (blksize_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_blocks = (blkcnt_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_atime = (time_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    /* atime nsecs */

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_mtime = (time_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    /* mtime nsecs */

    ptr += bayer_unpack_uint64(ptr, &val);
    s->st_ctime = (time_t) val;

    ptr += bayer_unpack_uint64(ptr, &val);
    /* ctime nsecs */

    size_t bytes = (ptr - (char*)buf);
    return bytes;
}

static void bayer_stat_bcast(struct stat* s)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* allocate a buffer to pack stat info */
    size_t bufsize = bayer_stat_pack_size();
    void* buf = BAYER_MALLOC(bufsize);

    /* pack stat structure on root */
    if (rank == 0) {
        bayer_stat_pack(buf, s);
    }

    /* broadcast stat from rank 0 */
    MPI_Bcast(buf, (int)bufsize, MPI_BYTE, 0, MPI_COMM_WORLD);

    /* unpack stat structure into stat data structure */
    if (rank != 0) {
        bayer_stat_unpack(buf, s);
    }

    /* free buffer */
    bayer_free(&buf);

    return;
}

static void bayer_str_bcast(char** pstr)
{
    if (pstr == NULL) {
        bayer_abort(1, "Invalid string");
    }

    /* get pointer to string */
    char* str = *pstr;

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* if we aren't the root, assume we won't get anything */
    if (rank != 0) {
        *pstr = NULL;
    }

    /* compute size of buffer needed to hold string */
    uint64_t size;
    if (rank == 0) {
        size_t len = 0;
        if (str != NULL) {
            len = strlen(str) + 1;
        }
        size = (uint64_t) len;
    }

    /* broadcast size of string */
    MPI_Bcast(&size, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* receive string */
    size_t bufsize = (size_t) size;
    if (bufsize > 0) {
        /* allocate buffer to hold string */
        char* buf = str;
        if (rank != 0) {
            buf = (char*) BAYER_MALLOC(bufsize);
        }

        /* broadcast bytes of string */
        int chars = (int) bufsize;
        MPI_Bcast(buf, chars, MPI_CHAR, 0, MPI_COMM_WORLD);

        /* set caller's pointer to newly allocated string */
        if (rank != 0) {
            *pstr = buf;
        }
    }

    return;
}

/* initialize fields in param */
static void bayer_param_path_init(bayer_param_path* param)
{
    /* initialize all fields */
    if(param != NULL) {
        param->orig = NULL;
        param->path = NULL;
        param->path_stat_valid = 0;
        param->target = NULL;
        param->target_stat_valid = 0;
    }

    return;
}

static void bayer_param_path_bcast(bayer_param_path* param)
{
    bayer_str_bcast(&param->orig);

    bayer_str_bcast(&param->path);
    MPI_Bcast(&param->path_stat_valid, 1, MPI_INT, 0, MPI_COMM_WORLD);
    bayer_stat_bcast(&param->path_stat);

    bayer_str_bcast(&param->target);
    MPI_Bcast(&param->target_stat_valid, 1, MPI_INT, 0, MPI_COMM_WORLD);
    bayer_stat_bcast(&param->target_stat);
}

/* set fields in param according to path */
void bayer_param_path_set(const char* path, bayer_param_path* param)
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* initialize all fields */
    bayer_param_path_init(param);

    /* have rank 0 do the real work */
    if (rank == 0) {
        if(path != NULL) {
            /* make a copy of original path */
            param->orig = BAYER_STRDUP(path);

            /* get absolute path and remove ".", "..", consecutive "/",
             * and trailing "/" characters */
            param->path = bayer_path_strdup_abs_reduce_str(path);

            /* get stat info for simplified path */
            if(bayer_lstat(param->path, &param->path_stat) == 0) {
                param->path_stat_valid = 1;
            }

            /* TODO: we use realpath below, which is nice since it takes out
             * ".", "..", symlinks, and adds the absolute path, however, it
             * fails if the file/directory does not already exist, which is
             * often the case for dest path. */

            /* resolve any symlinks */
            char target[PATH_MAX];
            if(realpath(path, target) != NULL) {
                /* make a copy of resolved name */
                param->target = BAYER_STRDUP(target);

                /* get stat info for resolved path */
                if(bayer_lstat(param->target, &param->target_stat) == 0) {
                    param->target_stat_valid = 1;
                }
            }
        }
    }

    /* broadcast findings to all other tasks */
    bayer_param_path_bcast(param);

    return;
}

/* free memory associated with param */
void bayer_param_path_free(bayer_param_path* param)
{
    if(param != NULL) {
        /* free all mememory */
        bayer_free(&param->orig);
        bayer_free(&param->path);
        bayer_free(&param->target);

        /* initialize all fields */
        bayer_param_path_init(param);
    }
    return;
}
