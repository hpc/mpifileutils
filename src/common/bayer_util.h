/* defines utility functions like memory allocation
 * and error / abort routines */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef BAYER_UTIL_H
#define BAYER_UTIL_H

#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include "mpi.h"

#include <stdio.h>
#include <time.h>

/* for struct stat */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

typedef enum {
    BAYER_LOG_FATAL = 1,
    BAYER_LOG_ERR   = 2,
    BAYER_LOG_WARN  = 3,
    BAYER_LOG_INFO  = 4,
    BAYER_LOG_DBG   = 5
} bayer_loglevel;

extern int bayer_initialized;

/* set during bayer_init, used in BAYER_LOG */
extern int bayer_rank;
extern FILE* bayer_debug_stream;
extern bayer_loglevel bayer_debug_level;

#define BAYER_LOG(level, ...) do {  \
        if (bayer_initialized && level <= bayer_debug_level) { \
            char timestamp[256]; \
            time_t ltime = time(NULL); \
            struct tm *ttime = localtime(&ltime); \
            strftime(timestamp, sizeof(timestamp), \
                     "%Y-%m-%dT%H:%M:%S", ttime); \
            if(level == BAYER_LOG_DBG) { \
                fprintf(bayer_debug_stream,"[%s] [%d] [%s:%d] ", \
                        timestamp, bayer_rank, \
                        __FILE__, __LINE__); \
            } else { \
                fprintf(bayer_debug_stream,"[%s] [%d] [%s:%d] ", \
                        timestamp, bayer_rank, \
                        __FILE__, __LINE__); \
            } \
            fprintf(bayer_debug_stream, __VA_ARGS__); \
            fprintf(bayer_debug_stream, "\n"); \
            fflush(bayer_debug_stream); \
        } \
    } while (0)

/* initialize bayer library,
 * reference counting allows for multiple init/finalize pairs */
int bayer_init(void);

/* finalize bayer library */
int bayer_finalize(void);

/* print abort message and call MPI_Abort to kill run */
#define BAYER_ABORT(X, ...) bayer_abort(__FILE__, __LINE__, X, __VA_ARGS__)
void bayer_abort(
  const char* file,
  int line,
  int rc,
  const char *fmt,
  ...
);

/* if size > 0 allocates size bytes and returns pointer,
 * calls bayer_abort if malloc fails, returns NULL if size == 0 */
#define BAYER_MALLOC(X) bayer_malloc(X, __FILE__, __LINE__)
void* bayer_malloc(
  size_t size,
  const char* file,
  int line
);

/* if size > 0, allocates size bytes aligned with specified alignment
 * and returns pointer, calls bayer_abort on failure,
 * returns NULL if size == 0 */
#define BAYER_MEMALIGN(X, Y) bayer_memalign(X, Y, __FILE__, __LINE__)
void* bayer_memalign(
  size_t size,
  size_t alignment,
  const char* file,
  int line
);

/* if str != NULL, call strdup and return pointer, calls bayer_abort
 * if strdup fails */
#define BAYER_STRDUP(X) bayer_strdup(X, __FILE__, __LINE__)
char* bayer_strdup(
  const char* str,
  const char* file,
  int line
);

/* broadcast string from root rank and allocate copy in recv on all
 * ranks including the root, caller must free recv str with bayer_recv */
void bayer_bcast_strdup(
  const char* send,
  char** recv,
  int root,
  MPI_Comm comm
);

/* caller passes in void** not void*, use void* to avoid excessive
 * compiler warnings, free memory if pointer is not NULL, set
 * pointer to NULL */
void bayer_free(void* p);

/* given a number of bytes, return value converted to returned units */
void bayer_format_bytes(uint64_t bytes, double* val, const char** units);

/* given a bandwidth in bytes/sec, return value converted to returned units */
void bayer_format_bw(double bw, double* val, const char** units);

/* given address of pointer to buffer, pack value into buffer in
 * network order and advance pointer */
void bayer_pack_uint32(char** pptr, uint32_t value);

/* given address of pointer to buffer, unpack value into buffer in
 * host order and advance pointer */
void bayer_unpack_uint32(const char** pptr, uint32_t* value);

/* given address of pointer to buffer, pack value into buffer in
 * network order and advance pointer */
void bayer_pack_uint64(char** pptr, uint64_t value);

/* given address of pointer to buffer, unpack value into buffer in
 * host order and advance pointer */
void bayer_unpack_uint64(const char** pptr, uint64_t* value);

#endif /* BAYER_UTIL_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
