/* defines utility functions like memory allocation
 * and error / abort routines */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_UTIL_H
#define MFU_UTIL_H

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
    MFU_LOG_FATAL   = 1,
    MFU_LOG_ERR     = 2,
    MFU_LOG_WARN    = 3,
    MFU_LOG_INFO    = 4,
    MFU_LOG_VERBOSE = 5,
    MFU_LOG_DBG     = 6
} mfu_loglevel;

extern int mfu_initialized;

/* set during mfu_init, used in MFU_LOG */
extern int mfu_rank;
extern FILE* mfu_debug_stream;
extern mfu_loglevel mfu_debug_level;

#define MFU_LOG(level, ...) do {  \
        if (mfu_initialized && level <= mfu_debug_level) { \
            char timestamp[256]; \
            time_t ltime = time(NULL); \
            struct tm *ttime = localtime(&ltime); \
            strftime(timestamp, sizeof(timestamp), \
                     "%Y-%m-%dT%H:%M:%S", ttime); \
            if(level == MFU_LOG_DBG) { \
                fprintf(mfu_debug_stream,"[%s] [%d] [%s:%d] ", \
                        timestamp, mfu_rank, \
                        __FILE__, __LINE__); \
            } else { \
                fprintf(mfu_debug_stream,"[%s] [%d] [%s:%d] ", \
                        timestamp, mfu_rank, \
                        __FILE__, __LINE__); \
            } \
            fprintf(mfu_debug_stream, __VA_ARGS__); \
            fprintf(mfu_debug_stream, "\n"); \
            fflush(mfu_debug_stream); \
        } \
    } while (0)

/* initialize mfu library,
 * reference counting allows for multiple init/finalize pairs */
int mfu_init(void);

/* finalize mfu library */
int mfu_finalize(void);

/* print abort message and call MPI_Abort to kill run */
#define MFU_ABORT(X, ...) mfu_abort(__FILE__, __LINE__, X, __VA_ARGS__)
void mfu_abort(
  const char* file,
  int line,
  int rc,
  const char *fmt,
  ...
);

/* if size > 0 allocates size bytes and returns pointer,
 * calls mfu_abort if malloc fails, returns NULL if size == 0 */
#define MFU_MALLOC(X) mfu_malloc(X, __FILE__, __LINE__)
void* mfu_malloc(
  size_t size,
  const char* file,
  int line
);

/* if size > 0, allocates size bytes aligned with specified alignment
 * and returns pointer, calls mfu_abort on failure,
 * returns NULL if size == 0 */
#define MFU_MEMALIGN(X, Y) mfu_memalign(X, Y, __FILE__, __LINE__)
void* mfu_memalign(
  size_t size,
  size_t alignment,
  const char* file,
  int line
);

/* if str != NULL, call strdup and return pointer, calls mfu_abort
 * if strdup fails */
#define MFU_STRDUP(X) mfu_strdup(X, __FILE__, __LINE__)
char* mfu_strdup(
  const char* str,
  const char* file,
  int line
);

/* broadcast string from root rank and allocate memory and return copy
 * in recv on all ranks including the root, caller must free recv str
 * with mfu_free */
void mfu_bcast_strdup(
  const char* send,
  char** recv,
  int root,
  MPI_Comm comm
);

/* allocate a formatted string */
#define MFU_STRDUPF(X, ...) mfu_strdupf(__FILE__, __LINE__, X, __VA_ARGS__);
char* mfu_strdupf(const char* file, int line, const char* format, ...);

/* caller passes in void** not void*, use void* to avoid excessive
 * compiler warnings, free memory if pointer is not NULL, set
 * pointer to NULL */
void mfu_free(void* p);

/* given a number of items, return value converted to returned units */
void mfu_format_count(uint64_t count, double* val, const char** units);

/* given a number of bytes, return value converted to returned units */
void mfu_format_bytes(uint64_t bytes, double* val, const char** units);

/* given a bandwidth in bytes/sec, return value converted to returned units */
void mfu_format_bw(double bw, double* val, const char** units);

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
int mfu_abtoull(const char* str, unsigned long long* val);

/* give a mode type, print permission bits in ls -l form,
 * e.g., -rwxr--r-- for a file of 755
 * buf must be at least 11 characters long */
void mfu_format_mode(mode_t mode, char* buf);

/* given address of pointer to buffer, pack value into buffer in
 * network order and advance pointer */
void mfu_pack_uint32(char** pptr, uint32_t value);

/* given address of pointer to buffer, unpack value into buffer in
 * host order and advance pointer */
void mfu_unpack_uint32(const char** pptr, uint32_t* value);

/* given address of pointer to buffer, pack value into buffer in
 * network order and advance pointer */
void mfu_pack_uint64(char** pptr, uint64_t value);

/* given address of pointer to buffer, unpack value into buffer in
 * host order and advance pointer */
void mfu_unpack_uint64(const char** pptr, uint64_t* value);

/* Bob Jenkins one-at-a-time hash: http://en.wikipedia.org/wiki/Jenkins_hash_function */
uint32_t mfu_hash_jenkins(const char* key, size_t len);

#endif /* MFU_UTIL_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
