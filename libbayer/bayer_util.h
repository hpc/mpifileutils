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

/* print abort message and call MPI_Abort to kill run */
void bayer_abort(
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
void* bayer_memalign(
  size_t size,
  size_t alignment,
  const char* desc,
  const char* file,
  int line
);

/* if str != NULL, call strdup and return pointer, calls bayer_abort
 * if strdup fails */
char* bayer_strdup(
  const char* str,
  const char* desc,
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

#endif /* BAYER_UTIL_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
