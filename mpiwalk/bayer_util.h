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

/* print abort message and call MPI_Abort to kill run */
void bayer_abort(
  int rc,
  const char *fmt,
  ...
);

/* if size > 0 allocates size bytes and returns pointer,
 * calls bayer_abort if malloc fails, returns NULL if size == 0 */
void* bayer_malloc(
  size_t size,
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

/* caller passes in void** not void*, use void* to avoid excessive
 * compiler warnings, free memory if pointer is not NULL, set
 * pointer to NULL */
void bayer_free(void* p);

#endif /* BAYER_UTIL_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
