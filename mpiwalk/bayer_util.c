#include "bayer.h"
#include "mpi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

/* print abort message and call MPI_Abort to kill run */
void bayer_abort(int rc, const char *fmt, ...)
{
  va_list argp;
  fprintf(stderr, "ABORT: rank X on HOST: ");
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");

  MPI_Abort(MPI_COMM_WORLD, 0);
}

/* if size > 0 allocates size bytes and returns pointer,
 * calls bayer_abort if malloc fails, returns NULL if size == 0 */
void* bayer_malloc(size_t size, const char* desc, const char* file, int line)
{
  /* only bother if size > 0 */
  if (size > 0) {
    /* try to allocate memory and check whether we succeeded */
    void* ptr = malloc(size);
    if (ptr == NULL) {
      /* allocate failed, abort */
      bayer_abort(1, "Failed to allocate %llu bytes for %s @ %s:%d",
        (unsigned long long) size, desc, file, line
      );
    }

    /* return the pointer */
    return ptr;
  }

  return NULL;
}

/* if str != NULL, call strdup and return pointer, calls bayer_abort if strdup fails */
char* bayer_strdup(const char* str, const char* desc, const char* file, int line)
{
  if (str != NULL) {
    /* TODO: check that str length is below some max? */
    char* ptr = strdup(str);
    if (ptr == NULL) {
      /* allocate failed, abort */
      bayer_abort(1, "Failed to allocate string for %s @ %s:%d",
        desc, file, line
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
