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
        *recv = (char*) bayer_malloc((size_t)len, "bcast recv str", __FILE__, __LINE__);

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
