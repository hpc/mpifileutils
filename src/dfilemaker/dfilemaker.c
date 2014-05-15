#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "mpi.h"

#include "bayer.h"

static void print_usage(void)
{
  printf("\n");
  printf("Usage: dfilemaker <#files> <size>\n");
  printf("\n");
  printf("Example: dfilemaker 1024 1MB\n");
  printf("\n");
  fflush(stdout);
  return;
}

int main(int argc, char* argv[])
{
  /* init our environment */
  MPI_Init(&argc, &argv);
  bayer_init();

  /* get our rank and the number of ranks in the job */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* process command line arguments */
  int usage = 0;

  /* check arguments */
  int files;
  unsigned long long size_ull;
  if (argc != 3) {
    /* should have exactly 3 arguments */
    usage = 1;
  } else {
    /* read number of files */
    files = atoi(argv[1]);

    /* read file size */
    const char* size_str = argv[2];
    if (bayer_abtoull(size_str, &size_ull) != BAYER_SUCCESS) {
      /* just have rank 0 print the error */
      if (rank == 0) {
        printf("Could not interpret argument as file size: %s\n", size_str);
        fflush(stdout);
      }

      usage = 1;
    }
  }

  /* print usage and exit */
  if (usage) {
    if (rank == 0) {
      print_usage();
    }
    bayer_finalize();
    MPI_Finalize();
    return 1;
  }

  /* assume we'll exit with success */
  int rc = 0;

  /* convert file size to size_t */
  size_t size = (size_t) size_ull;

  /* tell user what we're doing */
  if (rank == 0) {
    printf("Generating data: files = %d, size = %llu\n", files, (unsigned long long) size);
    fflush(stdout);
  }

  /* compute number of files per process */
  int myfiles = files / ranks;
  int remainder = files - ranks * myfiles;

  /* compute our offset into the set of files */
  int offset = rank * myfiles;
  if (remainder > 0) {
    if (rank < remainder) {
      /* if we don't evenly divide, give extras to lower ranks */
      myfiles++;
      offset = rank * myfiles;
    } else {
      offset = remainder * (myfiles+1) + (rank - remainder) * myfiles;
    }
  }

  /* allocate a buffer */
  size_t bufsize = 1024*1024;
  void* buf = BAYER_MALLOC(bufsize);

  /* loop over each file we need to create */
  int i;
  for (i = 0; i < myfiles; i++) {
    /* get index within file set */
    int idx = offset + i;

    /* fill buffer with some data (unique to each file) */
    memset(buf, idx+1, bufsize);

    /* create a file name */
    char file[256];
    sprintf(file, "file_%d.dat", idx);

    /* create the file and open it for writing */
    int fd = bayer_open(file, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd != -1) {
      /* we opened the file, now start writing */
      size_t written = 0;
      char* ptr = (char*) buf;
      while (written < (size_t) size) {
          /* determine amount to write */
          size_t left = bufsize;
          size_t remaining = size - written;
          if (remaining < bufsize) {
            left = remaining;
          }

          /* write data to file */
          ssize_t n = bayer_write(file, fd, ptr, left);
          if (n == -1) {
            printf("Failed to write to file: rank=%d file=%s errno=%d (%s)\n", rank, file, errno, strerror(errno));
            rc = 1;
            break;
          }

          /* update amount written */
          written += (size_t) n;
      }

      /* sync output to disk and close the file */
      bayer_fsync(file, fd);
      bayer_close(file, fd);
    } else {
      /* failed to open the file */
      printf("Failed to open file: rank=%d file=%s errno=%d (%s)\n", rank, file, errno, strerror(errno));
      rc = 1;
    }
  }

  /* free the buffer */
  bayer_free(&buf);

  /* shut down */
  bayer_finalize();
  MPI_Finalize();

  return rc;
}
