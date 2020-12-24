#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "mpi.h"

#include "mfu.h"

static void print_usage(void)
{
  printf("\n");
  printf("Usage: dfilemaker1 <#files> <size>\n");
  printf("\n");
  printf("Example: dfilemaker1 1024 1MB\n");
  printf("\n");
  fflush(stdout);
  return;
}

int main(int argc, char* argv[])
{
  /* init our environment */
  MPI_Init(&argc, &argv);
  mfu_init();

  /* get our rank and the number of ranks in the job */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* verbose by default */
  mfu_debug_level = MFU_LOG_VERBOSE;

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
    if (mfu_abtoull(size_str, &size_ull) != MFU_SUCCESS) {
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
    mfu_finalize();
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

  mfu_flist flist = mfu_flist_new();
  mfu_flist_set_detail(flist, 1);

  int files_per_dir = 100;

  /* loop over each file we need to create */
  int i;
  for (i = 0; i < myfiles; i++) {
    /* get index within file set */
    int idx = offset + i;

#if 0
    int diridx = idx / files_per_dir;

    /* define directory name */
    char dirname[256];
    sprintf(dirname, "dir_%d", diridx);

    if (idx % files_per_dir == 0) {
        uint64_t fidx = mfu_flist_file_create(flist);
        mfu_flist_file_set_name(flist, fidx, dirname);
        mfu_flist_file_set_type(flist, fidx, MFU_TYPE_DIR);
        mfu_flist_file_set_mode(flist, fidx, DCOPY_DEF_PERMS_DIR | S_IFDIR);
    }

    /* create a file name */
    char file[256];
    sprintf(file, "%s/file_%d.dat", dirname, idx);
#else
    /* create a file name */
    char file[256];
    sprintf(file, "file_%d.dat", idx);
#endif

    uint64_t fidx = mfu_flist_file_create(flist);
    mfu_flist_file_set_name(flist, fidx, file);
    mfu_flist_file_set_type(flist, fidx, MFU_TYPE_FILE);
    mfu_flist_file_set_size(flist, fidx, size);
    mfu_flist_file_set_mode(flist, fidx, DCOPY_DEF_PERMS_FILE | S_IFREG);
  }

  mfu_flist_summarize(flist);

  /* pointer to mfu_copy opts */
  mfu_copy_opts_t* mfu_copy_opts = mfu_copy_opts_new();

  /* create new mfu_file objects */
  mfu_file_t* mfu_file = mfu_file_new();

  mfu_create_opts_t* create_opts = mfu_create_opts_new();
  mfu_flist_mkdir(flist, create_opts);
  mfu_flist_mknod(flist, create_opts);
  mfu_flist_fill(flist, mfu_copy_opts, mfu_file);
  //mfu_flist_setmeta()

  mfu_create_opts_delete(&create_opts);

  mfu_flist_free(&flist);

  /* free the copy options */
  mfu_copy_opts_delete(&mfu_copy_opts);

  /* delete file objects */
  mfu_file_delete(&mfu_file);

  /* shut down */
  mfu_finalize();
  MPI_Finalize();

  return rc;
}
