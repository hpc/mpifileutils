// mpicc -g -O0 -o file_bcast_par13 file_bcast_par13.c bitonic_rank_str.c -I./libgcs.git/include ./libgcs.git/lib/libgcs.a

#define _GNU_SOURCE /* for O_DIRECT */

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <string.h>
#include <getopt.h>

#include <sys/vfs.h>

// mmap and friends for shared memory
#include <sys/mman.h>

//#include "gcs.h"
#include "mfu.h"
#include "strmap.h"
#include "dtcmp.h"

/* Run with two or more procs per node.  The task of one proc on each
 * node is to write segments to the file on the SSD.  The remaining
 * procs on the node read data from Lustre, send data to the writer,
 * and send data to each other.  The writer flow controls these worker
 * processes with messages. */

#define GCS_SUCCESS (0)

static int gcs_shm_file_key = MPI_KEYVAL_INVALID;

static strmap* gcs_shm_ptr_tree = NULL;

/* split input communicator into subcommunicators using string as a color,
 * str cannot be NULL */
static void comm_split_str(MPI_Comm comm, const char* str, MPI_Comm* newcomm)
{
    /* use DTCMP to rank strings */
    uint64_t groups;
    uint64_t group_id;
    uint64_t group_ranks;
    uint64_t group_rank;
    DTCMP_Rank_strings(1, &str, &groups, &group_id, &group_ranks, &group_rank, DTCMP_FLAG_NONE, comm);

    /* use our group id as our color value*/
    int color = (int) group_id;

    /* split the communicator */
    MPI_Comm_split(comm, color, 0, newcomm);

    return;
}

/* determine whether any value is true */
static int gcs_anytrue(int value, MPI_Comm comm)
{
  int global_or_of_value;
  MPI_Allreduce(&value, &global_or_of_value, 1, MPI_INT, MPI_LOR, comm);
  return global_or_of_value;
}

static char* gcs_build_shmem_file(MPI_Comm comm)
{
#if 0
  /* first check that all procs in comm can really use shared memory,
   * we make this check by splitting comm by hostname and then checking
   * that the newly created communicator is the same size */
  int size, size_tmp;
  MPI_Comm comm_tmp;
  GCS_Comm_hostname(comm, &comm_tmp);
  MPI_Comm_size(comm,     &size);
  MPI_Comm_size(comm_tmp, &size_tmp);
  MPI_Comm_free(&comm_tmp);
  if (size != size_tmp) {
    return NULL;
  }
#endif

  /* get our rank in the current communicator */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* have rank 0 read our jobid from the environment */
  int jobid_len = 0;
  char* jobid = NULL;
  if (rank == 0) {
#if 0
    char* value = NULL;
    if ((value = getenv("SLURM_JOBID")) != NULL) {
      /* we running within a SLURM allocation,
       * so we can use the jobid as a unique string */
      char* value2;
      if ((value2 = getenv("SLURM_STEPID")) != NULL) {
        /* the step id is defined, so let's append it */
        jobid_len = strlen(value) + strlen(value2) + 2;
        jobid = (char*) malloc(jobid_len);
        if (jobid != NULL) {
          snprintf(jobid, jobid_len, "%s.%s", value, value2);
        }
      } else {
        /* no step id, let's just go with the SLURM allocation id */
        jobid_len = strlen(value) + 1;
        jobid = strdup(value);
      }
    } else {
#endif
      /* we couldn't find a unique string for the job,
       * so let's use the pid of rank 0 instead */
      pid_t pid = getpid();
      jobid_len = snprintf(NULL, 0, "%d", pid);
      if (jobid_len > 0) {
        jobid_len++;
        jobid = (char*) malloc(jobid_len);
        if (jobid != NULL) {
          snprintf(jobid, jobid_len, "%d", pid);
        }
      }
#if 0
    }
#endif
    if (jobid == NULL) {
      jobid_len = 0;
    }
  }

  /* obtain length of jobid string from rank 0 and allocate space to hold it */
  MPI_Bcast(&jobid_len, 1, MPI_INT, 0, comm);
  if (rank != 0 && jobid_len > 0) {
    jobid = (char*) malloc(jobid_len);
  }

  /* check that jobid is not NULL on any process */
  if (gcs_anytrue((jobid == NULL), comm)) {
    /* someone failed, so return NULL on all processes */
    if (jobid != NULL) {
      free(jobid);
      jobid = NULL;
    }
    return NULL;
  }

  /* now broadcast the actual jobid string from rank 0 */
  MPI_Bcast(jobid, jobid_len, MPI_CHAR, 0, comm);

  /* get rank 0's rank within MPI_COMM_WORLD */
  int rank_zero_world;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank_zero_world);
  MPI_Bcast(&rank_zero_world, 1, MPI_INT, 0, comm);

  /* build name for shared memory file:
   * we use a jobid unique to this job and rank 0's rank in MPI_COMM_WORLD */
  char* file = NULL;
  int file_len = snprintf(NULL, 0, "/dev/shm/gcs-job-%s-rank-%d.shmem", jobid, rank_zero_world);
  if (file_len > 0) {
    /* add one for the terminating NULL character, then allocate space */
    file_len++;
    file = (char*) malloc(file_len);
    if (file != NULL) {
      snprintf(file, file_len, "/dev/shm/gcs-job-%s-rank-%d.shmem", jobid, rank_zero_world);
    }
  }

  /* check that file is not NULL on any process */
  if (gcs_anytrue((file == NULL), comm)) {
    /* someone failed, so return NULL on all processes */
    if (file != NULL) {
      free(file);
      file = NULL;
    }
    if (jobid != NULL) {
      free(jobid);
      jobid = NULL;
    }
    return NULL;
  }

  /* free our jobid string */
  if (jobid != NULL) {
    free(jobid);
    jobid = NULL;
  }

  /* return the filename string that we allocated and filled in */
  return file;
}

static int gcs_shm_file_attr_copy(MPI_Comm oldcomm, int keyval,
                     void* extra_state, void* attribute_val_in,
                     void* attribute_val_out, int* flag)
{
  *flag = 0;
  return MPI_SUCCESS;
}

static int gcs_shm_file_attr_del(MPI_Comm comm, int keyval, void* attribute_val, void* extra_state)
{
  /* TODO: delete memory associated with key */
  if (keyval != gcs_shm_file_key) {
    /* error */
  }
  if (attribute_val != NULL) {
    free(attribute_val);
  }
  return MPI_SUCCESS;
}

static int gcs_get_shmem_file(char* file, int len, MPI_Comm comm)
{
  /* create a new key if we need to */
  if (gcs_shm_file_key == MPI_KEYVAL_INVALID) {
    MPI_Comm_create_keyval(&gcs_shm_file_attr_copy, &gcs_shm_file_attr_del, &gcs_shm_file_key, (void*)0);
  }

  /* fetch attribute value associated with our key from the communicator */
  int flag;
  char* file_tmp = NULL;
  MPI_Comm_get_attr(comm, gcs_shm_file_key, &file_tmp, &flag);
  if (!flag) {
    /* if no value is set, build the filename for this communicator and set the attr value */
    file_tmp = gcs_build_shmem_file(comm);
    MPI_Comm_set_attr(comm, gcs_shm_file_key, (void*) file_tmp);
  }

  /* check that we got two valid pointers */
  if (file == NULL || file_tmp == NULL) {
    return (! GCS_SUCCESS);
  }

  /* check that buffer provided by the user is big enough */
  int n = strlen(file_tmp) + 1;
  if (len < n) {
    return (! GCS_SUCCESS);
  }

  /* copy the filename into the user's buffer */
  strcpy(file, file_tmp);

  return GCS_SUCCESS;
}

/* allocate a shared memory segment of the specified size for the specified communicator,
 * returns NULL on all processes if any process in comm fails to allocate the segment */
static void* GCS_Shmem_alloc(size_t size, MPI_Comm comm)
{
  /* get our rank in the communicator */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* get the filename for shared memory segment for this communicator */
  char file[1024];
  int tmp_rc = gcs_get_shmem_file(file, sizeof(file), comm);
  if (gcs_anytrue((tmp_rc != GCS_SUCCESS), comm)) {
    /* this may happen if the filename is too large *OR* if the processes
     * are not all on the same node */
    return NULL;
  }

  /* allocate a tree to associate sizes with pointer values (if we don't have one already) */
  if (gcs_shm_ptr_tree == NULL) {
    gcs_shm_ptr_tree = strmap_new();
  }

  /* ensure that all the processes in comm make it this far before we create the file */
  MPI_Barrier(comm);

  /* open the file on all processes,
   * we're careful to set permissions so only the current user can access the file,
   * which prevents another user from attaching to the file while we have it open */
  int fd = open(file, O_RDWR | O_CREAT, S_IRWXU);
  if (gcs_anytrue((fd < 0), comm)) {
    /* someone failed, so return NULL on all processes */
    if (fd >= 0) {
      close(fd);
    }
    if (rank == 0) {
      unlink(file);
    }
    return NULL;
  }

  /* set the size, but be careful not to actually touch any memory */
  if (rank == 0) {
    ftruncate(fd, 0);
    ftruncate(fd, (off_t) size);
    lseek(fd, 0, SEEK_SET);
  }

  /* ensure that size has been set */
  MPI_Barrier(comm);

  /* mmap the file on all tasks */
  void* ptr = mmap(0, (off_t) size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (gcs_anytrue((ptr == MAP_FAILED), comm)) {
    /* someone failed, so return NULL on all processes */
    if (ptr != MAP_FAILED) {
      munmap(ptr, (off_t) size);
    }
    if (fd >= 0) {
      close(fd);
    }
    if (rank == 0) {
      unlink(file);
    }
    return NULL;
  }

  /* close the file descriptor */
  int rc = close(fd);
  if (gcs_anytrue((rc == -1), comm)) {
    /* someone failed, so return NULL on all processes */
    if (ptr != MAP_FAILED) {
      munmap(ptr, (off_t) size);
    }
    if (rank == 0) {
      unlink(file);
    }
    return NULL;
  }

  /* after each process has mmap'd the file, we unlink it,
   * this way the OS cleans up the file but keep the shared segment active in memory  */
  if (rank == 0) {
    unlink(file);
  }

  /* record the size of the segment that is associated with this address
   * so that we can call munmap later in GCS_Shmem_free() */
  strmap_setf(gcs_shm_ptr_tree, "%x=%llu", ptr, (unsigned long long) size);

  return ptr;
}

/* free shared memory segment allocated with call to GCS_Shmem_alloc,
 * must provide same communicator used in call to GCS_Shmem_alloc */
static int GCS_Shmem_free(void* ptr, MPI_Comm comm)
{
  /* wait for all processes to reach this point, before we free the memory */
  MPI_Barrier(comm);

  if (gcs_shm_ptr_tree != NULL) {
    /* lookup size based on pointer address, then call munmap */
    const char* size_str = strmap_getf(gcs_shm_ptr_tree, "%x", ptr);
    if (size_str != NULL) {
      /* TODO: should code a function to extract this value */
      size_t size = strtoull(size_str, NULL, 10);

      /* unmap the memory associated with the address */
      int rc = munmap(ptr, size);
      if (gcs_anytrue((rc == -1), comm)) {
        /* someone failed to unmap the memory successfully */
        return (! GCS_SUCCESS);
      }

      /* after the memory is unmapped, remove it from the cache */
      strmap_unsetf(gcs_shm_ptr_tree, "%x", ptr);
    } else {
      /* error: this pointer is not registered */
      return (! GCS_SUCCESS);
    }
  } else {
    /* error: there is no tree, meaning that no memory is registered */
    return (! GCS_SUCCESS);
  }

  return GCS_SUCCESS;
}

int file_bcast_exit()
{
    MPI_Abort(MPI_COMM_WORLD, 1);
    MFU_LOG(MFU_LOG_ERR, "MPI Abort failed");
    exit(1);
}

void compute_offset_size(
  uint64_t bytes_read,  /* number of bytes read from file */
  uint64_t stripe_read, /* number of bytes read from current stripe */
  uint64_t file_size,   /* file size in bytes */
  uint64_t stripe_size, /* stripe size in bytes */
  size_t   chunk_size,  /* chunk size in bytes */
  int      rank,        /* rank of reader */
  uint64_t chunk_id,    /* chunk id to be read */
  off_t*   outpos,      /* offset where rank will read this chunk from */
  size_t*  outsize)     /* size which rank will read */
{
    /* we try to read a full chunk */
    size_t read_size = chunk_size;

    /* adjust read to not overrun end of stripe */
    uint64_t remainder = stripe_size - stripe_read;
    if (remainder < (uint64_t) read_size) {
        read_size = (size_t) remainder;
    }

    /* determine offset of first byte we'll read */
    uint64_t offset = bytes_read + (uint64_t)rank * stripe_size + chunk_id * (uint64_t)chunk_size;
    if (offset < file_size) {
        /* the first byte falls within the file size,
         * now check the last byte */
        uint64_t last = offset + (uint64_t) read_size;
        if (last > file_size) {
            /* the last byte is beyond the end, set read size
             * to the most we can read */
            read_size = (size_t) (file_size - offset);
        }
    } else {
        /* the first byte we need to read is past the end of
         * the file, so don't read anything */
        read_size = 0;
    }

    /* set output params */
    *outpos  = (off_t) offset;
    *outsize = read_size;

    return;
}

int mkdirp(const char* path)
{
    /* assume we'll succeed */
    int rc = 0;

    /* hardcode directory mode for now */
    int mode = S_IRWXU;

    /* get parent directory for file */
    mfu_path* parent = mfu_path_from_str(path);
    mfu_path_dirname(parent);
    const char* parent_str = mfu_path_strdup(parent);

    /* if we can read path then there's nothing to do,
     * otherwise, try to create it */
    if (mfu_access(parent_str, R_OK) < 0) {
      rc = mkdirp(parent_str);
    }
  
    /* if we can write to path, try to create subdir within path */
    errno = 0;
    int tmp_rc = mfu_mkdir(path, mode);
    if (tmp_rc < 0) {
      if (errno != EEXIST) {
        /* don't complain about mkdir for a directory that already exists */
        MFU_LOG(MFU_LOG_ERR, "Failed to create directory `%s` (%s)", path, strerror(errno));
        rc = tmp_rc;
      }
    }

    /* free parent directory */
    mfu_free(&parent_str);
    mfu_path_delete(&parent);

    return rc;
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dbcast [options] <SRC> <DEST>\n");
    printf("\n");
    printf("Options:\n");
    printf("  -s, --size <SIZE>  - block size to divide files (default 1MB)\n");
    printf("  -h, --help         - print usage\n");
    printf("For more information see https://mpifileutils.readthedocs.io.");
    printf("\n");
    fflush(stdout);
    return;
}

int main (int argc, char *argv[])
{
    int in_file = -1;
    int out_file = -1;

    /* set this to one to write blocks to file if they are different */
    int file_exists = 0;

    /* if a node fails to write the file, we'll set this to one,
     * it will no longer try to write, but it will delete the file
     * at the end (to clear partial files) and report its hostname */
    int write_error = 0;

    MPI_Init(&argc, &argv);
    mfu_init();

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE; 

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* TODO: set this to size of lustre stripe of the file/system */
    uint64_t stripe_size = 1024 * 1024;

    /* process any options */
    int option_index = 0;
    static struct option long_options[] = {
        {"size",         1, 0, 's'},
        {"help",         0, 0, 'h'},
        {0, 0, 0, 0}
    };

    unsigned long long byte_val;
    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "s:h",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 's':
                /* parse stripe_size from command line */
                if (mfu_abtoull(optarg, &byte_val) != MFU_SUCCESS) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to parse stripe size: %s", optarg);
                    }
                    usage = 1;
                }
                stripe_size = (uint64_t) byte_val;
                break;
            case 'h':
                usage = 1;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "?? getopt returned character code 0%o ??", c);
                }
        }
    }

    /* need source and destination file names */
    if (!usage && (argc - optind) < 2) {
        MFU_LOG(MFU_LOG_ERR, "Failed to find source and/or destination file names");
        usage = 1;
    }

    /* print usage and exit if needed */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        MPI_Finalize();
        return 0;
    }

    /* get source and destination paths */
    char* in_file_path  = mfu_path_strdup_abs_reduce_str(argv[optind]);
    char* out_file_path = mfu_path_strdup_abs_reduce_str(argv[optind + 1]);

    /* get our hostname for error reporting */
    char hostname[HOST_NAME_MAX+1];
    gethostname(hostname, sizeof(hostname));

    /* put procs on same node into a subcommunicator */
    /* split on hostname */
    MPI_Comm node_comm;
    comm_split_str(MPI_COMM_WORLD, hostname, &node_comm);

    /* get our rank and number of ranks in our node comm */
    int node_rank, node_size;
    MPI_Comm_rank(node_comm, &node_rank);
    MPI_Comm_size(node_comm, &node_size);

    /* we'll split into levels, but we want each level to be ordered
     * the same by node, so we use our node leader's rank as a key */
    int key = rank;
    MPI_Bcast(&key, 1, MPI_INT, 0, node_comm);

    /* split across nodes into levels */
    MPI_Comm level_comm;
    MPI_Comm_split(MPI_COMM_WORLD, node_rank, key, &level_comm);

    /* get our rank and number of ranks in the level communicator */
    int level_rank, level_size;
    MPI_Comm_rank(level_comm, &level_rank);
    MPI_Comm_size(level_comm, &level_size);

    /* ensure that all nodes have same number of procs,
     * makes math easier when dividing up work */

    /* get number of procs on our node and max number of procs on any node */
    int max_node_size;
    MPI_Allreduce(&node_size, &max_node_size, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    /* determine whether all nodes have the same number of procs */
    int same_count = 1;
    if (node_size != max_node_size) {
        same_count = 0;
    }
    int all_same_count;
    MPI_Allreduce(&same_count, &all_same_count, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

    /* bail out if we have different nodes have different numbers of procs */
    if (! all_same_count) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Must run with same number of tasks on all nodes");
        }
        MPI_Finalize();
        mfu_free(&out_file_path);
        mfu_free(&in_file_path);
        return 0;
    }

    /* ensure that each node has at least two procs */
    if (node_size < 2) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Must run with at least two tasks per node");
        }
        MPI_Finalize();
        mfu_free(&out_file_path);
        mfu_free(&in_file_path);
        return 0;
    }

    /* check that we can read the input from at least rank 0
     * to catch simple typos */
    int readable = 1;
    if (rank == 0) {
        if (mfu_access(in_file_path, R_OK) != 0) {
            readable = 0;
        }
    }
    MPI_Bcast(&readable, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (! readable) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Cannot read input file `%s`", in_file_path);
        }
        MPI_Finalize();
        mfu_free(&out_file_path);
        mfu_free(&in_file_path);
        return 0;
    }

    /* create destination directory (if needed) */
    int mkdir_rc = 0;
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating destination directories for `%s`", out_file_path);
    }
    if (node_rank == 0) {
        /* define path to destination file */
        mfu_path* parent_path = mfu_path_from_str(out_file_path);
        mfu_path_dirname(parent_path);
        const char* parent_path_str = mfu_path_strdup(parent_path);

        /* make directory for this path */
        mkdir_rc = mkdirp(parent_path_str);

        /* free path data structures */
        mfu_free(&parent_path_str);
        mfu_path_delete(&parent_path);
    }
    if (gcs_anytrue((mkdir_rc != 0), MPI_COMM_WORLD)) {
        /* someone failed to create the directory */
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to create directory for `%s`", out_file_path);
        }
        MPI_Finalize();
        mfu_free(&out_file_path);
        mfu_free(&in_file_path);
        return 0;
    }
    MPI_Barrier(MPI_COMM_WORLD);

    /* TODO: set this to a value which is efficient as a single read
     * from Lustre but small enough to send via MPI */
    size_t chunk_size = 1*1024*1024;

    size_t alignment = 1024*1024;

    /* we'll create multiple shared memory segments,
     * two for each reader process */
    int bufcounts = (node_size - 1) * 2;
    void** shmbuf_base = (void**) malloc(bufcounts * sizeof(void*));
    void** shmbuf      = (void**) malloc(bufcounts * sizeof(void*));

    /* initialize our shared memory pointers */
    int i;
    for (i = 0; i < bufcounts; i++) {
        /* allocate memory (two buffers for each reader) */
        shmbuf_base[i] = GCS_Shmem_alloc(chunk_size + alignment, node_comm);

        /* align buffers */
        void* base = shmbuf_base[i];
        shmbuf[i] = shmbuf_base[i];
        //shmbuf[i] = (char*)base + alignment - ((uint64_t)base & (alignment - 1)) ;
    }

    /* rank 0 reads flie size and mode */
    int mode;
    uint64_t file_size;
    const char *time_format = "%b %d %T";
    if (rank == 0) {
        /* post message to user about the file we're bcasting */
        MFU_LOG(MFU_LOG_INFO, "Broadcasting contents of `%s` to `%s`", in_file_path, out_file_path);

        /* get file size */
        errno = 0;
        struct stat file_stat;
        if (stat(in_file_path, &file_stat) < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to stat file `%s` (%s)", in_file_path, strerror(errno));
            file_bcast_exit();
        }

        /* get file mode and size from stat info */
        mode = (int) file_stat.st_mode;
        file_size = (uint64_t) file_stat.st_size;
    }

    /* broadcast file size to all procs */
    MPI_Bcast(&file_size, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* broadcast file mode to all procs */
    MPI_Bcast(&mode, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* identify number of reader tasks and assign a rank to each one */

    /* tasks with node_rank == 0 are excluded from this set */
    int reader_size = ranks - level_size;

    /* assign ranks so that readers on the same node are in
     * consecutive order (remember to exclude node_rank == 0 from
     * each node) */
    int reader_rank = level_rank * (node_size - 1) + (node_rank - 1);

    /* rank 0 on each node will write file, others will read from input */
    if (node_rank == 0) {
        /* open file for differently depending on whether it already exists */
        uint64_t existing_file_size = 0;
        if (mfu_access(out_file_path, F_OK) == 0) {
            /* record that file already exists */
            file_exists = 1;

            /* get size of existing file */
            errno = 0;
            struct stat file_stat;
            if (mfu_lstat(out_file_path, &file_stat) < 0) {
                /* can't measure free space so assume we have none */
                MFU_LOG(MFU_LOG_ERR, "Failed to stat file `%s` (%s)", out_file_path, strerror(errno));
            } else {
                /* we'll overwrite the existing file, so include its
                 * size in our measure of free space */
                existing_file_size = (uint64_t) file_stat.st_size;
            }

            /* file already exists, open for read/write,
             * to save wear on SSD, we'll only write if
             * there is a difference in what we read */
            int flags = O_RDWR;
            out_file = mfu_open(out_file_path, flags);
        } else {
            /* file does not exist, so create it */
            int flags = O_CREAT | O_TRUNC | O_WRONLY | O_DIRECT;
            out_file = mfu_open(out_file_path, flags, S_IRWXU | S_IRWXG | S_IRWXO);

            /* if the open failed, try again without O_DIRECT */
            if (out_file < 0) {
                flags = O_CREAT | O_TRUNC | O_WRONLY;
                out_file = mfu_open(out_file_path, flags, S_IRWXU | S_IRWXG | S_IRWXO);
            }
        }

        /* check whether our open worked */
        if (out_file < 0) {
            /* don't treat this as fatal incase just this node has
             * the problem, but remember that we hit the error to
             * report it at the end */
            write_error = 1;
        } else {
            /* open worked, so now we have an output file that we can
             * call statfs on to compute free space left on device */
            errno = 0;
            uint64_t free_size = 0;
            struct statfs fs_stat;
            if (statfs(out_file_path, &fs_stat) == 0) {
                free_size = (uint64_t)fs_stat.f_bavail * (uint64_t)fs_stat.f_bsize;
            } else {
                MFU_LOG(MFU_LOG_ERR, "Failed to stat file system for `%s` (%s)", out_file_path, strerror(errno));
            }

            /* we'll overwrite any existing file, so include its
             * space in our measure of available space */
            if (file_exists) {
                free_size += existing_file_size;
            }

            /* check whether we have space to write the file */
            if (file_size > free_size) {
                /* not enough space for file */
                MFU_LOG(MFU_LOG_ERR, "Insufficient space for file `%s` filesize=%llu free=%llu", out_file_path, file_size, free_size);
                write_error = 1;
            }
        }
    } else {
        /* open input file for reading if we're a reader */
        errno = 0;
        in_file = mfu_open(in_file_path, O_RDONLY);
        if (in_file < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file `%s` for reading (%s)", in_file_path, strerror(errno));
            file_bcast_exit();
        }
    }

    /* TODO: if all writers failed, throw a more serious error */

    /* first phase: all tasks read data and write to their local file */
    MPI_Barrier(MPI_COMM_WORLD);

    double time_start = MPI_Wtime();

    /* compute rank on left side */
    int left = level_rank - 1;
    if (left < 0) {
      left = level_size - 1;
    }

    /* compute rank on right size */
    int right = level_rank + 1;
    if (right == level_size) {
      right = 0;
    }

/* readers */
if (node_rank != 0) {
    /* read back parts of output file and broadcast */
    MPI_Request request[3];
    MPI_Status  status[3];
    size_t bytes_read = 0;
    while (bytes_read < file_size) {
        /* process series of chunks in this stripe */
        uint64_t chunk_id = 0;
        uint64_t stripe_read = 0;
        while (stripe_read < stripe_size) {
            /* we'll send from buf1 and receive into buf2,
             * read data from file into buf1 to begin */
            int shmid = (node_rank - 1) * 2;
            void* buf1 = shmbuf[shmid + 0];
            void* buf2 = shmbuf[shmid + 1];

            /* get offset and size of bytes to read */
            off_t pos1;
            size_t size1;
            compute_offset_size(
                bytes_read, stripe_read, file_size, stripe_size, chunk_size, reader_rank, chunk_id,
                &pos1, &size1
            );

            /* seek to offset in input file */
            if (size1 > 0) {
                errno = 0;
                off_t rc = mfu_lseek(in_file_path, in_file, pos1, SEEK_SET);
                if (rc == (off_t)-1) {
                    MFU_LOG(MFU_LOG_ERR, "Seek failed on file `%s` (%s)", out_file_path, strerror(errno));
                    file_bcast_exit();
                }

                /* read chunk from file */
                errno = 0;
                ssize_t return_size = mfu_read(in_file_path, in_file, buf1, size1);
                if (return_size != (ssize_t)size1) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to read contents from `%s` (%s)", out_file_path, strerror(errno));
                    file_bcast_exit();
                }
            }

            /* we send data to the left and receive from the right until
             * we've received and written all data for this chunk */
            int lev;
            for (lev = 1; lev < level_size; lev++) {
                /* determine source of data we'll receive in this step */
                int lev_incoming = level_rank + lev;
                if (lev_incoming >= level_size) {
                    lev_incoming -= level_size;
                }
                int read_rank_incoming = lev_incoming * (node_size - 1) + (node_rank - 1);

                /* get offset and size of incoming data */
                off_t pos2;
                size_t size2;
                compute_offset_size(
                    bytes_read, stripe_read, file_size, stripe_size, chunk_size, read_rank_incoming, chunk_id,
                    &pos2, &size2
                );

                /* signal writer that our buffer is ready */
                MPI_Send(&shmid, 1, MPI_INT, 0, 0, node_comm);

                /* receieve data from right, send data to left,
                 * and send data to writer on same node */
                MPI_Irecv(buf2, (int) size2, MPI_BYTE, right, 0, level_comm, &request[0]);
                MPI_Isend(buf1, (int) size1, MPI_BYTE, left,  0, level_comm, &request[1]);
                MPI_Waitall(2, request, status);

                /* wait for signal from writer to know that it's
                 * finished with our buffer */
                MPI_Recv(&shmid, 1, MPI_INT, 0, 0, node_comm, &status[0]);

                /* swap buffers to send data we just received */
                size1 = size2;
                char* buftmp = buf1;
                buf1 = buf2;
                buf2 = buftmp;
                shmid = (shmid & 0x1) ? (shmid - 1) : (shmid + 1);
            }

            /* signal writer that our buffer is ready */
            MPI_Send(&shmid, 1, MPI_INT, 0, 0, node_comm);

            /* wait for signal from writer to know that it's
             * finished with our buffer */
            MPI_Recv(&shmid, 1, MPI_INT, 0, 0, node_comm, &status[0]);

            /* go on to next chunk */
            stripe_read += chunk_size;
            chunk_id++;
        }

        /* record total number of bytes processed, the last set of stripes may
         * not be full but that doesn't matter in this accounting */
        bytes_read += stripe_size * (uint64_t)reader_size;
    }
}

/* writers */
if (node_rank == 0) {
    double percent = 2.0;
    char* readbuf = (char*) malloc(chunk_size);
//    shmbuf[i] = (char*)base + alignment - ((uint64_t)base & (alignment - 1)) ;

    /* read back parts of output file and broadcast */
    MPI_Request request[2];
    MPI_Status  status[2];
    size_t bytes_read = 0;
    while (bytes_read < file_size) {
        /* iterate over all procs on the node,
         * we process a full stripe one level at at time */
            /* process series of chunks in this stripe */
            uint64_t chunk_id = 0;
            uint64_t stripe_read = 0;
            while (stripe_read < stripe_size) {
                /* process this portion of the stripe for all procs at this level */
                int lev;
                for (lev = 0; lev < level_size; lev++) {
                int node;
                for (node = 1; node < node_size; node++) {
                    /* determine source of data we'll receive in this step */
                    int lev_incoming = level_rank + lev;
                    if (lev_incoming >= level_size) {
                        lev_incoming -= level_size;
                    }
                    int read_rank_incoming = lev_incoming * (node_size - 1) + (node - 1);

                    /* get offset and size of bytes for this reader */
                    off_t pos;
                    size_t size;
                    compute_offset_size(
                        bytes_read, stripe_read, file_size, stripe_size, chunk_size, read_rank_incoming, chunk_id,
                        &pos, &size
                    );

                    /* wait for node to signal us */
                    int shmid;
                    MPI_Recv(&shmid, 1, MPI_INT, node, 0, node_comm, &status[0]);

                    /* determine buffer to write data from */
                    void* copybuf = shmbuf[shmid];

                    /* write data to file */
                    if (size > 0) {
                        /* assume that we'll be writing data */
                        int write_data = 1;

                        /* if the file already exists, read in this segment and compare
                         * it to what we should be writing */
                        if (file_exists) {
                            /* file exists, now assume it's the same content so that
                             * we don't need to write this data */
                            write_data = 0;

                            /* seek to offset in output file */
                            errno = 0;
                            int rc = mfu_lseek(out_file_path, out_file, pos, SEEK_SET);
                            if (rc == (off_t)-1) {
                                /* consider this a write error since we can't read
                                 * to determine whether we need to write */
                                MFU_LOG(MFU_LOG_ERR, "Seek failed on file `%s` (%s)", out_file_path, strerror(errno));
                                write_error = 1;
                            }

                            /* read chunk from file */
                            errno = 0;
                            ssize_t return_size = mfu_read(out_file_path, out_file, readbuf, size);
                            if (return_size == (ssize_t)size) {
                                /* we read the correct number of bytes, now compare them */
                                if (memcmp(readbuf, copybuf, size) != 0) {
                                    /* found a difference so overwrite existing data */
                                    write_data = 1;
                                }
                            } else {
                                /* overwrite data if we got a short read or end of file,
                                 * otherwise, we got a read error  */
                                if (return_size >= 0) {
                                    write_data = 1;
                                } else {
                                    /* consider this a write error since we can't read
                                     * to determine whether we need to write */
                                    MFU_LOG(MFU_LOG_ERR, "Failed to read from existing file `%s` (%s)", out_file_path, strerror(errno));
                                    write_error = 1;
                                }
                            }
                        }

                        /* write data to output file */
                        if (write_data && !write_error) {
                            /* seek to offset in output file */
                            errno = 0;
                            int rc = mfu_lseek(out_file_path, out_file, pos, SEEK_SET);
                            if (rc == (off_t)-1) {
                                MFU_LOG(MFU_LOG_ERR, "Seek failed on file `%s` (%s)", out_file_path, strerror(errno));
                                write_error = 1;
                            }

                            /* to use O_DIRECT, we have to write in full blocks */
                            if (size != chunk_size) {
                                if (pos + size < file_size) {
                                    /* this is bad, so consider it to be fatal */
                                    MFU_LOG(MFU_LOG_ERR, "Trying to write past end of chunk in middle block `%s`", out_file_path);
                                    file_bcast_exit();
                                }
                                size = chunk_size;
                            }

                            /* write chunk to output file */
                            errno = 0;
                            ssize_t return_size = mfu_write(out_file_path, out_file, copybuf, size);
                            if (return_size == -1) {
                                /* remember that we had a write error,
                                 * we'll keep going and delete the
                                 * file at the end */
                                MFU_LOG(MFU_LOG_ERR, "Failed to write contents to `%s` (%s)", out_file_path, strerror(errno));
                                write_error = 1;
                            }
                        }
                    }

                    /* signal node that we're finished */
                    MPI_Send(&shmid, 1, MPI_INT, node, 0, node_comm);
                }
                }

                /* go on to next chunk */
                stripe_read += chunk_size;
                chunk_id++;
            }

        /* record total number of bytes processed, the last set of stripes may
         * not be full but that doesn't matter in this accounting */
        bytes_read += stripe_size * (uint64_t)reader_size;
        if (rank == 0) {
            size_t report_read = (bytes_read < file_size) ? bytes_read: file_size;
            double percent_read = ((double)(report_read) / (double)(file_size)) * 100.0;
            if (percent_read >= percent) {
                double time_end = MPI_Wtime();
                double time_diff = time_end - time_start;
                double rate = (double)report_read / (time_diff * 1024.0 * 1024.0);
                double time_remaining = ((double)(file_size - report_read)) / (rate * 1024.0 * 1024.0);
                MFU_LOG(MFU_LOG_INFO, "Progress: %0.1f%% %f MB/s %0.1f secs remaining",
                    percent_read, rate, time_remaining
                );

                percent = percent_read + 2.0;
                if (percent > 100.0) {
                    percent = 100.0;
                }
            }
        }
    }

    free(readbuf);
}

    MPI_Barrier(MPI_COMM_WORLD);

    /* every rank closes output file */
    if (node_rank == 0) {
        /* if we have a file open, sync and close it */
        if (out_file >= 0) {
            errno = 0;
            if (mfu_fsync(out_file_path, out_file) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to fsync file `%s` (%s)", out_file_path, strerror(errno));
                write_error = 1;
            }

            errno = 0;
            if (mfu_close(out_file_path, out_file) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to close file `%s` (%s)", out_file_path, strerror(errno));
                write_error = 1;
            }
        }

        /* every writer truncates file, we do this in case the user is copying
         * a file by the same name but of different size than a previous copy */
        errno = 0;
        if (mfu_truncate(out_file_path, (off_t) file_size) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to truncate file `%s`", out_file_path, strerror(errno));
            write_error = 1;
        }

        /* have every writer update file mode */
        errno = 0;
        if (mfu_chmod(out_file_path, (mode_t) mode) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to chmod file `%s`", out_file_path, strerror(errno));
            write_error = 1;
        }

        /* delete file if we hit an error while writing */
        if (write_error) {
            errno = 0;
            if (mfu_unlink(out_file_path) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to unlink file `%s`", out_file_path, strerror(errno));
            }
        }
    } else {
        /* readers close input file */
        errno = 0;
        if (mfu_close(in_file_path, in_file) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close file `%s`", in_file_path, strerror(errno));
        }
    }

    /* wait until each process has closed the file */
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        MFU_LOG(MFU_LOG_INFO, "Bcast complete: size=%llu, time=%f secs, speed=%f MB/sec",
            (unsigned long long) file_size, time_diff, (double) file_size / (time_diff * 1024.0 * 1024.0)
        );
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* if we failed to write the file, print an error message */
    if (write_error) {
        MFU_LOG(MFU_LOG_ERR, "Failed to write `%s`", out_file_path);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    /* free shared memory segments */
    for (i = 0; i < bufcounts; i++) {
        GCS_Shmem_free(shmbuf_base[i], node_comm);
    }

    /* free paths */
    mfu_free(&out_file_path);
    mfu_free(&in_file_path);

    /* free our node and level communicators */
    MPI_Comm_free(&level_comm);
    MPI_Comm_free(&node_comm);

    mfu_finalize();
    MPI_Finalize();

    return 0;
}
