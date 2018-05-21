#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
//#include "handle_args.h"
#include "mfu.h"
#include "strmap.h"
#define FILE_PERMS (S_IRUSR | S_IWUSR)
#define DIR_PERMS  (S_IRWXU)

/*--------------------------------------*/
/* Define types                         */
/*--------------------------------------*/
/* linked list element of stat data used during walk */
typedef struct list_elem {
    char* file;             /* file name (strdup'd) */
    int depth;              /* depth within directory tree */
    mfu_filetype type;    /* type of file object */
    int detail;             /* flag to indicate whether we have stat data */
    uint64_t mode;          /* stat mode */
    uint64_t uid;           /* user id */
    uint64_t gid;           /* group id */
    uint64_t atime;         /* access time */
    uint64_t atime_nsec;    /* access time nanoseconds */
    uint64_t mtime;         /* modify time */
    uint64_t mtime_nsec;    /* modify time nanoseconds */
    uint64_t ctime;         /* create time */
    uint64_t ctime_nsec;    /* create time nanoseconds */
    uint64_t size;          /* file size in bytes */
    struct list_elem* next; /* pointer to next item */
} elem_t;
/*--------------------------------------------------------*/
/* holds an array of objects: users, groups, or file data */
/*--------------------------------------------------------*/
typedef struct {
    void* buf;       /* pointer to memory buffer holding data */
    size_t bufsize;  /* number of bytes in buffer */
    uint64_t count;  /* number of items */
    uint64_t chars;  /* max name of item */
    MPI_Datatype dt; /* MPI datatype for sending/receiving/writing to file */
} buf_t;
/*---------------------------------------*/
/* abstraction for distributed file list */
/*---------------------------------------*/
typedef struct flist {
    int detail;              /* set to 1 if we have stat, 0 if just file name */
    uint64_t offset;         /* global offset of our file across all procs */
    uint64_t total_files;    /* total file count in list across all procs */
    uint64_t total_users;    /* number of users (valid if detail is 1) */
    uint64_t total_groups;   /* number of groups (valid if detail is 1) */
    uint64_t max_file_name;  /* maximum filename strlen()+1 in global list */
    uint64_t max_user_name;  /* maximum username strlen()+1 */
    uint64_t max_group_name; /* maximum groupname strlen()+1 */
    int min_depth;           /* minimum file depth */
    int max_depth;           /* maximum file depth */

    /* variables to track linked list of stat data during walk */
    uint64_t list_count; /* number of items in list */
    elem_t*  list_head;  /* points to item at head of list */
    elem_t*  list_tail;  /* points to item at tail of list */
    elem_t** list_index; /* an array with pointers to each item in list */

    /* buffers of users, groups, and files */
    buf_t users;
    buf_t groups;
    int have_users;        /* set to 1 if user map is valid */
    int have_groups;       /* set to 1 if group map is valid */
    strmap* user_id2name;  /* map linux uid to user name */
    strmap* group_id2name; /* map linux gid to group name */
} flist_t;


/* append element to tail of linked list */
static void list_insert_elem(flist_t* flist, elem_t* elem)
{
    /* set head if this is the first item */
    if (flist->list_head == NULL) {
        flist->list_head = elem;
    }

    /* update last element to point to this new element */
    elem_t* tail = flist->list_tail;
    if (tail != NULL) {
        tail->next = elem;
    }

    /* make this element the new tail */
    flist->list_tail = elem;
    elem->next = NULL;

    /* increase list count by one */
    flist->list_count++;

    /* delete the index if we have one, it's out of date */
    mfu_free(&flist->list_index);

    return;
}
static int get_depth(const char* path)
/*------------------------------------------------------*/
/* given path, return level within directory tree,      */
/* counts '/' characters assuming path is standardized  */
/* and absolute                                         */
/*------------------------------------------------------*/
{
    const char* c;
    int depth = 0;
    for (c = path; *c != '\0'; c++) {
        if (*c == '/') {
            depth++;
        }
    }
    return depth;
}
static void list_compute_summary(flist_t* flist)
{
    /* initialize summary values */
    flist->max_file_name  = 0;
    flist->max_user_name  = 0;
    flist->max_group_name = 0;
    flist->min_depth      = 0;
    flist->max_depth      = 0;
    flist->total_files    = 0;
    flist->offset         = 0;

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* get total number of files in list */
    uint64_t total;
    uint64_t count = flist->list_count;
    MPI_Allreduce(&count, &total, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    flist->total_files = total;

    /* bail out early if no one has anything */
    if (total <= 0) {
        return;
    }

    /* compute the global offset of our first item */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }
    flist->offset = offset;

    /* compute local min/max values */
    int min_depth = -1;
    int max_depth = -1;
    uint64_t max_name = 0;
    elem_t* current = flist->list_head;
    while (current != NULL) {
        uint64_t len = (uint64_t)(strlen(current->file) + 1);
        if (len > max_name) {
            max_name = len;
        }

        int depth = current->depth;
        if (depth < min_depth || min_depth == -1) {
            min_depth = depth;
        }
        if (depth > max_depth || max_depth == -1) {
            max_depth = depth;
        }

        /* go to next item */
        current = current->next;
    }

    /* get global maximums */
    int global_max_depth;
    MPI_Allreduce(&max_depth, &global_max_depth, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    uint64_t global_max_name;
    MPI_Allreduce(&max_name, &global_max_name, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    /* since at least one rank has an item and max will be -1 on ranks
     * without an item, set our min to global max if we have no items,
     * this will ensure that our contribution is >= true global min */
    int global_min_depth;
    if (count == 0) {
        min_depth = global_max_depth;
    }
    MPI_Allreduce(&min_depth, &global_min_depth, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

    /* set summary values */
    flist->max_file_name = global_max_name;
    flist->min_depth = global_min_depth;
    flist->max_depth = global_max_depth;

    /* set summary on users and groups */
    if (flist->detail) {
        flist->total_users    = flist->users.count;
        flist->total_groups   = flist->groups.count;
        flist->max_user_name  = flist->users.chars;
        flist->max_group_name = flist->groups.chars;
    }

    return;
}
/* keep stats during walk */
uint64_t total_dirs    = 0;
uint64_t total_files   = 0;
uint64_t total_links   = 0;
uint64_t total_unknown = 0;
uint64_t total_bytes   = 0;
static void print_summary(mfu_flist flist)
{
    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* step through and print data */
    uint64_t idx = 0;
    uint64_t max = mfu_flist_size(flist);
    //if (rank == 0) printf("From print_summary: mfu_flist_have_detail(flist) = %li\n",mfu_flist_have_detail(flist));
    while (idx < max) {
        if (mfu_flist_have_detail(flist)) {
            /* get mode */
            mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);

            /* set file type */
            if (S_ISDIR(mode)) {
                total_dirs++;
            }
            else if (S_ISREG(mode)) {
                total_files++;
            }
            else if (S_ISLNK(mode)) {
                total_links++;
            }
            else {
                /* unknown file type */
                total_unknown++;
            }

            uint64_t size = mfu_flist_file_get_size(flist, idx);
            total_bytes += size;
        }
        else {
            /* get type */
            mfu_filetype type = mfu_flist_file_get_type(flist, idx);

            if (type == MFU_TYPE_DIR) {
                total_dirs++;
            }
            else if (type == MFU_TYPE_FILE) {
                total_files++;
            }
            else if (type == MFU_TYPE_LINK) {
                total_links++;
            }
            else {
                /* unknown file type */
                total_unknown++;
            }
        }

        /* go to next file */
        idx++;
    }

    /* get total directories, files, links, and bytes */
    uint64_t all_dirs, all_files, all_links, all_unknown, all_bytes;
    uint64_t all_count = mfu_flist_global_size(flist);
    MPI_Allreduce(&total_dirs,    &all_dirs,    1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_files,   &all_files,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_links,   &all_links,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_unknown, &all_unknown, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&total_bytes,   &all_bytes,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* convert total size to units */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && rank == 0) {
        printf("Items: %llu\n", (unsigned long long) all_count);
        printf("  Directories: %llu\n", (unsigned long long) all_dirs);
        printf("  Files: %llu\n", (unsigned long long) all_files);
        printf("  Links: %llu\n", (unsigned long long) all_links);
        /* printf("  Unknown: %lu\n", (unsigned long long) all_unknown); */

        if (mfu_flist_have_detail(flist)) {
            double agg_size_tmp;
            const char* agg_size_units;
            mfu_format_bytes(all_bytes, &agg_size_tmp, &agg_size_units);

            uint64_t size_per_file = 0.0;
            if (all_files > 0) {
                size_per_file = (uint64_t)((double)all_bytes / (double)all_files);
            }
            double size_per_file_tmp;
            const char* size_per_file_units;
            mfu_format_bytes(size_per_file, &size_per_file_tmp, &size_per_file_units);

            printf("     Data: %.3lf %s (%.3lf %s per file)\n", agg_size_tmp, agg_size_units, size_per_file_tmp, size_per_file_units);
        }
    }

    return;
}


/*-----------------------*/
/*   create a directory  */
/*-----------------------*/
static int create_directory_jl(mfu_flist list, uint64_t idx)
{
    /* get name of directory */
    const char* name = mfu_flist_file_get_name(list, idx);

    /* get destination name */
     const char* dest_path = name;

   /* create the destination directory */
    MFU_LOG(MFU_LOG_DBG, "Creating directory `%s'", dest_path);
    int rc = mfu_mkdir(dest_path, DIR_PERMS);
    if(rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to create directory `%s' (errno=%d %s)", \
            dest_path, errno, strerror(errno));
        return -1;
    }

    /* we do this now in case there are Lustre attributes for
     * creating / striping files in the directory */

    /* free the directory name */
    mfu_free(&dest_path);

    return 0;
}
static int create_directories_jl(int levels, int minlevel, mfu_flist* lists)
/*--------------------------------------------------------------------*/
/* create directories, we work from shallowest level to the deepest   */
/* with a barrier in between levels, so that we don't try to create   */
/* a child directory until the parent exists                          */
/*--------------------------------------------------------------------*/
{
    int rc = 0;

    int verbose = (mfu_debug_level <= MFU_LOG_INFO);

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* indicate to user what phase we're in */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating directories.");
    }
    /* work from shallowest level to deepest level */
    int level;
    for (level = 0; level < levels; level++) {
        /* time how long this takes */
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* create each directory we have at this level */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* check whether we have a directory */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);
            if (type == MFU_TYPE_DIR) {
                /* create the directory */
                int tmp_rc = create_directory_jl(list, idx);
                if (tmp_rc != 0) {
                    rc = tmp_rc;
                }

                count++;
            }
        }

        /* wait for all procs to finish before we start
         * creating directories at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        /* stop our timer */
        double end = MPI_Wtime();

         /* print statistics */
        if (verbose) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            double secs = end - start;
            if (secs > 0.0) {
              rate = (double)sum / secs;
            }
            if (rank == 0) {
                printf("  level=%d min=%lu max=%lu sum=%lu rate=%f/sec secs=%f\n",
                  (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
                );
                fflush(stdout);
            }
        }
    }

    return rc;
}
static int create_file(mfu_flist list, uint64_t idx)
/*--------------------*/
/* create file node   */
/*--------------------*/
{
    /* get source name */
    const char* src_path = mfu_flist_file_get_name(list, idx);
    int havd = mfu_flist_have_detail(list);

    /* get destination name */
    const char* dest_path = src_path;

    /* since file systems like Lustre require xattrs to be set before file is opened,
     * we first create it with mknod and then set xattrs */

    /* create file with mknod
    * for regular files, dev argument is supposed to be ignored,
    * see makedev() to create valid dev */
    dev_t dev;
    memset(&dev, 0, sizeof(dev_t));
    int mknod_rc = mfu_mknod(dest_path, FILE_PERMS | S_IFREG, dev);

    if(mknod_rc < 0) {
        if(errno == EEXIST) {
            /* TODO: should we unlink and mknod again in this case? */
        }

        MFU_LOG(MFU_LOG_ERR, "File `%s' mknod() errno=%d %s",
            dest_path, errno, strerror(errno)
        );
    }


    return 0;
}
static int create_files(int levels, int minlevel, mfu_flist* lists)
/*-------------------------------------*/
/* create file nodes in directory      */
/*-------------------------------------*/
{
    int rc = 0;

    int verbose = (mfu_debug_level <= MFU_LOG_INFO);

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* indicate to user what phase we're in */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating files.");
    }

    int level;
    for (level = 0; level < levels; level++) {
        /* time how long this takes */
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];
  
        /* iterate over items and set write bit on directories if needed */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* get type of item */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);

            /* process files and links */
            if (type == MFU_TYPE_FILE) {
                create_file(list, idx);
                count++;
            }
        }

        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        /* stop our timer */
        double end = MPI_Wtime();

        /* print timing statistics */
        if (verbose) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            double secs = end - start;
            if (secs > 0.0) {
              rate = (double)sum / secs;
            }
            if (rank == 0) {
                printf("  level=%d min=%lu max=%lu sum=%lu rate=%f secs=%f\n",
                  (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
                );
                fflush(stdout);
            }
        }
    }

    return rc;
}

void fillelem(elem_t *element,char* fname,int depthval,long int flen,long int ftype)
/*--------------------------------------*/
/* write specified info to list element */
/*--------------------------------------*/
{
     long int fmode;
     //-----------------------------------------------------------
     // the following numbers are from /usr/include/bits/stats.h
     //-----------------------------------------------------------
     if (ftype==MFU_TYPE_DIR)  fmode = 0040000;        // _S_IFDIR
     else if (ftype==MFU_TYPE_FILE) fmode = 0100000;   // _S_IFREG
     else if (ftype==MFU_TYPE_LINK) fmode= 0120000;    // _S_IFLNK
     else  
     {
          printf("from fillelem ftype = %d is not legal value\n",ftype);
          exit(0);
     }
    //----------------------------------
    // fill element with information
    //----------------------------------
    element->file = MFU_STRDUP(fname); // char* file;            /* file name (strdup'd) */
    element->depth=depthval;             // int depth;             /* depth within directory tree */
    element->type=ftype;                 // mfu_filetype type;   /* type of file object */
    element->detail=1;                   // int detail;            /* flag to indicate whether we have stat data */
    element->mode = fmode;               // uint64_t mode;         /* stat mode */
    element->uid=getuid();               // uint64_t uid;          /* user id */
    element->gid=getgid();               // uint64_t gid;          /* group id */
    element->atime=0;                    // uint64_t atime;        /* access time */
    element->atime_nsec=0;               // uint64_t atime_nsec;   /* access time nanoseconds */
    element->mtime=0;                    // uint64_t mtime;        /* modify time */
    element->mtime_nsec=0;               // uint64_t mtime_nsec;   /* modify time nanoseconds */
    element->ctime=0;                    // uint64_t ctime;        /* create time */
    element->ctime_nsec=0;               // uint64_t ctime_nsec;   /* create time nanoseconds */
    element->size=flen;                  // uint64_t size;         /* file size in bytes */
    element->next=NULL;                  //struct list_elem* next; /* pointer to next item */
}

static int numfile=0,numdir=0,numlink=0;
void setname(char* aname,unsigned long int ftype,int n,const char* path)
//--------------------------------------
// set name and type of flist item
//--------------------------------------
{
   switch (ftype)
   {
      case MFU_TYPE_NULL:
        // printf("MFU_TYPE_NULL\n");
         break;
      case MFU_TYPE_UNKNOWN:
        // printf("MFU_TYPE_UNKNOWN\n");
         break;
      case MFU_TYPE_FILE:
       //  printf("MFU_TYPE_FILE\n");
         sprintf(aname,"%s/file_%08d",path,n);
         break;
      case MFU_TYPE_DIR:
        // printf("MFU_TYPE_DIR\n");
         sprintf(aname,"%s/dir_%08d",path,n);
         break;
      case MFU_TYPE_LINK:
        // printf("MFU_TYPE_LINK\n");
         sprintf(aname,"%s/link_%08d",path,n);
         break;
      default:
        // printf("bad value\n");
         break;
   }
}

int getnum(const char* fname)
//------------------------------
// get number from file name
//------------------------------
{
     const char* cp;
     cp = strrchr(fname,'_');
     return atoi(cp+1);
}

void fillbuff(int* ibuff,int nwds)
//-----------------------------------
// put nwds random ints into buffer
//------------------------------------
{
    int i;
    for (i=0;i<nwds;i++) ibuff[i]=rand();
}
size_t bufsize = 1024*1024;
char* buf;
size_t size,isize;
int nnum;

static int write_file(mfu_flist list, uint64_t idx)
/*----------------------------------------------*/
/* add content to a node created by create_file */
/*----------------------------------------------*/
 {
     int rc=0;  
 
     /* get destination name */
     const char* dest_path = mfu_flist_file_get_name(list, idx);
     uint64_t fsize =  mfu_flist_file_get_size(list, idx);
     size = fsize;
     isize = (size+1)/2;
     //printf("writing file %s, fsize = %li, size = %li\n",dest_path,fsize,size);
     nnum = getnum(dest_path);
     srand(nnum);    
     fillbuff((int*)buf,isize);
 
     /* open file */
     int fd = mfu_open(dest_path,  O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
 
     /*  write stuff to destination file  */
     if (fd != -1) {
       /* we opened the file, now start writing */
       size_t written = 0;
       char* ptr = (char*) buf;
       while (written < (size_t) size) {
           /* determine amount to write */
           size_t left = fsize;
           size_t remaining = size - written;
           if (remaining < fsize) { left = remaining; }
 
           /* write data to file */
           ssize_t n = mfu_write(dest_path, fd, ptr, left);
           if (n == -1) {
             printf("Failed to write to file: dest_path=%s errno=%d (%s)\n", dest_path, errno, strerror(errno));
             rc = 1;
             break;
           }
 
           /* update amount written */
           written += (size_t) n;
       }
 
       /* sync output to disk and close the file */
       mfu_fsync(dest_path, fd);
       mfu_close(dest_path, fd);
     } else {
       /* failed to open the file */
       printf("Failed to open file: dest_path=%s errno=%d (%s)\n", dest_path, errno, strerror(errno));
       rc = 1;
     }
 
     /* free destination path */
     mfu_free(&dest_path);
 
     return rc;
 }

static int write_files(int levels, int minlevel, mfu_flist *lists)
/*----------------------------------------------*/
/* add content to nodes created by create_files */
/*----------------------------------------------*/
{
    int rc = 0;
    int i;

    int verbose = (mfu_debug_level <= MFU_LOG_INFO);

    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* indicate to user what phase we're in */
    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing content to files.");
    }

    int level;
    for (level = 0; level < levels; level++) {
        /* time how long this takes */
        double start = MPI_Wtime();

        /* get list of items for this level */
        mfu_flist list = lists[level];

        /* iterate over items and set write bit on directories if needed */
        uint64_t idx;
        uint64_t size = mfu_flist_size(list);
        uint64_t count = 0;
        for (idx = 0; idx < size; idx++) {
            /* get type of item */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);

            /* process files and links */
            if (type == MFU_TYPE_FILE) {
                /* TODO: skip file if it's not readable */
                write_file(list, idx);
                count++;
            } else if (type == MFU_TYPE_LINK) {
                // write_link(list, idx);  // nothing for now
                count++;
            }
        }

        /* wait for all procs to finish before we start
         * with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        /* stop our timer */
        double end = MPI_Wtime();

        /* print timing statistics */
        if (verbose) {
            uint64_t min, max, sum;
            MPI_Allreduce(&count, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&count, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            double secs = end - start;
            if (secs > 0.0) {
              rate = (double)sum / secs;
            }
            if (rank == 0) {
                printf("  level=%d min=%lu max=%lu sum=%lu rate=%f secs=%f\n",
                  (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, secs
                );
                fflush(stdout);
            }
        }
    }

    return rc;
}

static void create_targets(int nlevels, int linktot, int* nfiles,uint64_t *targIDs, char **tnames, char** tarray)
//------------------------------------------------------
// get targets for links from list of target IDs
//-------------------------------------------------------
{
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    //---------------------------------
    // first get ranges for each level
    //---------------------------------
    uint64_t *ifst,*ilst;
    ifst = (uint64_t*)MFU_MALLOC(nlevels*sizeof(uint64_t));
    ilst = (uint64_t*)MFU_MALLOC(nlevels*sizeof(uint64_t));
    uint64_t ist=0;
    int ilev;
    for (ilev=0;ilev<nlevels;ilev++) 
    {
       ifst[ilev] = ist;
       ilst[ilev] = ist+nfiles[ilev];
       ist = ist+nfiles[ilev];
    }

    int tnamlen = PATH_MAX;
    int i,j;  
    for (i=0;i<linktot;i++)
    {
        for (ilev=0;ilev<nlevels;ilev++)
        {
            if (targIDs[i]>=ifst[ilev] && targIDs[i]<ilst[ilev]) break;
        }
        j = (int)(targIDs[i]-ifst[ilev]);    
        strcpy(tarray[i],tnames[ilev]+j*tnamlen);
    }
}
static void write_links(int nlink, char* linknames, char* targnames)
//-------------------------------------------------------------
// writes links to file, dirs, and other links for one proc
//--------------------------------------------------------------
{
    /* get our rank and number of ranks in job */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);
    if (rank==0) MFU_LOG(MFU_LOG_INFO, "Creating and writing links.");

    int i;
    int lnamlen = PATH_MAX;
    int tnamlen = PATH_MAX;
    for (i=0;i<nlink;i++)
    {
        const char* link_path = linknames+i*lnamlen;
        const char* targ_path = targnames+i*tnamlen;
        //printf("from write_links: mfu_symlink(%s,%s)\n",targ_path,link_path);
        int linkdesc = mfu_symlink(targ_path, link_path);
    }

    return;
     
}
void dnamsort(char **buff,int nitems)
//----------------------------------------
// sort directories by base (last) name
//----------------------------------------
{
   int i,j;
   char *cp1,*cp2,*cptemp;
   for (j=nitems-1;j>=1;j--)
   {
      for (i=0;i<j;i++)
      {
          cp1 = 1 + strrchr(buff[i],'/');
          cp2 = 1 + strrchr(buff[j],'/');
          if (strcmp(cp1,cp2)>0) 
          {
             cptemp=buff[i];
             buff[i]=buff[j];
             buff[j]=cptemp;
          }
      }
   }
}
void tnamsort(char **buff,int nitems)
//----------------------------------------
// sort targets by number at end of name
//----------------------------------------
{
   int i,j;
   char *cp1,*cp2,*cptemp;
   for (j=nitems-1;j>=1;j--)
   {
      for (i=0;i<j;i++)
      {
          cp1 = 1 + strrchr(buff[i],'_');
          cp2 = 1 + strrchr(buff[j],'_');
          if (strcmp(cp1,cp2)>0) 
          {
             cptemp=buff[i];
             buff[i]=buff[j];
             buff[j]=cptemp;
          }
      }
   }
}
void lnamsort(char **buff,int nitems,int* lind)
//----------------------------------------
// sort links by base (last) name
// and an order index lind with them
//----------------------------------------
{
   int i,j,ltemp;
   char *cp1,*cp2,*cptemp;
   for (j=nitems-1;j>=1;j--)
   {
      for (i=0;i<j;i++)
      {
          cp1 = 1 + strrchr(buff[i],'/');
          cp2 = 1 + strrchr(buff[j],'/');
          if (strcmp(cp1,cp2)>0) 
          {
             cptemp=buff[i];
             ltemp = lind[i];
             buff[i]=buff[j];
             lind[i]=lind[j];
             buff[j]=cptemp;
             lind[j]=ltemp;
          }
      }
   }
}
void lnamunsort(char **buff,char** tarray,int* lind,int nitems)
//----------------------------------------
//  unsort link names and targIDs
//  with order index to restore order
//----------------------------------------
{
   int i,j;
   int tempi;
   char *cptemp;
   char *cptemp2;
   for (j=nitems-1;j>=1;j--)
   {
      for (i=0;i<j;i++)
      {
          if (lind[i]>lind[j]) 
          {
             tempi=lind[i];
             cptemp=buff[i];
             cptemp2=tarray[i];
             lind[i]=lind[j];
             buff[i]=buff[j];    
             tarray[i]=tarray[j];
             lind[j]=tempi;
             buff[j]=cptemp;
             tarray[j]=cptemp2;
          }
      }
   }
}


main(int narg, char **arg)
/*****************************************************************
/*
/*  Create trees of directories, files, links
/*  Usage: dfmake <numitems> <relmaxdepth> <maxflength>
/*         where 
/*         numitems    = total number of dirs, files, links
/*         relmaxdepth = maximum directory depth rel to start
/*         maxflength  = maximum length of any regular file
/*
/****************************************************************/
{
    char *cbuff;
    uint64_t i,j,ifst,ilst;
    int namlen;
    long int *ftypes,*flens;
    long int maxflen=1024L;
    elem_t ***element; // elements at top level, second level down
    elem_t *elmout;
    int ifrac,*nfiles;  // nfiles is number of files at levels from 0 top
    int ntotal;
    int nfsum=0;
    int nlevels=2,nsum,ilev; // number of levels with top (./)
    int outlevels,outmin;
    mfu_flist* outlists;
    char *cp;
    int homedepth;
    char *dirname;
    unsigned int iseed=1;
    uint64_t idx; 
    uint64_t *idlist;
    int ndir=0,*ndirs;
    uint64_t size,gsize,goffset;
    uint64_t* randir;
    int dirtot;
    int linktot;
    int nitot;
    int dfirst,dlast;
    uint64_t *idflist,*idllist;
    int root = 0;
    int dnamlen; // directory name lengths
    int lnamlen; // link name lengths
    int tnamlen; // length of all items
    int *lndirs,*ddispls; // directory displacements for each proc
    int *lnlinks,*ldispls; // directory displacements for each proc
    int *lnitems,*tdispls; // directory displacements for each proc
    char *ldnames,*dnames; // lists of directory paths
    char** darray;  
    char** larray;
    char** tarray;
    char *lnames; // global lists of link names
    char **tnames; // global lists of items as targets over all levels
    int nlink, *linksg;
    int nitem, *itemsg;
    uint64_t *targIDs;  // global indices of things that links point to for a dir level 
    int* nlinksg;  // number of links for each rank bcast to all procs
    uint64_t *gidlist; // global (gathered) link array
    char* tnamelist; // list of path names of items associated with targIDs
    int *lind; // list of ints in order to resort things
    int initsum,noff;

    /*--------------------------
    /* initialize mfu and MPI 
    /*--------------------------*/
    MPI_Init(&narg, &arg);
    mfu_init();
    mfu_debug_level = MFU_LOG_VERBOSE;

    int rank, nrank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nrank);

    /*----------------------------------------------
    /* get nfiles = number of files to create basic
    /*----------------------------------------------*/
    if (narg < 4)
    {
       if (rank == 0) printf("Usage: dfmake <nfiles> <nlevels> <maxflen>\n");
       MPI_Finalize();
       exit(0);
    }
    ntotal = atoi(arg[1]);
    nlevels = atoi(arg[2]);
    maxflen = atoi(arg[3]);

    /*-------------------------------------------------------
    /* each level has nfiles[0] more than the one above
    /* on this first pass
    /*-------------------------------------------------------*/
    nsum = nlevels*(nlevels+1)/2;
    nfiles = (int *)MFU_MALLOC(nlevels*sizeof(int));
    nfiles[0] = ntotal/nsum; 
    if (nfiles[0] < 1)
    {
       if (rank == 0) printf("nfiles must be greater than nlevels\n");
       MPI_Finalize();
       exit(0);
    }
    for (ilev=1;ilev<nlevels;ilev++) nfiles[ilev] = (ilev+1)*nfiles[0];

    //------------------------------------------------------------
    // adjust nfiles for levels so they sum to ntotal
    //------------------------------------------------------------
    initsum=0;
    for (ilev=0;ilev<nlevels;ilev++) initsum+=nfiles[ilev];
    if (initsum<ntotal)
    {
       noff=ntotal-initsum;
       for (i=0;i<100;i++)
       {  
          for (ilev=0;ilev<nlevels;ilev++)
          {
                nfiles[ilev]+=1;
                noff--;
                if (noff==0) break;
          }
          if (noff==0) break;
       }
    }

    //-------------------------------------
    // instantiate highest level element
    //------------------------------------- 
    element = (elem_t ***)MFU_MALLOC(nlevels*sizeof(elem_t **));

    //-----------------------
    // fill buff with stuff
    //-----------------------
    buf = MFU_MALLOC(bufsize);

    //-----------------------------------
    // get depth of './' or top 
    //-----------------------------------
    ifrac = (nfiles[0]+nrank-1)/nrank;
    dirname = (char*)MFU_MALLOC(PATH_MAX+1);
    mfu_getcwd(dirname,PATH_MAX);
    homedepth=get_depth(dirname);
    if (rank == 0)  printf("depth of home = %d\n", homedepth);

    //---------------------------------------------------
    // instantiate flist and elements of flist top level
    //---------------------------------------------------
    element[0] = (elem_t**)MFU_MALLOC(nfiles[0]*sizeof(elem_t*));
    mfu_flist mybflist = mfu_flist_new();
    flist_t* flist = (flist_t*) mybflist; // for next line
    flist->detail=1;                      // important to set this
    ifst = rank*ifrac;
    ilst = (rank+1)*ifrac;
    if (nfiles[0]<ilst) ilst=nfiles[0];

    //------------------------------------------------------
    // set properties of elements and add to flist for proc
    //------------------------------------------------------
    ftypes = (long int *)MFU_MALLOC(nfiles[0]*sizeof(long int));
    flens = (long int *)MFU_MALLOC(nfiles[0]*sizeof(long int));
    srand(iseed);
    for (i=0;i<ifst;i++) rand();
    for (i=ifst;i<ilst;i++) ftypes[i] = rand()%3+2;
    srand(iseed+100);
    for (i=0;i<ifst;i++) rand();
    for (i=ifst;i<ilst;i++) flens[i] = rand()%maxflen;
    cbuff = (char*)MFU_MALLOC(strlen(dirname)+20*sizeof(char));

    for (i=ifst;i<ilst;i++) 
    {
          setname(cbuff,ftypes[i],i,dirname);
          element[0][i] = (elem_t*)MFU_MALLOC(sizeof(elem_t));
          fillelem(element[0][i],cbuff,homedepth+1,flens[i],ftypes[i]);
          list_insert_elem(mybflist, element[0][i]);
    }

    //----------------------------
    // generate summary of flist
    //-----------------------------
    MPI_Barrier(MPI_COMM_WORLD);
    list_compute_summary(mybflist);
    print_summary(mybflist);
    MPI_Barrier(MPI_COMM_WORLD);
    free(ftypes);
    free(flens);
    free(cbuff);


//**********************************************************************************************************
    const char* directory_name=(char*)MFU_MALLOC(PATH_MAX+1);
    char* dir_name=(char*)MFU_MALLOC(PATH_MAX+1);
    lndirs = (int*)malloc(nrank*sizeof(int));
    ddispls = (int*)malloc(nrank*sizeof(int));
    for (ilev=1;ilev<nlevels;ilev++)
    {
       if (rank == 0) printf("ilev = %d\n",ilev);
       nfsum += nfiles[ilev-1];
       ifrac = (nfiles[ilev]+nrank-1)/nrank;
       ifst = rank*ifrac;
       ilst = (rank+1)*ifrac;
       if (nfiles[ilev]<ilst) ilst=nfiles[ilev];

       //---------------------------------------------------------
       /* get number of levels and number of files at each level */
       //---------------------------------------------------------
       mfu_flist_array_by_depth(mybflist, &outlevels, &outmin, &outlists);
       // if (rank==0) printf("\nnum levels: %d\nminlevel: %d\n\n",outlevels,outmin);
   
       //-------------------------------------------------
       // get list of items for this level
       //-------------------------------------------------
       mfu_flist list = outlists[ilev-1];
       size = mfu_flist_size(list);
       gsize = mfu_flist_global_size(list);
       goffset = mfu_flist_global_offset(list);
       // printf("ilev=%d, rank=%d, size=%d, gsize=%d, goffset=%d\n",ilev,rank,size,gsize,goffset);
   
       //------------------------------------------------
       // list each directory for this level 
       //-------------------------------------------------
       idlist = (uint64_t*)MFU_MALLOC(nfiles[ilev-1]*sizeof(uint64_t)); // directory id's for proc
       ndir=0;
       for (idx = 0; idx < size; idx++)
       {
          /* check whether we have a directory */
           mfu_filetype type = mfu_flist_file_get_type(list, idx);
           if (type == MFU_TYPE_DIR) 
           {
                /* print dir name */
                directory_name=mfu_flist_file_get_name(list, idx);
                idlist[ndir]=idx;
                ndir++;
           }
       }
//       printf("ndir for rank %d = %d, %s\n",rank,ndir,directory_name);
       MPI_Barrier(MPI_COMM_WORLD);
       dnamlen = strlen(dirname)+20*ilev; // length of paths above this level
       ldnames = (char*)MFU_MALLOC(ndir*dnamlen); // array to hold path list
       for (i=0;i<ndir;i++) 
       {
            idx=idlist[i];
            directory_name=mfu_flist_file_get_name(list, idx);
            strcpy(ldnames+i*dnamlen,directory_name);
       }
   
       //-----------------------------------------------------------
       // count total directories at this level (dirtot)
       // get first and last dir for this processor at this level
       // randomly selected indices stored in randir
       //-----------------------------------------------------------
       ndirs = (int*)MFU_MALLOC(nrank*sizeof(int)); // array to hold ndir for all procs
       MPI_Allgather(&ndir, 1, MPI_INT, ndirs, 1, MPI_INT, MPI_COMM_WORLD);
       dirtot=0;
       for (i=0;i<nrank;i++) dirtot+=ndirs[i];  // could use MPI_Allreduce for this
       dnames = (char*)MFU_MALLOC(dirtot*dnamlen); // length of names of all dirs in level above
       for (i=0;i<nrank;i++) lndirs[i]=ndirs[i]*dnamlen; // total length of all dirnames on this proc
       ddispls[0] = 0;
       for (i=1;i<nrank;i++) ddispls[i] = ddispls[i-1]+lndirs[i-1]; // displacements of dirnames
       MPI_Allgatherv(ldnames,ndir*dnamlen,MPI_CHAR,dnames,lndirs,ddispls,MPI_CHAR,MPI_COMM_WORLD);

       //---------------------------------------------------------------------
       // sort dnames so that randir always points to same directory path
       //---------------------------------------------------------------------
       darray = (char**)MFU_MALLOC(dirtot*sizeof(char*));
       for (i=0;i<dirtot;i++) darray[i]=(char*)MFU_MALLOC(dnamlen*sizeof(char));
       for (i=0;i<dirtot;i++) strncpy(darray[i],dnames+i*dnamlen,dnamlen);
       dnamsort(darray,dirtot);
       for (i=0;i<dirtot;i++) strncpy(dnames+i*dnamlen,darray[i],dnamlen);
       // if (rank==0) for (i=0;i<dirtot;i++) printf("%s\n",dnames+i*dnamlen);
       
       
       //-----------------------------------------------------------
       // get first and last dir for this processor at this level
       // randomly selected indices stored in randir
       //-----------------------------------------------------------
       dfirst=0;
       dlast=0;
       for (i=0;i<rank;i++) dfirst+=ndirs[i]; // index of first dir for processor
       for (i=0;i<rank+1;i++) dlast+=ndirs[i]; // index of last dir for proc
       // printf("rank=%d, dfirst = %d, dlast = %d, dirtot = %d\n",rank,dfirst,dlast,dirtot);
       randir=(uint64_t*)MFU_MALLOC(nfiles[ilev]*sizeof(uint64_t));
       ftypes = (long int *)MFU_MALLOC(nfiles[ilev]*sizeof(long int));
       flens = (long int *)MFU_MALLOC(nfiles[ilev]*sizeof(long int));
       srand(iseed+1);
       for (i=0;i<nfiles[ilev];i++) randir[i]=(uint64_t)(rand()%dirtot); // was 305
       srand(iseed+2);
       for (i=0;i<nfiles[ilev];i++) ftypes[i]=(long int)(rand()%3+2);   // item type f,d,l
       srand(iseed+3);
       for (i=0;i<nfiles[ilev];i++) flens[i]=(long int)(rand()%maxflen);   // item length could be zero
/*
       if (rank==0) 
       {
          printf("randir values for rank=0, nfiles=%d:\n",nfiles[ilev]);
          for (i=0;i<nfiles[ilev]-1;i++) printf("%d ",randir[i]);
          printf("%d\n",randir[nfiles[ilev]-1]);
       }
*/
         
       //------------------------------------------------------------------------
       // get type of object and put in element if there is a directory for it
       //------------------------------------------------------------------------
       namlen = strlen(directory_name)+20*sizeof(char);
       cbuff=(char*)MFU_MALLOC(namlen);
       element[ilev] = (elem_t**)MFU_MALLOC(nfiles[ilev]*sizeof(elem_t*));
       for (i=0;i<nfiles[ilev];i++) if (randir[i]>=dfirst && randir[i]<dlast)
       {
           strcpy(dir_name,dnames+randir[i]*dnamlen);
           //printf("dir_name = %s\n",dir_name);
           homedepth=get_depth(dir_name);
           // printf("depth = %d, ftypes[%d] = %li\n",homedepth,i,ftypes[i]);
           setname(cbuff,ftypes[i],i+nfsum,dir_name);
           //printf("cbuff = %s\n",cbuff);
           element[ilev][i] = (elem_t*)MFU_MALLOC(sizeof(elem_t));
           fillelem(element[ilev][i],cbuff,homedepth+1,flens[i],ftypes[i]);
           list_insert_elem(mybflist, element[ilev][i]);
       }
       free(cbuff);

       //-----------------------------------------------------------
       // pass data for all new elements in RANK ORDER to proc 0
       //-----------------------------------------------------------
       MPI_Barrier(MPI_COMM_WORLD);
   
       //----------------------------
       // generate summary of flist
       //-----------------------------
       total_dirs    = 0;
       total_files   = 0;
       total_links   = 0;
       total_unknown = 0;
       total_bytes   = 0;
       list_compute_summary(mybflist);
       print_summary(mybflist);
       MPI_Barrier(MPI_COMM_WORLD);
       free(randir);
       free(ftypes);
       free(flens);
       free(idlist);
       free(ndirs);
       free(ldnames);
       free(dnames);
       free(darray);
   
       //---------------------------------------------------------
       /* get number of levels and number of files at each level */
       //---------------------------------------------------------
       mfu_flist_array_by_depth(mybflist, &outlevels, &outmin, &outlists);
       // if ( rank == 0 ) printf("\nnum levels: %d\nminlevel: %d\n\n",outlevels,outmin);
       iseed+=3;
    }  // end of ilev loop for creating files and directories

//**********************************************************************************************************
    //--------------------------------
    //  create directories and files 
    //---------------------------------
     create_directories_jl(outlevels, outmin, outlists);
     create_files(outlevels, outmin, outlists);
     write_files(outlevels, outmin, outlists);

    //------------------------------------
    //  reset statistics at this point
    //  before writing links 
    //------------------------------------
     total_dirs    = 0;
     total_files   = 0;
     total_links   = 0;
     total_unknown = 0;
     total_bytes   = 0;
     // print_summary(mybflist);
     // printf("rank = %d, total_files = %d\n",rank,total_files);


//*****************************************************************************
//
//   make lists of link target items for each level, sorted by number in name 
//   targets may be files, directories, or links
//
//*****************************************************************************
    char* itemnames; // local to proc 
    tnames = (char**)MFU_MALLOC(nlevels*sizeof(char*));
    itemsg = (int*)MFU_MALLOC(nrank*sizeof(int));
    const char* item_name=(char*)MFU_MALLOC(PATH_MAX+1);
    lnitems = (int*)malloc(nrank*sizeof(int));
    tdispls = (int*)malloc(nrank*sizeof(int));

 
    //---------------------------------------------------------
    /* get number of levels and number of files at each level */
    //---------------------------------------------------------
    mfu_flist_array_by_depth(mybflist, &outlevels, &outmin, &outlists);
    for (ilev=0;ilev<nlevels;ilev++)
    {
       //------------------------------------------------
       // list items at this level for each processor
       //------------------------------------------------
       mfu_flist list = outlists[ilev];
       size = mfu_flist_size(list);
       gsize = mfu_flist_global_size(list);
       goffset = mfu_flist_global_offset(list);
       // printf("ilev=%d, rank=%d, size=%d, gsize=%d, goffset=%d\n",ilev,rank,size,gsize,goffset);

       //----------------------------------------------------
       // get number everthing at this level on a processor
       //----------------------------------------------------
       tnamlen = PATH_MAX;
       itemnames = (char*)MFU_MALLOC(size*tnamlen);
       nitem = 0;
       for (idx = 0; idx < size; idx++)
       {
             item_name=mfu_flist_file_get_name(list, idx);
             strcpy(itemnames+nitem*tnamlen,item_name);
             nitem++;
       }
       MPI_Barrier(MPI_COMM_WORLD);

       //-----------------------------------------------
       // make global array of all items at this level
       //-----------------------------------------------
       MPI_Allgather(&nitem, 1, MPI_INT, itemsg, 1, MPI_INT, MPI_COMM_WORLD);  // fill itemsg
       nitot = 0.;
       for (i=0;i<nrank;i++) nitot+=itemsg[i];
 //      printf("nitot = %d, gsize = %d, nfiles[%d] = %d\n",nitot,gsize,ilev,nfiles[ilev]); // compare sum with global size, hope same
       tdispls[0] = 0;
       for (i=0;i<nrank;i++) lnitems[i]= itemsg[i]*tnamlen; // total length of all item names on each proc
       for (i=1;i<nrank;i++) tdispls[i] = tdispls[i-1]+lnitems[i-1]; // rel. displacements of item names for each proc
       tnames[ilev] = (char*)MFU_MALLOC(nitot*tnamlen); // length of names of all items in this level
       MPI_Allgatherv(itemnames,nitem*tnamlen,MPI_CHAR,tnames[ilev],lnitems,tdispls,MPI_CHAR,MPI_COMM_WORLD);
       
       //-------------------------
       // sort tnames[ilev]
       //-------------------------
       tarray = (char**)MFU_MALLOC(nitot*sizeof(char*));
       for (i=0;i<nitot;i++) tarray[i]=(char*)MFU_MALLOC(tnamlen*sizeof(char));
       for (i=0;i<nitot;i++) strncpy(tarray[i],tnames[ilev]+i*tnamlen,tnamlen);
     //if (rank==0) for (i=0;i<nitot;i++) printf("%s\n",tarray[i]);
       tnamsort(tarray,nitot);
       for (i=0;i<nitot;i++) strncpy(tnames[ilev]+i*tnamlen,tarray[i],tnamlen);
       for (i=0;i<nitot;i++) free(tarray[i]);
       free(itemnames);
       free(tarray);
       MPI_Barrier(MPI_COMM_WORLD);
    }

//**********************************************************************************************************
//
//  set up links pointing to any file, dir, or other link
//  must be indep of number of processor
//
//**********************************************************************************************************
    char* linknames; // local to proc 
    const char* link_name=(char*)MFU_MALLOC(PATH_MAX+1);
    lnamlen = PATH_MAX;
    lnlinks = (int*)malloc(nrank*sizeof(int));
    ldispls = (int*)malloc(nrank*sizeof(int));

    //--------------------------------
    // generate links
    //--------------------------------
    for (ilev=0;ilev<nlevels;ilev++)
    {
       if (rank==0) printf("ilev=%d\n",ilev);

       /*--------------------------------------------------------*/
       /* get number of levels and number of files at each level */
       /*--------------------------------------------------------*/
       mfu_flist_array_by_depth(mybflist, &outlevels, &outmin, &outlists);

       //------------------------------------------------
       // list items at this level for each processor
       //------------------------------------------------
       mfu_flist list = outlists[ilev];
       size = mfu_flist_size(list);
       gsize = mfu_flist_global_size(list);
       goffset = mfu_flist_global_offset(list);
       // printf("ilev=%d, rank=%d, size=%d, gsize=%d, goffset=%d\n",ilev,rank,size,gsize,goffset);

       //------------------------------------------------
       // get  number links at this level on a processor
       //-------------------------------------------------
       nlink=0;
       for (idx = 0; idx < size; idx++)
       {
           mfu_filetype type = mfu_flist_file_get_type(list, idx);
           if (type == MFU_TYPE_LINK) nlink++;
       }
       linknames = (char*)MFU_MALLOC(nlink*lnamlen);
       nlink = 0;
       for (idx = 0; idx < size; idx++)
       {
           mfu_filetype type = mfu_flist_file_get_type(list, idx);
           if (type == MFU_TYPE_LINK) 
           {
               link_name=mfu_flist_file_get_name(list, idx);
               strcpy(linknames+nlink*lnamlen,link_name);
               nlink++;
           }
       }
       //if (rank==0)for (i=0;i<nlink;i++) printf("%d %s\n",rank,linknames+i*lnamlen);
       MPI_Barrier(MPI_COMM_WORLD);

       //----------------------------------------------------------
       // make larray, global array of linknames at this dir level
       // should be indep of number of processes
       //----------------------------------------------------------
       srand(200+iseed);  // seed for list of links
       nlinksg = (int*)MFU_MALLOC(nrank*sizeof(int));  // global array of number of links on each proc
       MPI_Allgather(&nlink, 1, MPI_INT, nlinksg, 1, MPI_INT, MPI_COMM_WORLD);  // fill nlinksg
       linktot=0;  // total number of links over all procs at this directory level
       for (i=0;i<nrank;i++) linktot+=nlinksg[i];  // could use MPI_Allreduce for this
       targIDs = (uint64_t*)MFU_MALLOC(linktot*sizeof(uint64_t));
       ldispls[0] = 0;
       for (i=0;i<nrank;i++) lnlinks[i]= nlinksg[i]*lnamlen; // total length of all linknames on each proc
       for (i=1;i<nrank;i++) ldispls[i] = ldispls[i-1]+lnlinks[i-1]; // rel. displacements of link names for each proc
       lnames = (char*)MFU_MALLOC(linktot*lnamlen); // length of names of all links in this level
       MPI_Allgatherv(linknames,nlink*lnamlen,MPI_CHAR,lnames,lnlinks,ldispls, MPI_CHAR, MPI_COMM_WORLD);

       //-----------------------------------------------
       // create global lnames and separate index array
       //-----------------------------------------------
       larray = (char**)MFU_MALLOC(linktot*sizeof(char*));
       for (i=0;i<linktot;i++) larray[i]=(char*)MFU_MALLOC(lnamlen*sizeof(char));
       for (i=0;i<linktot;i++) strncpy(larray[i],lnames+i*lnamlen,lnamlen);
       lind = (int*)MFU_MALLOC(linktot*sizeof(int)); // link index to permit unsorting later
       for (i=0;i<linktot;i++) lind[i]=i;        // set 0 to linktot

       //---------------------------------------------------------
       // assign random target ID to each linkname is sorted list
       //----------------------------------------------------------
       for (i=0;i<linktot;i++) targIDs[i]=rand()%ntotal;  // idx from 1 to tot number of items

       //----------------------------------------------------------------
       // sort linknames and associate actual target names with targIDs
       //-----------------------------------------------------------------
       lnamsort(larray,linktot,lind);
       tarray = (char**)MFU_MALLOC(linktot*sizeof(char*));
       for (i=0;i<linktot;i++) tarray[i] = (char*)MFU_MALLOC(tnamlen*sizeof(char));
       MPI_Barrier(MPI_COMM_WORLD);
   
       //------------------------
       // create target names
       //-------------------------
       create_targets(nlevels, linktot, nfiles, targIDs, tnames, tarray);

       //-----------------------------------------
       // unsort links and targIDs with them 
       // will associate targID's with tnames
       //-----------------------------------------
       lnamunsort(larray,tarray,lind,linktot);
       for (i=0;i<linktot;i++) strncpy(lnames+i*lnamlen,larray[i],lnamlen);
       free(larray);
       tnamelist = (char*)MFU_MALLOC(linktot*tnamlen); // length of names of all links in this level
       for (i=0;i<linktot;i++) strncpy(tnamelist+i*tnamlen,tarray[i],tnamlen);
       free(tarray);
       MPI_Barrier(MPI_COMM_WORLD);

       /*--------------------------------------------------------------------------*/
       /* scatterv linknames (lnames) and target names (tarray)to each processor   */
       /* reuse and redefine lnitems, tdispls,itemnames with length for linklist   */
       /*--------------------------------------------------------------------------*/
       MPI_Scatterv(lnames,   lnlinks,ldispls,MPI_CHAR,linknames,nlink*lnamlen,MPI_CHAR,0,MPI_COMM_WORLD);
       for (i=0;i<nrank;i++) lnitems[i]= nlinksg[i]*tnamlen; // total length of all linknames on each proc
       tdispls[0] = 0;
       for (i=1;i<nrank;i++) tdispls[i] = tdispls[i-1]+lnitems[i-1]; // rel. displacements of link names for each proc
       itemnames = (char*)MFU_MALLOC(nlink*tnamlen*sizeof(char));
       MPI_Scatterv(tnamelist,lnitems,tdispls,MPI_CHAR,itemnames,nlink*tnamlen,MPI_CHAR,0,MPI_COMM_WORLD);
       MPI_Barrier(MPI_COMM_WORLD);

       /*---------------------------------*/
       /* write links with targets        */
       /*---------------------------------*/
       write_links(nlink,linknames,itemnames); // write links for this processor
       free(linknames);
       free(tnamelist);
       free(nlinksg);
       free(targIDs);
       free(lnames);
       free(lind);
       free(itemnames);
       iseed++;
       MPI_Barrier(MPI_COMM_WORLD);
   } /* end of ilev loop for links */
   free(tnames);


//****************************************************************************************************

    /*------------
    /*  delete
    /*------------*/
    if (rank == 0 ) printf("about to free mybflist\n");
    mfu_flist_free((void**)&mybflist);

    mfu_finalize();
    MPI_Finalize();

}
