#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h> /* asctime / localtime */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"

#include <map>
#include <string>

using namespace std;

/****************************************
 * Define types
 ***************************************/

/* linked list element of stat data used during walk */
typedef struct list_elem {
  char* file;             /* file name */
  bayer_filetype type;         /* record type of object */
  struct stat* sb;        /* stat info */
  struct list_elem* next; /* pointer to next item */
} elem_t;

/* holds an array of objects: users, groups, or file data */
typedef struct {
  void* buf;
  uint64_t count;
  uint64_t chars;
  MPI_Datatype dt;
} buf_t;

/* abstraction for distributed file list */
typedef struct flist {
  int detail;           /* set to 1 if we have stat, 0 if just file name */
  uint64_t total_files; /* total file count in list across all procs */

  /* variables to track linked list of stat data during walk */
  uint64_t stat_count;
  elem_t*  stat_head;
  elem_t*  stat_tail;

  /* buffers of users, groups, and files */
  buf_t users;
  buf_t groups;
  buf_t files;

  size_t file_elem_size; /* number of bytes of single element in files buf_t */

  /* map linux userid to user name, and map groupid to group name */
  map<string,uint32_t>* user_name2id;
  map<uint32_t,string>* user_id2name;
  map<string,uint32_t>* group_name2id;
  map<uint32_t,string>* group_id2name;
} flist_t;

/****************************************
 * Globals
 ***************************************/

/* Need global variables during walk to record top directory
 * and file list */
static char CURRENT_DIR[PATH_MAX];
static flist_t* CURRENT_LIST;

/****************************************
 * Functions on types
 ***************************************/

static void buft_init(buf_t* items)
{
  items->buf   = NULL;
  items->count = 0;
  items->chars = 0;
  items->dt    = MPI_DATATYPE_NULL;
}

static void buft_free(buf_t* items)
{
  bayer_free(&items->buf);

  if (items->dt != MPI_DATATYPE_NULL) {
    MPI_Type_free(&(items->dt));
  }

  items->count = 0;
  items->chars = 0;

  return;
}

static bayer_filetype get_bayer_filetype(mode_t mode)
{
  /* set file type */
  bayer_filetype type;
  if (S_ISDIR(mode)) {
    type = TYPE_DIR;
  } else if (S_ISREG(mode)) {
    type = TYPE_FILE;
  } else if (S_ISLNK(mode)) {
    type = TYPE_LINK;
  } else {
    /* unknown file type */
    type = TYPE_UNKNOWN;
  }
  return type;
}

/* appends file name and stat info to linked list */
static void stat_insert(flist_t* flist, const char *fpath, const struct stat *sb, mode_t mode)
{
  /* create new element to record file path, file type, and stat info */
  elem_t* elem = (elem_t*) bayer_malloc(sizeof(elem_t), "File element", __FILE__, __LINE__);

  /* copy path */
  elem->file = bayer_strdup(fpath, "File name", __FILE__, __LINE__);

  /* set file type */
  elem->type = get_bayer_filetype(mode);

  /* copy stat info */
  if (sb != NULL) {
    elem->sb = (struct stat*) bayer_malloc(sizeof(struct stat), "Stat struct", __FILE__, __LINE__);
    memcpy(elem->sb, sb, sizeof(struct stat));
  } else {
    elem->sb = NULL;
  }

  /* append element to tail of linked list */
  elem->next = NULL;
  if (flist->stat_head == NULL) {
    flist->stat_head = elem;
  }
  if (flist->stat_tail != NULL) {
    flist->stat_tail->next = elem;
  }
  flist->stat_tail = elem;
  flist->stat_count++;

  return;
}

/* delete linked list of stat items */
static void stat_delete(flist_t* flist)
{
  elem_t* current = flist->stat_head;
  while (current != NULL) {
    elem_t* next = current->next;
    bayer_free(&current->file);
    bayer_free(&current->sb);
    bayer_free(&current);
    current = next;
  }

  return;
}

/****************************************
 * File list user API
 ***************************************/

/* create object that BAYER_FLIST_NULL points to */
static flist_t flist_null;
bayer_flist BAYER_FLIST_NULL = &flist_null;

/* initialize file list */
static bayer_flist bayer_flist_new()
{
  /* allocate memory for file list, cast it to handle, initialize and return */
  flist_t* flist = (flist_t*) bayer_malloc(sizeof(flist_t), "File list handle", __FILE__, __LINE__);

  flist->detail = 0;
  flist->total_files = 0;

  /* initialize linked list */
  flist->stat_count = 0;
  flist->stat_head  = NULL;
  flist->stat_tail  = NULL;

  /* initialize user, group, and file buffers */
  buft_init(&flist->users);
  buft_init(&flist->groups);
  buft_init(&flist->files);

  flist->file_elem_size = 0;

  /* allocate memory for maps */
  flist->user_name2id  = new map<string,uint32_t>;
  flist->user_id2name  = new map<uint32_t,string>;
  flist->group_name2id = new map<string,uint32_t>;
  flist->group_id2name = new map<uint32_t,string>;

  bayer_flist bflist = (bayer_flist) flist;
  return bflist;
}

/* free resouces in file list */
void bayer_flist_free(bayer_flist* pbflist)
{
  /* convert handle to flist_t */
  flist_t* flist = *(flist_t**)pbflist;

  buft_free(&flist->users);
  buft_free(&flist->groups);
  buft_free(&flist->files);

  delete flist->user_name2id;
  delete flist->user_id2name;
  delete flist->group_name2id;
  delete flist->group_id2name;

  bayer_free(&flist);

  /* set caller's pointer to NULL */
  *pbflist = BAYER_FLIST_NULL;

  return;
}

/* return number of files across all procs */
uint64_t bayer_flist_total_size(bayer_flist bflist)
{
  flist_t* flist = (flist_t*) bflist;
  uint64_t size = flist->total_files;
  return size;
}

/* return number of files in local list */
uint64_t bayer_flist_size(bayer_flist bflist)
{
  flist_t* flist = (flist_t*) bflist;
  uint64_t size = flist->files.count;
  return size;
}

/* return number of users */
uint64_t bayer_flist_user_count(bayer_flist bflist)
{
  flist_t* flist = (flist_t*) bflist;
  uint64_t count = flist->users.count;
  return count;
}

/* return number of groups */
uint64_t bayer_flist_group_count(bayer_flist bflist)
{
  flist_t* flist = (flist_t*) bflist;
  uint64_t count = flist->groups.count;
  return count;
}

/* return maximum length of file names */
uint64_t bayer_flist_file_max_name(bayer_flist bflist)
{
  flist_t* flist = (flist_t*) bflist;
  uint64_t count = flist->files.chars;
  return count;
}

/* return maximum length of user names */
uint64_t bayer_flist_user_max_name(bayer_flist bflist)
{
  flist_t* flist = (flist_t*) bflist;
  uint64_t count = flist->users.chars;
  return count;
}

/* return maximum length of group names */
uint64_t bayer_flist_group_max_name(bayer_flist bflist)
{
  flist_t* flist = (flist_t*) bflist;
  uint64_t count = flist->groups.chars;
  return count;
}

/* return max/min user,group,filename string
 * return max/min depth
 */

static char* get_file(flist_t* flist, int index)
{
  uint64_t max = flist->files.count;
  if (index >= 0 && index < max) {
    /* TODO: compute this once when creating list and store */
    /* compute element size */
    size_t elem_size;
    if (flist->detail == 0) {
      elem_size = flist->files.chars + 1 * sizeof(uint32_t);
    } else {
      elem_size = flist->files.chars + 6 * sizeof(uint32_t) + sizeof(uint64_t);
    }

    /* return pointer to file data */
    char* ptr = ((char*)flist->files.buf) + index * elem_size;
    return ptr;
  }
  return NULL;
}

int bayer_flist_file_name(bayer_flist bflist, int index, char** name)
{
  flist_t* flist = (flist_t*) bflist;
  char* ptr = get_file(flist, index);
  if (ptr != NULL) {
    /* for both detailed and non-detailed, pointer starts on file name */
    *name = ptr;
    return 0;
  }
  return -1;
}

/* given path, return level within directory tree */
static int compute_depth(const char* path)
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

int bayer_flist_file_depth(bayer_flist bflist, int index, int* depth)
{
  flist_t* flist = (flist_t*) bflist;
  char* ptr = get_file(flist, index);
  if (ptr != NULL) {
    /* for both detailed and non-detailed, pointer starts on file name */
    int d = compute_depth(ptr);
    *depth = d;
    return 0;
  }
  return -1;
}

int bayer_flist_file_type(bayer_flist bflist, int index, bayer_filetype* type)
{
  flist_t* flist = (flist_t*) bflist;
  char* ptr = get_file(flist, index);
  if (ptr != NULL) {
    /* advance pointer to second field */
    ptr += flist->files.chars;
    uint32_t val = *(uint32_t*)ptr;
    if (flist->detail == 0) {
      /* for a non-detailed record, we store the bayer_filetype */
      *type = (bayer_filetype) val;
    } else {
      /* for a detailed record, we store the mode_t */
      mode_t mode = (mode_t) val;
      bayer_filetype t = get_bayer_filetype(mode);
      *type = t;
    }
    return 0;
  }
  return -1;
}

int bayer_flist_file_mode(bayer_flist bflist, int index, mode_t* mode)
{
  flist_t* flist = (flist_t*) bflist;
  if (flist->detail > 0) {
    char* ptr = get_file(flist, index);
    if (ptr != NULL) {
      /* advance pointer to second field */
      ptr += flist->files.chars;
      uint32_t val = *(uint32_t*)ptr;
      *mode = (mode_t) val;
      return 0;
    }
  }
  return -1;
}

/****************************************
 * Walk directory tree using stat at top level and readdir
 ***************************************/

static void walk_readdir_process_dir(char* dir, CIRCLE_handle* handle)
{
  /* TODO: may need to try these functions multiple times */
  DIR* dirp = bayer_opendir(dir);

  if (! dirp) {
    /* TODO: print error */
  } else {
    /* Read all directory entries */
    while (1) {
      /* read next directory entry */
      struct dirent* entry = bayer_readdir(dirp);
      if (entry == NULL) {
        break;
      }

      /* process component, unless it's "." or ".." */
      char* name = entry->d_name;
      if((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
        /* <dir> + '/' + <name> + '/0' */
        char newpath[CIRCLE_MAX_STRING_LEN];
        size_t len = strlen(dir) + 1 + strlen(name) + 1;
        if (len < sizeof(newpath)) {
          /* build full path to item */
          strcpy(newpath, dir);
          strcat(newpath, "/");
          strcat(newpath, name);

          #ifdef _DIRENT_HAVE_D_TYPE
            /* record info for item */
            mode_t mode;
            int have_mode = 0;
            if (entry->d_type != DT_UNKNOWN) {
              /* we can read object type from directory entry */
              have_mode = 1;
              mode = DTTOIF(entry->d_type);
              stat_insert(CURRENT_LIST, newpath, NULL, mode);
            } else {
              /* type is unknown, we need to stat it */
              struct stat st;
              int status = bayer_lstat(newpath, &st);
              if (status == 0) {
                have_mode = 1;
                mode = st.st_mode;
                stat_insert(CURRENT_LIST, newpath, &st, mode);
              } else {
                /* error */
              }
            }

            /* recurse into directories */
            if (have_mode && S_ISDIR(mode)) {
              handle->enqueue(newpath);
            }
          #endif
        } else {
          /* TODO: print error in correct format */
          /* name is too long */
          printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
          fflush(stdout);
        }
      }
    }
  }

  bayer_closedir(dirp);

  return;
}

/** Call back given to initialize the dataset. */
static void walk_readdir_create(CIRCLE_handle* handle)
{
  char* path = CURRENT_DIR;

  /* stat top level item */
  struct stat st;
  int status = bayer_lstat(path, &st);
  if (status != 0) {
    /* TODO: print error */
    return;
  }

  /* record item info */
  stat_insert(CURRENT_LIST, path, &st, st.st_mode);

  /* recurse into directory */
  if (S_ISDIR(st.st_mode)) {
    walk_readdir_process_dir(path, handle);
  }

  return;
}

/** Callback given to process the dataset. */
static void walk_readdir_process(CIRCLE_handle* handle)
{
  /* in this case, only items on queue are directories */
  char path[CIRCLE_MAX_STRING_LEN];
  handle->dequeue(path);
  walk_readdir_process_dir(path, handle);
  return;
}

/****************************************
 * Walk directory tree using stat on every object
 ***************************************/

static void walk_stat_process_dir(char* dir, CIRCLE_handle* handle)
{
  /* TODO: may need to try these functions multiple times */
  DIR* dirp = bayer_opendir(dir);

  if (! dirp) {
    /* TODO: print error */
  } else {
    while (1) {
      /* read next directory entry */
      struct dirent* entry = bayer_readdir(dirp);
      if (entry == NULL) {
        break;
      }
       
      /* We don't care about . or .. */
      char* name = entry->d_name;
      if ((strncmp(name, ".", 2)) && (strncmp(name, "..", 3))) {
        /* <dir> + '/' + <name> + '/0' */
        char newpath[CIRCLE_MAX_STRING_LEN];
        size_t len = strlen(dir) + 1 + strlen(name) + 1;
        if (len < sizeof(newpath)) {
          /* build full path to item */
          strcpy(newpath, dir);
          strcat(newpath, "/");
          strcat(newpath, name);

          /* add item to queue */
          handle->enqueue(newpath);
        } else {
          /* TODO: print error in correct format */
          /* name is too long */
          printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
          fflush(stdout);
        }
      }
    }
  }

  bayer_closedir(dirp);

  return;
}

/** Call back given to initialize the dataset. */
static void walk_stat_create(CIRCLE_handle* handle)
{
  /* we'll call stat on every item */
  handle->enqueue(CURRENT_DIR);
}

/** Callback given to process the dataset. */
static void walk_stat_process(CIRCLE_handle* handle)
{
  /* get path from queue */
  char path[CIRCLE_MAX_STRING_LEN];
  handle->dequeue(path);

  /* stat item */
  struct stat st;
  int status = bayer_lstat(path, &st);
  if (status != 0) {
    /* print error */
    return;
  }

  /* TODO: filter items by stat info */

  /* record info for item in list */
  stat_insert(CURRENT_LIST, path, &st, st.st_mode);

  /* recurse into directory */
  if (S_ISDIR(st.st_mode)) {
    /* TODO: check that we can recurse into directory */
    walk_stat_process_dir(path, handle);
  }

  return;
}

/****************************************
 * Functions to read user and group info
 ***************************************/

/* create a type consisting of chars number of characters
 * immediately followed by a uint32_t */
static void create_stridtype(int chars, MPI_Datatype* dt)
{
  /* build type for string */
  MPI_Datatype dt_str;
  MPI_Type_contiguous(chars, MPI_CHAR, &dt_str);

  /* build keysat type */
  MPI_Datatype types[2];
  types[0] = dt_str;       /* file name */
  types[1] = MPI_UINT32_T; /* id */
  DTCMP_Type_create_series(2, types, dt);

  MPI_Type_free(&dt_str);
  return;
}

/* element for a linked list of name/id pairs */
typedef struct strid {
  char* name;
  uint32_t id;
  struct strid* next;
} strid_t;

/* insert specified name and id into linked list given by
 * head, tail, and count, also increase maxchars if needed */
static void strid_insert(
  const char* name,
  uint32_t id,
  strid_t** head,
  strid_t** tail,
  int* count,
  int* maxchars)
{
  /* add username and id to linked list */
  strid_t* elem = (strid_t*) malloc(sizeof(strid_t));
  elem->name = strdup(name);
  elem->id = id;
  elem->next = NULL;
  if (*head == NULL) {
    *head = elem;
  }
  if (*tail != NULL) {
    (*tail)->next = elem;
  }
  *tail = elem;
  (*count)++;

  /* increase maximum username if we need to */
  size_t len = strlen(name) + 1;
  if (*maxchars < (int)len) {
    /* round up to nearest multiple of 4 */
    size_t len4 = len / 4;
    if (len4 * 4 < len) {
      len4++;
    }
    len4 *= 4;

    *maxchars = (int)len4;
  }

  return;
}

/* copy data from linked list to array */
static void strid_serialize(strid_t* head, int chars, void* buf)
{
  char* ptr = (char*)buf;
  strid_t* current = head;
  while (current != NULL) {
    char* name  = current->name;
    uint32_t id = current->id;

    strcpy(ptr, name);
    ptr += chars;

    uint32_t* p32 = (uint32_t*) ptr;
    *p32 = id;
    ptr += 4;

    current = current->next;
  }
  return;
}

/* build a name-to-id map and an id-to-name map */
static void create_maps(
  const buf_t* items,
  map<string,uint32_t>* name2id,
  map<uint32_t,string>* id2name)
{
  int i;
  const char* ptr = (const char*)items->buf;
  for (i = 0; i < items->count; i++) {
    const char* name = ptr;
    ptr += items->chars;

    uint32_t id = *(uint32_t*)ptr;
    ptr += 4;

    (*name2id)[name] = id;
    (*id2name)[id] = name;
  }
  return;
}

/* given an id, lookup its corresponding name, returns id converted
 * to a string if no matching name is found */
static const char* get_name_from_id(uint32_t id, int chars, map<uint32_t,string>& id2name)
{
  map<uint32_t,string>::iterator it = id2name.find(id);
  if (it != id2name.end()) {
    const char* name = (*it).second.c_str();
    return name;
  } else {
    /* store id as name and return that */
    char temp[12];
    size_t len = snprintf(temp, sizeof(temp), "%d", id);
    if (len > (sizeof(temp) - 1) || len > (chars - 1)) {
      /* TODO: ERROR! */
      printf("ERROR!!!\n");
    }

    string newname = temp;
    id2name[id] = newname;

    it = id2name.find(id);
    if (it != id2name.end()) {
      const char* name = (*it).second.c_str();
      return name;
    } else {
      /* TODO: ERROR! */
      printf("ERROR!!!\n");
    }
  }
  return NULL;
}

/* delete linked list and reset head, tail, and count values */
static void strid_delete(strid_t** head, strid_t** tail, int* count)
{
  /* free memory allocated in linked list */
  strid_t* current = *head;
  while (current != NULL) {
    strid_t* next = current->next;
    bayer_free(&current->name);
    bayer_free(&current);
    current = next;
  }

  /* set list data structure values back to NULL */
  *head  = NULL;
  *tail  = NULL;
  *count = 0;

  return;
}

/* read user array from file system using getpwent() */
static void get_users(buf_t* items)
{
  /* initialize output parameters */
  buft_init(items);

  /* get our rank */
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /* rank 0 iterates over users with getpwent */
  strid_t* head = NULL;
  strid_t* tail = NULL;
  int count = 0;
  int chars = 0;
  if (rank == 0) {
    struct passwd* p;
    while (1) {
      /* get next user, this function can fail so we retry a few times */
      int retries = 3;
retry:
      p = getpwent();
      if (p == NULL) {
        if (errno == EIO) {
          retries--;
        } else {
          /* TODO: ERROR! */
          retries = 0;
        }
        if (retries > 0) {
          goto retry;
        }
      }

      if (p != NULL) {
        /*
        printf("User=%s Pass=%s UID=%d GID=%d Name=%s Dir=%s Shell=%s\n",
          p->pw_name, p->pw_passwd, p->pw_uid, p->pw_gid, p->pw_gecos, p->pw_dir, p->pw_shell
        );
        printf("User=%s UID=%d GID=%d\n",
          p->pw_name, p->pw_uid, p->pw_gid
        );
        */
        char* name  = p->pw_name;
        uint32_t id = p->pw_uid;
        strid_insert(name, id, &head, &tail, &count, &chars);
      } else {
        /* hit the end of the user list */
        endpwent();
        break;
      }
    }

//    printf("Max username %d, count %d\n", (int)chars, count);
  }

  /* bcast count and number of chars */
  MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(&chars, 1, MPI_INT, 0, MPI_COMM_WORLD);

  /* create datatype to represent a username/id pair */
  MPI_Datatype dt;
  create_stridtype(chars, &dt);

  /* get extent of type */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(dt, &lb, &extent);

  /* allocate an array to hold all user names and ids */
  char* buf = NULL;
  size_t bufsize = count * extent;
  if (bufsize > 0) {
    buf = (char*) malloc(bufsize);
  }

  /* copy items from list into array */
  if (rank == 0) {
    strid_serialize(head, chars, buf);
  }

  /* broadcast the array of usernames and ids */
  MPI_Bcast(buf, count, dt, 0, MPI_COMM_WORLD);

  /* set output parameters */
  items->buf   = buf;
  items->count = (uint64_t) count;
  items->chars = (uint64_t) chars; 
  items->dt    = dt;

  /* delete the linked list */
  if (rank == 0) {
    strid_delete(&head, &tail, &count);
  }

  return;
}

/* read group array from file system using getgrent() */
static void get_groups(buf_t* items)
{
  /* initialize output parameters */
  buft_init(items);

  /* get our rank */
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /* rank 0 iterates over users with getpwent */
  strid_t* head = NULL;
  strid_t* tail = NULL;
  int count = 0;
  int chars = 0;
  if (rank == 0) {
    struct group* p;
    while (1) {
      /* get next user, this function can fail so we retry a few times */
      int retries = 3;
retry:
      p = getgrent();
      if (p == NULL) {
        if (errno == EIO || errno == EINTR) {
          retries--;
        } else {
          /* TODO: ERROR! */
          retries = 0;
        }
        if (retries > 0) {
          goto retry;
        }
      }

      if (p != NULL) {
/*
        printf("Group=%s GID=%d\n",
          p->gr_name, p->gr_gid
        );
*/
        char* name  = p->gr_name;
        uint32_t id = p->gr_gid;
        strid_insert(name, id, &head, &tail, &count, &chars);
      } else {
        /* hit the end of the group list */
        endgrent();
        break;
      }
    }

//    printf("Max groupname %d, count %d\n", chars, count);
  }

  /* bcast count and number of chars */
  MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(&chars, 1, MPI_INT, 0, MPI_COMM_WORLD);

  /* create datatype to represent a username/id pair */
  MPI_Datatype dt;
  create_stridtype(chars, &dt);

  /* get extent of type */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(dt, &lb, &extent);

  /* allocate an array to hold all user names and ids */
  char* buf = NULL;
  size_t bufsize = count * extent;
  if (bufsize > 0) {
    buf = (char*) malloc(bufsize);
  }

  /* copy items from list into array */
  if (rank == 0) {
    strid_serialize(head, chars, buf);
  }

  /* broadcast the array of usernames and ids */
  MPI_Bcast(buf, count, dt, 0, MPI_COMM_WORLD);

  /* set output parameters */
  items->buf   = buf;
  items->count = (uint64_t) count;
  items->chars = (uint64_t) chars; 
  items->dt    = dt;

  /* delete the linked list */
  if (rank == 0) {
    strid_delete(&head, &tail, &count);
  }

  return;
}

static void create_stattype(int detail, int chars, MPI_Datatype* dt_stat)
{
  /* build type for file path */
  MPI_Datatype dt_filepath;
  MPI_Type_contiguous(chars, MPI_CHAR, &dt_filepath);

  /* build keysat type */
  int fields;
  MPI_Datatype types[8];
  if (detail) {
    fields = 8;
    types[0] = dt_filepath;  /* file name */
    types[1] = MPI_UINT32_T; /* mode */
    types[2] = MPI_UINT32_T; /* uid */
    types[3] = MPI_UINT32_T; /* gid */
    types[4] = MPI_UINT32_T; /* atime */
    types[5] = MPI_UINT32_T; /* mtime */
    types[6] = MPI_UINT32_T; /* ctime */
    types[7] = MPI_UINT64_T; /* size */
  } else {
    fields = 2;
    types[0] = dt_filepath;  /* file name */
    types[1] = MPI_UINT32_T; /* file type */
  }
  DTCMP_Type_create_series(fields, types, dt_stat);

  MPI_Type_free(&dt_filepath);
  return;
}

static int convert_stat_to_dt(flist_t* flist)
{
  int detail = flist->detail;
  elem_t* head = flist->stat_head;
  buf_t* items = &flist->files;

  /* initialize output params */
  items->buf   = NULL;
  items->count = 0;
  items->chars = 0;
  items->dt    = MPI_DATATYPE_NULL;

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* count number of items in list and identify longest filename */
  int max = 0;
  uint64_t count = 0;
  elem_t* current = head;
  while (current != NULL) {
    const char* file = current->file;
    size_t len = strlen(file) + 1;
    if (len > max) {
      max = (int) len;
    }
    count++;
    current = current->next;
  }

  /* find smallest length that fits max and consists of integer
   * number of 8 byte segments */
  int max8 = max / 8;
  if (max8 * 8 < max) {
    max8++;
  }
  max8 *= 8;

  /* compute longest file path across all ranks */
  int chars;
  MPI_Allreduce(&max8, &chars, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

  /* nothing to do if no one has anything */
  if (chars <= 0) {
    return 0;
  }

  /* build stat type */
  MPI_Datatype dt;
  create_stattype(detail, chars, &dt);

  /* get extent of stat type */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(dt, &lb, &extent);

  /* record size of file buffer element */
  flist->file_elem_size = (size_t) extent;

  /* allocate buffer */
  size_t bufsize = extent * count;
  void* buf = bayer_malloc(bufsize, "array for stat data", __FILE__, __LINE__);

  /* copy stat data into stat datatypes */
  char* ptr = (char*) buf;
  current = head;
  while (current != NULL) {
    /* get pointer to file name and stat structure */
    char* file = current->file;
    const struct stat* sb = current->sb;

    /* TODO: copy stat info via function */

    uint32_t* ptr_uint32t;
    uint64_t* ptr_uint64t;

    /* copy in file name */
    strcpy(ptr, file);
    ptr += chars;

    if (detail) {
      /* copy in mode time */
      ptr_uint32t = (uint32_t*) ptr;
      *ptr_uint32t = (uint32_t) sb->st_mode;
      ptr += sizeof(uint32_t);

      /* copy in user id */
      ptr_uint32t = (uint32_t*) ptr;
      *ptr_uint32t = (uint32_t) sb->st_uid;
      ptr += sizeof(uint32_t);

      /* copy in group id */
      ptr_uint32t = (uint32_t*) ptr;
      *ptr_uint32t = (uint32_t) sb->st_gid;
      ptr += sizeof(uint32_t);

      /* copy in access time */
      ptr_uint32t = (uint32_t*) ptr;
      *ptr_uint32t = (uint32_t) sb->st_atime;
      ptr += sizeof(uint32_t);

      /* copy in modify time */
      ptr_uint32t = (uint32_t*) ptr;
      *ptr_uint32t = (uint32_t) sb->st_mtime;
      ptr += sizeof(uint32_t);

      /* copy in create time */
      ptr_uint32t = (uint32_t*) ptr;
      *ptr_uint32t = (uint32_t) sb->st_ctime;
      ptr += sizeof(uint32_t);

      /* copy in size */
      ptr_uint64t = (uint64_t*) ptr;
      *ptr_uint64t = (uint64_t) sb->st_size;
      ptr += sizeof(uint64_t);
    } else {
      /* just have the file type */
      ptr_uint32t = (uint32_t*) ptr;
      *ptr_uint32t = (uint32_t) current->type;
      ptr += sizeof(uint32_t);
    }

    /* go to next element */
    current = current->next;
  }

  /* set output params */
  items->buf   = buf;
  items->count = count;
  items->chars = (uint64_t)chars;
  items->dt    = dt;

  /* delete linked list */
  current = head;
  while (current != NULL) {
    elem_t* next = current->next;
    bayer_free(&current->file);
    bayer_free(&current->sb);
    bayer_free(&current);
    current = next;
  }
  flist->stat_count = 0;
  flist->stat_head  = NULL;
  flist->stat_tail  = NULL;

  return 0;
}

/* Set up and execute directory walk */
void bayer_flist_walk_path(const char* dirpath, int use_stat, bayer_flist* pbflist)
{
  /* check that we got a valid pointer */
  if (pbflist == NULL) {
  }

  /* allocate a new file list */
  *pbflist = bayer_flist_new();

  /* convert handle to flist_t */
  flist_t* flist = *(flist_t**)pbflist;

  /* initialize libcircle */
  CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL);

  /* set libcircle verbosity level */
  enum CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
  CIRCLE_enable_logging(loglevel);

  /* set some global variables to do the file walk */
  strncpy(CURRENT_DIR, dirpath, PATH_MAX);
  CURRENT_LIST = flist;

  /* we lookup users and groups first in case we can use
   * them to filter the walk */
  flist->detail = 0;
  if (use_stat) {
    flist->detail = 1;
    get_users(&flist->users);
    get_groups(&flist->groups);
    create_maps(&flist->users, flist->user_name2id, flist->user_id2name);
    create_maps(&flist->groups, flist->group_name2id, flist->group_id2name);
  }

  /* register callbacks */
  if (use_stat) {
    /* walk directories by calling stat on every item */
    CIRCLE_cb_create(&walk_stat_create);
    CIRCLE_cb_process(&walk_stat_process);
  } else {
    /* walk directories using file types in readdir */
    CIRCLE_cb_create(&walk_readdir_create);
    CIRCLE_cb_process(&walk_readdir_process);
  }

  /* run the libcircle job */
  CIRCLE_begin();
  CIRCLE_finalize();

  /* convert stat structs to datatypes */
  convert_stat_to_dt(flist);

  /* get total file count */
  uint64_t total;
  uint64_t count = flist->files.count;
  MPI_Allreduce(&count, &total, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  flist->total_files = total;

  return;
}

/****************************************
 * Read file list from file
 ***************************************/

/* file format:
 *   uint64_t timestamp when walk started
 *   uint64_t timestamp when walk ended
 *   uint64_t total number of files
 *   uint64_t max filename length
 *   list of <filenames(str), filetype(uint32_t)> */
static void read_cache_v2(
  const char* name,
  MPI_Offset* outdisp,
  MPI_File fh,
  char* datarep,
  uint64_t* outstart,
  uint64_t* outend,
  flist_t* flist)
{
  MPI_Status status;

  MPI_Offset disp = *outdisp;

  /* indicate that we just have file names */
  flist->detail = 0;

  /* pointer to file buffer data structure */
  buf_t* files = &flist->files;

  /* get our rank */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* rank 0 reads and broadcasts header */
  uint64_t header[4];
  MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
  if (rank == 0) {
    MPI_File_read_at(fh, disp, header, 4, MPI_UINT64_T, &status);
  }
  MPI_Bcast(header, 4, MPI_UINT64_T, 0, MPI_COMM_WORLD);
  disp += 4 * 8; /* 4 consecutive uint64_t types in external32 */

  uint64_t all_count;
  *outstart     = header[0];
  *outend       = header[1];
  all_count     = header[2];
  files->chars  = header[3];

  /* compute count for each process */
  uint64_t count = all_count / ranks;
  uint64_t remainder = all_count - count * ranks;
  if (rank < remainder) {
    count++;
  }
  files->count = count;

  /* get our offset */
  uint64_t offset;
  MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  if (rank == 0) {
    offset = 0;
  }

  /* create file datatype and read in file info if there are any */
  if (all_count > 0 && files->chars > 0) {
    /* create types */
    create_stattype(flist->detail, (int)files->chars,   &(files->dt));

    /* get extents */
    MPI_Aint lb_file, extent_file;
    MPI_Type_get_extent(files->dt, &lb_file, &extent_file);

    /* record size of file buffer element */
    flist->file_elem_size = (size_t) extent_file;

    /* allocate memory to hold data */
    size_t bufsize_file = files->count * extent_file;
    files->buf = (void*) bayer_malloc(bufsize_file, "File data buffer", __FILE__, __LINE__);

    /* collective read of stat info */
    MPI_File_set_view(fh, disp, files->dt, files->dt, datarep, MPI_INFO_NULL);
    MPI_Offset read_offset = disp + offset * extent_file;
    MPI_File_read_at_all(fh, read_offset, files->buf, (int)files->count, files->dt, &status);
    disp += all_count * extent_file;
  }

  *outdisp = disp;
  return;
}

/* file format:
 *   uint64_t timestamp when walk started
 *   uint64_t timestamp when walk ended
 *   uint64_t total number of users
 *   uint64_t max username length
 *   uint64_t total number of groups
 *   uint64_t max groupname length
 *   uint64_t total number of files
 *   uint64_t max filename length
 *   list of <username(str), userid(uint32_t)>
 *   list of <groupname(str), groupid(uint32_t)>
 *   list of <files(str)>
 *   */
static void read_cache_v3(
  const char* name,
  MPI_Offset* outdisp,
  MPI_File fh,
  char* datarep,
  uint64_t* outstart,
  uint64_t* outend,
  flist_t* flist)
{
  MPI_Status status;

  MPI_Offset disp = *outdisp;

  /* indicate that we have stat data */
  flist->detail = 1;

  /* pointer to users, groups, and file buffer data structure */
  buf_t* users  = &flist->users;
  buf_t* groups = &flist->groups;
  buf_t* files  = &flist->files;

  /* get our rank */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* rank 0 reads and broadcasts header */
  uint64_t header[8];
  MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
  if (rank == 0) {
    MPI_File_read_at(fh, disp, header, 8, MPI_UINT64_T, &status);
  }
  MPI_Bcast(header, 8, MPI_UINT64_T, 0, MPI_COMM_WORLD);
  disp += 8 * 8; /* 8 consecutive uint64_t types in external32 */

  uint64_t all_count;
  *outstart     = header[0];
  *outend       = header[1];
  users->count  = header[2];
  users->chars  = header[3];
  groups->count = header[4];
  groups->chars = header[5];
  all_count     = header[6];
  files->chars  = header[7];

  /* compute count for each process */
  uint64_t count = all_count / ranks;
  uint64_t remainder = all_count - count * ranks;
  if (rank < remainder) {
    count++;
  }
  files->count = count;

  /* get our offset */
  uint64_t offset;
  MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  if (rank == 0) {
    offset = 0;
  }

  /* read users, if any */
  if (users->count > 0 && users->chars > 0) {
    /* create type */
    create_stridtype((int)users->chars,  &(users->dt));

    /* get extent */
    MPI_Aint lb_user, extent_user;
    MPI_Type_get_extent(users->dt, &lb_user, &extent_user);

    /* allocate memory to hold data */
    size_t bufsize_user = users->count * extent_user;
    users->buf = (void*) bayer_malloc(bufsize_user, "User data buffer", __FILE__, __LINE__);

    /* read data */
    MPI_File_set_view(fh, disp, users->dt, users->dt, datarep, MPI_INFO_NULL);
    if (rank == 0) {
      MPI_File_read_at(fh, disp, users->buf, (int)users->count, users->dt, &status);
    }
    MPI_Bcast(users->buf, (int)users->count, users->dt, 0, MPI_COMM_WORLD);
    disp += bufsize_user;
  }

  /* read groups, if any */
  if (groups->count > 0 && groups->chars > 0) {
    /* create type */
    create_stridtype((int)groups->chars, &(groups->dt));

    /* get extent */
    MPI_Aint lb_group, extent_group;
    MPI_Type_get_extent(groups->dt, &lb_group, &extent_group);

    /* allocate memory to hold data */
    size_t bufsize_group = groups->count * extent_group;
    groups->buf = (void*) bayer_malloc(bufsize_group, "Group data buffer", __FILE__, __LINE__);

    /* read data */
    MPI_File_set_view(fh, disp, groups->dt, groups->dt, datarep, MPI_INFO_NULL);
    if (rank == 0) {
      MPI_File_read_at(fh, disp, groups->buf, (int)groups->count, groups->dt, &status);
    }
    MPI_Bcast(groups->buf, (int)groups->count, groups->dt, 0, MPI_COMM_WORLD);
    disp += bufsize_group;
  }

  /* read files, if any */
  if (all_count > 0 && files->chars > 0) {
    /* create types */
    create_stattype(flist->detail, (int)files->chars,   &(files->dt));

    /* get extents */
    MPI_Aint lb_file, extent_file;
    MPI_Type_get_extent(files->dt, &lb_file, &extent_file);

    /* record size of file buffer element */
    flist->file_elem_size = (size_t) extent_file;

    /* allocate memory to hold data */
    size_t bufsize_file = files->count * extent_file;
    files->buf = (void*) bayer_malloc(bufsize_file, "File data buffer", __FILE__, __LINE__);

    /* collective read of stat info */
    MPI_File_set_view(fh, disp, files->dt, files->dt, datarep, MPI_INFO_NULL);
    MPI_Offset read_offset = disp + offset * extent_file;
    MPI_File_read_at_all(fh, read_offset, files->buf, (int)files->count, files->dt, &status);
    disp += all_count * extent_file;
  }

  /* create maps of users and groups */
  create_maps(&flist->users, flist->user_name2id, flist->user_id2name);
  create_maps(&flist->groups, flist->group_name2id, flist->group_id2name);

  *outdisp = disp;
  return;
}

void bayer_flist_read_cache(
  const char* name,
  bayer_flist* pbflist)
{
  /* check that we got a valid pointer */
  if (pbflist == NULL) {
  }

  /* allocate a new file list */
  *pbflist = bayer_flist_new();

  /* convert handle to flist_t */
  flist_t* flist = *(flist_t**)pbflist;

  /* get our rank */
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /* open file */
  int rc;
  MPI_Status status;
  MPI_File fh;
  char datarep[] = "external32";
  int amode = MPI_MODE_RDONLY;
  rc = MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, MPI_INFO_NULL, &fh);
  if (rc != MPI_SUCCESS) {
    if (rank == 0) {
      printf("Failed to open file %s", name);
    }
    return;
  }

  /* set file view */
  MPI_Offset disp = 0;

  /* rank 0 reads and broadcasts version */
  uint64_t version;
  MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
  if (rank == 0) {
    MPI_File_read_at(fh, disp, &version, 1, MPI_UINT64_T, &status);
  }
  MPI_Bcast(&version, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
  disp += 1 * 8; /* 9 consecutive uint64_t types in external32 */

  /* need a couple of dummy params to record walk start and end times */
  uint64_t outstart = 0;
  uint64_t outend = 0;

  /* read data from file */
  if (version == 2) {
    read_cache_v2(name, &disp, fh, datarep, &outstart, &outend, flist);
  } else if (version == 3) {
    read_cache_v3(name, &disp, fh, datarep, &outstart, &outend, flist);
  } else {
    /* TODO: unknown file format */
  }

  /* close file */
  MPI_File_close(&fh);

  return;
}
