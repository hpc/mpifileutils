#include <dirent.h>
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

#include <stdarg.h> /* variable length args */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"

#include <map>
#include <string>

using namespace std;

/* TODO: these types may be encoded in files */
enum filetypes {
  TYPE_NULL    = 0,
  TYPE_UNKNOWN = 1,
  TYPE_FILE    = 2,
  TYPE_DIR     = 3,
  TYPE_LINK    = 4,
};

// getpwent getgrent to read user and group entries

/* TODO: change globals to struct */
static int verbose   = 0;
static int walk_stat = 1;

/* struct for elements in linked list */
typedef struct list_elem {
  char* file;             /* file name */
  filetypes type;         /* record type of object */
  struct stat* sb;        /* stat info */
  struct list_elem* next; /* pointer to next item */
} elem_t;

/* declare types and function prototypes for DFTW code */
typedef int (*DFTW_cb_t)(const char* fpath, const struct stat* sb, mode_t type);

/* keep stats during walk */
uint64_t total_dirs  = 0;
uint64_t total_files = 0;
uint64_t total_links = 0;
uint64_t total_bytes = 0;

/* variables to track linked list during walk */
uint64_t        list_count = 0;
static elem_t*  list_head  = NULL;
static elem_t*  list_tail  = NULL;

/** The top directory specified. */
char _DFTW_TOP_DIR[PATH_MAX];

/** The callback. */
DFTW_cb_t _DFTW_CB;

/* appends file name and stat info to linked list */
static int record_info(const char *fpath, const struct stat *sb, mode_t type)
{
  /* TODO: check that memory allocation doesn't fail */
  /* create new element to record file path, file type, and stat info */
  elem_t* elem = (elem_t*) bayer_malloc(sizeof(elem_t), "file info element", __FILE__, __LINE__);

  /* copy path */
  elem->file = bayer_strdup(fpath, "file path", __FILE__, __LINE__);

  /* set file type */
  if (S_ISDIR(type)) {
    elem->type = TYPE_DIR;
    total_dirs++;
  } else if (S_ISREG(type)) {
    elem->type = TYPE_FILE;
    total_files++;
  } else if (S_ISLNK(type)) {
    elem->type = TYPE_LINK;
    total_links++;
  } else {
    /* unknown file type */
    elem->type = TYPE_UNKNOWN;
//    printf("Unknown file type for %s (%lx)\n", fpath, (unsigned long)type);
//    fflush(stdout);
  }

  /* copy stat info */
  if (sb != NULL) {
    elem->sb = (struct stat*) bayer_malloc(sizeof(struct stat), "stat structure", __FILE__, __LINE__);
    memcpy(elem->sb, sb, sizeof(struct stat));

    /* if it's a file, sum the number of bytes */
    if (S_ISREG(type)) {
      total_bytes += (uint64_t) sb->st_size;
    }
  } else {
    elem->sb = NULL;
  }

  /* append element to tail of linked list */
  elem->next = NULL;
  if (list_head == NULL) {
    list_head = elem;
  }
  if (list_tail != NULL) {
    list_tail->next = elem;
  }
  list_tail = elem;
  list_count++;

  /* To tell dftw() to continue */
  return 0;
}

/****************************************
 * Walk directory tree using stat at top level and readdir
 ***************************************/

static void DFTW_process_dir_readdir(char* dir, CIRCLE_handle* handle)
{
  /* TODO: may need to try these functions multiple times */
  DIR* dirp = opendir(dir);

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
              _DFTW_CB(newpath, NULL, mode);
            } else {
              /* type is unknown, we need to stat it */
              struct stat st;
              int status = bayer_lstat(newpath, &st);
              if (status == 0) {
                have_mode = 1;
                mode = st.st_mode;
                _DFTW_CB(newpath, &st, mode);
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
          /* name is too long */
          printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
          fflush(stdout);
        }
      }
    }
  }

  closedir(dirp);

  return;
}

/** Call back given to initialize the dataset. */
static void DFTW_create_readdir(CIRCLE_handle* handle)
{
  char* path = _DFTW_TOP_DIR;

  /* stat top level item */
  struct stat st;
  int status = bayer_lstat(path, &st);
  if (status != 0) {
    /* TODO: print error */
    return;
  }

  /* record item info */
  _DFTW_CB(path, &st, st.st_mode);

  /* recurse into directory */
  if (S_ISDIR(st.st_mode)) {
    DFTW_process_dir_readdir(path, handle);
  }

  return;
}

/** Callback given to process the dataset. */
static void DFTW_process_readdir(CIRCLE_handle* handle)
{
  /* in this case, only items on queue are directories */
  char path[CIRCLE_MAX_STRING_LEN];
  handle->dequeue(path);
  DFTW_process_dir_readdir(path, handle);
  return;
}

/****************************************
 * Walk directory tree using stat on every object
 ***************************************/

static void DFTW_process_dir_stat(char* dir, CIRCLE_handle* handle)
{
  /* TODO: may need to try these functions multiple times */
  DIR* dirp = opendir(dir);

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
          /* name is too long */
          printf("Path name is too long: %lu chars exceeds limit %lu\n", len, sizeof(newpath));
          fflush(stdout);
        }
      }
    }
  }

  closedir(dirp);

  return;
}

/** Call back given to initialize the dataset. */
static void DFTW_create_stat(CIRCLE_handle* handle)
{
  /* we'll call stat on every item */
  handle->enqueue(_DFTW_TOP_DIR);
}

/** Callback given to process the dataset. */
static void DFTW_process_stat(CIRCLE_handle* handle)
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

  /* record info for item */
  _DFTW_CB(path, &st, st.st_mode);

  /* recurse into directory */
  if (S_ISDIR(st.st_mode)) {
    /* TODO: check that we can recurse into directory */
    DFTW_process_dir_stat(path, handle);
  }

  return;
}

/****************************************
 * Set up and execute directory walk
 ***************************************/

void dftw(
  const char* dirpath,
  DFTW_cb_t fn)
//int (*fn)(const char* fpath, const struct stat* sb, mode_t type))
{
  /* set some global variables to do the file walk */
  _DFTW_CB = fn;
  strncpy(_DFTW_TOP_DIR, dirpath, PATH_MAX);

  /* initialize libcircle */
  CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL);

  /* set libcircle verbosity level */
  enum CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
  if (verbose) {
    loglevel = CIRCLE_LOG_INFO;
  }
  CIRCLE_enable_logging(loglevel);

  /* register callbacks */
  if (walk_stat) {
    /* walk directories by calling stat on every item */
    CIRCLE_cb_create(&DFTW_create_stat);
    CIRCLE_cb_process(&DFTW_process_stat);
  } else {
    /* walk directories using file types in readdir */
    CIRCLE_cb_create(&DFTW_create_readdir);
    CIRCLE_cb_process(&DFTW_process_readdir);
  }

  /* run the libcircle job */
  CIRCLE_begin();
  CIRCLE_finalize();
}

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
  strid_t* elem = (strid_t*) bayer_malloc(sizeof(strid_t), "string-to-id element", __FILE__, __LINE__);
  elem->name = bayer_strdup(name, "name", __FILE__, __LINE__);
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

typedef struct {
  void* buf;
  uint64_t count;
  uint64_t chars;
  MPI_Datatype dt;
} buf_t;

static void init_buft(buf_t* items)
{
  /* initialize output parameters */
  items->buf = NULL;
  items->count = 0;
  items->chars = 0;
  items->dt    = MPI_DATATYPE_NULL;
}

static void free_buft(buf_t* items)
{
  bayer_free(&(items->buf));

  if (items->dt != MPI_DATATYPE_NULL) {
    MPI_Type_free(&(items->dt));
  }

  items->count = 0;
  items->chars = 0;

  return;
}

/* build a name-to-id map and an id-to-name map */
static void create_maps(
  const buf_t* items,
  map<string,uint32_t>& name2id,
  map<uint32_t,string>& id2name)
{
  int i;
  const char* ptr = (const char*)items->buf;
  for (i = 0; i < items->count; i++) {
    const char* name = ptr;
    ptr += items->chars;

    uint32_t id = *(uint32_t*)ptr;
    ptr += 4;

    name2id[name] = id;
    id2name[id] = name;
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
    bayer_free(&(current->name));
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
  init_buft(items);

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
    buf = (char*) bayer_malloc(bufsize, "user name array", __FILE__, __LINE__);
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
  init_buft(items);

  /* get our rank */
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /* rank 0 iterates over groups with getgrent */
  strid_t* head = NULL;
  strid_t* tail = NULL;
  int count = 0;
  int chars = 0;
  if (rank == 0) {
    struct group* p;
    while (1) {
      /* get next group, this function can fail so we retry a few times */
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

  /* create datatype to represent a group/id pair */
  MPI_Datatype dt;
  create_stridtype(chars, &dt);

  /* get extent of type */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(dt, &lb, &extent);

  /* allocate an array to hold all group names and ids */
  char* buf = NULL;
  size_t bufsize = count * extent;
  if (bufsize > 0) {
    buf = (char*) bayer_malloc(bufsize, "group name array", __FILE__, __LINE__);
  }

  /* copy items from list into array */
  if (rank == 0) {
    strid_serialize(head, chars, buf);
  }

  /* broadcast the array of groupnames and ids */
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

static void create_stattype(int chars, MPI_Datatype* dt_stat)
{
  /* build type for file path */
  MPI_Datatype dt_filepath;
  MPI_Type_contiguous(chars, MPI_CHAR, &dt_filepath);

  /* build keysat type */
  int fields;
  MPI_Datatype types[8];
  if (walk_stat) {
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

static int convert_stat_to_dt(const elem_t* head, buf_t* items)
{
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
  const elem_t* current = head;
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
  create_stattype(chars, &dt);

  /* get extent of stat type */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(dt, &lb, &extent);

  /* allocate buffer */
  size_t bufsize = extent * count;
  void* buf = NULL;
  if (bufsize > 0) {
    buf = bayer_malloc(bufsize, "stat datatype array", __FILE__, __LINE__);
  }

  /* copy stat data into stat datatypes */
  char* ptr = (char*) buf;
  current = list_head;
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

    if (walk_stat) {
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

  return 0;
}

/* file version
 * 1: version, start, end, files, file chars, list (file)
 * 2: version, start, end, files, file chars, list (file, type)
 * 3: version, start, end, files, users, user chars, groups, group chars,
 *    files, file chars, list (user, userid), list (group, groupid),
 *    list (stat) */
static void write_cache_readdir(
  const char* name,
  buf_t* files,
  uint64_t walk_start,
  uint64_t walk_end)
{
  /* get our rank and number of ranks in job */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* get total file count */
  uint64_t all_count;
  uint64_t count = files->count;
  MPI_Allreduce(&count, &all_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

  /* get our offset */
  uint64_t offset;
  MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  if (rank == 0) {
    offset = 0;
  }

  /* report the filename we're writing to */
  if (verbose && rank == 0) {
    printf("Writing to cache file: %s\n", name);
    fflush(stdout);
  }

  /* open file */
  MPI_Status status;
  MPI_File fh;
  char datarep[] = "external32";
  //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
  int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;
  MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, MPI_INFO_NULL, &fh);

  /* truncate file to 0 bytes */
  MPI_File_set_size(fh, 0);

  /* prepare header */
  uint64_t header[5];
  header[0] = 2;             /* file version */
  header[1] = walk_start;    /* time_t when file walk started */
  header[2] = walk_end;      /* time_t when file walk stopped */
  header[3] = all_count;     /* total number of stat entries */
  header[4] = files->chars;  /* number of chars in file name */

  /* write the header */
  MPI_Offset disp = 0;
  MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
  if (rank == 0) {
    MPI_File_write_at(fh, disp, header, 5, MPI_UINT64_T, &status);
  }
  disp += 5 * 8;

  if (files->dt != MPI_DATATYPE_NULL) {
    /* get extents of file datatypes */
    MPI_Aint lb_file, extent_file;
    MPI_Type_get_extent(files->dt, &lb_file, &extent_file);

    /* collective write of file info */
    MPI_File_set_view(fh, disp, files->dt, files->dt, datarep, MPI_INFO_NULL);
    MPI_Offset write_offset = disp + offset * extent_file;
    int write_count = (int) count;
    MPI_File_write_at_all(fh, write_offset, files->buf, write_count, files->dt, &status);
    disp += all_count * extent_file;
  }

  /* close file */
  MPI_File_close(&fh);

  return;
}

static void write_cache_stat(
  const char* name,
  buf_t* users,
  buf_t* groups,
  buf_t* files,
  uint64_t walk_start,
  uint64_t walk_end)
{
  /* get our rank and number of ranks in job */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* get total file count */
  uint64_t all_count;
  uint64_t count = files->count;
  MPI_Allreduce(&count, &all_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

  /* get our offset */
  uint64_t offset;
  MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  if (rank == 0) {
    offset = 0;
  }

  /* report the filename we're writing to */
  if (verbose && rank == 0) {
    printf("Writing to cache file: %s\n", name);
    fflush(stdout);
  }

  /* open file */
  MPI_Status status;
  MPI_File fh;
  char datarep[] = "external32";
  //int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_SEQUENTIAL;
  int amode = MPI_MODE_WRONLY | MPI_MODE_CREATE;
  MPI_File_open(MPI_COMM_WORLD, (char*)name, amode, MPI_INFO_NULL, &fh);

  /* truncate file to 0 bytes */
  MPI_File_set_size(fh, 0);

  /* prepare header */
  uint64_t header[9];
  header[0] = 3;             /* file version */
  header[1] = walk_start;    /* time_t when file walk started */
  header[2] = walk_end;      /* time_t when file walk stopped */
  header[3] = users->count;  /* number of user records */
  header[4] = users->chars;  /* number of chars in user name */
  header[5] = groups->count; /* number of group records */
  header[6] = groups->chars; /* number of chars in group name */
  header[7] = all_count;     /* total number of stat entries */
  header[8] = files->chars;  /* number of chars in file name */

  /* write the header */
  MPI_Offset disp = 0;
  MPI_File_set_view(fh, disp, MPI_UINT64_T, MPI_UINT64_T, datarep, MPI_INFO_NULL);
  if (rank == 0) {
    MPI_File_write_at(fh, disp, header, 9, MPI_UINT64_T, &status);
  }
  disp += 9 * 8;

  if (users->dt != MPI_DATATYPE_NULL) {
    /* get extent user */
    MPI_Aint lb_user, extent_user;
    MPI_Type_get_extent(users->dt, &lb_user, &extent_user);

    /* write out users */
    MPI_File_set_view(fh, disp, users->dt, users->dt, datarep, MPI_INFO_NULL);
    if (rank == 0) {
      MPI_File_write_at(fh, disp, users->buf, users->count, users->dt, &status);
    }
    disp += users->count * extent_user;
  }

  if (groups->dt != MPI_DATATYPE_NULL) {
    /* get extent group */
    MPI_Aint lb_group, extent_group;
    MPI_Type_get_extent(groups->dt, &lb_group, &extent_group);

    /* write out groups */
    MPI_File_set_view(fh, disp, groups->dt, groups->dt, datarep, MPI_INFO_NULL);
    if (rank == 0) {
      MPI_File_write_at(fh, disp, groups->buf, groups->count, groups->dt, &status);
    }
    disp += groups->count * extent_group;
  }

  if (files->dt != MPI_DATATYPE_NULL) {
    /* get extent file */
    MPI_Aint lb_file, extent_file;
    MPI_Type_get_extent(files->dt, &lb_file, &extent_file);

    /* collective write of stat info */
    MPI_File_set_view(fh, disp, files->dt, files->dt, datarep, MPI_INFO_NULL);
    MPI_Offset write_offset = disp + offset * extent_file;
    int write_count = (int) count;
    MPI_File_write_at_all(fh, write_offset, files->buf, write_count, files->dt, &status);
    disp += all_count * extent_file;
  }

  /* close file */
  MPI_File_close(&fh);

  return;
}

static void read_cache_v2(
  const char* name,
  MPI_Offset* outdisp,
  MPI_File fh,
  char* datarep,
  buf_t* files,
  uint64_t* outstart,
  uint64_t* outend)
{
  MPI_Status status;

  MPI_Offset disp = *outdisp;

  /* TODO: hacky way to indicate that we just have file names */
  walk_stat = 0;

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
    create_stattype((int)files->chars,   &(files->dt));

    /* get extents */
    MPI_Aint lb_file, extent_file;
    MPI_Type_get_extent(files->dt, &lb_file, &extent_file);

    /* allocate memory to hold data */
    size_t bufsize_file = files->count * extent_file;
    if (bufsize_file > 0) {
      files->buf = (void*) bayer_malloc(bufsize_file, "file buffer", __FILE__, __LINE__);
    }

    /* collective read of stat info */
    MPI_File_set_view(fh, disp, files->dt, files->dt, datarep, MPI_INFO_NULL);
    MPI_Offset read_offset = disp + offset * extent_file;
    MPI_File_read_at_all(fh, read_offset, files->buf, (int)files->count, files->dt, &status);
    disp += all_count * extent_file;
  }

  *outdisp = disp;
  return;
}

static void read_cache_v3(
  const char* name,
  MPI_Offset* outdisp,
  MPI_File fh,
  char* datarep,
  buf_t* users,
  buf_t* groups,
  buf_t* files,
  uint64_t* outstart,
  uint64_t* outend)
{
  MPI_Status status;

  MPI_Offset disp = *outdisp;

  /* TODO: hacky way to indicate that we're processing stat data */
  walk_stat = 1;

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
    if (bufsize_user > 0) {
      users->buf = (void*) bayer_malloc(bufsize_user, "user buffer", __FILE__, __LINE__);
    }

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
    if (bufsize_group > 0) {
      groups->buf = (void*) bayer_malloc(bufsize_group, "group buffer", __FILE__, __LINE__);
    }

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
    create_stattype((int)files->chars,   &(files->dt));

    /* get extents */
    MPI_Aint lb_file, extent_file;
    MPI_Type_get_extent(files->dt, &lb_file, &extent_file);

    /* allocate memory to hold data */
    size_t bufsize_file = files->count * extent_file;
    if (bufsize_file > 0) {
      files->buf = (void*) bayer_malloc(bufsize_file, "file buffer", __FILE__, __LINE__);
    }

    /* collective read of stat info */
    MPI_File_set_view(fh, disp, files->dt, files->dt, datarep, MPI_INFO_NULL);
    MPI_Offset read_offset = disp + offset * extent_file;
    MPI_File_read_at_all(fh, read_offset, files->buf, (int)files->count, files->dt, &status);
    disp += all_count * extent_file;
  }

  *outdisp = disp;
  return;
}

static void read_cache(
  const char* name,
  buf_t* users,
  buf_t* groups,
  buf_t* files,
  uint64_t* outstart,
  uint64_t* outend)
{
  /* intialize output variables */
  init_buft(users);
  init_buft(groups);
  init_buft(files);
  *outstart = 0;
  *outend   = 0;

  /* get our rank */
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  /* inform user about file name that we're reading */
  if (verbose && rank == 0) {
    printf("Reading from cache file: %s\n", name);
    fflush(stdout);
  }

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

  if (version == 2) {
    read_cache_v2(name, &disp, fh, datarep, files, outstart, outend);
  } else if (version == 3) {
    read_cache_v3(name, &disp, fh, datarep, users, groups, files, outstart, outend);
  } else {
    /* TODO: unknown file format */
  }

  /* close file */
  MPI_File_close(&fh);

  return;
}

/* routine for sorting strings in ascending order */
static int my_strcmp(const void* a, const void* b)
{
  return strcmp((const char*)a, (const char*)b);
}

static int my_strcmp_rev(const void* a, const void* b)
{
  return strcmp((const char*)b, (const char*)a);
}

static int sort_files_readdir(
  const char* sortfields,
  buf_t* files)
{
  void* inbuf      = files->buf;
  uint64_t incount = files->count;
  int chars        = (int)files->chars;
  MPI_Datatype dt_sat = files->dt;

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* build type for file path */
  MPI_Datatype dt_filepath;
  MPI_Type_contiguous(chars,       MPI_CHAR, &dt_filepath);
  MPI_Type_commit(&dt_filepath);

  /* build comparison op for filenames */
  DTCMP_Op op_filepath;
  DTCMP_Op_create(dt_filepath, my_strcmp, &op_filepath);

  /* build comparison op for filenames */
  DTCMP_Op op_filepath_rev;
  DTCMP_Op_create(dt_filepath, my_strcmp_rev, &op_filepath_rev);

  /* TODO: process sort fields */
  const int MAXFIELDS = 1;
  MPI_Datatype types[MAXFIELDS];
  DTCMP_Op ops[MAXFIELDS];
  size_t offsets[MAXFIELDS];
  size_t lengths[MAXFIELDS];
  int    sources[MAXFIELDS];
  int nfields = 0;
  for (nfields = 0; nfields < MAXFIELDS; nfields++) {
    types[nfields]   = MPI_DATATYPE_NULL;
    ops[nfields]     = DTCMP_OP_NULL;
    sources[nfields] = 0;
  }
  nfields = 0;
  char* sortfields_copy = bayer_strdup(sortfields, "sort fields", __FILE__, __LINE__);
  char* token = strtok(sortfields_copy, ",");
  while (token != NULL) {
    int valid = 1;
    if (strcmp(token, "name") == 0) {
      types[nfields]   = dt_filepath;
      ops[nfields]     = op_filepath;
      offsets[nfields] = 0;
      lengths[nfields] = chars;
      sources[nfields] = 1;
    } else if (strcmp(token, "-name") == 0) {
      types[nfields]   = dt_filepath;
      ops[nfields]     = op_filepath_rev;
      offsets[nfields] = 0;
      lengths[nfields] = chars;
      sources[nfields] = 1;
    } else {
      /* invalid token */
      valid = 0;
      if (rank == 0) {
        printf("Invalid sort field: %s\n", token);
      }
    }
    if (valid) {
      nfields++;
    }
    if (nfields > MAXFIELDS) {
      /* TODO: print warning if we have too many fields */
      break;
    }
    token = strtok(NULL, ",");
  }
  bayer_free(&sortfields_copy);

  /* build key type */
  MPI_Datatype dt_key;
  DTCMP_Type_create_series(nfields, types, &dt_key);

  /* create op to sort by access time, then filename */
  DTCMP_Op op_key;
  DTCMP_Op_create_series(nfields, ops, &op_key);

  /* build keysat type */
  MPI_Datatype dt_keysat;
  types[0] = dt_key;
  types[1] = dt_sat;
  DTCMP_Type_create_series(2, types, &dt_keysat);

  /* get extent of key type */
  MPI_Aint key_lb, key_extent;
  MPI_Type_get_extent(dt_key, &key_lb, &key_extent);

  /* get extent of keysat type */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(dt_keysat, &keysat_lb, &keysat_extent);

  /* get extent of sat type */
  MPI_Aint sat_lb, sat_extent;
  MPI_Type_get_extent(dt_sat, &sat_lb, &sat_extent);

  /* compute size of sort element and allocate buffer */
  size_t sortbufsize = keysat_extent * incount;
  void* sortbuf = NULL;
  if (sortbufsize > 0) {
    sortbuf = bayer_malloc(sortbufsize, "presort buffer", __FILE__, __LINE__);
  }

  /* copy data into sort elements */
  int index = 0;
  char* inptr   = (char*) inbuf;
  char* sortptr = (char*) sortbuf;
  while (index < incount) {
    /* copy in access time */
    int i;
    for (i = 0; i < nfields; i++) {
      memcpy(sortptr, inptr + offsets[i], lengths[i]);
      sortptr += lengths[i];
    }

    /* TODO: copy sat info via function */
    memcpy(sortptr, inptr, sat_extent);
    sortptr += sat_extent;
    inptr   += sat_extent;

    index++;
  }

  /* sort data */
  void* outsortbuf;
  int outsortcount;
  DTCMP_Handle handle;
  DTCMP_Sortz(
    sortbuf, incount, &outsortbuf, &outsortcount,
    dt_key, dt_keysat, op_key, DTCMP_FLAG_NONE,
    MPI_COMM_WORLD, &handle
  );

  /* allocate array to hold sorted info */
  void* newbuf = NULL;
  size_t newbufsize = sat_extent * outsortcount;
  if (newbufsize > 0) {
    newbuf = bayer_malloc(newbufsize, "postsort buffer", __FILE__, __LINE__);
  }

  /* step through sorted data filenames */
  index = 0;
  sortptr = (char*) outsortbuf;
  char* newptr = (char*) newbuf;
  while (index < outsortcount) {
    sortptr += key_extent;
    memcpy(newptr, sortptr, sat_extent);
    sortptr += sat_extent;
    newptr  += sat_extent;
    index++;
  }

  /* free memory */
  DTCMP_Free(&handle);
    
  /* free ops */
  DTCMP_Op_free(&op_key);
  DTCMP_Op_free(&op_filepath_rev);
  DTCMP_Op_free(&op_filepath);

  /* free types */
  MPI_Type_free(&dt_keysat);
  MPI_Type_free(&dt_key);
  MPI_Type_free(&dt_filepath);

  /* free input buffer holding sort elements */
  bayer_free(&sortbuf);

  /* set output params */
  bayer_free(&(files->buf));
  files->buf   = newbuf;
  files->count = outsortcount;

  return 0;
}

static int sort_files_stat(
  const char* sortfields,
  buf_t* users,
  buf_t* groups,
  buf_t* files,
  map<uint32_t,string>& user_id2name,
  map<uint32_t,string>& group_id2name)
{
  void* inbuf      = files->buf;
  uint64_t incount = files->count;
  int chars        = (int)files->chars;
  int chars_user   = (int)users->chars;
  int chars_group  = (int)groups->chars;
  MPI_Datatype dt_stat = files->dt;

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* build type for file path */
  MPI_Datatype dt_filepath, dt_user, dt_group;
  MPI_Type_contiguous(chars,       MPI_CHAR, &dt_filepath);
  MPI_Type_contiguous(chars_user,  MPI_CHAR, &dt_user);
  MPI_Type_contiguous(chars_group, MPI_CHAR, &dt_group);
  MPI_Type_commit(&dt_filepath);
  MPI_Type_commit(&dt_user);
  MPI_Type_commit(&dt_group);

  /* build comparison op for filenames */
  DTCMP_Op op_filepath, op_user, op_group;
  DTCMP_Op_create(dt_filepath, my_strcmp, &op_filepath);
  DTCMP_Op_create(dt_user,     my_strcmp, &op_user);
  DTCMP_Op_create(dt_group,    my_strcmp, &op_group);

  /* build comparison op for filenames */
  DTCMP_Op op_filepath_rev, op_user_rev, op_group_rev;
  DTCMP_Op_create(dt_filepath, my_strcmp_rev, &op_filepath_rev);
  DTCMP_Op_create(dt_user,     my_strcmp_rev, &op_user_rev);
  DTCMP_Op_create(dt_group,    my_strcmp_rev, &op_group_rev);

  /* TODO: process sort fields */
  MPI_Datatype types[7];
  DTCMP_Op ops[7];
  size_t offsets[7];
  size_t lengths[7];
  int    sources[7];
  int nfields = 0;
  for (nfields = 0; nfields < 7; nfields++) {
    types[nfields]   = MPI_DATATYPE_NULL;
    ops[nfields]     = DTCMP_OP_NULL;
    sources[nfields] = 0;
  }
  nfields = 0;
  char* sortfields_copy = bayer_strdup(sortfields, "sort fields", __FILE__, __LINE__);
  char* token = strtok(sortfields_copy, ",");
  while (token != NULL) {
    int valid = 1;
    if (strcmp(token, "name") == 0) {
      types[nfields]   = dt_filepath;
      ops[nfields]     = op_filepath;
      offsets[nfields] = 0;
      lengths[nfields] = chars;
      sources[nfields] = 1;
    } else if (strcmp(token, "-name") == 0) {
      types[nfields]   = dt_filepath;
      ops[nfields]     = op_filepath_rev;
      offsets[nfields] = 0;
      lengths[nfields] = chars;
      sources[nfields] = 1;
    } else if (strcmp(token, "user") == 0) {
      types[nfields]   = dt_user;
      ops[nfields]     = op_user;
      offsets[nfields] = chars + 1 * 4;
      lengths[nfields] = chars_user;
      sources[nfields] = 2;
    } else if (strcmp(token, "-user") == 0) {
      types[nfields]   = dt_user;
      ops[nfields]     = op_user_rev;
      offsets[nfields] = chars + 1 * 4;
      lengths[nfields] = chars_user;
      sources[nfields] = 2;
    } else if (strcmp(token, "group") == 0) {
      types[nfields]   = dt_group;
      ops[nfields]     = op_group;
      offsets[nfields] = chars + 2 * 4;
      lengths[nfields] = chars_group;
      sources[nfields] = 3;
    } else if (strcmp(token, "-group") == 0) {
      types[nfields]   = dt_group;
      ops[nfields]     = op_group_rev;
      offsets[nfields] = chars + 2 * 4;
      lengths[nfields] = chars_group;
      sources[nfields] = 3;
    } else if (strcmp(token, "uid") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      offsets[nfields] = chars + 1 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "-uid") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      offsets[nfields] = chars + 1 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "gid") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      offsets[nfields] = chars + 2 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "-gid") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      offsets[nfields] = chars + 2 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "atime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      offsets[nfields] = chars + 3 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "-atime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      offsets[nfields] = chars + 3 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "mtime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      offsets[nfields] = chars + 4 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "-mtime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      offsets[nfields] = chars + 4 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "ctime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      offsets[nfields] = chars + 5 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "-ctime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      offsets[nfields] = chars + 5 * 4;
      lengths[nfields] = 4;
      sources[nfields] = 1;
    } else if (strcmp(token, "size") == 0) {
      types[nfields]   = MPI_UINT64_T;
      ops[nfields]     = DTCMP_OP_UINT64T_ASCEND;
      offsets[nfields] = chars + 6 * 4;
      lengths[nfields] = 8;
      sources[nfields] = 1;
    } else if (strcmp(token, "-size") == 0) {
      types[nfields]   = MPI_UINT64_T;
      ops[nfields]     = DTCMP_OP_UINT64T_DESCEND;
      offsets[nfields] = chars + 6 * 4;
      lengths[nfields] = 8;
      sources[nfields] = 1;
    } else {
      /* invalid token */
      valid = 0;
      if (rank == 0) {
        printf("Invalid sort field: %s\n", token);
      }
    }
    if (valid) {
      nfields++;
    }
    if (nfields > 7) {
      /* TODO: print warning if we have too many fields */
      break;
    }
    token = strtok(NULL, ",");
  }
  bayer_free(&sortfields_copy);

  /* build key type */
  MPI_Datatype dt_key;
  DTCMP_Type_create_series(nfields, types, &dt_key);

  /* create op to sort by access time, then filename */
  DTCMP_Op op_key;
  DTCMP_Op_create_series(nfields, ops, &op_key);

  /* build keysat type */
  MPI_Datatype dt_keysat;
  types[0] = dt_key;
  types[1] = dt_stat;
  DTCMP_Type_create_series(2, types, &dt_keysat);

  /* get extent of key type */
  MPI_Aint key_lb, key_extent;
  MPI_Type_get_extent(dt_key, &key_lb, &key_extent);

  /* get extent of keysat type */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(dt_keysat, &keysat_lb, &keysat_extent);

  /* get extent of stat type */
  MPI_Aint stat_lb, stat_extent;
  MPI_Type_get_extent(dt_stat, &stat_lb, &stat_extent);

  /* compute size of sort element and allocate buffer */
  size_t sortbufsize = keysat_extent * incount;
  void* sortbuf = NULL;
  if (sortbufsize > 0) {
    sortbuf = bayer_malloc(sortbufsize, "presort buffer", __FILE__, __LINE__);
  }

  /* copy data into sort elements */
  int index = 0;
  char* inptr   = (char*) inbuf;
  char* sortptr = (char*) sortbuf;
  while (index < incount) {
    /* copy in access time */
    int i;
    for (i = 0; i < nfields; i++) {
      if (sources[i] == 1) {
        memcpy(sortptr, inptr + offsets[i], lengths[i]);
      } else if (sources[i] == 2) {
        uint32_t id = *(uint32_t*)(inptr + offsets[i]);
        const char* name = get_name_from_id(id, chars_user, user_id2name);
        strncpy(sortptr, name, lengths[i]);
      } else if (sources[i] == 3) {
        uint32_t id = *(uint32_t*)(inptr + offsets[i]);
        const char* name = get_name_from_id(id, chars_group, group_id2name);
        strncpy(sortptr, name, lengths[i]);
      }
      sortptr += lengths[i];
    }

    /* TODO: copy stat info via function */
    memcpy(sortptr, inptr, stat_extent);
    sortptr += stat_extent;
    inptr   += stat_extent;

    index++;
  }

  /* sort data */
  void* outsortbuf;
  int outsortcount;
  DTCMP_Handle handle;
  DTCMP_Sortz(
    sortbuf, incount, &outsortbuf, &outsortcount,
    dt_key, dt_keysat, op_key, DTCMP_FLAG_NONE,
    MPI_COMM_WORLD, &handle
  );

  /* allocate array to hold sorted info */
  void* newbuf = NULL;
  size_t newbufsize = stat_extent * outsortcount;
  if (newbufsize > 0) {
    newbuf = bayer_malloc(newbufsize, "postsort buffer", __FILE__, __LINE__);
  }

  /* step through sorted data filenames */
  index = 0;
  sortptr = (char*) outsortbuf;
  char* newptr = (char*) newbuf;
  while (index < outsortcount) {
    sortptr += key_extent;
    memcpy(newptr, sortptr, stat_extent);
    sortptr += stat_extent;
    newptr  += stat_extent;
    index++;
  }

  /* free memory */
  DTCMP_Free(&handle);
    
  /* free ops */
  DTCMP_Op_free(&op_key);
  DTCMP_Op_free(&op_group_rev);
  DTCMP_Op_free(&op_user_rev);
  DTCMP_Op_free(&op_filepath_rev);
  DTCMP_Op_free(&op_group);
  DTCMP_Op_free(&op_user);
  DTCMP_Op_free(&op_filepath);

  /* free types */
  MPI_Type_free(&dt_keysat);
  MPI_Type_free(&dt_key);
  MPI_Type_free(&dt_group);
  MPI_Type_free(&dt_user);
  MPI_Type_free(&dt_filepath);

  /* free input buffer holding sort elements */
  bayer_free(&sortbuf);

  /* set output params */
  bayer_free(&(files->buf));
  files->buf   = newbuf;
  files->count = outsortcount;

  return 0;
}

static void print_files(
  const buf_t* users,
  const buf_t* groups,
  const buf_t* files,
  map<uint32_t,string>& user_id2name,
  map<uint32_t,string>& group_id2name)
{
  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* step through and print data */
  if (walk_stat) {
    int index = 0;
    char* ptr = (char*) files->buf;
    while (index < files->count) {
      /* extract stat values from function */

      /* get filename */
      char* file = ptr;
      ptr += files->chars;

      /* get mode */
      uint32_t mode = *(uint32_t*)ptr;
      ptr += sizeof(uint32_t);

      /* get uid */
      uint32_t uid = *(uint32_t*)ptr;
      ptr += sizeof(uint32_t);

      /* get gid */
      uint32_t gid = *(uint32_t*)ptr;
      ptr += sizeof(uint32_t);

      /* get access time */
      uint32_t access = *(uint32_t*)ptr;
      ptr += sizeof(uint32_t);

      /* get modify time */
      uint32_t modify = *(uint32_t*)ptr;
      ptr += sizeof(uint32_t);

      /* get create time */
      uint32_t create = *(uint32_t*)ptr;
      ptr += sizeof(uint32_t);

      /* get size */
      uint64_t size = *(uint64_t*)ptr;
      ptr += sizeof(uint64_t);

      const char* username  = get_name_from_id(uid, users->chars,  user_id2name);
      const char* groupname = get_name_from_id(gid, groups->chars, group_id2name);

      char access_s[30];
      char modify_s[30];
      char create_s[30];
      time_t access_t = (time_t) access;
      time_t modify_t = (time_t) modify;
      time_t create_t = (time_t) create;
      size_t access_rc = strftime(access_s, sizeof(access_s)-1, "%FT%T", localtime(&access_t));
      size_t modify_rc = strftime(modify_s, sizeof(modify_s)-1, "%FT%T", localtime(&modify_t));
      size_t create_rc = strftime(create_s, sizeof(create_s)-1, "%FT%T", localtime(&create_t));
      if (access_rc == 0 || modify_rc == 0 || create_rc == 0) {
        /* error */
        access_s[0] = '\0';
        modify_s[0] = '\0';
        create_s[0] = '\0';
      }

      if (index < 10 || (files->count - index) <= 10) {
        //printf("Rank %d: Mode=%lx UID=%d GUI=%d Access=%lu Modify=%lu Create=%lu Size=%lu File=%s\n",
        //  rank, mode, uid, gid, (unsigned long)access, (unsigned long)modify,
        printf("Rank %d: Mode=%lx UID=%d(%s) GUI=%d(%s) Access=%s Modify=%s Create=%s Size=%lu File=%s\n",
          rank, mode, uid, username, gid, groupname,
          access_s, modify_s, create_s, (unsigned long)size, file
        );
      } else if (index == 10) {
        printf("<snip>\n");
      }

      index++;
    }
  } else {
    int index = 0;
    char* ptr = (char*) files->buf;
    while (index < files->count) {
      /* extract stat values from function */

      /* get filename */
      char* file = ptr;
      ptr += files->chars;

      /* get type */
      uint32_t type = *(uint32_t*)ptr;
      ptr += sizeof(uint32_t);

      if (index < 10 || (files->count - index) <= 10) {
        printf("Rank %d: Type=%d File=%s\n",
          rank, type, file
        );
      } else if (index == 10) {
        printf("<snip>\n");
      }

      index++;
    }
  }

  return;
}

static void print_usage()
{
  printf("\n");
  printf("Usage: dwalk [options] <path>\n");
  printf("\n");
  printf("Options:\n");
  printf("  -c, --cache <file>  - read/write directories using cache file\n");
  printf("  -l, --lite          - walk file system without stat\n");
  printf("  -s, --sort <fields> - sort output by comma-delimited fields\n");
  printf("  -p, --print         - print files to screen\n");
  printf("  -v, --verbose       - verbose output\n");
  printf("  -h, --help          - print usage\n");
  printf("\n");
  printf("Fields: name,user,group,uid,gid,atime,mtime,ctime,size\n");
  printf("\n");
  fflush(stdout);
  return;
}

int main(int argc, char **argv)
{
  /* initialize MPI */
  MPI_Init(&argc, &argv);

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* TODO: extend options
   *   - allow user to cache scan result in file
   *   - allow user to load cached scan as input
   *
   *   - allow user to filter by user, group, or filename using keyword or regex
   *   - allow user to specify time window
   *   - allow user to specify file sizes
   *
   *   - allow user to sort by different fields
   *   - allow user to group output (sum all bytes, group by user) */

  char* cachename = NULL;
  char* sortfields = NULL;
  int walk = 0;
  int print = 0;

  int option_index = 0;
  static struct option long_options[] = {
    {"cache",    1, 0, 'c'},
    {"lite",     0, 0, 'l'},
    {"sort",     1, 0, 's'},
    {"print",    0, 0, 'p'},
    {"help",     0, 0, 'h'},
    {"verbose",  0, 0, 'v'},
    {0, 0, 0, 0}
  };

  int usage = 0;
  while (1) {
    int c = getopt_long(
      argc, argv, "c:ls:phv",
      long_options, &option_index
    );

    if (c == -1) {
      break;
    }

    switch (c) {
    case 'c':
      cachename = bayer_strdup(optarg, "cache file", __FILE__, __LINE__);
      break;
    case 'l':
      walk_stat = 0;
      break;
    case 's':
      sortfields = bayer_strdup(optarg, "sort fields", __FILE__, __LINE__);
      break;
    case 'p':
      print = 1;
      break;
    case 'h':
      usage = 1;
      break;
    case 'v':
      verbose = 1;
      break;
    case '?':
      usage = 1;
      break;
    default:
      if (rank == 0) {
        printf("?? getopt returned character code 0%o ??\n", c);
      }
    }
  }

  /* paths to walk come after the options */
  char* target = NULL;
  if (optind < argc) {
    /* got a path to walk */
    walk = 1;

    /* get absolute path and remove ".", "..", consecutive "/",
     * and trailing "/" characters */
    char* path = argv[optind];
    target = bayer_path_strdup_abs_reduce_str(path);

    /* currently only allow one path */
    if (argc - optind > 1) {
      usage = 1;
    }
  } else {
    /* if we're not walking, we must be reading,
     * and for that we need a file */
    if (cachename == NULL) {
      usage = 1;
    }
  }

  /* if user is trying to sort, verify the sort fields are valid */
  if (sortfields != NULL) {
    int maxfields;
    int nfields = 0;
    char* sortfields_copy = bayer_strdup(sortfields, "sort fields", __FILE__, __LINE__);
    if (walk_stat) {
      maxfields = 7;
      char* token = strtok(sortfields_copy, ",");
      while (token != NULL) {
        int valid = 0;
        if (strcmp(token,  "name")  != 0 &&
            strcmp(token, "-name")  != 0 &&
            strcmp(token,  "user")  != 0 &&
            strcmp(token, "-user")  != 0 &&
            strcmp(token,  "group") != 0 &&
            strcmp(token, "-group") != 0 &&
            strcmp(token,  "uid")   != 0 &&
            strcmp(token, "-uid")   != 0 &&
            strcmp(token,  "gid")   != 0 &&
            strcmp(token, "-gid")   != 0 &&
            strcmp(token,  "atime") != 0 &&
            strcmp(token, "-atime") != 0 &&
            strcmp(token,  "mtime") != 0 &&
            strcmp(token, "-mtime") != 0 &&
            strcmp(token,  "ctime") != 0 &&
            strcmp(token, "-ctime") != 0 &&
            strcmp(token,  "size")  != 0 &&
            strcmp(token, "-size")  != 0)
        {
          /* invalid token */
          if (rank == 0) {
            printf("Invalid sort field: %s\n", token);
          }
          usage = 1;
        }
        nfields++;
        token = strtok(NULL, ",");
      }
    } else {
      maxfields = 1;
      char* token = strtok(sortfields_copy, ",");
      while (token != NULL) {
        int valid = 0;
        if (strcmp(token,  "name")  != 0 &&
            strcmp(token, "-name")  != 0)
        {
          /* invalid token */
          if (rank == 0) {
            printf("Invalid sort field: %s\n", token);
          }
          usage = 1;
        }
        nfields++;
        token = strtok(NULL, ",");
      }
    }
    if (nfields > maxfields) {
      printf("Exceeded maximum number of sort fields: %d\n", maxfields);
      usage = 1;
    }
    bayer_free(&sortfields_copy);
  }

  if (usage) {
    if (rank == 0) {
      print_usage();
    }
    MPI_Finalize();
    return 0;
  }

  /* TODO: check stat fields fit within MPI types */
  // if (sizeof(st_uid) > uint64_t) error(); etc...

  /* initialize our sorting library */
  DTCMP_Init();

  uint64_t all_count = 0;
  uint64_t walk_start, walk_end;

  /* initialize users, groups, and files */
  buf_t users, groups, files;
  init_buft(&users);
  init_buft(&groups);
  init_buft(&files);

  map<string,uint32_t> user_name2id;
  map<uint32_t,string> user_id2name;
  map<string,uint32_t> group_name2id;
  map<uint32_t,string> group_id2name;

  if (walk) {
    /* we lookup users and groups first in case we can use
     * them to filter the walk */
    if (walk_stat) {
      get_users(&users);
      get_groups(&groups);
      create_maps(&users, user_name2id, user_id2name);
      create_maps(&groups, group_name2id, group_id2name);
    }

    time_t walk_start_t = time(NULL);
    if (walk_start_t == (time_t)-1) {
      /* TODO: ERROR! */
    }
    walk_start = (uint64_t) walk_start_t;

    /* report walk count, time, and rate */
    if (verbose && rank == 0) {
      char walk_s[30];
      size_t rc = strftime(walk_s, sizeof(walk_s)-1, "%FT%T", localtime(&walk_start_t));
      if (rc == 0) {
        walk_s[0] = '\0';
      }
      printf("%s: Walking directory: %s\n", walk_s, target);
      fflush(stdout);
    }

    /* walk file tree and record stat data for each file */
    double start_walk = MPI_Wtime();
    dftw(target, record_info);
    double end_walk = MPI_Wtime();

    time_t walk_end_t = time(NULL);
    if (walk_end_t == (time_t)-1) {
      /* TODO: ERROR! */
    }
    walk_end = (uint64_t) walk_end_t;

    /* convert stat structs to datatypes */
    convert_stat_to_dt(list_head, &files);

    /* free linked list */
    elem_t* current = list_head;
    while (current != NULL) {
      elem_t* next = current->next;
      bayer_free(&(current->file));
      if (current->sb != NULL) {
        bayer_free(&(current->sb));
      }
      bayer_free(&current);
      current = next;
    }

    /* get total file count */
    uint64_t my_count = files.count;
    MPI_Allreduce(&my_count, &all_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* report walk count, time, and rate */
    if (verbose && rank == 0) {
      double time = end_walk - start_walk;
      double rate = 0.0;
      if (time > 0.0) {
        rate = ((double)all_count) / time;
      }
      printf("Walked %lu files in %f seconds (%f files/sec)\n",
        all_count, time, rate
      );

      /* convert total size to units */
      double agg_size_tmp;
      const char* agg_size_units;
      bayer_format_bytes(total_bytes, &agg_size_tmp, &agg_size_units);

      printf("Items: %lu\n", (unsigned long long) total_dirs + total_files + total_links);
      printf("  Directories: %lu\n", (unsigned long long) total_dirs);
      printf("  Files: %lu\n", (unsigned long long) total_files);
      printf("  Links: %lu\n", (unsigned long long) total_links);
      if (walk_stat) {
        printf("Data: %.3lf %s\n", agg_size_tmp, agg_size_units);
      }
    }
  } else {
    /* read data from cache file */
    double start_read = MPI_Wtime();
    read_cache(cachename, &users, &groups, &files, &walk_start, &walk_end);
    double end_read = MPI_Wtime();

    if (walk_stat) {
      create_maps(&users, user_name2id, user_id2name);
      create_maps(&groups, group_name2id, group_id2name);
    }

    /* get total file count */
    uint64_t my_count = files.count;
    MPI_Allreduce(&my_count, &all_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* report read count, time, and rate */
    if (verbose && rank == 0) {
      double time = end_read - start_read;
      double rate = 0.0;
      if (time > 0.0) {
        rate = ((double)all_count) / time;
      }
      printf("Read %lu files in %f seconds (%f files/sec)\n",
        all_count, time, rate
      );
    }
  }

  /* write data to cache file */
  if (walk && cachename != NULL) {
    double start_write = MPI_Wtime();
    if (walk_stat) {
      write_cache_stat(cachename, &users, &groups, &files, walk_start, walk_end);
    } else {
      write_cache_readdir(cachename, &files, walk_start, walk_end);
    }
    double end_write = MPI_Wtime();

    /* report write count, time, and rate */
    if (verbose && rank == 0) {
      double time = end_write - start_write;
      double rate = 0.0;
      if (time > 0.0) {
        rate = ((double)all_count) / time;
      }
      printf("Wrote %lu files in %f seconds (%f files/sec)\n",
        all_count, time, rate
      );
    }
  }

  /* TODO: filter files */

  /* sort files */
  if (sortfields != NULL) {
    void* newbuf;
    uint64_t newcount;

    /* TODO: don't sort unless all_count > 0 */

    double start_sort = MPI_Wtime();
    if (walk_stat) {
      sort_files_stat(sortfields, &users, &groups, &files, user_id2name, group_id2name);
    } else {
      sort_files_readdir(sortfields, &files);
    }
    double end_sort = MPI_Wtime();

    /* report sort count, time, and rate */
    if (verbose && rank == 0) {
      double time = end_sort - start_sort;
      double rate = 0.0;
      if (time > 0.0) {
        rate = ((double)all_count) / time;
      }
      printf("Sorted %lu files in %f seconds (%f files/sec)\n",
        all_count, time, rate
      );
    }
  }

  /* print files */
  if (print) {
    print_files(&users, &groups, &files, user_id2name, group_id2name);
  }

  /* free users, groups, and files objects */
  free_buft(&users);
  free_buft(&groups);
  free_buft(&files);

  /* free memory allocated for options */
  bayer_free(&sortfields);
  bayer_free(&cachename);

  /* shut down the sorting library */
  DTCMP_Finalize();

  bayer_free(&target);

  /* shut down MPI */
  MPI_Finalize();

  return 0;
}
