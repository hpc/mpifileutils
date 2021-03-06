from mpi4py import MPI

from cffi import FFI

ffi = FFI()

# Describe the data type and function prototype to cffi.
ffi.cdef('''
typedef long uid_t;
typedef long gid_t;
typedef long mode_t;

int mfu_init(void);

int mfu_finalize(void);

typedef void* mfu_flist;

mfu_flist mfu_flist_new(void);

void mfu_flist_free(mfu_flist* flist);

typedef struct {
    int dir_perms;      /* flag option to update dir perms during walk */
    int remove;         /* flag option to remove files during walk */
    int use_stat;       /* flag option on whether or not to stat files during walk */
    int dereference;    /* flag option to dereference symbolic links */
} mfu_walk_opts_t;

mfu_walk_opts_t* mfu_walk_opts_new(void);

void mfu_walk_opts_delete(mfu_walk_opts_t** opts);

typedef struct {
    enum                 {POSIX, DFS, DAOS} type;
    int                  fd;
} mfu_file_t;

mfu_file_t* mfu_file_new(void);

void mfu_file_delete(mfu_file_t** mfile);

void mfu_flist_walk_path(
    const char* path,           /* IN  - path to be walked */
    mfu_walk_opts_t* walk_opts, /* IN  - functions to perform during the walk */
    mfu_flist flist,            /* OUT - flist to insert walked items into */
    mfu_file_t* mfu_file        /* IN  - I/O filesystem functions to use during the walk */
);

/* create list as subset of another list
 * (returns emtpy list with same user and group maps) */
mfu_flist mfu_flist_subset(mfu_flist srclist);

/* run this to enable query functions on list after adding elements */
int mfu_flist_summarize(mfu_flist flist);

/* read file list from file */
void mfu_flist_read_cache(
    const char* name,
    mfu_flist flist
);

/* write file list to file */
void mfu_flist_write_cache(
    const char* name,
    mfu_flist flist
);

/* write file list to text file */
void mfu_flist_write_text(
    const char* name,
    mfu_flist flist
);

/* print count of items, directories, files, links, and bytes */
void mfu_flist_print_summary(mfu_flist flist);

/* options to configure creation of directories and files */
typedef struct {
    bool overwrite;       /* whether to replace unlink existing items (non-directories) */
    bool set_owner;       /* whether to copy uid/gid from flist to item */
    bool set_timestamps;  /* whether to copy timestamps from flist to item */
    bool set_permissions; /* whether to copy permission bits from flist to item */
    mode_t umask;         /* umask to apply when setting permissions (default current umask) */
    bool lustre_stripe;   /* whether to apply lustre striping parameters */
    uint64_t lustre_stripe_minsize; /* min file size in bytes for which to stripe file */
    uint64_t lustre_stripe_width;   /* size of a single stripe in bytes */
    uint64_t lustre_stripe_count;   /* number of stripes */
} mfu_create_opts_t;

/* return a newly allocated create opts structure */
mfu_create_opts_t* mfu_create_opts_new(void);

/* free create options allocated from mfu_create_opts_new */
void mfu_create_opts_delete(mfu_create_opts_t** popts);

/* create all directories in flist */
void mfu_flist_mkdir(
    mfu_flist flist,
    mfu_create_opts_t* opts
);

/* create inodes for all regular files in flist, assumes directories exist */
void mfu_flist_mknod(
    mfu_flist flist,
    mfu_create_opts_t* opts
);

/* apply metadata updates to items in list */
void mfu_flist_metadata_apply(
    mfu_flist flist,
    mfu_create_opts_t* opts
);

/* we parse the mode string given by the user and build a linked list of
 * permissions operations, this defines one element in that list.  This
 * enables the user to specify a sequence of operations separated with
 * commas like "u+r,g+x" */
typedef struct mfu_perms_t {
    int octal;           /* set to 1 if mode_octal is valid */
    long mode_octal;     /* records octal mode (converted to an integer) */
    int usr;             /* set to 1 if user (owner) bits should be set (e.g. u+r) */
    int group;           /* set to 1 if group bits should be set (e.g. g+r) */
    int other;           /* set to 1 if other bits should be set (e.g. o+r) */
    int all;             /* set to 1 if all bits should be set (e.g. a+r) */
    int assume_all;      /* if this flag is set umask is taken into account */
    int plus;            /* set to 1 if mode has plus, set to 0 for minus */
    int read;            /* set to 1 if 'r' is given */
    int write;           /* set to 1 if 'w' is given */
    int execute;         /* set to 1 if 'x' is given */
    int capital_execute; /* set to 1 if 'X' is given */
    int assignment;      /* set to 1 if operation is an assignment (e.g. g=u) */
    char source;         /* records source of target: 'u', 'g', 'a' */
    struct mfu_perms_t* next;  /* pointer to next perms struct in linked list */
} mfu_perms;

/* given a mode string like "u+r,g-x", fill in a linked list of permission
 * struct pointers returns 1 on success, 0 on failure */
int mfu_perms_parse(const char* modestr, mfu_perms** pperms);

/* free the permissions linked list allocated in mfu_perms_parse,
 * sets pointer to NULL on return */
void mfu_perms_free(mfu_perms** pperms);

typedef struct {
    uid_t getuid;   /* result from getuid */
    uid_t geteuid;  /* result from geteuid */
    uid_t uid;      /* new user id for item's owner, -1 for no change */
    gid_t gid;      /* new group id for item's group, -1 for no change  */
    mode_t umask;   /* umask to apply when setting item permissions */
    bool capchown;  /* whether process has CAP_CHOWN capability */
    bool capfowner; /* whether process has CAP_FOWNER capability */
    bool force;     /* always call chmod/chgrp on every item */
    bool silence;   /* avoid printing EPERM errors */
} mfu_chmod_opts_t;

/* return a newly allocated chmod structure */
mfu_chmod_opts_t* mfu_chmod_opts_new(void);

/* free chmod options allocated from mfu_chmod_opts_new */
void mfu_chmod_opts_delete(mfu_chmod_opts_t** popts);

/* given an input flist,
 * change owner on items if usrname != NULL,
 * change group on items if grname != NULL
 * set permissions on items according to perms list if head != NULL */
void mfu_flist_chmod(
  mfu_flist flist,
  const char* usrname,
  const char* grname,
  const mfu_perms* head,
  mfu_chmod_opts_t* opts
);

/* unlink all items in flist,
 * if traceless=1, restore timestamps on parent directories after unlinking children */
void mfu_flist_unlink(mfu_flist flist, bool traceless, mfu_file_t* mfu_file);

/* takes a list, spreads it evenly among processes with respect to item count,
 * and then returns the newly created list to the caller */
mfu_flist mfu_flist_spread(mfu_flist flist);

mfu_flist mfu_flist_sort(const char* fields, mfu_flist flist);

/* create a new empty entry in the file list and return its index */
uint64_t mfu_flist_file_create(mfu_flist flist);

typedef enum mfu_filetypes_e {
    MFU_TYPE_NULL    = 0, /* type not set */
    MFU_TYPE_UNKNOWN = 1, /* type not known */
    MFU_TYPE_FILE    = 2, /* regular file */
    MFU_TYPE_DIR     = 3, /* directory */
    MFU_TYPE_LINK    = 4, /* symlink */
} mfu_filetype;

uint64_t mfu_flist_global_size(mfu_flist flist);
uint64_t mfu_flist_global_offset(mfu_flist flist);
uint64_t mfu_flist_size(mfu_flist flist);

const char* mfu_flist_file_get_name(mfu_flist flist, uint64_t index);
int mfu_flist_file_get_depth(mfu_flist flist, uint64_t index);
mfu_filetype mfu_flist_file_get_type(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_mode(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_uid(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_gid(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_atime(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_atime_nsec(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_mtime(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_mtime_nsec(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_ctime(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_ctime_nsec(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_size(mfu_flist flist, uint64_t index);
uint64_t mfu_flist_file_get_perm(mfu_flist flist, uint64_t index);
const char* mfu_flist_file_get_username(mfu_flist flist, uint64_t index);
const char* mfu_flist_file_get_groupname(mfu_flist flist, uint64_t index);

void mfu_flist_file_set_name(mfu_flist flist, uint64_t index, const char* name);
void mfu_flist_file_set_type(mfu_flist flist, uint64_t index, mfu_filetype type);
void mfu_flist_file_set_detail(mfu_flist flist, uint64_t index, int detail);
void mfu_flist_file_set_mode(mfu_flist flist, uint64_t index, uint64_t mode);
void mfu_flist_file_set_uid(mfu_flist flist, uint64_t index, uint64_t uid);
void mfu_flist_file_set_gid(mfu_flist flist, uint64_t index, uint64_t gid);
void mfu_flist_file_set_atime(mfu_flist flist, uint64_t index, uint64_t atime);
void mfu_flist_file_set_atime_nsec(mfu_flist flist, uint64_t index, uint64_t atime_nsec);
void mfu_flist_file_set_mtime(mfu_flist flist, uint64_t index, uint64_t mtime);
void mfu_flist_file_set_mtime_nsec(mfu_flist flist, uint64_t index, uint64_t mtime_nsec);
void mfu_flist_file_set_ctime(mfu_flist flist, uint64_t index, uint64_t ctime);
void mfu_flist_file_set_ctime_nsec(mfu_flist flist, uint64_t index, uint64_t ctime_nsec);
void mfu_flist_file_set_size(mfu_flist flist, uint64_t index, uint64_t size);

''')

libmfu = ffi.dlopen('./install/lib64/libmfu.so')
#print('Loaded lib {0}'.format(libmfu))

# initialize libmfu (just do this once)
libmfu.mfu_init()

# represents an item from a list
# provides attributes to access item information
class FItem:
  def __init__(self):
    self.name  = None
    self.type  = None
    self.size  = None
    self.uid   = None
    self.gid   = None
    self.mode  = None
    self.user  = None
    self.group = None
    self.atime   = None
    self.atimens = None
    self.mtime   = None
    self.mtimens = None
    self.ctime   = None
    self.ctimens = None

  # use name to represent item
  def __repr__(self):
    return self.name

class FList:
  def __init__(self, walk=None, read=None, flist=None):
    self.idx = None

    self.flist = None
    if walk != None:
      # given a path to walk
      self.walk(walk)
    elif read != None:
      # given file to read
      self.read(read)
    elif flist != None:
      # given an explicit flist to use
      self.flist = flist
    else:
      # create an empty list if not given something else
      self.flist = libmfu.mfu_flist_new()

  # we may hold a pointer to an flist that was allocated in __init__
  # free this during the desctructor
  def __del__(self):
    self.free_flist()

  # free existing flist, this takes a pointer, which is a bit cumbersome
  def free_flist(self):
    if self.flist != None:
      flist_ptr = ffi.new("mfu_flist[1]")
      flist_ptr[0] = self.flist
      libmfu.mfu_flist_free(flist_ptr)
      self.flist = None
    
  # MPI communicator
  def comm(self):
    comm = MPI.COMM_WORLD
    return comm

  # rank of current process in comm
  def rank(self):
    rank = MPI.COMM_WORLD.rank
    return rank

  # number of ranks in comm
  def num_ranks(self):
    num_ranks = MPI.COMM_WORLD.size
    return num_ranks 

  # return size of local list for len(flist)
  def __len__(self):
    size = libmfu.mfu_flist_size(self.flist)
    return int(size)

  # report global number of items in list as string representation
  def __repr__(self):
    size = self.global_size()
    return "Total items: " + str(size)

  # global list size
  def global_size(self):
    size = libmfu.mfu_flist_global_size(self.flist)
    return int(size)

  # global offset of this rank
  def global_offset(self):
    size = libmfu.mfu_flist_global_offset(self.flist)
    return int(size)

  # given an index into our local list, allocate and return an item
  def extract_item(self, i):
    # check that index is in range of our local list
    if i < 0 or i >= self.__len__():
      raise IndexError

    item = FItem()
    item.name    = ffi.string(libmfu.mfu_flist_file_get_name(self.flist, i))
    item.type    = libmfu.mfu_flist_file_get_type(self.flist, i)
    item.size    = libmfu.mfu_flist_file_get_size(self.flist, i)
    item.mode    = libmfu.mfu_flist_file_get_mode(self.flist, i)
    item.uid     = libmfu.mfu_flist_file_get_uid(self.flist, i)
    item.gid     = libmfu.mfu_flist_file_get_gid(self.flist, i)
    item.user    = ffi.string(libmfu.mfu_flist_file_get_username(self.flist, i))
    item.group   = ffi.string(libmfu.mfu_flist_file_get_groupname(self.flist, i))
    item.atime   = libmfu.mfu_flist_file_get_atime(self.flist, i)
    item.atimens = libmfu.mfu_flist_file_get_atime_nsec(self.flist, i)
    item.mtime   = libmfu.mfu_flist_file_get_mtime(self.flist, i)
    item.mtimens = libmfu.mfu_flist_file_get_mtime_nsec(self.flist, i)
    item.ctime   = libmfu.mfu_flist_file_get_ctime(self.flist, i)
    item.ctimens = libmfu.mfu_flist_file_get_ctime_nsec(self.flist, i)

    return item

  # get a single item or a list of items from a slice
  def __getitem__(self, idx):
    if isinstance(idx, slice):
      i = idx.start

      step = 1
      if idx.step != None:
        step = idx.step

      l = []
      while i < idx.stop:
        l.append(self.extract_item(i))
        i += step

      return l
    else:
      return self.extract_item(idx)

  # support the iterator interface to step through our local list
  def __iter__(self):
    self.idx = 0
    return self

  # return next item in iteration
  # use __next__ in python3
  #def __next__(self):
  def next(self):
    idx = self.idx
    size = self.__len__()
    if idx < size:
      self.idx += 1
      return self.__getitem__(idx)
    raise StopIteration

  # walk given path and fill in flist
  def walk(self, path):
    self.free_flist()
    self.flist = libmfu.mfu_flist_new()

    opts = libmfu.mfu_walk_opts_new()
    mfufile = libmfu.mfu_file_new()
    libmfu.mfu_flist_walk_path(path, opts, self.flist, mfufile)

    mfufile_ptr = ffi.new("mfu_file_t*[1]")
    mfufile_ptr[0] = mfufile
    libmfu.mfu_file_delete(mfufile_ptr)

    opts_ptr = ffi.new("mfu_walk_opts_t*[1]")
    opts_ptr[0] = opts
    libmfu.mfu_walk_opts_delete(opts_ptr)

  # read flist from file name
  def read(self, fname):
    self.free_flist()
    self.flist = libmfu.mfu_flist_new()
    libmfu.mfu_flist_read_cache(fname, self.flist)

  # write flist to file name
  def write(self, fname, text=False):
    if text:
      libmfu.mfu_flist_write_text(fname, self.flist)
    else:
      libmfu.mfu_flist_write_cache(fname, self.flist)

  # create an empty subset list for this list object
  def subset(self):
    flist = libmfu.mfu_flist_subset(self.flist)
    return FList(flist=flist)

  # append a file item to the current list object
  #def __append__(self, item):
  def append(self, item):
    idx = libmfu.mfu_flist_file_create(self.flist)

    libmfu.mfu_flist_file_set_name(self.flist,       idx, item.name)
    libmfu.mfu_flist_file_set_type(self.flist,       idx, item.type)
    libmfu.mfu_flist_file_set_size(self.flist,       idx, item.size)
    libmfu.mfu_flist_file_set_mode(self.flist,       idx, item.mode)
    libmfu.mfu_flist_file_set_uid(self.flist,        idx, item.uid)
    libmfu.mfu_flist_file_set_gid(self.flist,        idx, item.gid)
    #libmfu.mfu_flist_file_set_username(self.flist,   idx, item.user)
    #libmfu.mfu_flist_file_set_groupname(self.flist,  idx, item.group)
    libmfu.mfu_flist_file_set_atime(self.flist,      idx, item.atime)
    libmfu.mfu_flist_file_set_atime_nsec(self.flist, idx, item.atimens)
    libmfu.mfu_flist_file_set_mtime(self.flist,      idx, item.mtime)
    libmfu.mfu_flist_file_set_mtime_nsec(self.flist, idx, item.mtimens)
    libmfu.mfu_flist_file_set_ctime(self.flist,      idx, item.ctime)
    libmfu.mfu_flist_file_set_ctime_nsec(self.flist, idx, item.ctimens)

  # compute global properties of flist
  def summarize(self):
    libmfu.mfu_flist_summarize(self.flist)

  # sort the list given a comma-delimited list of fields
  def sort(self, fields="name"):
    flist = libmfu.mfu_flist_sort(fields, self.flist)
    self.free_flist()
    self.flist = flist

  # spread the list evenly among ranks
  def spread(self):
    flist = libmfu.mfu_flist_spread(self.flist)
    self.free_flist()
    self.flist = flist

  # change mode, owner, or group of items in list
  def chmod(self, mode=None, user=None, group=None):
    perms_ptr = None
    perms = None
    if mode:
      perms_ptr = ffi.new("mfu_perms*[1]")
      libmfu.mfu_perms_parse(mode, perms_ptr)
      perms = perms_ptr[0]

    opts = libmfu.mfu_chmod_opts_new()

    if not user:
      user = ffi.NULL
    if not group:
      group = ffi.NULL
    if not perms:
      perms = ffi.NULL

    libmfu.mfu_flist_chmod(self.flist, user, group, perms, opts)

    opts_ptr = ffi.new("mfu_chmod_opts_t*[1]")
    opts_ptr[0] = opts
    libmfu.mfu_chmod_opts_delete(opts_ptr)

    if perms_ptr:
      libmfu.mfu_perms_free(perms_ptr)

  # delete items in list from file system
  def unlink(self):
    mfufile = libmfu.mfu_file_new()
    libmfu.mfu_flist_unlink(self.flist, 0, mfufile)
    mfufile_ptr = ffi.new("mfu_file_t*[1]")
    mfufile_ptr[0] = mfufile
    libmfu.mfu_file_delete(mfufile_ptr)

# shut down libmfu, way to do this on exit?
#libmfu.mfu_finalize()
