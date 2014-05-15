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

// getpwent getgrent to read user and group entries

/* TODO: change globals to struct */
static int verbose   = 0;
static int walk_stat = 1;

/* keep stats during walk */
uint64_t total_dirs    = 0;
uint64_t total_files   = 0;
uint64_t total_links   = 0;
uint64_t total_unknown = 0;
uint64_t total_bytes   = 0;

typedef enum {
  NULLFIELD = 0,
  FILENAME,
  USERNAME,
  GROUPNAME,
  USERID,
  GROUPID,
  ATIME,
  MTIME,
  CTIME,
  FILESIZE,
} sort_field;

/* routine for sorting strings in ascending order */
static int my_strcmp(const void* a, const void* b)
{
  return strcmp((const char*)a, (const char*)b);
}

static int my_strcmp_rev(const void* a, const void* b)
{
  return strcmp((const char*)b, (const char*)a);
}

static int sort_files_readdir(const char* sortfields, bayer_flist* pflist)
{
  /* get list from caller */
  bayer_flist flist = *pflist;

  /* create a new list as subset of original list */
  bayer_flist flist2 = bayer_flist_subset(flist);

  uint64_t incount = bayer_flist_size(flist);
  uint64_t chars   = bayer_flist_file_max_name(flist);

  /* create datatype for packed file list element */
  MPI_Datatype dt_sat;
  size_t bytes = bayer_flist_file_pack_size(flist);
  MPI_Type_contiguous((int)bytes, MPI_BYTE, &dt_sat);

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* build type for file path */
  MPI_Datatype dt_filepath;
  MPI_Type_contiguous((int)chars, MPI_CHAR, &dt_filepath);
  MPI_Type_commit(&dt_filepath);

  /* build comparison op for filenames */
  DTCMP_Op op_filepath;
  if (DTCMP_Op_create(dt_filepath, my_strcmp, &op_filepath) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create sorting operation for filepath");
  }

  /* build comparison op for filenames */
  DTCMP_Op op_filepath_rev;
  if (DTCMP_Op_create(dt_filepath, my_strcmp_rev, &op_filepath_rev) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create reverse sorting operation for filepath");
  }

  /* TODO: process sort fields */
  const int MAXFIELDS = 1;
  MPI_Datatype types[MAXFIELDS];
  DTCMP_Op ops[MAXFIELDS];
  sort_field fields[MAXFIELDS];
  size_t lengths[MAXFIELDS];
  int nfields = 0;
  for (nfields = 0; nfields < MAXFIELDS; nfields++) {
    types[nfields]   = MPI_DATATYPE_NULL;
    ops[nfields]     = DTCMP_OP_NULL;
  }
  nfields = 0;
  char* sortfields_copy = BAYER_STRDUP(sortfields);
  char* token = strtok(sortfields_copy, ",");
  while (token != NULL) {
    int valid = 1;
    if (strcmp(token, "name") == 0) {
      types[nfields]   = dt_filepath;
      ops[nfields]     = op_filepath;
      fields[nfields]  = FILENAME;
      lengths[nfields] = chars;
    } else if (strcmp(token, "-name") == 0) {
      types[nfields]   = dt_filepath;
      ops[nfields]     = op_filepath_rev;
      fields[nfields]  = FILENAME;
      lengths[nfields] = chars;
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
  if (DTCMP_Type_create_series(nfields, types, &dt_key) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create type for key");
  }

  /* create sort op */
  DTCMP_Op op_key;
  if (DTCMP_Op_create_series(nfields, ops, &op_key) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create sorting operation for key");
  }

  /* build keysat type */
  MPI_Datatype dt_keysat, keysat_types[2];
  keysat_types[0] = dt_key;
  keysat_types[1] = dt_sat;
  if (DTCMP_Type_create_series(2, keysat_types, &dt_keysat) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create type for keysat");
  }

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
  size_t sortbufsize = (size_t)keysat_extent * incount;
  void* sortbuf = BAYER_MALLOC(sortbufsize);

  /* copy data into sort elements */
  uint64_t idx = 0;
  char* sortptr = (char*) sortbuf;
  while (idx < incount) {
    /* copy in access time */
    int i;
    for (i = 0; i < nfields; i++) {
      if (fields[i] == FILENAME) {
        const char* name = bayer_flist_file_get_name(flist, idx);
        strcpy(sortptr, name);
      }
      sortptr += lengths[i];
    }

    /* pack file element */
    sortptr += bayer_flist_file_pack(sortptr, flist, idx);

    idx++;
  }

  /* sort data */
  void* outsortbuf;
  int outsortcount;
  DTCMP_Handle handle;
  int sort_rc = DTCMP_Sortz(
    sortbuf, (int)incount, &outsortbuf, &outsortcount,
    dt_key, dt_keysat, op_key, DTCMP_FLAG_NONE,
    MPI_COMM_WORLD, &handle
  );
  if (sort_rc != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to sort data");
  }

  /* step through sorted data filenames */
  idx = 0;
  sortptr = (char*) outsortbuf;
  while (idx < (uint64_t)outsortcount) {
    sortptr += key_extent;
    sortptr += bayer_flist_file_unpack(sortptr, flist2);
    idx++;
  }

  /* build summary of new list */
  bayer_flist_summarize(flist2);

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

  /* free the satellite type */
  MPI_Type_free(&dt_sat);

  /* return new list and free old one */
  *pflist = flist2;
  bayer_flist_free(&flist);

  return 0;
}

static uint32_t gettime(void)
{
  uint32_t t = 0;
  time_t now = time(NULL);
  if (now != (time_t)-1) {
    t = (uint32_t) now;
  }
  return t;
}

static void filter_files(bayer_flist* pflist)
{
  bayer_flist flist = *pflist;

  // for each file, if (now - atime) > 60d and (now - ctime) > 60d, add to list
  bayer_flist eligible = bayer_flist_subset(flist);

  static uint32_t limit = 60 * 24 * 3600; /* 60 days */
  uint32_t now = gettime();
  uint64_t idx = 0;
  uint64_t files = bayer_flist_size(flist);
  while (idx < files) {
    bayer_filetype type = bayer_flist_file_get_type(flist, idx);
    if (type == BAYER_TYPE_FILE || type == BAYER_TYPE_LINK) {
      /* we only purge files and links */
      uint32_t acc = (uint32_t) bayer_flist_file_get_atime(flist, idx);
      uint32_t cre = (uint32_t) bayer_flist_file_get_ctime(flist, idx);
      if ((now - acc) > limit && (now - cre) > limit) {
        /* only purge items that have not been
         * accessed or changed in past limit seconds */
        bayer_flist_file_copy(flist, idx, eligible);
      }
    }
    idx++;
  }

  bayer_flist_summarize(eligible);

  bayer_flist_free(&flist);
  *pflist = eligible;
  return;
}

static int sort_files_stat(const char* sortfields, bayer_flist* pflist)
{
  /* get list from caller */
  bayer_flist flist = *pflist;

  /* create a new list as subset of original list */
  bayer_flist flist2 = bayer_flist_subset(flist);

  uint64_t incount     = bayer_flist_size(flist);
  uint64_t chars       = bayer_flist_file_max_name(flist);
  uint64_t chars_user  = bayer_flist_user_max_name(flist);
  uint64_t chars_group = bayer_flist_group_max_name(flist);

  /* create datatype for packed file list element */
  MPI_Datatype dt_sat;
  size_t bytes = bayer_flist_file_pack_size(flist);
  MPI_Type_contiguous((int)bytes, MPI_BYTE, &dt_sat);

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* build type for file path */
  MPI_Datatype dt_filepath, dt_user, dt_group;
  MPI_Type_contiguous((int)chars,       MPI_CHAR, &dt_filepath);
  MPI_Type_contiguous((int)chars_user,  MPI_CHAR, &dt_user);
  MPI_Type_contiguous((int)chars_group, MPI_CHAR, &dt_group);
  MPI_Type_commit(&dt_filepath);
  MPI_Type_commit(&dt_user);
  MPI_Type_commit(&dt_group);

  /* build comparison op for filenames */
  DTCMP_Op op_filepath, op_user, op_group;
  if (DTCMP_Op_create(dt_filepath, my_strcmp, &op_filepath) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create sorting operation for filepath");
  }
  if (DTCMP_Op_create(dt_user, my_strcmp, &op_user) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create sorting operation for username");
  }
  if (DTCMP_Op_create(dt_group, my_strcmp, &op_group) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create sorting operation for groupname");
  }

  /* build comparison op for filenames */
  DTCMP_Op op_filepath_rev, op_user_rev, op_group_rev;
  if (DTCMP_Op_create(dt_filepath, my_strcmp_rev, &op_filepath_rev) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create reverse sorting operation for groupname");
  }
  if (DTCMP_Op_create(dt_user, my_strcmp_rev, &op_user_rev) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create reverse sorting operation for groupname");
  }
  if (DTCMP_Op_create(dt_group, my_strcmp_rev, &op_group_rev) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create reverse sorting operation for groupname");
  }

  /* TODO: process sort fields */
  const int MAXFIELDS = 7;
  MPI_Datatype types[MAXFIELDS];
  DTCMP_Op ops[MAXFIELDS];
  sort_field fields[MAXFIELDS];
  size_t lengths[MAXFIELDS];
  int nfields = 0;
  for (nfields = 0; nfields < MAXFIELDS; nfields++) {
    types[nfields]   = MPI_DATATYPE_NULL;
    ops[nfields]     = DTCMP_OP_NULL;
  }
  nfields = 0;
  char* sortfields_copy = BAYER_STRDUP(sortfields);
  char* token = strtok(sortfields_copy, ",");
  while (token != NULL) {
    int valid = 1;
    if (strcmp(token, "name") == 0) {
      types[nfields]   = dt_filepath;
      ops[nfields]     = op_filepath;
      fields[nfields]  = FILENAME;
      lengths[nfields] = chars;
    } else if (strcmp(token, "-name") == 0) {
      types[nfields]   = dt_filepath;
      ops[nfields]     = op_filepath_rev;
      fields[nfields]  = FILENAME;
      lengths[nfields] = chars;
    } else if (strcmp(token, "user") == 0) {
      types[nfields]   = dt_user;
      ops[nfields]     = op_user;
      fields[nfields]  = USERNAME;
      lengths[nfields] = chars_user;
    } else if (strcmp(token, "-user") == 0) {
      types[nfields]   = dt_user;
      ops[nfields]     = op_user_rev;
      fields[nfields]  = USERNAME;
      lengths[nfields] = chars_user;
    } else if (strcmp(token, "group") == 0) {
      types[nfields]   = dt_group;
      ops[nfields]     = op_group;
      fields[nfields]  = GROUPNAME;
      lengths[nfields] = chars_group;
    } else if (strcmp(token, "-group") == 0) {
      types[nfields]   = dt_group;
      ops[nfields]     = op_group_rev;
      fields[nfields]  = GROUPNAME;
      lengths[nfields] = chars_group;
    } else if (strcmp(token, "uid") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      fields[nfields]  = USERID;
      lengths[nfields] = 4;
    } else if (strcmp(token, "-uid") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      fields[nfields]  = USERID;
      lengths[nfields] = 4;
    } else if (strcmp(token, "gid") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      fields[nfields]  = GROUPID;
      lengths[nfields] = 4;
    } else if (strcmp(token, "-gid") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      fields[nfields]  = GROUPID;
      lengths[nfields] = 4;
    } else if (strcmp(token, "atime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      fields[nfields]  = ATIME;
      lengths[nfields] = 4;
    } else if (strcmp(token, "-atime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      fields[nfields]  = ATIME;
      lengths[nfields] = 4;
    } else if (strcmp(token, "mtime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      fields[nfields]  = MTIME;
      lengths[nfields] = 4;
    } else if (strcmp(token, "-mtime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      fields[nfields]  = MTIME;
      lengths[nfields] = 4;
    } else if (strcmp(token, "ctime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_ASCEND;
      fields[nfields]  = CTIME;
      lengths[nfields] = 4;
    } else if (strcmp(token, "-ctime") == 0) {
      types[nfields]   = MPI_UINT32_T;
      ops[nfields]     = DTCMP_OP_UINT32T_DESCEND;
      fields[nfields]  = CTIME;
      lengths[nfields] = 4;
    } else if (strcmp(token, "size") == 0) {
      types[nfields]   = MPI_UINT64_T;
      ops[nfields]     = DTCMP_OP_UINT64T_ASCEND;
      fields[nfields]  = FILESIZE;
      lengths[nfields] = 8;
    } else if (strcmp(token, "-size") == 0) {
      types[nfields]   = MPI_UINT64_T;
      ops[nfields]     = DTCMP_OP_UINT64T_DESCEND;
      fields[nfields]  = FILESIZE;
      lengths[nfields] = 8;
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
  if (DTCMP_Type_create_series(nfields, types, &dt_key) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create key type");
  }

  /* create op to sort by access time, then filename */
  DTCMP_Op op_key;
  if (DTCMP_Op_create_series(nfields, ops, &op_key) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create sorting operation for key");
  }

  /* build keysat type */
  MPI_Datatype dt_keysat, keysat_types[2];
  keysat_types[0] = dt_key;
  keysat_types[1] = dt_sat;
  if (DTCMP_Type_create_series(2, keysat_types, &dt_keysat) != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to create keysat type");
  }

  /* get extent of key type */
  MPI_Aint key_lb, key_extent;
  MPI_Type_get_extent(dt_key, &key_lb, &key_extent);

  /* get extent of keysat type */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(dt_keysat, &keysat_lb, &keysat_extent);

  /* get extent of sat type */
  MPI_Aint stat_lb, stat_extent;
  MPI_Type_get_extent(dt_sat, &stat_lb, &stat_extent);

  /* compute size of sort element and allocate buffer */
  size_t sortbufsize = (size_t)keysat_extent * incount;
  void* sortbuf = BAYER_MALLOC(sortbufsize);

  /* copy data into sort elements */
  uint64_t idx = 0;
  char* sortptr = (char*) sortbuf;
  while (idx < incount) {
    /* copy in access time */
    int i;
    for (i = 0; i < nfields; i++) {
      if (fields[i] == FILENAME) {
        const char* name = bayer_flist_file_get_name(flist, idx);
        strcpy(sortptr, name);
      } else if (fields[i] == USERNAME) {
        const char* name = bayer_flist_file_get_username(flist, idx);
        strcpy(sortptr, name);
      } else if (fields[i] == GROUPNAME) {
        const char* name = bayer_flist_file_get_groupname(flist, idx);
        strcpy(sortptr, name);
      } else if (fields[i] == USERID) {
        uint32_t val32 = (uint32_t) bayer_flist_file_get_uid(flist, idx);
        memcpy(sortptr, &val32, 4);
      } else if (fields[i] == GROUPID) {
        uint32_t val32 = (uint32_t) bayer_flist_file_get_gid(flist, idx);
        memcpy(sortptr, &val32, 4);
      } else if (fields[i] == ATIME) {
        uint32_t val32 = (uint32_t) bayer_flist_file_get_atime(flist, idx);
        memcpy(sortptr, &val32, 4);
      } else if (fields[i] == MTIME) {
        uint32_t val32 = (uint32_t) bayer_flist_file_get_mtime(flist, idx);
        memcpy(sortptr, &val32, 4);
      } else if (fields[i] == CTIME) {
        uint32_t val32 = (uint32_t) bayer_flist_file_get_ctime(flist, idx);
        memcpy(sortptr, &val32, 4);
      } else if (fields[i] == FILESIZE) {
        uint64_t val64 = bayer_flist_file_get_size(flist, idx);
        memcpy(sortptr, &val64, 8);
      }
      
      sortptr += lengths[i];
    }

    /* pack file element */
    sortptr += bayer_flist_file_pack(sortptr, flist, idx);

    idx++;
  }

  /* sort data */
  void* outsortbuf;
  int outsortcount;
  DTCMP_Handle handle;
  int sort_rc = DTCMP_Sortz(
    sortbuf, (int)incount, &outsortbuf, &outsortcount,
    dt_key, dt_keysat, op_key, DTCMP_FLAG_NONE,
    MPI_COMM_WORLD, &handle
  );
  if (sort_rc != DTCMP_SUCCESS) {
    BAYER_ABORT(1, "Failed to sort data");
  }

  /* step through sorted data filenames */
  idx = 0;
  sortptr = (char*) outsortbuf;
  while (idx < (uint64_t)outsortcount) {
    sortptr += key_extent;
    sortptr += bayer_flist_file_unpack(sortptr, flist2);
    idx++;
  }

  /* build summary of new list */
  bayer_flist_summarize(flist2);

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

  /* free the satellite type */
  MPI_Type_free(&dt_sat);

  /* return new list and free old one */
  *pflist = flist2;
  bayer_flist_free(&flist);

  return 0;
}

static void print_summary(bayer_flist flist)
{
  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* step through and print data */
  uint64_t idx = 0;
  uint64_t max = bayer_flist_size(flist);
  while (idx < max) {
    if (bayer_flist_have_detail(flist)) {
      /* get mode */
      mode_t mode = (mode_t) bayer_flist_file_get_mode(flist, idx);

      /* set file type */
      if (S_ISDIR(mode)) {
        total_dirs++;
      } else if (S_ISREG(mode)) {
        total_files++;
      } else if (S_ISLNK(mode)) {
        total_links++;
      } else {
        /* unknown file type */
        total_unknown++;
      }

      uint64_t size = bayer_flist_file_get_size(flist, idx);
      total_bytes += size;
    } else {
      /* get type */
      bayer_filetype type = bayer_flist_file_get_type(flist, idx);

      if (type == BAYER_TYPE_DIR) {
        total_dirs++;
      } else if (type == BAYER_TYPE_FILE) {
        total_files++;
      } else if (type == BAYER_TYPE_LINK) {
        total_links++;
      } else {
        /* unknown file type */
        total_unknown++;
      }
    }

    /* go to next file */
    idx++;
  }

  /* get total directories, files, links, and bytes */
  uint64_t all_dirs, all_files, all_links, all_unknown, all_bytes;
  uint64_t all_count = bayer_flist_global_size(flist);
  MPI_Allreduce(&total_dirs,    &all_dirs,    1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  MPI_Allreduce(&total_files,   &all_files,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  MPI_Allreduce(&total_links,   &all_links,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  MPI_Allreduce(&total_unknown, &all_unknown, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
  MPI_Allreduce(&total_bytes,   &all_bytes,   1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

  /* convert total size to units */
  if (verbose && rank == 0) {
    printf("Items: %llu\n", (unsigned long long) all_count);
    printf("  Directories: %llu\n", (unsigned long long) all_dirs);
    printf("  Files: %llu\n", (unsigned long long) all_files);
    printf("  Links: %llu\n", (unsigned long long) all_links);
    /* printf("  Unknown: %lu\n", (unsigned long long) all_unknown); */

    if (bayer_flist_have_detail(flist)) {
      double agg_size_tmp;
      const char* agg_size_units;
      bayer_format_bytes(all_bytes, &agg_size_tmp, &agg_size_units);

      uint64_t size_per_file = 0.0;
      if (all_files > 0) {
        size_per_file = (uint64_t)((double)all_bytes/(double)all_files);
      }
      double size_per_file_tmp;
      const char* size_per_file_units;
      bayer_format_bytes(size_per_file, &size_per_file_tmp, &size_per_file_units);

      printf("Data: %.3lf %s (%.3lf %s per file)\n", agg_size_tmp, agg_size_units, size_per_file_tmp, size_per_file_units);
    }
  }

  return;
}

static char mode_format[11];
static void prepare_mode_format(mode_t mode)
{
  if (S_ISDIR(mode)) {
    mode_format[0] = 'd';
  } else if (S_ISLNK(mode)) {
    mode_format[0] = 'l';
  } else {
    mode_format[0] = '-';
  }

  if (S_IRUSR & mode) {
    mode_format[1] = 'r';
  } else {
    mode_format[1] = '-';
  }

  if (S_IWUSR & mode) {
    mode_format[2] = 'w';
  } else {
    mode_format[2] = '-';
  }

  if (S_IXUSR & mode) {
    mode_format[3] = 'x';
  } else {
    mode_format[3] = '-';
  }

  if (S_IRGRP & mode) {
    mode_format[4] = 'r';
  } else {
    mode_format[4] = '-';
  }

  if (S_IWGRP & mode) {
    mode_format[5] = 'w';
  } else {
    mode_format[5] = '-';
  }

  if (S_IXGRP & mode) {
    mode_format[6] = 'x';
  } else {
    mode_format[6] = '-';
  }

  if (S_IROTH & mode) {
    mode_format[7] = 'r';
  } else {
    mode_format[7] = '-';
  }

  if (S_IWOTH & mode) {
    mode_format[8] = 'w';
  } else {
    mode_format[8] = '-';
  }

  if (S_IXOTH & mode) {
    mode_format[9] = 'x';
  } else {
    mode_format[9] = '-';
  }

  mode_format[10] = '\0';

  return;
}

static char type_str_unknown[] = "UNK";
static char type_str_dir[]     = "DIR";
static char type_str_file[]    = "REG";
static char type_str_link[]    = "LNK";

static void print_file(bayer_flist flist, uint64_t idx, int rank)
{
  /* get filename */
  const char* file = bayer_flist_file_get_name(flist, idx);

  if (bayer_flist_have_detail(flist)) {
    /* get mode */
    mode_t mode = (mode_t) bayer_flist_file_get_mode(flist, idx);

    uint32_t uid = (uint32_t) bayer_flist_file_get_uid(flist, idx);
    uint32_t gid = (uint32_t) bayer_flist_file_get_gid(flist, idx);
    uint64_t acc = bayer_flist_file_get_atime(flist, idx);
    uint64_t mod = bayer_flist_file_get_mtime(flist, idx);
    uint64_t cre = bayer_flist_file_get_ctime(flist, idx);
    uint64_t size = bayer_flist_file_get_size(flist, idx);
    const char* username  = bayer_flist_file_get_username(flist, idx);
    const char* groupname = bayer_flist_file_get_groupname(flist, idx);

    char access_s[30];
    char modify_s[30];
    char create_s[30];
    time_t access_t = (time_t) acc;
    time_t modify_t = (time_t) mod;
    time_t create_t = (time_t) cre;
    size_t access_rc = strftime(access_s, sizeof(access_s)-1, "%FT%T", localtime(&access_t));
    size_t modify_rc = strftime(modify_s, sizeof(modify_s)-1, "%FT%T", localtime(&modify_t));
    size_t create_rc = strftime(create_s, sizeof(create_s)-1, "%FT%T", localtime(&create_t));
    if (access_rc == 0 || modify_rc == 0 || create_rc == 0) {
      /* error */
      access_s[0] = '\0';
      modify_s[0] = '\0';
      create_s[0] = '\0';
    }

    prepare_mode_format(mode);

    printf("Mode=%lx(%s) UID=%d(%s) GUI=%d(%s) Access=%s Modify=%s Create=%s Size=%lu File=%s\n",
      (unsigned long)mode, mode_format, uid, username, gid, groupname,
      access_s, modify_s, create_s, (unsigned long)size, file
    );
  } else {
    /* get type */
    bayer_filetype type = bayer_flist_file_get_type(flist, idx);
    char* type_str = type_str_unknown;
    if (type == BAYER_TYPE_DIR) {
      type_str = type_str_dir;
    } else if (type == BAYER_TYPE_FILE) {
      type_str = type_str_file;
    } else if (type == BAYER_TYPE_LINK) {
      type_str = type_str_link;
    }

    printf("Type=%s File=%s\n",
      type_str, file
    );
  }
}

static void print_files(bayer_flist flist)
{
  /* number of items to print from start and end of list */
  uint64_t range = 10;

  /* allocate send and receive buffers */
  size_t pack_size = bayer_flist_file_pack_size(flist);
  size_t bufsize = 2 * range * pack_size;
  void* sendbuf = BAYER_MALLOC(bufsize);
  void* recvbuf = BAYER_MALLOC(bufsize);

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* identify the number of items we have, the total number,
   * and our offset in the global list */
  uint64_t count  = bayer_flist_size(flist);
  uint64_t total  = bayer_flist_global_size(flist);
  uint64_t offset = bayer_flist_global_offset(flist);

  /* count the number of items we'll send */
  int num = 0;
  uint64_t idx = 0;
  while (idx < count) {
    uint64_t global = offset + idx;
    if (global < range || (total - global) <= range) {
      num++;
    }
    idx++;
  }

  /* allocate arrays to store counts and displacements */
  int* counts = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));
  int* disps  = (int*) BAYER_MALLOC((size_t)ranks * sizeof(int));

  /* tell rank 0 where the data is coming from */
  int bytes = num * (int)pack_size;
  MPI_Gather(&bytes, 1, MPI_INT, counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

  /* pack items into sendbuf */
  idx = 0;
  char* ptr = (char*) sendbuf;
  while (idx < count) {
    uint64_t global = offset + idx;
    if (global < range || (total - global) <= range) {
      ptr += bayer_flist_file_pack(ptr, flist, idx);
    }
    idx++;
  }

  /* compute displacements and total bytes */
  int recvbytes = 0;
  if (rank == 0) {
    int i;
    disps[0] = 0;
    recvbytes += counts[0];
    for (i = 1; i < ranks; i++) {
      disps[i] = disps[i-1] + counts[i-1];
      recvbytes += counts[i];
    }
  }

  /* gather data to rank 0 */
  MPI_Gatherv(sendbuf, bytes, MPI_BYTE, recvbuf, counts, disps, MPI_BYTE, 0, MPI_COMM_WORLD);

  /* create temporary list to unpack items into */
  bayer_flist tmplist = bayer_flist_subset(flist);

  /* unpack items into new list */
  if (rank == 0) {
    ptr = (char*) recvbuf;
    char* end = ptr + recvbytes;
    while (ptr < end) {
      bayer_flist_file_unpack(ptr, tmplist);
      ptr += pack_size;
    }
  }

  /* summarize list */
  bayer_flist_summarize(tmplist);

  /* print files */
  if (rank == 0) {
    printf("\n");
    uint64_t tmpidx = 0;
    uint64_t tmpsize = bayer_flist_size(tmplist);
    while (tmpidx < tmpsize) {
      print_file(tmplist, tmpidx, rank);
      tmpidx++;
      if (tmpidx == range) {
        printf("\n<snip>\n\n");
      }
    }
    printf("\n");
  }

  /* free our temporary list */
  bayer_flist_free(&tmplist);

  /* free memory */
  bayer_free(&disps);
  bayer_free(&counts);
  bayer_free(&sendbuf);
  bayer_free(&recvbuf);

  return;
}

static void print_usage(void)
{
  printf("\n");
  printf("Usage: dwalk [options] <path> ...\n");
  printf("\n");
  printf("Options:\n");
  printf("  -i, --input <file>  - read list from file\n");
  printf("  -o, --output <file> - write processed list to file\n");
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
  int i;

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

  char* inputname  = NULL;
  char* outputname = NULL;
  char* sortfields = NULL;
  int walk = 0;
  int print = 0;

  int option_index = 0;
  static struct option long_options[] = {
    {"input",    1, 0, 'i'},
    {"output",   1, 0, 'o'},
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
      argc, argv, "i:o:ls:phv",
      long_options, &option_index
    );

    if (c == -1) {
      break;
    }

    switch (c) {
    case 'i':
      inputname = BAYER_STRDUP(optarg);
      break;
    case 'o':
      outputname = BAYER_STRDUP(optarg);
      break;
    case 'l':
      walk_stat = 0;
      break;
    case 's':
      sortfields = BAYER_STRDUP(optarg);
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
  int numpaths = 0;
  bayer_param_path* paths = NULL;
  if (optind < argc) {
    /* got a path to walk */
    walk = 1;

    /* determine number of paths specified by user */
    numpaths = argc - optind;

    /* allocate space for each path */
    paths = (bayer_param_path*) BAYER_MALLOC((size_t)numpaths * sizeof(bayer_param_path));

    /* process each path */
    for (i = 0; i < numpaths; i++) {
      const char* path = argv[optind];
      bayer_param_path_set(path, &paths[i]);
      optind++;
    }

    /* don't allow user to specify input file with walk */
    if (inputname != NULL) {
      usage = 1;
    }
  } else {
    /* if we're not walking, we must be reading,
     * and for that we need a file */
    if (inputname == NULL) {
      usage = 1;
    }
  }

  /* if user is trying to sort, verify the sort fields are valid */
  if (sortfields != NULL) {
    int maxfields;
    int nfields = 0;
    char* sortfields_copy = BAYER_STRDUP(sortfields);
    if (walk_stat) {
      maxfields = 7;
      char* token = strtok(sortfields_copy, ",");
      while (token != NULL) {
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

  /* create an empty file list */
  bayer_flist flist = bayer_flist_new();

  if (walk) {
    time_t walk_start_t = time(NULL);
    if (walk_start_t == (time_t)-1) {
      /* TODO: ERROR! */
    }
    walk_start = (uint64_t) walk_start_t;

    /* report walk count, time, and rate */
    double start_walk = MPI_Wtime();
    for (i = 0; i < numpaths; i++) {
      /* get path for this step */
      const char* target = paths[i].path;

      /* print message to user that we're starting */
      if (verbose && rank == 0) {
        char walk_s[30];
        size_t rc = strftime(walk_s, sizeof(walk_s)-1, "%FT%T", localtime(&walk_start_t));
        if (rc == 0) {
          walk_s[0] = '\0';
        }
        printf("%s: Walking %s\n", walk_s, target);
        fflush(stdout);
      }

      /* walk file tree and record stat data for each file */
      bayer_flist_walk_path(target, walk_stat, flist);
    }
    double end_walk = MPI_Wtime();

    time_t walk_end_t = time(NULL);
    if (walk_end_t == (time_t)-1) {
      /* TODO: ERROR! */
    }
    walk_end = (uint64_t) walk_end_t;

    /* get total file count */
    all_count = bayer_flist_global_size(flist);

    /* report walk count, time, and rate */
    if (verbose && rank == 0) {
      double secs = end_walk - start_walk;
      double rate = 0.0;
      if (secs > 0.0) {
        rate = ((double)all_count) / secs;
      }
      printf("Walked %lu files in %f seconds (%f files/sec)\n",
        all_count, secs, rate
      );
    }
  } else {
    /* read data from cache file */
    double start_read = MPI_Wtime();
    bayer_flist_read_cache(inputname, flist);
    double end_read = MPI_Wtime();

    /* get total file count */
    all_count = bayer_flist_global_size(flist);

    /* report read count, time, and rate */
    if (verbose && rank == 0) {
      double secs = end_read - start_read;
      double rate = 0.0;
      if (secs > 0.0) {
        rate = ((double)all_count) / secs;
      }
      printf("Read %lu files in %f seconds (%f files/sec)\n",
        all_count, secs, rate
      );
    }
  }

  /* TODO: filter files */
  //filter_files(&flist);

  /* sort files */
  if (sortfields != NULL) {
    /* TODO: don't sort unless all_count > 0 */

    double start_sort = MPI_Wtime();
    if (bayer_flist_have_detail(flist)) {
      sort_files_stat(sortfields, &flist);
    } else {
      sort_files_readdir(sortfields, &flist);
    }
    double end_sort = MPI_Wtime();

    /* report sort count, time, and rate */
    if (verbose && rank == 0) {
      double secs = end_sort - start_sort;
      double rate = 0.0;
      if (secs > 0.0) {
        rate = ((double)all_count) / secs;
      }
      printf("Sorted %lu files in %f seconds (%f files/sec)\n",
        all_count, secs, rate
      );
    }
  }

  /* print files */
  if (print) {
    print_files(flist);
  }

  print_summary(flist);

  /* write data to cache file */
  if (outputname != NULL) {
    /* report the filename we're writing to */
    if (verbose && rank == 0) {
      printf("Writing to output file: %s\n", outputname);
      fflush(stdout);
    }

    double start_write = MPI_Wtime();
    bayer_flist_write_cache(outputname, flist);
    double end_write = MPI_Wtime();

    /* report write count, time, and rate */
    if (verbose && rank == 0) {
      double secs = end_write - start_write;
      double rate = 0.0;
      if (secs > 0.0) {
        rate = ((double)all_count) / secs;
      }
      printf("Wrote %lu files in %f seconds (%f files/sec)\n",
        all_count, secs, rate
      );
    }
  }

  /* free users, groups, and files objects */
  bayer_flist_free(&flist);

  /* free memory allocated for options */
  bayer_free(&sortfields);
  bayer_free(&outputname);
  bayer_free(&inputname);

  /* shut down the sorting library */
  DTCMP_Finalize();

  /* free the path parameters */
  for (i = 0; i < numpaths; i++) {
    bayer_param_path_free(&paths[i]);
  }
  bayer_free(&paths);

  /* shut down MPI */
  MPI_Finalize();

  return 0;
}
