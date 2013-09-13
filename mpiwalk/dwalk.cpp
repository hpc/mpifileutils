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

#if 0
/* routine for sorting strings in ascending order */
static int my_strcmp(const void* a, const void* b)
{
  return strcmp((const char*)a, (const char*)b);
}

static int my_strcmp_rev(const void* a, const void* b)
{
  return strcmp((const char*)b, (const char*)a);
}

static int sort_files_readdir(const char* sortfields, bayer_flist flist)
{
  void* inbuf      = files->buf;
  uint64_t incount = bayer_flist_size(flist);
  int chars        = bayer_flist_file_max_name(flist);
  MPI_Datatype dt_sat = files->dt;

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* build type for file path */
  MPI_Datatype dt_filepath;
  MPI_Type_contiguous(chars, MPI_CHAR, &dt_filepath);
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
  MPI_Datatype dt_keysat, keysat_types[2];
  keysat_types[0] = dt_key;
  keysat_types[1] = dt_sat;
  DTCMP_Type_create_series(2, keysat_types, &dt_keysat);

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
  void* sortbuf = bayer_malloc(sortbufsize, "presort buffer", __FILE__, __LINE__);

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
  size_t newbufsize = sat_extent * outsortcount;
  void* newbuf = bayer_malloc(newbufsize, "postsort buffer", __FILE__, __LINE__);

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

static int sort_files_stat(const char* sortfields, bayer_flist flist)
{
  void* inbuf      = files->buf;
  uint64_t incount = bayer_flist_size(flist);
  int chars        = bayer_flist_file_max_name(flist);
  int chars_user   = bayer_flist_user_max_name(flist);
  int chars_group  = bayer_flist_group_max_name(flist);
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
  MPI_Datatype dt_keysat, keysat_types[2];
  keysat_types[0] = dt_key;
  keysat_types[1] = dt_stat;
  DTCMP_Type_create_series(2, keysat_types, &dt_keysat);

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
  void* sortbuf = bayer_malloc(sortbufsize, "presort buffer", __FILE__, __LINE__);

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
  size_t newbufsize = stat_extent * outsortcount;
  void* newbuf = bayer_malloc(newbufsize, "postsort buffer", __FILE__, __LINE__);

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
#endif

static void print_summary(bayer_flist flist)
{
  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* step through and print data */
  int index = 0;
  uint64_t max = bayer_flist_size(flist);
  while (index < max) {
    /* get filename */
    char* file;
    bayer_flist_file_name(flist, index, &file);

    if (bayer_flist_have_detail(flist)) {
      /* get mode */
      mode_t mode;
      bayer_flist_file_mode(flist, index, &mode);

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

      uint64_t size = bayer_flist_file_get_size(flist, index);
      total_bytes += size;
    } else {
      /* get type */
      bayer_filetype type;
      bayer_flist_file_type(flist, index, &type);

      if (type == TYPE_DIR) {
        total_dirs++;
      } else if (type == TYPE_FILE) {
        total_files++;
      } else if (type == TYPE_LINK) {
        total_links++;
      } else {
        /* unknown file type */
        total_unknown++;
      }
    }

    /* go to next file */
    index++;
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
    printf("Items: %lu\n", (unsigned long long) all_count);
    printf("  Directories: %lu\n", (unsigned long long) all_dirs);
    printf("  Files: %lu\n", (unsigned long long) all_files);
    printf("  Links: %lu\n", (unsigned long long) all_links);
    /* printf("  Unknown: %lu\n", (unsigned long long) all_unknown); */

    if (walk_stat) {
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
static char type_str_dir[]  = "DIR";
static char type_str_file[] = "REG";
static char type_str_link[] = "LNK";

static void print_files(bayer_flist flist)
{
  /* gather first 10 to rank 0 */

  /* get our rank and the size of comm_world */
  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* step through and print data */
  int index = 0;
  uint64_t max = bayer_flist_size(flist);
  while (index < max) {
    /* get filename */
    char* file;
    bayer_flist_file_name(flist, index, &file);

    if (bayer_flist_have_detail(flist)) {
      /* get mode */
      mode_t mode;
      bayer_flist_file_mode(flist, index, &mode);

      uint32_t uid = bayer_flist_file_get_uid(flist, index);
      uint32_t gid = bayer_flist_file_get_gid(flist, index);
      uint32_t access = bayer_flist_file_get_atime(flist, index);
      uint32_t modify = bayer_flist_file_get_mtime(flist, index);
      uint32_t create = bayer_flist_file_get_ctime(flist, index);
      uint64_t size = bayer_flist_file_get_size(flist, index);
      const char* username  = bayer_flist_file_get_username(flist, index);
      const char* groupname = bayer_flist_file_get_groupname(flist, index);

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

      prepare_mode_format(mode);

      if (index < 10 || (max - index) <= 10) {
        //printf("Rank %d: Mode=%lx UID=%d GUI=%d Access=%lu Modify=%lu Create=%lu Size=%lu File=%s\n",
        //  rank, mode, uid, gid, (unsigned long)access, (unsigned long)modify,
        printf("Rank %d: Mode=%lx(%s) UID=%d(%s) GUI=%d(%s) Access=%s Modify=%s Create=%s Size=%lu File=%s\n",
          rank, mode, mode_format, uid, username, gid, groupname,
          access_s, modify_s, create_s, (unsigned long)size, file
        );
      } else if (index == 10) {
        printf("<snip>\n");
      }
    } else {
      /* get type */
      bayer_filetype type;
      bayer_flist_file_type(flist, index, &type);

      char* type_str = type_str_unknown;
      if (type == TYPE_DIR) {
        type_str = type_str_dir;
      } else if (type == TYPE_FILE) {
        type_str = type_str_file;
      } else if (type == TYPE_LINK) {
        type_str = type_str_link;
      }

        printf("Rank %d: Type=%s File=%s\n",
          rank, type_str, file
        );
#if 0
      if (index < 10 || (max - index) <= 10) {
        printf("Rank %d: Type=%s File=%s\n",
          rank, type_str, file
        );
      } else if (index == 10) {
        printf("<snip>\n");
      }
#endif
    }

    /* go to next file */
    index++;
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

  /* create an empty file list */
  bayer_flist flist;

  if (walk) {
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
    bayer_flist_walk_path(target, walk_stat, &flist);
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
      double time = end_walk - start_walk;
      double rate = 0.0;
      if (time > 0.0) {
        rate = ((double)all_count) / time;
      }
      printf("Walked %lu files in %f seconds (%f files/sec)\n",
        all_count, time, rate
      );
    }
  } else {
    /* read data from cache file */
    double start_read = MPI_Wtime();
    bayer_flist_read_cache(cachename, &flist);
    double end_read = MPI_Wtime();

    /* get total file count */
    all_count = bayer_flist_global_size(flist);

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
    /* report the filename we're writing to */
    if (verbose && rank == 0) {
      printf("Writing to cache file: %s\n", cachename);
      fflush(stdout);
    }

    double start_write = MPI_Wtime();
    bayer_flist_write_cache(cachename, flist);
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

#if 0
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
#endif

  /* print files */
  if (print) {
    print_files(flist);
  }

  print_summary(flist);

  /* free users, groups, and files objects */
  bayer_flist_free(&flist);

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
