#include "mfu.h"

#include <daos.h>
#include "mfu_flist_internal.h"

#define ENUM_KEY_BUF		32 /* size of each dkey/akey */
#define ENUM_LARGE_KEY_BUF	(512 * 1024) /* 512k large key */
#define ENUM_DESC_NR		5 /* number of keys/records returned by enum */
#define ENUM_DESC_BUF		512 /* all keys/records returned by enum */

enum handleType {
    POOL_HANDLE,
    CONT_HANDLE,
    ARRAY_HANDLE
};

typedef enum {
    DAOS_API_AUTO,
    DAOS_API_DFS,
    DAOS_API_DAOS
} daos_api_t;

/* struct for holding DAOS arguments */
typedef struct {
    daos_handle_t src_poh; /* source pool handle */
    daos_handle_t dst_poh; /* destination pool handle */
    daos_handle_t src_coh; /* source container handle */
    daos_handle_t dst_coh; /* destination container handle */
    uuid_t src_pool_uuid;  /* source pool UUID */
    uuid_t dst_pool_uuid;  /* destination pool UUID */
    uuid_t src_cont_uuid;  /* source container UUID */
    uuid_t dst_cont_uuid;  /* destination container UUID */
    char* dfs_prefix;      /* prefix for UNS */
    char* src_path;        /* allocated src path */
    char* dst_path;        /* allocated dst path */
    daos_api_t api;        /* API to use */
    daos_epoch_t src_epc;  /* src container epoch */
    daos_epoch_t dst_epc;  /* dst container epoch */
} daos_args_t;

/* Return a newly allocated daos_args_t structure.
 * Set default values on its fields. */
daos_args_t* daos_args_new(void);

/* free a daos_args_t structure */
void daos_args_delete(daos_args_t** pda);

/* Parse a string representation of the API */
int daos_parse_api_str(
    const char* api_str,
    daos_api_t* api);

/* Parse a string representation of the epoch */
int daos_parse_epc_str(
    const char* epc_str,
    daos_epoch_t* epc);

/* Setup DAOS arguments.
 * Connect to pools.
 * Open containers.
 * Mount DFS. 
 * Returns 1 on error, 0 on success */
int daos_setup(
  int rank,
  char** argpaths,
  daos_args_t* da,
  mfu_file_t* mfu_src_file,
  mfu_file_t* mfu_dst_file
);

/* Unmount DFS.
 * Disconnect from pool/cont.
 * Cleanup DAOS-related vars, handles. 
 * Finalize DAOS. */
int daos_cleanup(
  daos_args_t* da,
  mfu_file_t* mfu_src_file,
  mfu_file_t* mfu_dst_file
);

int mfu_daos_map_oid_fn(
  mfu_flist flist,
  uint64_t idx,
  int ranks,
  void *args);

/* Locally copy an mfu_flist to a mfu_file_chunk list. */
mfu_file_chunk* mfu_daos_file_chunk_list_create(
  mfu_flist list);

/* Compare two objects and optionally overwrite dest with source.
 * returns -1 on error, 0 if equal, 1 if different */
int mfu_daos_compare_contents(
  daos_args_t* da,                  /* DAOS args */
  uint64_t src_oid_lo,              /* source oid.lo */
  uint64_t src_oid_hi,              /* source oid.hi */
  uint64_t dst_oid_lo,              /* destination oid.lo */
  uint64_t dst_oid_hi,              /* destination oid.hi */
  int overwrite,                    /* whether to replace dest with source contents (1) or not (0) */ 
  uint64_t* count_bytes_read,       /* number of bytes read (src + dest) */
  uint64_t* count_bytes_written,    /* number of bytes written to dest */
  mfu_progress* prg,                /* progress message structure */
  mfu_file_t* mfu_src_file,
  mfu_file_t* mfu_dst_file);

/* walk objects in daos and insert to given flist */
int mfu_flist_walk_daos(
  daos_args_t* da,
  daos_handle_t coh,
  daos_epoch_t* epoch,
  mfu_flist flist
);

/* copy objects in flist to destination listed in daos args,
 * copies DAOS data at object level (non-posix) */
int mfu_flist_copy_daos(
  daos_args_t* da,
  mfu_flist flist
);

/* Punch each object in the flist.
 * Returns -1 on failure, 0 on success. */
int mfu_daos_flist_punch(
  daos_handle_t coh,
  mfu_flist flist);
