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
    daos_epoch_t epc;      /* src container epoch */
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

/* walk objects in daos and insert to given flist */
int mfu_flist_walk_daos(
    daos_args_t* da,
    mfu_flist flist
);

/* copy objects in flist to destination listed in daos args,
 * copies DAOS data at object level (non-posix) */
int mfu_flist_copy_daos(
    daos_args_t* da,
    mfu_flist flist
);
