#include "mfu.h"

#include <daos.h>
#ifdef HDF5_SUPPORT
#include <hdf5.h>
#endif
#include "mfu_flist_internal.h"

#define ENUM_KEY_BUF		128 /* size of each dkey/akey */
#define ENUM_LARGE_KEY_BUF	(512 * 1024) /* 512k large key */
#define ENUM_DESC_NR		5 /* number of keys/records returned by enum */
#define ENUM_DESC_BUF		512 /* all keys/records returned by enum */
#define OID_ARR_SIZE        8

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

#ifdef HDF5_SUPPORT
/* for oid dataset */
typedef struct {
	uint64_t oid_hi;
	uint64_t oid_low;
	uint64_t dkey_offset;
} oid_t;

/* for dkey dataset */
typedef struct {
	/* array of vlen structure */
	hvl_t dkey_val;
	uint64_t akey_offset;
} dkey_t;

/* for akey dataset */
typedef struct {
	/* array of vlen structure */
	hvl_t akey_val;
	uint64_t rec_dset_id;
} akey_t;

struct hdf5_args {
	hid_t status;
	hid_t file;
	/* OID Data */
	hid_t oid_memtype;
	hid_t oid_dspace;
	hid_t oid_dset;
	hid_t oid_dtype;
	/* DKEY Data */
	hid_t dkey_memtype;
	hid_t dkey_vtype;
	hid_t dkey_dspace;
	hid_t dkey_dset;
	/* AKEY Data */
	hid_t akey_memtype;
	hid_t akey_vtype;
	hid_t akey_dspace;
	hid_t akey_dset;
	/* recx Data */
	hid_t plist;
	hid_t rx_dspace;
	hid_t rx_memspace;
	hid_t attr_dspace;
	hid_t attr_dtype;
	hid_t rx_dset;
	hid_t single_dspace;
	hid_t single_dset;
	hid_t rx_dtype;
	hid_t usr_attr_num;
	hid_t usr_attr;
	hid_t cont_attr;
	hid_t selection_attr;
	hid_t version_attr;
	hid_t single_dtype;
	hid_t version_attr_dspace;
	hid_t version_attr_type;
	/* dims for dsets */
	hsize_t oid_dims[1];
	hsize_t dkey_dims[1];     
	hsize_t akey_dims[1];     
	hsize_t rx_dims[1];
	hsize_t	mem_dims[1];
	hsize_t	attr_dims[1];
	hsize_t rx_chunk_dims[1];
	hsize_t rx_max_dims[1];
	hsize_t single_dims[1];
	hsize_t version_attr_dims[1];
	/* data for keys */
	oid_t *oid_data;
	dkey_t *dkey_data;
	akey_t *akey_data;
	oid_t  **oid;
	dkey_t **dk;
	akey_t **ak;
};
#endif

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

/* parse a daos path of the form
 * daos://pool/cont, /pool/cont/,
 * or UNS path */
int daos_parse_path(
    char *path,
    size_t path_len,
    uuid_t *p_uuid,
    uuid_t *c_uuid,
    bool daos_no_prefix
);

/* connect to DAOS pool,
 * and then open container */
int daos_connect(
  int rank,
  uuid_t pool_uuid,
  uuid_t cont_uuid,
  daos_handle_t* poh,
  daos_handle_t* coh,
  bool connect_all_ranks,
  bool connect_pool,
  bool create_cont
);

/* serialize a container to
 * an HDF5 file */
int cont_serialize_hdlr(
    uuid_t pool_uuid,
    uuid_t cont_uuid,
    daos_handle_t poh,
    daos_handle_t coh
);

/* serialize a container to
 * an HDF5 file */
int cont_deserialize_hdlr(
    uuid_t pool_uuid,
    uuid_t cont_uuid,
    daos_handle_t *poh,
    daos_handle_t *coh,
    char *h5filename
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
