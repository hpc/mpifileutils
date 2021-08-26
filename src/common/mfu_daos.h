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
#define OID_ARR_SIZE        50
#define ATTR_NAME_LEN       64
#define FILENAME_LEN        1024 
#define UUID_LEN            128

enum handleType {
    POOL_HANDLE,
    CONT_HANDLE,
    ARRAY_HANDLE
};

typedef enum {
    DAOS_API_AUTO,
    DAOS_API_DFS,
    DAOS_API_DAOS,
    DAOS_API_HDF5
} daos_api_t;

/* struct for holding DAOS arguments */
typedef struct {
    daos_handle_t src_poh;  /* source pool handle */
    daos_handle_t dst_poh;  /* destination pool handle */
    daos_handle_t src_coh;  /* source container handle */
    daos_handle_t dst_coh;  /* destination container handle */
    char src_pool[DAOS_PROP_LABEL_MAX_LEN + 1];
    char src_cont[DAOS_PROP_LABEL_MAX_LEN + 1];
    char dst_pool[DAOS_PROP_LABEL_MAX_LEN + 1];
    char dst_cont[DAOS_PROP_LABEL_MAX_LEN + 1];
    /* if destination container is not created new UUID is generated */
    uuid_t dst_cont_uuid;   /* destination container UUID */
    char* dfs_prefix;       /* prefix for UNS */
    char* src_path;         /* allocated src path */
    char* dst_path;         /* allocated dst path */
    daos_api_t api;         /* API to use */
    daos_epoch_t src_epc;   /* src container epoch */
    daos_epoch_t dst_epc;   /* dst container epoch */
    bool allow_exist_dst_cont;          /* whether to allow the dst container to exist for DAOS API */
    enum daos_cont_props src_cont_type; /* type of the source container */
    enum daos_cont_props dst_cont_type; /* type of the destination container */
    bool daos_preserve;                 /* preserve daos cont props and user attrs */
    char *daos_preserve_path;           /* set path to write daos props and user attrs */
} daos_args_t;

/* struct for holding statistics */
typedef struct mfu_daos_stats {
    uint64_t total_oids;    /* sum of object ids */
    uint64_t total_dkeys;   /* sum of dkeys */
    uint64_t total_akeys;   /* sum of akeys */
    uint64_t bytes_read;    /* sum of bytes read (src and dst) */
    uint64_t bytes_written; /* sum of bytes written */
    time_t   time_started;  /* time when started */
    time_t   time_ended;    /* time when ended */
    double   wtime_started; /* relative time when started */
    double   wtime_ended;   /* relative time when ended */
} mfu_daos_stats_t;

/* initialize the mfu_daos_stats_t */
void mfu_daos_stats_init(mfu_daos_stats_t* stats);

/* set the stats start time */
void mfu_daos_stats_start(mfu_daos_stats_t* stats);

/* set the stats end time */
void mfu_daos_stats_end(mfu_daos_stats_t* stats);

/* sum the stats into another stats struct */
void mfu_daos_stats_sum(mfu_daos_stats_t* stats, mfu_daos_stats_t* stats_sum);

/* print the stats */
void mfu_daos_stats_print(
    mfu_daos_stats_t* stats,
    bool print_read,
    bool print_write,
    bool print_read_rate,
    bool print_write_rate
);

/* sum and printthe stats */
void mfu_daos_stats_print_sum(
    int rank,
    mfu_daos_stats_t* stats,
    bool print_read,
    bool print_write,
    bool print_read_rate,
    bool print_write_rate
);

#ifdef HDF5_SUPPORT
/* daos_obj_id_t type defined for hdf5 attribute. The DAOS_PROP_CO_ROOTS
 * points to an array of daos_obj_id_t, which is a struct that contains
 * hi, lo uint64_t's. In order to write these to an hdf5 file a similar
 * type has to be defined to describe it to hdf5 */
typedef struct {
    uint64_t hi;
    uint64_t lo;
} obj_id_t;

/* for user attr dataset */
typedef struct {
    char* attr_name;
    hvl_t attr_val;
} usr_attr_t;

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
    /* for for kv values that can be
     * written with daos_kv.h */
	hvl_t rec_kv_val;
} dkey_t;

/* for akey dataset */
typedef struct {
	/* array of vlen structure */
	hvl_t akey_val;
	uint64_t rec_dset_id;
	hvl_t rec_single_val;
} akey_t;

struct hdf5_args {
    hid_t status;
    hid_t file;
    /* User attribute data */
    hid_t usr_attr_memtype;
    hid_t usr_attr_name_vtype;
    hid_t usr_attr_val_vtype;
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
    hid_t rx_dtype;
    hid_t usr_attr;
    hid_t cont_attr;
    hid_t selection_attr;
    hid_t version_attr;
    hid_t version_attr_dspace;
    hid_t version_attr_type;
    /* dims for dsets */
    hsize_t oid_dims[1];
    hsize_t dkey_dims[1];     
    hsize_t akey_dims[1];     
    hsize_t rx_dims[1];
    hsize_t mem_dims[1];
    hsize_t attr_dims[1];
    hsize_t rx_chunk_dims[1];
    hsize_t rx_max_dims[1];
    hsize_t version_attr_dims[1];
    /* data for keys */
    oid_t *oid_data;
    dkey_t *dkey_data;
    akey_t *akey_data;
    oid_t  **oid;
    dkey_t **dk;
    akey_t **ak;
};

/* serialize a container to
 * an HDF5 file */
int daos_cont_serialize_hdlr(
    int rank,
    struct hdf5_args *hdf5,
    char *output_dir,
    uint64_t *files_written,
    daos_args_t *da,
    mfu_flist flist,
    uint64_t num_oids,
    mfu_daos_stats_t* stats
);

/* serialize a container to
 * an HDF5 file */
int daos_cont_deserialize_hdlr(
    int rank,
    daos_args_t *da,
    const char *h5filename,
    mfu_daos_stats_t* stats
);

/* connect to pool, read cont properties because
 * MAX_OID can only works if set on container creation,
 * and then broadcast handles to all ranks */
int daos_cont_deserialize_connect(
    daos_args_t *daos_args,
    struct hdf5_args *hdf5,
    daos_cont_layout_t *cont_type,
    char *label
);

/* serialize the number of hdf5 files generated by
 * daos_cont_serialize_hdlr, this allows deserialization
 * to check if all data is present */
int daos_cont_serialize_files_generated(
    struct hdf5_args *hdf5,
    uint64_t *files_generated
);

/* check if source or destination is HDF5 container, and
 * if so, then launch/run h5repack */
int mfu_daos_hdf5_copy(char **argpaths,
                       daos_args_t *daos_args);

/* serialize DAOS container properties */
int cont_serialize_props(struct hdf5_args *hdf5,
                         daos_handle_t cont);

/* serialize DAOS user attributes*/
int cont_serialize_usr_attrs(struct hdf5_args *hdf5,
                             daos_handle_t cont);

int cont_deserialize_all_props(struct hdf5_args *hdf5,
                               daos_prop_t **prop, 
                               struct daos_prop_co_roots *roots,
                               daos_cont_layout_t *cont_type,
                               daos_handle_t poh);

int cont_deserialize_usr_attrs(struct hdf5_args* hdf5,
                               daos_handle_t coh);
#endif

/* Check whether a uuid is valid */
bool daos_uuid_valid(const uuid_t uuid);

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
  int numpaths,
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
    char (*pool_str)[],
    char (*cont_str)[]
);

/* connect to DAOS pool,
 * and then open container */
int daos_connect(
  int rank,
  daos_args_t* da,
  char (*pool)[],
  char (*cont)[],
  daos_handle_t* poh,
  daos_handle_t* coh,
  bool force_serialize,
  bool connect_pool,
  bool create_cont,
  bool require_new_cont,
  bool preserve,
  mfu_file_t* mfu_src_file,
  bool dst_cont_passed
);

/* broadcast a pool or cont handle
 * from rank 0 */
void daos_bcast_handle(
    int rank,
    daos_handle_t* handle,
    daos_handle_t* poh,
    enum handleType type
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

/* Walk objects in daos and insert to given flist.
 * Returns -1 on failure, 0 on success. */
int mfu_daos_flist_walk(
    daos_args_t* da,
    daos_handle_t coh,
    daos_epoch_t* epoch,
    mfu_flist flist
);

/* copy/sync objects in flist to destination listed in daos args,
 * copies DAOS data at object level (non-posix) */
int mfu_daos_flist_sync(
    daos_args_t* da,    /* DAOS args */
    mfu_flist flist,    /* flist containing oids */
    bool compare_dst,   /* whether to compare the dst before writing */
    bool write_dst      /* whether to actually write to the dst */
);
