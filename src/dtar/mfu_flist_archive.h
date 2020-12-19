typedef struct {
    char*   dest_path;
    bool    preserve;
    int     flags;
    size_t  chunk_size;
    size_t  block_size;
} mfu_archive_opts_t;

/* return a newly allocated archive_opts structure, set default values on its fields */
mfu_archive_opts_t* mfu_archive_opts_new(void);

void mfu_archive_opts_delete(mfu_archive_opts_t** popts);

void mfu_param_path_check_archive(
    int numparams,
    mfu_param_path* srcparams,
    mfu_param_path destparam,
    mfu_archive_opts_t* opts,
    int* valid
);

int mfu_flist_archive_create(
    mfu_flist flist,
    const char* filename,
    int numpaths,
    const mfu_param_path* paths,
    const mfu_param_path* cwdpath,
    mfu_archive_opts_t* opts
);

int mfu_flist_archive_extract(
    const char* filename,
    const mfu_param_path* cwdpath,
    mfu_archive_opts_t* opts
);
