typedef struct {
    size_t  chunk_size;
    size_t  block_size;
    char*   dest_path;
    bool    preserve;
    int     flags;
} mfu_archive_options_t;

void mfu_param_path_check_archive(int numparams, mfu_param_path* srcparams, mfu_param_path destparam, int* valid);

void mfu_flist_archive_create(mfu_flist flist, const char* archivefile, mfu_archive_options_t* opts);

void mfu_flist_archive_extract(const char* filename, bool verbose, int flags);
