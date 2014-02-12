/**
 * @file dtar.c - parallel tar main file
 *
 * @author - Feiyi Wang
 *
 *
 */
#include "common.h"


static gboolean opts_create = FALSE;
static gboolean opts_verbose = FALSE;
static gboolean opts_debug = FALSE;
static gboolean opts_extract = FALSE;
static gboolean opts_preserve = FALSE;
static gchar*   opts_tarfile = NULL;
static gint     opts_chunksize = 1;

static GOptionEntry entries[] = {
        {"create", 'c', 0, G_OPTION_ARG_NONE, &opts_create, "Create archive", NULL  },
        {"extract", 'x', 0, G_OPTION_ARG_NONE, &opts_extract, "Extract archive", NULL },
        {"verbose", 'v', 0, G_OPTION_ARG_NONE, &opts_verbose, "Verbose output", NULL },
        {"debug", 'd', 0, G_OPTION_ARG_NONE, &opts_debug, "Debug output", NULL},
        {"preserve", 'p', 0, G_OPTION_ARG_NONE, &opts_preserve, "Preserve attributes", NULL},
        {"chunksize",'s', 0, G_OPTION_ARG_INT, &opts_chunksize, "Chunk size", NULL},
        {"file", 'f', 0, G_OPTION_ARG_FILENAME, &opts_tarfile, "Target output file", NULL },
        { NULL }
};

/* initialize */


int DTAR_global_rank;
DTAR_options_t DTAR_user_opts;
DTAR_writer_t DTAR_writer;
DTAR_statistics_t DTAR_statistics;
bayer_flist DTAR_flist;
uint64_t* DTAR_fsizes = NULL;
uint64_t* DTAR_offsets = NULL;
uint64_t DTAR_total = 0;
uint64_t DTAR_count = 0;
uint64_t DTAR_goffset = 0;
int DTAR_rank, DTAR_size;

static void process_flist() {
    uint64_t idx;
    for (idx = 0; idx < DTAR_count; idx++) {
        DTAR_fsizes[idx] = 0;
        bayer_filetype type = bayer_flist_file_get_type(DTAR_flist, idx);
        if (type == BAYER_TYPE_DIR || type == BAYER_TYPE_LINK) {
            DTAR_fsizes[idx] = DTAR_HDR_LENGTH;
        } else if (type == BAYER_TYPE_FILE) {
            uint64_t fsize = bayer_flist_file_get_size(DTAR_flist, idx);
            uint64_t rem = (fsize) % 512;
            if (rem == 0) {
                DTAR_fsizes[idx] = fsize + DTAR_HDR_LENGTH;
            } else {
                DTAR_fsizes[idx] = (fsize / 512 + 4) * 512;
            }

        }

        DTAR_offsets[idx] = DTAR_total;
        DTAR_total += DTAR_fsizes[idx];
    }
}


static void update_offsets() {
    for (uint64_t idx = 0; idx < DTAR_count; idx++) {
        DTAR_offsets[idx] += DTAR_goffset;
    }
}

static void create_archive(char *filename) {

    DTAR_writer_init();

    /* walk path to get stats info on all files */
    DTAR_flist = bayer_flist_new();
    for (int i = 0; i < num_src_params; i++) {
        bayer_flist_walk_path(src_params[i].path, 1, DTAR_flist);
    }

    DTAR_count = bayer_flist_size(DTAR_flist);

    /* allocate memory for DTAR_fsizes */
    DTAR_fsizes = (uint64_t*) BAYER_MALLOC( DTAR_count * sizeof(uint64_t));
    DTAR_offsets = (uint64_t*) BAYER_MALLOC(DTAR_count * sizeof(uint64_t));

    /* calculate size, offset for each file as well as global offset*/
    process_flist();
    MPI_Scan(&DTAR_total, &DTAR_goffset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    DTAR_goffset -= DTAR_total;
    update_offsets();


    /* write header first*/
    struct archive* ar = DTAR_new_archive();
    archive_write_open_fd(ar, DTAR_writer.fd_tar);

    for (uint64_t idx = 0; idx < DTAR_count; idx++ ) {
        bayer_filetype type = bayer_flist_file_get_type(DTAR_flist, idx);
        if (type == BAYER_TYPE_FILE || type == BAYER_TYPE_DIR || type == BAYER_TYPE_LINK) {
            DTAR_write_header(ar, idx, DTAR_offsets[idx]);
        }
    }

    DTAR_global_rank = CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL);
    CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    CIRCLE_enable_logging(loglevel);

    /* register callbacks */
    CIRCLE_cb_create(&DTAR_enqueue_copy);
    CIRCLE_cb_process(&DTAR_perform_copy);

    /* run the libcircle job */
    CIRCLE_begin();
    CIRCLE_finalize();

    uint64_t archive_size = 0;
    MPI_Allreduce(&DTAR_total, &archive_size, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    DTAR_statistics.total_size = archive_size;

    /* clean up */

    archive_write_free(ar);
    bayer_free(&DTAR_fsizes);
    bayer_free(&DTAR_offsets);
    bayer_flist_free(&DTAR_flist);
    bayer_close(DTAR_writer.name, DTAR_writer.fd_tar);

}

static void errmsg(const char* m) {
    fprintf(stderr, "%s\n", m);
}

static void msg(const char *m) {
    fprintf(stdout, "%s", m);
}

static int
copy_data(struct archive *ar, struct archive *aw) {
    int r;
    const void *buff;
    size_t size;
    off_t offset;
    for (;;) {
        r = archive_read_data_block(ar, &buff, &size, &offset);
        if (r == ARCHIVE_EOF) {
            return (ARCHIVE_OK);
        }

        if (r != ARCHIVE_OK)
            return (r);

        r = archive_write_data_block(aw, buff, size, offset);

        if (r != ARCHIVE_OK) {
            errmsg(archive_error_string(ar));
            return (r);
        }
    }
    return 0;
}

static void
extract_archive(const char *filename, bool verbose, int flags) {

    struct archive *a;
    struct archive *ext;
    struct archive_entry *entry;
    int r;
    /* initiate archive object for reading */
    a = archive_read_new();
    /* initiate archive object for writing */
    ext = archive_write_disk_new();
    archive_write_disk_set_options(ext, flags);

    /* we want all the format supports */
    archive_read_support_filter_bzip2(a);
    archive_read_support_filter_gzip(a);
    archive_read_support_filter_compress(a);
    archive_read_support_format_tar(a);

    archive_write_disk_set_standard_lookup(ext);

    if (filename != NULL && strcmp(filename, "-") == 0)
        filename = NULL;

    /* blocksize set to 1024K */
    if (( r = archive_read_open_filename(a, filename, 10240))) {
        errmsg(archive_error_string(a));
        exit(r);
    }

    for (;;) {
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF)
            break;
        if (r != ARCHIVE_OK) {
            errmsg(archive_error_string(a));
            exit(r);
        }

        if (verbose)
            msg("x ");

        if (verbose)
            msg(archive_entry_pathname(entry));

        r = archive_write_header(ext, entry);
        if (r != ARCHIVE_OK)
            errmsg(archive_error_string(a));
        else
            copy_data(a, ext);

        if (verbose)
            msg("\n");
    }

    archive_read_close(a);
    archive_read_free(a);
    exit(0);
}

int main(int argc, char **argv) {

    MPI_Init(&argc, &argv);
    bayer_init();

    MPI_Comm_rank(MPI_COMM_WORLD, &DTAR_rank);
    MPI_Comm_rank(MPI_COMM_WORLD, &DTAR_size);


    GError *error = NULL;
    GOptionContext *context = NULL;
    context = g_option_context_new(" [sources ... ] [destination file]");
    g_option_context_add_main_entries(context, entries, NULL);
    if (!g_option_context_parse(context, &argc, &argv, &error)) {
        BAYER_LOG(BAYER_LOG_ERR, "Command line option parsing error: %s", error->message);
        g_option_context_get_help(context, TRUE, NULL);
        DTAR_exit(EXIT_FAILURE);

    }

    if (opts_debug)
        bayer_debug_level = BAYER_LOG_DBG;
    else if (opts_verbose)
        bayer_debug_level = BAYER_LOG_INFO;
    else
        bayer_debug_level = BAYER_LOG_ERR;

    if (!opts_create  &&  !opts_extract && DTAR_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_ERR, "One of extract(x) or create(c) need to be specified");
        DTAR_exit(EXIT_FAILURE);
    }

    if (opts_create && opts_extract && DTAR_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Only one of extraction(x) or create(c) can be specified");
        DTAR_exit(EXIT_FAILURE);
    }


    /* done by default */
    DTAR_user_opts.flags = ARCHIVE_EXTRACT_TIME;
    DTAR_user_opts.flags |= ARCHIVE_EXTRACT_OWNER;
    DTAR_user_opts.flags |= ARCHIVE_EXTRACT_PERM;
    DTAR_user_opts.flags |= ARCHIVE_EXTRACT_ACL;
    DTAR_user_opts.flags |= ARCHIVE_EXTRACT_FFLAGS;

    if (opts_preserve) {
        DTAR_user_opts.flags |= ARCHIVE_EXTRACT_XATTR;
        DTAR_user_opts.preserve = TRUE;
        if (DTAR_global_rank == 0)
            BAYER_LOG(BAYER_LOG_INFO, "Creating archive with extended attributes");
    }

    DTAR_user_opts.chunk_size = opts_chunksize * 1024 * 1024;

    /* init statistics */
    DTAR_statistics.total_dirs  = 0;
    DTAR_statistics.total_files = 0;
    DTAR_statistics.total_links = 0;
    DTAR_statistics.total_size  = 0;
    DTAR_statistics.total_bytes_copied = 0;

    if (DTAR_rank == 0) {
        BAYER_LOG(BAYER_LOG_INFO, "Chunk size = %" PRIu64, DTAR_user_opts.chunk_size);
    }

    time(&(DTAR_statistics.time_started));
    DTAR_statistics.wtime_started = MPI_Wtime();

    if (opts_create) {
        DTAR_parse_path_args(argc, argv, opts_tarfile);
        create_archive( opts_tarfile );
    } else if (opts_extract) {
        extract_archive(opts_tarfile, opts_verbose, DTAR_user_opts.flags);
    } else {
        if (DTAR_rank == 0) {
            BAYER_LOG(BAYER_LOG_ERR, "Neither creation or extraction is specified");
            DTAR_exit(EXIT_FAILURE);
        }

    }

    DTAR_statistics.wtime_ended = MPI_Wtime();
    time(&(DTAR_statistics.time_ended));

    /* free context */
    g_option_context_free(context);

    DTAR_epilogue();
    DTAR_exit(EXIT_SUCCESS);
    return 0;
}


