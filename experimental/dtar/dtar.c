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
static gboolean opts_extract = FALSE;
static gboolean opts_preserve = FALSE;
static gchar*   opts_tarfile = NULL;

static GOptionEntry entries[] = {
        {"create", 'c', 0, G_OPTION_ARG_NONE, &opts_create, "Create archive", NULL  },
        {"extract", 'x', 0, G_OPTION_ARG_NONE, &opts_extract, "Extract archive", NULL },
        {"verbose", 'v', 0, G_OPTION_ARG_NONE, &opts_verbose, "Verbose output", NULL },
        {"preserve", 'p', 0, G_OPTION_ARG_NONE, &opts_preserve, "Preserve attributes", NULL},
        {"file", 'f', 0, G_OPTION_ARG_FILENAME, &opts_tarfile, "Target output file", NULL },
        { NULL }
};

/* initialize */


int DTAR_global_rank;
DTAR_options_t DTAR_user_opts;
DTAR_writer_t DTAR_writer;
bayer_flist DTAR_flist;
uint64_t* DTAR_fsizes = NULL;
uint64_t DTAR_total = 0;
uint64_t DTAR_count = 0;

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

        DTAR_total += DTAR_fsizes[idx];
    }
}

static void create_archive(char *filename) {
    /* open archive file for writing */
    //DTAR_writer_init();

    /* walk path to get stats info on all files */
    int i;
    DTAR_flist = bayer_flist_new();
    for (i = 0; i < num_src_params; i++) {
        bayer_flist_walk_path(src_params[i].path, 1, DTAR_flist);
    }
    DTAR_count = bayer_flist_size(DTAR_flist);

    /* allocate memory for DTAR_fsizes */
    DTAR_fsizes = (uint64_t*) BAYER_MALLOC( DTAR_count * sizeof(uint64_t));

    process_flist();

    //fsync(DTAR_writer.fd_tar);

}

int main(int argc, char **argv) {

    int flags = ARCHIVE_EXTRACT_TIME;

    MPI_Init(&argc, &argv);
    bayer_init();

    DTAR_global_rank = CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_INFO;
    bayer_debug_level = BAYER_LOG_INFO;

    GError *error = NULL;
    GOptionContext *context = NULL;
    context = g_option_context_new(" [sources ... ] [destination file]");
    g_option_context_add_main_entries(context, entries, NULL);
    if (!g_option_context_parse(context, &argc, &argv, &error)) {
        BAYER_LOG(BAYER_LOG_ERR, "Command line option parsing error: %s", error->message);
        g_option_context_get_help(context, TRUE, NULL);
        DTAR_exit(EXIT_FAILURE);

    }

    if (!opts_create  &&  !opts_extract && DTAR_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_ERR, "One of extract(x) or create(c) need to be specified");
        DTAR_exit(EXIT_FAILURE);
    }

    if (opts_create && opts_extract && DTAR_global_rank == 0) {
        BAYER_LOG(BAYER_LOG_ERR, "Only one of extraction(x) or create(c) can be specified");
        DTAR_exit(EXIT_FAILURE);
    }


    if (opts_preserve) {
        flags |= ARCHIVE_EXTRACT_OWNER;
        flags |= ARCHIVE_EXTRACT_PERM;
        flags |= ARCHIVE_EXTRACT_ACL;
        flags |= ARCHIVE_EXTRACT_FFLAGS;
        flags |= ARCHIVE_EXTRACT_XATTR;
    }

    DTAR_parse_path_args(argc, argv, opts_tarfile);
    CIRCLE_enable_logging(CIRCLE_debug);

    if (opts_create)
        create_archive( opts_tarfile );


    /* free context */
    g_option_context_free(context);


    DTAR_exit(EXIT_SUCCESS);
    return 0;
}


