#include <libcircle.h>

#include "dtar.h"

void DTAR_writer_init()
{
	
        char compress=DTAR_user_options->compress;
        char filename=DTAR_user_options->dest_path;
        struct archive * a=DTAR_writer->a;
  
        DTAR_writer->flags= O_WRONLY | O_CREAT | O_TRUNC | O_BINARY | O_CLOEXEC;

	a=archive_write_new();
	switch (compress) {
	case 'j': case 'y':
		archive_write_add_filter_bzip2(a);
		break;
	case 'Z':
		archive_write_add_filter_compress(a);
		break;
	case 'z':
		archive_write_add_filter_gzip(a);
		break;
	default:
		archive_write_add_filter_none(a);
		break;
	}
	archive_write_set_format_ustar(a);
	if (strcmp(filename, "-") == 0)
		filename = NULL;

         DTAR_writer->fd_tar=open(filename,DTAR_writer->flags,0666);
         archive_write_open_fd(a, DTAR_writer->fd_ta);

}

void DTAR_parse_path_args(const chat * filename, char compress, const char ** argv);
{
    int i=0;
    DTAR_user_opts->dest_path=filename
    
    while (*argv != NULL) {
    argv++;
    i++; 
    }

    DTAR_user_opts->num_src_paths=i;
    DTAR_user_opts->src_path=argv;

}

void DTAR_add_objects(CIRCLE_handle* handle)
{
    DTAR_enqueue_work_objects(handle);
}


void DTAR_enqueue_work_objects(CIRCLE_handle* handle)
{

    char* opts_dest_path_dirname;
    char* src_path_dirname;

    uint32_t number_of_source_files = DTAR_user_options->

    if(number_of_source_files < 1) {
        LOG(DCOPY_LOG_ERR, "At least one valid source file must be specified.");
        DCOPY_abort(EXIT_FAILURE);
    }

    if(dest_is_file) {
        LOG(DCOPY_LOG_DBG, "Infered that the destination is a file.");

        /*
         * If the destination is a file, there must be only one source object, and it
         * must be a file.
         */
        if(number_of_source_files == 1 && DCOPY_is_regular_file(DCOPY_user_opts.src_path[0])) {
            /* Make a copy of the dest path so we can run dirname on it. */
            size_t dest_size = sizeof(char) * PATH_MAX;
            opts_dest_path_dirname = (char*) malloc(dest_size);

            if(opts_dest_path_dirname == NULL) {
                LOG(DCOPY_LOG_DBG, "Failed to allocate %llu bytes for dest path.", (long long unsigned) dest_size);
                DCOPY_abort(EXIT_FAILURE);
            }

            int dest_written = snprintf(opts_dest_path_dirname, dest_size, "%s", DCOPY_user_opts.dest_path);

            if(dest_written < 0 || (size_t)(dest_written) > dest_size - 1) {
                LOG(DCOPY_LOG_DBG, "Destination path too long.");
                DCOPY_abort(EXIT_FAILURE);
            }

            opts_dest_path_dirname = dirname(opts_dest_path_dirname);

            /* Make a copy of the src path so we can run dirname on it. */
            size_t src_size = sizeof(char) * PATH_MAX;
            src_path_dirname = (char*) malloc(sizeof(char) * PATH_MAX);

            if(src_path_dirname == NULL) {
                LOG(DCOPY_LOG_DBG, "Failed to allocate %llu bytes for dest path.", (long long unsigned) src_size);
                DCOPY_abort(EXIT_FAILURE);
            }

            int src_written = snprintf(src_path_dirname, src_size, "%s", DCOPY_user_opts.src_path[0]);

            if(src_written < 0 || (size_t)(src_written) > src_size - 1) {
                LOG(DCOPY_LOG_DBG, "Source path too long.");
                DCOPY_abort(EXIT_FAILURE);
            }

            src_path_dirname = dirname(src_path_dirname);

            /* LOG(DCOPY_LOG_DBG, "Enqueueing only a single source path `%s'.", DCOPY_user_opts.src_path[0]); */
            char* op = DCOPY_encode_operation(TREEWALK, 0, DCOPY_user_opts.src_path[0], \
                                              (uint16_t)strlen(src_path_dirname), NULL, 0);

            handle->enqueue(op);
            free(op);

            free(opts_dest_path_dirname);
            free(src_path_dirname);
        }
        else {
            /*
             * Determine if we're trying to copy one or more directories into
             * a file.
             */
            int i;

            for(i = 0; i < DCOPY_user_opts.num_src_paths; i++) {
                char* src_path = DCOPY_user_opts.src_path[i];

                if(DCOPY_is_directory(src_path)) {
                    LOG(DCOPY_LOG_ERR, "Copying a directory into a file is not supported.");
                    DCOPY_abort(EXIT_FAILURE);
                }
            }

            /*
             * The only remaining possible condition is that the user wants to
             * copy multiple files into a single file (hopefully).
             */
            LOG(DCOPY_LOG_ERR, "Copying several files into a single file is not supported.");
            DCOPY_abort(EXIT_FAILURE);
        }
    }
    else if(dest_is_dir) {
        LOG(DCOPY_LOG_DBG, "Infered that the destination is a directory.");
        bool dest_already_exists = DCOPY_is_directory(DCOPY_user_opts.dest_path);

        int i;

        for(i = 0; i < DCOPY_user_opts.num_src_paths; i++) {
            char* src_path = DCOPY_user_opts.src_path[i];
            LOG(DCOPY_LOG_DBG, "Enqueueing source path `%s'.", src_path);

            char* src_path_basename = NULL;
            size_t src_len = strlen(src_path) + 1;
            char* src_path_basename_tmp = (char*) malloc(src_len);

            if(src_path_basename_tmp == NULL) {
                LOG(DCOPY_LOG_ERR, "Failed to allocate tmp for src_path_basename.");
                DCOPY_abort(EXIT_FAILURE);
            }

            /*
             * If the destination directory already exists, we want to place
             * new files inside it. To do this, we send a path fragment along
             * with the source path message and append it to the options dest
             * path whenever the options dest path is used.
             */
            if(dest_already_exists && !DCOPY_user_opts.conditional) {
                /* Make a copy of the src path so we can run basename on it. */
                strncpy(src_path_basename_tmp, src_path, src_len);
                src_path_basename = basename(src_path_basename_tmp);
            }

            char* op = DCOPY_encode_operation(TREEWALK, 0, src_path, \
                                              (uint16_t)(src_len - 1), \
                                              src_path_basename, 0);
            handle->enqueue(op);
            free(src_path_basename_tmp);
        }
    }
    else {
        /*
         * This is the catch-all for all of the object types we haven't
         * implemented yet.
         */
        LOG(DCOPY_LOG_ERR, "We've encountered an unsupported filetype.");
        DCOPY_abort(EXIT_FAILURE);
    }

}





