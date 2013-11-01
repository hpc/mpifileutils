#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <mpi.h>
#include <libcircle.h>
#include <linux/limits.h>
#include <libgen.h>
#include <errno.h>
#include "log.h"

typedef struct {
    char* dest_path;
    char* src_path;
} DCMP_options_t;

/** Where we should store options specified by the user. */
DCMP_options_t DCMP_user_opts;

/** The loglevel that this instance of DCMP will output. */
DCMP_loglevel DCMP_debug_level;

/** Where debug output should go. */
FILE* DCMP_debug_stream;

extern int CIRCLE_global_rank;
/**
 * Print a usage message.
 */
void DCMP_print_usage(char** argv)
{
    printf("usage: %s [] [--] source_file target_file\n",
           argv[0]);
    fflush(stdout);
}

static struct option const longopts[] =
{
    {"help", 0, 0, 'h'},
    {0, 0, 0, 0}
};

static char const shortopts[] = "h";

/* called by single process upon detection of a problem */
void DCMP_abort(int code)
{
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

/* called globally by all procs to exit */
void DCMP_exit(int code)
{
    /* CIRCLE_finalize or will this hang? */
    MPI_Finalize();
    exit(code);
}

/**
 * Rank 0 passes in pointer to string to be bcast -- all others pass in a
 * pointer to a string which will be newly allocated and filled in with copy.
 */
static bool DCMP_bcast_str(char* send, char** recv)
{
    /* First, we broadcast the number of characters in the send string. */
    int len = 0;

    if(recv == NULL) {
        LOG(DCMP_LOG_ERR, "Attempted to receive a broadcast into invalid memory. " \
                "Please report this as a bug!");
        return false;
    }

    if(CIRCLE_global_rank == 0) {
        if(send != NULL) {
            len = (int)(strlen(send) + 1);

            if(len > CIRCLE_MAX_STRING_LEN) {
                LOG(DCMP_LOG_ERR, "Attempted to send a larger string (`%d') than what "
                        "libcircle supports. Please report this as a bug!", len);
                return false;
            }
        }
    }

    if(MPI_SUCCESS != MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD)) {
        LOG(DCMP_LOG_DBG, "While preparing to copy, broadcasting the length of a string over MPI failed.");
        return false;
    }

    /* If the string is non-zero bytes, allocate space and bcast it. */
    if(len > 0) {
        /* allocate space to receive string */
        *recv = (char*) malloc((size_t)len);

        if(*recv == NULL) {
            LOG(DCMP_LOG_ERR, "Failed to allocate string of %d bytes", len);
            return false;
        }

        /* Broadcast the string. */
        if(CIRCLE_global_rank == 0) {
            strncpy(*recv, send, len);
        }

        if(MPI_SUCCESS != MPI_Bcast(*recv, len, MPI_CHAR, 0, MPI_COMM_WORLD)) {
            LOG(DCMP_LOG_DBG, "While preparing to copy, broadcasting the length of a string over MPI failed.");
            return false;
        }

    }
    else {
        /* Root passed in a NULL value, so set the output to NULL. */
        *recv = NULL;
    }

    return true;
}

static void DCMP_parse_path(char* path, char **buff)
{
    /* identify destination path */
    char tmp_path[PATH_MAX];

    if(CIRCLE_global_rank == 0) {
        if(realpath(path, tmp_path) == NULL) {
            /*
             * If realpath doesn't work, we might be working with a file.
             * Since this might be a file, lets get the absolute base path.
             */
            char dest_base[PATH_MAX];
            strncpy(dest_base, path, PATH_MAX);
            char* dir_path = dirname(dest_base);

            if(realpath(dir_path, tmp_path) == NULL) {
                /* If realpath didn't work this time, we're really in trouble. */
                LOG(DCMP_LOG_ERR, "Could not determine the path for `%s'. %s", \
                    path, strerror(errno));
                DCMP_abort(EXIT_FAILURE);
            }

            /* Now, lets get the base name. */
            char file_name_buf[PATH_MAX];
            strncpy(file_name_buf, path, PATH_MAX);
            char* file_name = basename(file_name_buf);

            /* Finally, lets put everything together. */
            char norm_path[PATH_MAX];
            sprintf(norm_path, "%s/%s", dir_path, file_name);
            strncpy(tmp_path, norm_path, PATH_MAX);
        }

        LOG(DCMP_LOG_DBG, "Using path `%s'.", tmp_path);
    }

    /* Copy the destination path to user opts structure on each rank. */
    if(!DCMP_bcast_str(tmp_path, buff)) {
        LOG(DCMP_LOG_ERR, "Could not send the proper path to other nodes (`%s'). "
            "The MPI broadcast operation failed. %s",
            path, strerror(errno));
        DCMP_abort(EXIT_FAILURE);
    }

    return;
}

/**
 * Convert the destination to an absolute path and check sanity.
 */
static void DCMP_parse_dest_path(char* path)
{
    DCMP_parse_path(path, &DCMP_user_opts.dest_path);
}

/**
 * Parse the source and destination paths that the user has provided.
 */
void DCMP_parse_path_args(char** argv,
                 int optind_local,
                 int argc)
{
     if(argv == NULL || argc - optind_local != 2) {
          if(CIRCLE_global_rank == 0) {
               DCMP_print_usage(argv);
               LOG(DCMP_LOG_ERR, "You must specify a source and destination path.");
          }

          DCMP_exit(EXIT_FAILURE);
     }

     /* TODO: we use realpath below, which is nice since it takes out
     * ".", "..", symlinks, and adds the absolute path, however, it
     * fails if the file/directory does not already exist, which is
     * often the case for dest path. */

     /* Grab the source path. */
     DCMP_parse_path(argv[optind_local], &DCMP_user_opts.src_path);

     /* Grab the destination path. */
     DCMP_parse_path(argv[optind_local + 1], &DCMP_user_opts.dest_path);
}

int main(int argc, char **argv)
{
    int c;

    MPI_Init(&argc, &argv);
    CIRCLE_global_rank = CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);

    DCMP_debug_stream = stdout;

    /* By default, show info log messages. */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_INFO;
    DCMP_debug_level = DCMP_LOG_INFO;

    while ((c = getopt_long(argc, argv, shortopts, longopts, NULL)) != -1)
    {
         switch (c)
         {
         case 'h':
              if(CIRCLE_global_rank == 0) {
                   DCMP_print_usage(argv);
              }
              DCMP_exit(EXIT_SUCCESS);
              break;
         case '?':
         default:
              DCMP_exit(EXIT_FAILURE);
              break;
         }
    }
    /** Parse the source and destination paths. */
    DCMP_parse_path_args(argv, optind, argc);
}