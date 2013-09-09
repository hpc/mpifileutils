#include "dtar.h"
#include "log.h"
#include <archive.h>
#include <archive_entry.h>

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libcircle.h>

MPI_Comm new_comm;
MPI_Comm inter_comm;

int64_t g_tar_offset = 0;
int verbose = 0;

DTAR_options_t DTAR_user_opts;
DTAR_writer_t DTAR_writer;
DTAR_loglevel DTAR_debug_level;
FILE * DTAR_debug_stream;


void (*DTAR_jump_table[3])(DTAR_operation_t* op, CIRCLE_handle* handle);

static void create(char *filename, char compress, int opt_index, int argc,
        char **argv);

void usage(void) {

}

int main(int argc, char **argv) {
    char *filename = NULL;
    char compress, mode, opt;
    int flags;
    int opt_index = 0;
    int numprocs, rank;

    MPI_Init(&argc, &argv);

    /* By default, show info log message */
    DTAR_debug_stream = stdout;
    DTAR_debug_level = DTAR_LOG_INFO;

    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0 && numprocs < 3) {
        LOG(DTAR_LOG_FATAL, "DTAR requires at three 3 process to run!");
        MPI_Finalize();
        exit(-1);
    }
    mode = 'x';
    verbose = 0;
    compress = '\0';
    flags = ARCHIVE_EXTRACT_TIME;

    /* Among other sins, getopt(3) pulls in printf(3). */
    while (*++argv != NULL && **argv == '-') {
        char *p = *argv + 1;
        opt_index++;

        while ((opt = *p++) != '\0') {
            switch (opt) {
            case 'c':
                mode = opt;
                break;
            case 'f':
                if (*p != '\0')
                    filename = p;
                else
                    filename = *++argv;
                p += strlen(p);
                break;
            case 'j':
                compress = opt;
                break;
            case 'p':
                flags |= ARCHIVE_EXTRACT_PERM;
                flags |= ARCHIVE_EXTRACT_ACL;
                flags |= ARCHIVE_EXTRACT_FFLAGS;
                break;
            case 't':
                mode = opt;
                break;
            case 'v':
                verbose++;
                break;
            case 'x':
                mode = opt;
                break;
            case 'y':
                compress = opt;
                break;
            case 'Z':
                compress = opt;
                break;
            case 'z':
                compress = opt;
                break;
            default:
                usage();
            }
        }
    }

    switch (mode) {
    case 'c':
        create(filename, compress, opt_index, argc, argv);
        break;
    case 't':
//        extract(filename, 0, flags);
        break;
    case 'x':
//        extract(filename, 1, flags);
        break;
    }

    MPI_Finalize();
    return (0);
}

inline void server_stuff(void) {

    MPI_Status status;
    MPI_Request request, req_offset;
    int token;
    int flag = 0, flag2 = 0;
    int64_t buffer[2];

    MPI_Irecv(&token, 1, MPI_INT, 0, 10, inter_comm, &request);
    MPI_Test(&request, &flag, &status);

    MPI_Recv_init(buffer, 2, MPI_LONG_LONG,
    MPI_ANY_SOURCE, 0, inter_comm, &req_offset);

    MPI_Start(&req_offset);

    while (!flag) {

        MPI_Test(&req_offset, &flag2, &status);

        if (flag2) {
            MPI_Send(&g_tar_offset, 1, MPI_LONG_LONG, buffer[0], 0, inter_comm);
            g_tar_offset += buffer[1];
            MPI_Start(&req_offset);
        }

        MPI_Test(&request, &flag, &status);

    }

    char * buff_null = (char*) calloc(1024, 1);
    printf("this is global offset %llx\n", g_tar_offset);
    lseek64(DTAR_writer.fd_tar, g_tar_offset, SEEK_SET);
    write(DTAR_writer.fd_tar, buff_null, 1024);

    printf("All is done! Token is %d\n", token);

}

static void create(char *filename, char compress, int opt_index, int argc,
        char **argv) {
    char ** argv_beg = argv - opt_index - 2;
    int color = 1;
    int my_rank;

    DTAR_parse_path_args(filename, compress, argv);
    DTAR_writer_init();
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    if (my_rank == 0) {
        color = 0;
    }
    MPI_Comm_split(MPI_COMM_WORLD, color, my_rank, &new_comm);

    if (my_rank == 0) {
        MPI_Intercomm_create(new_comm, 0, MPI_COMM_WORLD, 1, 1, &inter_comm);
    } else {
        MPI_Intercomm_create(new_comm, 0, MPI_COMM_WORLD, 0, 1, &inter_comm);
    }

    if (my_rank == 0) {
        server_stuff();
        close(DTAR_writer.fd_tar);
        MPI_Comm_free(&inter_comm);
        MPI_Comm_free(&new_comm);
        return;
    }

    CIRCLE_global_rank = CIRCLE_init2(argc, argv_beg, CIRCLE_DEFAULT_FLAGS,
            &new_comm, &inter_comm);
    CIRCLE_cb_create(&DTAR_add_objects);
    CIRCLE_cb_process(&DTAR_process_objects);

    DTAR_jump_table[TREEWALK] = DTAR_do_treewalk;
    DTAR_jump_table[COPY] = DTAR_do_copy;

    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_INFO;
    CIRCLE_enable_logging(CIRCLE_debug);
    DTAR_debug_level = DTAR_LOG_INFO;

    CIRCLE_begin();
    CIRCLE_finalize();

    close(DTAR_writer.fd_tar);
    MPI_Comm_free(&inter_comm);
    MPI_Comm_free(&new_comm);

}
