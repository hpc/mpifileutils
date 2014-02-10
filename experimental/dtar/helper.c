#include "dtar.h"

#include <archive.h>
#include <archive_entry.h>

#include <libcircle.h>

#include <dirent.h>
#include <sys/types.h>
#include <libgen.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "helper.h"
#include "bayer.h"

extern DTAR_options_t DTAR_user_opts;
extern DTAR_writer_t DTAR_writer;
extern void (*DTAR_jump_table[3])(DTAR_operation_t* op, CIRCLE_handle* handle);

void DTAR_writer_init()
{
    char* filename = DTAR_user_opts.dest_path;
    DTAR_writer.name = filename;
    //DTAR_writer.flags= O_WRONLY | O_CREAT | O_TRUNC | O_BINARY | O_CLOEXEC;
    DTAR_writer.flags = O_WRONLY | O_CREAT | O_BINARY | O_CLOEXEC;
    DTAR_writer.fd_tar = open(filename, DTAR_writer.flags, 0666);
}

void DTAR_parse_path_args(char* filename, char compress, char** argv)
{
    int i = 0;
    DTAR_user_opts.dest_path = filename;

    while(*argv != NULL) {
        argv++;
        i++;
    }

    DTAR_user_opts.num_src_paths = i;
    DTAR_user_opts.src_path = argv - i;

}

void DTAR_add_objects(CIRCLE_handle* handle)
{
    DTAR_enqueue_work_objects(handle);
}

void DTAR_process_objects(CIRCLE_handle* handle)
{
    char op[CIRCLE_MAX_STRING_LEN];

    /* Pop an item off the queue */
    handle->dequeue(op);
    DTAR_operation_t* opt = DTAR_decode_operation(op);

    DTAR_jump_table[opt->code](opt, handle);

    // DTAR_opt_free(&opt);
    return;
}

void DTAR_enqueue_work_objects(CIRCLE_handle* handle)
{

    char* dirc, *dname;
    char* basec, *bname;
    uint32_t number_of_source_files = DTAR_user_opts.num_src_paths;

    if(number_of_source_files < 1) {
        BAYER_LOG(BAYER_LOG_ERR, "At least one valid source file must be specified.");
        DTAR_abort(EXIT_FAILURE);
    }

    int exist = access(DTAR_user_opts.dest_path, F_OK);

    if(-1 != exist) {

        int i;

        for(i = 0; i < DTAR_user_opts.num_src_paths; i++) {

            char* src_path = DTAR_user_opts.src_path[i];
            dirc = strdup(src_path);
            basec = strdup(src_path);
            dname = dirname(dirc);
            bname = basename(basec);

            size_t src_len = strlen(src_path) + 1;
            char* op = DTAR_encode_operation(TREEWALK, 0, bname, 0, 0);
            handle->enqueue(op);
        }

        free(basec);
        free(dirc);
    }
    else {

        BAYER_LOG(BAYER_LOG_ERR, "Destination File Already Exists\n");
        DTAR_abort(EXIT_FAILURE);
    }

}

/**
 * Encode an operation code for use on the distributed queue structure.
 */
char* DTAR_encode_operation(DTAR_operation_code_t code, int64_t chunk,
                            const char* operand, uint64_t offset, int64_t file_size)
{
    size_t opsize = (size_t) CIRCLE_MAX_STRING_LEN;
    char* op = (char*) BAYER_MALLOC(opsize);

    size_t len = strlen(operand);

    int written = snprintf(op, opsize,
                           "%" PRId64 ":%" PRId64 ":%" PRIu64 ":%d:%d:%s", file_size,
                           chunk, offset, code, (int) len, operand);

    if(written >= opsize) {
        BAYER_LOG(BAYER_LOG_ERR,
                  "Exceeded libcircle message size due to large file path. "
                  "This is a known bug in dcp that we intend to fix. Sorry!");
        DTAR_abort(EXIT_FAILURE);
    }

    return op;
}

/**
 * Decode the operation code from a message on the distributed queue structure.
 */
DTAR_operation_t* DTAR_decode_operation(char* op)
{
    DTAR_operation_t* ret = (DTAR_operation_t*) BAYER_MALLOC(
                                sizeof(DTAR_operation_t));

    if(sscanf(strtok(op, ":"), "%" SCNd64, &(ret->file_size)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode file size attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if(sscanf(strtok(NULL, ":"), "%" SCNd64, &(ret->chunk)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode chunk index attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if(sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->offset)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode source base offset attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if(sscanf(strtok(NULL, ":"), "%d", (int*) & (ret->code)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode stage code attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* get number of characters in operand string */
    int op_len;
    char* str = strtok(NULL, ":");

    if(sscanf(str, "%d", &op_len) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode operand string length.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* skip over digits and trailing ':' to get pointer to operand */
    char* operand = str + strlen(str) + 1;
    operand[op_len] = '\0';
    ret->operand = operand;
    str = operand + op_len + 1;

    return ret;
}

/* called by single process upon detection of a problem */
void DTAR_abort(int code)
{
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

/* called globally by all procs to exit */
void DTAR_exit(int code)
{
    /* CIRCLE_finalize or will this hang? */
    MPI_Finalize();
    exit(code);
}

char* userNameFromId(uid_t uid)
{
    struct passwd* pwd;
    pwd = getpwuid(uid);

    return (pwd == NULL) ? NULL : pwd->pw_name;

}

char* groupNameFromId(gid_t gid)
{
    struct group* grp;
    grp = getgrgid(gid);
    return (grp == NULL) ? NULL : grp->gr_name;
}
