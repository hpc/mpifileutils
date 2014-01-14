/*
 * common.c
 *
 *  Created on: Jan 13, 2014
 *      Author: fwang2
 */

#include "common.h"

void DTAR_writer_init() {
    char * filename = DTAR_user_opts.dest_path;
    DTAR_writer.name = filename;
    DTAR_writer.flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_LARGEFILE;
    DTAR_writer.fd_tar = open(filename, DTAR_writer.flags, 0664);

}

void DTAR_abort(int code) {
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

void DTAR_exit(int code) {
    bayer_finalize();
    MPI_Finalize();
    exit(code);
}


struct archive * new_archive() {
    struct archive *a = archive_write_new();
    int r = archive_write_set_format_pax(a);
    if ( r != ARCHIVE_OK) {
        BAYER_LOG(BAYER_LOG_ERR, archive_error_string(a));
        return NULL;
    }
    archive_write_set_bytes_per_block(a, 0);
    return a;
}

int DTAR_write_header(struct archive *a, uint64_t idx, uint64_t offset) {
    struct archive_entry *entry = archive_entry_new();

}


char * DTAR_encode_operation(
        DTAR_operation_code_t code,
        const char* operand,
        uint64_t fsize,
        uint64_t chunk,
        uint64_t offset
        ) {

    size_t opsize = (size_t) CIRCLE_MAX_STRING_LEN;
    char* op = (char*) BAYER_MALLOC(opsize);
    size_t len = strlen(operand);

    int written = snprintf(op, opsize,
            "%" PRIu64 ":%" PRIu64 ":%" PRIu64 ":%d:%d:%s",
            fsize, chunk, offset, code, (int) len, operand);

    if (written >= opsize) {
        BAYER_LOG(BAYER_LOG_ERR, "Exceed libcirlce message size");
        DTAR_abort(EXIT_FAILURE);
    }

    return op;

}

DTAR_operation_t* DTAR_decode_operation(char *op) {

    DTAR_operation_t* ret = (DTAR_operation_t*) BAYER_MALLOC(
            sizeof(DTAR_operation_t));

    if (sscanf(strtok(op, ":"), "%" SCNu64, &(ret->file_size)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode file size attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->chunk)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode chunk index attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->offset)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode source base offset attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    if (sscanf(strtok(NULL, ":"), "%d", (int*) &(ret->code)) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode stage code attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* get number of characters in operand string */
    int op_len;
    char* str = strtok(NULL, ":");
    if (sscanf(str, "%d", &op_len) != 1) {
        BAYER_LOG(BAYER_LOG_ERR, "Could not decode operand string length.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* skip over digits and trailing ':' to get pointer to operand */
    char* operand = str + strlen(str) + 1;
    operand[op_len] = '\0';
    ret->operand = operand;

    return ret;

}
