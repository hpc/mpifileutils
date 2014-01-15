/*
 * common.h
 *
 *  Created on: Jan 13, 2014
 *      Author: fwang2
 */

#ifndef COMMON_H_
#define COMMON_H_


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif


#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <attr/xattr.h>
#include <mpi.h>
#include <glib.h>
#include <libcircle.h>
#include <archive.h>
#include <archive_entry.h>

#include "bayer.h"


#define DTAR_HDR_LENGTH 1536


typedef enum {
    DO_HEADER, DO_DATA
} DTAR_operation_code_t;


/* common structures */

typedef struct {
    size_t  chunk_size;
    size_t  block_size;
    char*   dest_path;
    bool    preserve;
    int     flags;
} DTAR_options_t;


typedef struct {
    const char* name;
    int fd_tar;
    int flags;
} DTAR_writer_t;



typedef struct {
    uint64_t file_size;
    uint64_t chunk;
    uint64_t offset;
    DTAR_operation_code_t code;
    char* operand;
} DTAR_operation_t;

/* global variables */

extern int DTAR_global_rank;
extern DTAR_options_t DTAR_user_opts;
extern DTAR_writer_t DTAR_writer;
extern uint64_t DTAR_total;
extern uint64_t DTAR_count;
extern uint64_t DTAR_goffset;
extern bayer_flist DTAR_flist;
extern uint64_t* DTAR_fsizes;
extern uint64_t* DTAR_offsets;
extern bayer_param_path* src_params;
extern bayer_param_path dest_param;
extern int num_src_params;

extern void (*DTAR_jump_table[3])(DTAR_operation_t* op, CIRCLE_handle* handle);


/* function declaration */

void DTAR_abort(int code);
void DTAR_exit(int code);
void DTAR_parse_path_args(int, char **, const char *);
void DTAR_writer_init();

struct archive* DTAR_new_archive();
void DTAR_write_header(struct archive * a, uint64_t idx, uint64_t offset);

DTAR_operation_t* DTAR_decode_operation(char *op);
char * DTAR_encode_operation( DTAR_operation_code_t code,
        const char* operand, uint64_t fsize, uint64_t chunk, uint64_t offset);


#endif /* COMMON_H_ */
