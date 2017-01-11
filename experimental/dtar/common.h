/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2015, DataDirect Networks, Inc.
 * 
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/

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

#define _LARGEFILE64_SOURCE


#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <mpi.h>
#include <glib.h>
#include <libcircle.h>
#include <archive.h>
#include <archive_entry.h>

#include "mfu.h"


#define DTAR_HDR_LENGTH 1536
#define FD_BLOCK_SIZE (1024*1024)

typedef enum {
    COPY_DATA
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
    uint64_t total_dirs;
    uint64_t total_files;
    uint64_t total_links;
    uint64_t total_size;
    uint64_t total_bytes_copied;
    double  wtime_started;
    double  wtime_ended;
    time_t  time_started;
    time_t  time_ended;
} DTAR_statistics_t;

typedef struct {
    uint64_t file_size;
    uint64_t chunk_index;
    uint64_t offset;
    DTAR_operation_code_t code;
    char* operand;
} DTAR_operation_t;

/* global variables */

extern int DTAR_global_rank;
extern DTAR_options_t DTAR_user_opts;
extern DTAR_writer_t DTAR_writer;
extern DTAR_statistics_t DTAR_statistics;
extern uint64_t DTAR_total;
extern uint64_t DTAR_count;
extern uint64_t DTAR_goffset;
extern mfu_flist DTAR_flist;
extern uint64_t* DTAR_fsizes;
extern uint64_t* DTAR_offsets;
extern mfu_param_path* src_params;
extern mfu_param_path dest_param;
extern int num_src_params;

extern int DTAR_rank;
extern int DTAR_size;

/* function declaration */

void DTAR_abort(int code);
void DTAR_exit(int code);
void DTAR_parse_path_args(int, char **, const char *);
void DTAR_writer_init();
void DTAR_epilogue();

struct archive* DTAR_new_archive();
void DTAR_write_header(struct archive * a, uint64_t idx, uint64_t offset);

DTAR_operation_t* DTAR_decode_operation(char *op);
char * DTAR_encode_operation( DTAR_operation_code_t code,
        const char* operand, uint64_t fsize, uint64_t chunk, uint64_t offset);

void DTAR_enqueue_copy(CIRCLE_handle* handle);
void DTAR_perform_copy(CIRCLE_handle* handle);

#endif /* COMMON_H_ */
