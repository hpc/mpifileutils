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

/* common structures */

typedef struct {
    size_t  chunk_size;
    size_t  block_size;

    char*   dest_path;
    bool    preserve;
} DTAR_options_t;


typedef struct {
    const char* name;
    int fd_tar;
    int flags;
} DTAR_writer_t;

/* global variables */

extern int DTAR_global_rank;
extern DTAR_options_t DTAR_user_opts;
extern DTAR_writer_t DTAR_writer;
extern uint64_t DTAR_total;
extern uint64_t DTAR_count;
extern bayer_flist DTAR_flist;
extern uint64_t* DTAR_fsizes;
extern bayer_param_path* src_params;
extern bayer_param_path dest_param;
extern int num_src_params;

/* function declaration */

void DTAR_abort(int code);
void DTAR_exit(int code);
void DTAR_parse_path_args(int, char **, const char *);
void DTAR_writer_init();

#endif /* COMMON_H_ */
