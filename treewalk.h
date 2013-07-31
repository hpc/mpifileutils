/* See the file "COPYING" for the full license governing this code. */

#ifndef __DTAR_TREEWALK_H
#define __DTAR_TREEWALK_H

#include "common.h"

void DTAR_do_treewalk(DTAR_operation_t* op, \
                       CIRCLE_handle* handle);

void DTAR_stat_process_link(DTAR_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle);

void DTAR_stat_process_file(DTAR_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle);

void DTAR_stat_process_dir(DTAR_operation_t* op,
                            const struct stat64* statbuf,
                            CIRCLE_handle* handle);

#endif /* __DTAR_TREEWALK_H */
