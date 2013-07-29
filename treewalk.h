/* See the file "COPYING" for the full license governing this code. */

#ifndef __DCP_TREEWALK_H
#define __DCP_TREEWALK_H

#include "common.h"

void DCOPY_do_treewalk(DCOPY_operation_t* op, \
                       CIRCLE_handle* handle);

void DCOPY_stat_process_link(DCOPY_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle);

void DCOPY_stat_process_file(DCOPY_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle);

void DCOPY_stat_process_dir(DCOPY_operation_t* op,
                            const struct stat64* statbuf,
                            CIRCLE_handle* handle);

#endif /* __DCP_TREEWALK_H */
