/* See the file "COPYING" for the full license governing this code. */

#ifndef __DCOPY_HANDLE_ARGS_H
#define __DCOPY_HANDLE_ARGS_H

#include "common.h"

void DCOPY_parse_path_args(char** argv, int optind, int argc);

void DCOPY_free_path_args(void);

void DCOPY_enqueue_work_objects(CIRCLE_handle* handle);

#endif /* __DCOPY_HANDLE_ARGS_H */
