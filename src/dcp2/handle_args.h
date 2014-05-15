/* See the file "COPYING" for the full license governing this code. */

#ifndef __DCOPY_HANDLE_ARGS_H
#define __DCOPY_HANDLE_ARGS_H

#include "common.h"

void DCOPY_parse_path_args(char** argv, int optind, int argc);

/* walks each source path and adds entries to flist */
void DCOPY_walk_paths(bayer_flist flist);

/* given a file name, return destination file name,
 * caller must free using bayer_free when done */
char* DCOPY_build_dest(const char* name);

void DCOPY_free_path_args(void);

void DCOPY_enqueue_work_objects(CIRCLE_handle* handle);

#endif /* __DCOPY_HANDLE_ARGS_H */
