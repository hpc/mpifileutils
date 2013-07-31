/* See the file "COPYING" for the full license governing this code. */

#ifndef __DTAR_COPY_H
#define __DTAR_COPY_H

#include "common.h"

void DTAR_do_copy(DTAR_operation_t* op, \
                   CIRCLE_handle* handle);

int DTAR_perform_copy(DTAR_operation_t* op, \
                       int in_fd, \
                       int out_fd, \
                       off64_t offset);


#endif /* __DTAR_COPY_H */
