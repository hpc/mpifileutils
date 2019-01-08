#ifndef _DPARALLEL_DPARALLEL_H
#define _DPARALLEL_DPARALLEL_H

#include <libcircle.h>

#include "log.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

char* DPARALLEL_readline(void);
void DPARALLEL_process(CIRCLE_handle* handle);

#endif /* _DPARALLEL_DPARALLEL_H */
