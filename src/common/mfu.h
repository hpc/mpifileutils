/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_H
#define MFU_H

#define MFU_SUCCESS (0)
#define MFU_FAILURE (1)

/* TODO: ugly hack until we get a configure test */
// HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
#define HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC 1
// HAVE_STRUCT_STAT_ST_MTIME_N
// HAVE_STRUCT_STAT_ST_UMTIME
// HAVE_STRUCT_STAT_ST_MTIME_USEC

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif
#include <limits.h>

#include "mfu_util.h"
#include "mfu_path.h"
#include "mfu_io.h"
#include "mfu_param_path.h"
#include "mfu_flist.h"
#include "mfu_pred.h"
#include "mfu_progress.h"
#include "mfu_bz2.h"

#endif /* MFU_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
