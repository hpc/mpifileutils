/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef BAYER_H
#define BAYER_H

#define BAYER_SUCCESS (0)
#define BAYER_FAILURE (1)

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

#include "bayer_util.h"
#include "bayer_path.h"
#include "bayer_io.h"
#include "bayer_flist.h"
#include "bayer_param_path.h"
#include "strmap.h"

#endif /* BAYER_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
