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
#include "mfu_flist.h"
#include "mfu_param_path.h"

#endif /* MFU_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
