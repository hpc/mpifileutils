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
#include "dtcmp.h"

#endif /* BAYER_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
