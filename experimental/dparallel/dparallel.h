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

#ifndef _DPARALLEL_DPARALLEL_H
#define _DPARALLEL_DPARALLEL_H

#include <config.h>
#include <libcircle.h>

#include "log.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

char* DPARALLEL_readline(void);
void DPARALLEL_process(CIRCLE_handle* handle);

#endif /* _DPARALLEL_DPARALLEL_H */
