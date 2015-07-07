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

#ifndef _PRED_H
#define _PRED_H

struct target
{
	char * fname;
	struct stat * statbuf;
};

typedef int (*pred_t)(struct target, void * arg);

struct pred_item_t
{
	pred_t f;
	void * arg;
	struct pred_item_t * next;
};

typedef struct pred_item_t pred_item;
pred_item * pred_head;
void pred_add(pred_t, void *);
void pred_commit(void);

int name (struct target, void *);
int pred_exec (struct target, void *);
int pred_print (struct target, void *);
int pred_newer (struct target, void *);
int pred_type (struct target, void *);

int execute (char *, pred_item *);

#endif
