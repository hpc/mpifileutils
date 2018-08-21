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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <libgen.h>
#include <fnmatch.h>

#include "common.h"
#include "pred.h"

void pred_add(pred_t predicate, void * arg)
{
	if (!pred_head)
	{
		pred_head = (pred_item *)malloc(sizeof(pred_item));
		pred_head->f = predicate;
		pred_head->arg = arg;
		pred_head->next = NULL;
		return;
	}

	pred_item * p = pred_head;

	while (p->next)
		p = p->next;

	p->next = (pred_item *)malloc(sizeof(pred_item));
	p = p->next;
	p->f = predicate;
	p->arg = arg;
	p->next = NULL;
}

void pred_commit (void)
{
	int need_print = 1;
	pred_item * cur = pred_head;

	while (cur)
	{
		if (cur->f == pred_print || cur->f == pred_exec)
		{
			need_print = 0;
			break;
		}
		cur = cur->next;
	}

	if (need_print)
		pred_add(pred_print, NULL);
}

int execute (char * fname, pred_item * root)
{
	pred_item * p = root;
	struct target t;

	t.fname = fname;
	t.statbuf = NULL;
	while (p)
	{
		if (p->f(t, p->arg) <= 0)
			return -1;
		p = p->next;
	}
	
	return 0;
}

void statif (struct target * t)
{
	if (!t->statbuf)
	{
		t->statbuf = (struct stat *)malloc(sizeof(struct stat));
		if (stat(t->fname, t->statbuf) < 0 )
		{
			printf("warning: file %s not found\n", t->fname);
			return;
		}
	}
	return;
}

int pred_type (struct target t, void * arg)
{
	statif(&t);

	mode_t mode = (mode_t)arg;
	
	if ( (t.statbuf->st_mode & mode) == mode)
		return 1;
	else
		return 0;
}

int name (struct target t, void * arg)
{
	char * pattern = (char *)arg;
	
	return fnmatch(pattern, basename(t.fname), FNM_PERIOD) ? 0 : 1;
}

int pred_exec (struct target t, void * arg)
{
	int argmax = sysconf(_SC_ARG_MAX);
	int written = 0;
	int ret;
	char * command = strdup((char *)arg);
	char * cmdline = (char *)malloc(argmax);
	char * subst = strstr(command, "{}");

	if (subst)
	{
		subst[0] = '\0';
		subst += 2; /* Point to the first char after '{}' */
	}
	written = snprintf(cmdline, argmax/sizeof(char), "%s%s%s",
					command, t.fname, subst);
	free(command);
	if (written > argmax/sizeof(char))
	{
		fprintf(stderr, "argument %s to exec too long.\n", cmdline);
		return -1;
	}

	ret = system(cmdline);
	free(cmdline);
	return ret ? 0 : 1;
}

int pred_print (struct target t, void * arg)
{
	printf("%s\n", t.fname);
	return 1;
}

int pred_newer (struct target t, void * arg)
{
	statif(&t);

	if (t.statbuf->st_mtime > (time_t)arg)
		return 1;
	else
		return 0;
}
