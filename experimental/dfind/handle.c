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
#include <sys/types.h>
#include <string.h>
#include <dirent.h>
#include <limits.h>

#include "common.h"
#include "pred.h"
#include "queue.h"

void handle_dir (char * path, int depth)
{
	DIR * target;
	struct dirent * entry;

	char child[PATH_MAX];

	execute(path, pred_head);

	if (depth >= options.maxdepth)
		return;

	target = opendir(path);
	if (!target)
	{
		fprintf(stderr, "error accessing %s\n", path);
		return;
	}

	while ((entry = readdir(target)))
	{
		dbprintf("read: %s\n", entry->d_name);
		if (!strcmp(entry->d_name, ".."))
			continue;
		if (!strcmp(entry->d_name, "."))
			continue;
		if ((snprintf(child, PATH_MAX, "%s/%s", path, entry->d_name)) >= PATH_MAX)
		{
			fprintf(stderr, "filename %s too long\n", entry->d_name);
			continue;
		}

		switch (entry->d_type)
		{
			case DT_DIR:
				queue_dir(child, ++depth);
			break;

			case DT_REG:
				queue_file(child);
			break;

			case DT_LNK:
				queue_file(child);
			break;

			default:
				dbprintf("not handled: %s\n", child);
			break;
		}
	}

	closedir(target);
	return;
}

void handle_file (char * fname)
{
	dbprintf("ran on %s\n", fname);
	execute(fname, pred_head);
}
