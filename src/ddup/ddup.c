/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   Written by Adam Moody <moody20@llnl.gov>.
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2016, DataDirect Networks, Inc.
 *
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <openssl/sha.h>
#include <assert.h>
#include <inttypes.h>
#include "mpi.h"
#include "dtcmp.h"
#include "bayer.h"
#include "list.h"

#define DDUP_KEY_SIZE 5
static void mpi_type_init(MPI_Datatype *key, MPI_Datatype *keysat)
{
	assert(SHA256_DIGEST_LENGTH == (DDUP_KEY_SIZE - 1) * 8);

	/*
	 * Build MPI datatype for key.
	 * 1 for group ID + (SHA256_DIGEST_LENGTH / 8)
	 */
	MPI_Type_contiguous(DDUP_KEY_SIZE, MPI_UINT64_T, key);
	MPI_Type_commit(key);

	/*
	 * Build MPI datatype for key + satellite
	 * length of key + 1 for index in flist
	 */
	MPI_Type_contiguous(DDUP_KEY_SIZE + 1, MPI_UINT64_T, keysat);
	MPI_Type_commit(keysat);
}

static void mpi_type_fini(MPI_Datatype *key, MPI_Datatype *keysat)
{
	MPI_Type_free(keysat);
	MPI_Type_free(key);
}

static int mtcmp_cmp_init(DTCMP_Op *cmp)
{
	DTCMP_Op series[DDUP_KEY_SIZE];
	int i;

	for (i = 0; i < DDUP_KEY_SIZE; i++)
		series[i] = DTCMP_OP_UINT64T_ASCEND;
	return DTCMP_Op_create_series(DDUP_KEY_SIZE, series, cmp);
}

static void mtcmp_cmp_fini(DTCMP_Op *cmp)
{
	DTCMP_Op_free(cmp);
}

#define DDUP_CHUNK_SIZE 1048576

static int read_data(const char *fname, char *chunk_buf, uint64_t chunk_id,
		     uint64_t chunk_size, uint64_t file_size,
		     uint64_t *data_size)
{
	int status = 0;
	int fd;
	uint64_t offset;
	ssize_t read_size;

	assert(chunk_id > 0);
	offset = (chunk_id - 1) * chunk_size;
	memset(chunk_buf, 0, chunk_size);

	fd = bayer_open(fname, O_RDONLY);
	if (fd < 0)
		return -1;

	if (bayer_lseek(fname, fd, offset, SEEK_SET) == (off_t)-1) {
		status = -1;
		goto out;
	}

	read_size = bayer_read(fname, fd, chunk_buf, chunk_size);
	if (read_size < 0) {
		status = -1;
		goto out;
	}

	if (file_size >= chunk_id * chunk_size) {
		/* File size has been changed */
		if ((uint64_t)read_size != chunk_size) {
			status = -1;
			goto out;
		}
	} else {
		/* File size has been changed */
		if ((uint64_t)read_size != file_size - offset) {
			status = -1;
			goto out;
		}
	}
	*data_size = (uint64_t)read_size;
out:
	bayer_close(fname, fd);
	return status;
}

struct file_item {
	SHA256_CTX ctx;
};

static void dump_sha256_digest(char *digest_string, unsigned char digest[])
{
	int i;

	for (i = 0; i < SHA256_DIGEST_LENGTH; i++)
		sprintf(&digest_string[i * 2], "%02x",
		        (unsigned int)digest[i]);
}

int main(int argc, char **argv)
{
	MPI_Datatype key;
	MPI_Datatype keysat;
	DTCMP_Op cmp;
	bayer_flist flist;
	uint64_t checking_files;
	uint64_t new_checking_files;
	size_t list_bytes;
	uint64_t file_size;
	uint64_t sum_checking_files;
	uint64_t *list;
	uint64_t *new_list;
	uint64_t *tmp_list;
	uint64_t i;
	uint64_t output_bytes;
	uint64_t *group_id;
	uint64_t *group_ranks;
	uint64_t *group_rank;
	uint64_t *ptr;
	uint64_t groups;
	uint64_t index;
	uint64_t chunk_id;
	uint64_t chunk_size = DDUP_CHUNK_SIZE;
	uint64_t data_size;
	char *chunk_buf;
	mode_t mode;
	const char *fname;
	const char *dir;
	int status;
	struct file_item *file_items;
	SHA256_CTX *ctx_ptr;
	char digest_string[SHA256_DIGEST_LENGTH * 2 + 1];
	unsigned char digest[SHA256_DIGEST_LENGTH];
	SHA256_CTX ctx_tmp;
	int rank, ranks;
	int option_index = 0;
	int c;
	static struct option long_options[] = {
		{"debug",    0, 0, 'd'},
		{0, 0, 0, 0}
	};

	MPI_Init(NULL, NULL);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &ranks);
	bayer_init();

	bayer_debug_level = BAYER_LOG_INFO;
	/* Parse options */
	while((c = getopt_long(argc, argv, "d:", \
			       long_options, &option_index)) != -1) {
		switch(c) {
		case 'd':
			if(strncmp(optarg, "fatal", 5) == 0) {
				bayer_debug_level = BAYER_LOG_FATAL;

				if(rank == 0)
					BAYER_LOG(BAYER_LOG_INFO,
						  "Debug level set to: fatal");
			} else if(strncmp(optarg, "err", 3) == 0) {
				bayer_debug_level = BAYER_LOG_ERR;

				if(rank == 0)
					BAYER_LOG(BAYER_LOG_INFO,
						  "Debug level set to: "
						  "errors");
			} else if(strncmp(optarg, "warn", 4) == 0) {
				bayer_debug_level = BAYER_LOG_WARN;

				if(rank == 0)
					BAYER_LOG(BAYER_LOG_INFO,
						  "Debug level set to: "
						  "warnings");
			} else if(strncmp(optarg, "info", 4) == 0) {
				bayer_debug_level = BAYER_LOG_INFO;

				if(rank == 0)
					BAYER_LOG(BAYER_LOG_INFO,
						  "Debug level set to: info");
			} else if(strncmp(optarg, "dbg", 3) == 0) {
				bayer_debug_level = BAYER_LOG_DBG;

				if(rank == 0)
					BAYER_LOG(BAYER_LOG_INFO,
						  "Debug level set to: debug");
			} else {
				if(rank == 0)
					BAYER_LOG(BAYER_LOG_INFO,
						  "Debug level `%s' not "
						  "recognized. Defaulting to "
						  "`info'.", optarg);
			}
		}
                break;
	}

	if (argv[optind] == NULL) {
		if(rank == 0)
			BAYER_LOG(BAYER_LOG_ERR,
				  "You must specify a directory path");
		MPI_Barrier(MPI_COMM_WORLD);
		status = -1;
		goto out;
	}
	dir = argv[optind];

	mpi_type_init(&key, &keysat);
	mtcmp_cmp_init(&cmp);
	flist = bayer_flist_new();
	chunk_buf = (char *)BAYER_MALLOC(DDUP_CHUNK_SIZE);
	/* Walk the path(s) to build the flist */
	bayer_flist_walk_path(dir, 1, flist);

	checking_files = bayer_flist_size(flist);
	file_items = (struct file_item *)BAYER_MALLOC(checking_files *
						      sizeof(*file_items));
	/* Allocate two lists of length size, where each
	 * element has (DDUP_KEY_SIZE + 1) uint64_t values
	 * (id, checksum, index)
	 */
	list_bytes = checking_files * (DDUP_KEY_SIZE + 1) * sizeof(uint64_t);
	list = (uint64_t *) BAYER_MALLOC(list_bytes);
	new_list = (uint64_t *) BAYER_MALLOC(list_bytes);

	/* Initialize the list */
	ptr = list;
	new_checking_files = 0;
	for (i = 0; i < checking_files; i++) {
		mode = (mode_t) bayer_flist_file_get_mode(flist, i);
		if (!S_ISREG(mode))
			continue;
		file_size = bayer_flist_file_get_size(flist, i);
		/* Files with size zero is not interesting at all */
		if (file_size == 0)
			continue;
		ptr[0] = file_size;
		ptr[DDUP_KEY_SIZE] = i; /* Index in flist */
		ptr += DDUP_KEY_SIZE + 1;
		SHA256_Init(&file_items[i].ctx);
		new_checking_files++;
	}
	checking_files = new_checking_files;

	output_bytes = checking_files * sizeof(uint64_t);
	group_id = (uint64_t *) BAYER_MALLOC(output_bytes);
	group_ranks = (uint64_t *) BAYER_MALLOC(output_bytes);
	group_rank = (uint64_t *) BAYER_MALLOC(output_bytes);

	MPI_Allreduce(&checking_files, &sum_checking_files, 1,
		      MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
	chunk_id = 0;
	while (sum_checking_files > 1) {
		chunk_id++;
		ptr = list;
		for (i = 0; i < checking_files; i++) {
			index = *(list + i * (DDUP_KEY_SIZE + 1) +
				  DDUP_KEY_SIZE);
			file_size = bayer_flist_file_get_size(flist, index);
			fname = bayer_flist_file_get_name(flist, index);
			status = read_data(fname, chunk_buf, chunk_id,
					   chunk_size, file_size, &data_size);
			/* File size has been changed, TODO: handle */
			if (status) {
				printf("failed to read file %s, maybe file "
				       "size has been modified during the "
				       "process", fname);
			}
			ctx_ptr = &file_items[index].ctx;
			SHA256_Update(ctx_ptr, chunk_buf, data_size);
			/*
			 * This is actually an hack, but SHA256_Final can't
			 * be called multiple times with out changing ctx
			 */
			memcpy(&ctx_tmp, ctx_ptr, sizeof(ctx_tmp));
			SHA256_Final((unsigned char *)(ptr + 1), &ctx_tmp);
			ptr += DDUP_KEY_SIZE + 1;
		}

		/* Assign group ids and compute group sizes */
		DTCMP_Rankv((int)checking_files, list, &groups, group_id,
			    group_ranks, group_rank, key, keysat, cmp,
			    DTCMP_FLAG_NONE, MPI_COMM_WORLD);

		new_checking_files = 0;
		ptr = new_list;
		for (i = 0; i < checking_files; i++) {
			/* Get index into flist for this item */
			index = *(list + i * (DDUP_KEY_SIZE + 1) +
				  DDUP_KEY_SIZE);
			file_size = bayer_flist_file_get_size(flist, index);
			fname = bayer_flist_file_get_name(flist, index);
			ctx_ptr = &file_items[index].ctx;
			if (group_ranks[i] == 1) {
				/*
				 * Only one file in this group,
				 * bayer_flist_file_name(flist, idx) is unique
				 */
			} else if (file_size < (chunk_id * chunk_size)) {
				/*
				 * We've run out of bytes to checksum, and we
				 * still have a group size > 1
				 * bayer_flist_file_name(flist, idx) is a
				 * duplicate with other files that also have
				 * matching group_id[i]
				 */
				SHA256_Final(digest, ctx_ptr);
				dump_sha256_digest(digest_string, digest);
				printf("%s %s\n", fname, digest_string);
			} else {
				/* Have multiple files with the same checksum,
				 * but still have bytes left to read, so keep
				 * this file
				 */
				/* use new group ID */
				ptr[0] = group_id[i];
				/* Copy over flist index */
				ptr[DDUP_KEY_SIZE] = index;
				ptr += DDUP_KEY_SIZE + 1;
				new_checking_files++;
				BAYER_LOG(BAYER_LOG_DBG, "checking file "
					  "\"%s\" for chunk index %d of size %"
					  PRIu64"\n", fname, chunk_id,
					  chunk_size);
			}
		}
		/* Swap list buffers */
		tmp_list = list;
		list = new_list;
		new_list = tmp_list;

		/* Update size of current list */
		checking_files = new_checking_files;

		/* Get new global list size */
		MPI_Allreduce(&checking_files, &sum_checking_files, 1,
			      MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
	}

	bayer_free(&group_rank);
	bayer_free(&group_ranks);
	bayer_free(&group_id);
	bayer_free(&new_list);
	bayer_free(&list);
	bayer_free(&file_items);
	bayer_free(&chunk_buf);
	bayer_flist_free(&flist);
	mtcmp_cmp_fini(&cmp);
	mpi_type_fini(&key, &keysat);
	status = 0;
out:
	bayer_finalize();
	MPI_Finalize();
	return status;
}
