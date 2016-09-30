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

#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h> /* asctime / localtime */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"

/* TODO: change globals to struct */
static int walk_stat = 0;
static int dir_perm = 0;

/*****************************
 * Driver functions
 ****************************/

static void print_usage(void)
{
    printf("\n");
    printf("Usage: drm [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input   <file>   - read list from file\n");
    printf("  -l, --lite             - walk file system without stat\n");
    printf("      --exclude <regex>  - exclude a list of files from command\n");
    printf("      --match   <regex>  - match a list of files from command\n");
    printf("  -n, --name             - exclude a list of files from command\n");
    printf("  -v, --verbose          - verbose output\n");
    printf("      --dryrun           - print out list of files that would be deleted\n");
    printf("  -h, --help             - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    int i;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    bayer_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* parse command line options */
    char* inputname = NULL;
    char* regex_exp = NULL;
    int walk        = 0;
    int exclude     = 0;
    int name        = 0;
    int dryrun      = 0;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",    1, 0, 'i'},
        {"lite",     0, 0, 'l'},
        {"exclude",  1, 0, 'e'},
        {"match",    1, 0, 'a'},
        {"name",     0, 0, 'n'},        
        {"help",     0, 0, 'h'},
        {"dryrun",   0, 0, 'd'},
        {"verbose",  0, 0, 'v'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:nlhv",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = BAYER_STRDUP(optarg);
                break;
            case 'l':
                walk_stat = 0;
                break;
            case 'e':
                regex_exp = BAYER_STRDUP(optarg);
                exclude = 1;
                break;
            case 'a':
                regex_exp = BAYER_STRDUP(optarg);
                exclude = 0;
                break;
            case 'n':
                name = 1;
                break;
            case 'h':
                usage = 1;
                break;
            case 'd':
                dryrun = 1;
                break;            
            case 'v':
                bayer_debug_level = BAYER_LOG_VERBOSE;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if (rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* paths to walk come after the options */
    int numpaths = 0;
    bayer_param_path* paths = NULL;
    if (optind < argc) {
        /* got a path to walk */
        walk = 1;

        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (bayer_param_path*) BAYER_MALLOC((size_t)numpaths * sizeof(bayer_param_path));

        /* process each path */
        char** argpaths = &argv[optind];
        bayer_param_path_set_all(numpaths, argpaths, paths);

        /* advance to next set of options */
        optind += numpaths;

        /* don't allow input file and walk */
        if (inputname != NULL) {
            usage = 1;
        }
    }
    else {
        /* if we're not walking, we must be reading,
         * and for that we need a file */
        if (inputname == NULL) {
            usage = 1;
        }
    }

    /* print usage if we need to */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        bayer_finalize();
        MPI_Finalize();
        return 1;
    }

    /* create an empty file list */
    bayer_flist flist = bayer_flist_new();

    /* get our list of files, either by walking or reading an
     * input file */
    if (walk) {
        /* walk list of input paths */
        bayer_param_path_walk(numpaths, paths, walk_stat, flist, dir_perm);
    }
    else {
        /* read list from file */
        bayer_flist_read_cache(inputname, flist);
    }

    /* assume we'll use the full list */
    bayer_flist srclist = flist;

    /* filter the list if needed */
    bayer_flist filtered_flist = BAYER_FLIST_NULL;
    if (regex_exp != NULL) {
        /* filter the list based on regex */
        filtered_flist = bayer_flist_filter_regex(flist, regex_exp, exclude, name);

        /* update our source list to use the filtered list instead of the original */
        srclist = filtered_flist;
    }

    /* only actually delete files if the user wasn't doing a dry run */
    if (dryrun) {
        /* just print what we would delete without actually doing anything,
         * this is useful if the user is trying to get a regex right */
        bayer_flist_print(srclist);
    } else {
        /* remove files */
        bayer_flist_unlink(srclist);
    }

    /* free list if it was used */
    if (filtered_flist != BAYER_FLIST_NULL){
        /* free the filtered flist (if any) */
        bayer_flist_free(&filtered_flist);
    }

    /* free the file list */
    bayer_flist_free(&flist);

    /* free the path parameters */
    bayer_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    bayer_free(&paths);

    /* free the regex string if we have one */
    bayer_free(&regex_exp);

    /* free the input file name */
    bayer_free(&inputname);

    /* shut down MPI */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
