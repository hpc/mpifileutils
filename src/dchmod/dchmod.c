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
static int walk_stat = 1;

/*****************************
 * Driver functions
 ****************************/

static int lookup_gid(const char* name, gid_t* gid)
{
    uint64_t values[2];

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* have rank 0 lookup the group id */
    if (rank == 0) {
        /* lookup specified group name */
        errno = 0;
        struct group* gr = getgrnam(name);
        if (gr != NULL) {
            /* lookup succeeded */
            values[0] = 1;
            values[1] = (uint64_t) gr->gr_gid;
        } else {
            /* indicate that lookup failed */
            values[0] = 0;

            /* print error message if we can */
            if (errno != 0) {
                BAYER_LOG(BAYER_LOG_ERR, "Failed to find entry for group %s errno=%d %s",
                    name, errno, strerror(errno)
                   );
            }
        }
    }

    /* broadcast result from lookup */
    MPI_Bcast(values, 2, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* set the group id */
    int rc = (int) values[0];
    if (values[0] == 1) {
        *gid = (gid_t) values[1];
    }

    return rc;
}

static void flist_chmod(
    bayer_flist flist,
    const char* grname)
{
    /* lookup groupid if set, bail out if not */
    gid_t gid;
    if (grname != NULL) {
        int lookup_rc = lookup_gid(grname, &gid);
        if (lookup_rc == 0) {
            /* failed to get group id, bail out */
            return;
        }
    }

    /* each process directly removes its elements */
    uint64_t idx;
    uint64_t size = bayer_flist_size(flist);
    for (idx = 0; idx < size; idx++) {
        /* get file name */
        const char* dest_path = bayer_flist_file_get_name(flist, idx);

        /* update group if user gave a group name */
        if (grname != NULL) {
            /* get user id and group id of file */
            uid_t uid = (uid_t) bayer_flist_file_get_uid(flist, idx);
            gid_t oldgid = (gid_t) bayer_flist_file_get_gid(flist, idx);

            /* only bother to change group if it's different */
            if (oldgid != gid) {
                /* note that we use lchown to change ownership of link itself, it path happens to be a link */
                if(bayer_lchown(dest_path, uid, gid) != 0) {
                    /* TODO: are there other EPERM conditions we do want to report? */

                    /* since the user running dcp may not be the owner of the
                     * file, we could hit an EPERM error here, and the file
                     * will be left with the effective uid and gid of the dcp
                     * process, don't bother reporting an error for that case */
                    if (errno != EPERM) {
                        BAYER_LOG(BAYER_LOG_ERR, "Failed to change ownership on %s lchown() errno=%d %s",
                            dest_path, errno, strerror(errno)
                           );
                    }
                }
            }
        }

        /* get mode and type */
        bayer_filetype type = bayer_flist_file_get_type(flist, idx);
        mode_t mode = (mode_t) bayer_flist_file_get_mode(flist, idx);

        /* change mode, unless item is a link */
        if(type != BAYER_TYPE_LINK) {
            /* flip on group read bit, if user bit is set */
            if(mode & S_IRUSR) {
                mode |= S_IRGRP;
            }

            /* flip on group execute bit, if user execute bit is set */
            if(mode & S_IXUSR) {
                mode |= S_IXGRP;
            }

            /* set the mode on the file */
            if(bayer_chmod(dest_path, mode) != 0) {
                BAYER_LOG(BAYER_LOG_ERR, "Failed to change permissions on %s chmod() errno=%d %s",
                    dest_path, errno, strerror(errno)
                   );
            }
        }
    }

    return;
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dchmod [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file>  - read list from file\n");
    printf("  -g, --group <name>  - change group to specified group name\n");
    printf("  -v, --verbose       - verbose output\n");
    printf("  -h, --help          - print usage\n");
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
    char* groupname = NULL;
    int walk = 0;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",    1, 0, 'i'},
        {"group",    1, 0, 'g'},
        {"help",     0, 0, 'h'},
        {"verbose",  0, 0, 'v'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:g:lhv",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = BAYER_STRDUP(optarg);
                break;
            case 'g':
                groupname = BAYER_STRDUP(optarg);
                break;
            case 'h':
                usage = 1;
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
        bayer_param_path_walk(numpaths, paths, walk_stat, flist);
    }
    else {
        /* read list from file */
        bayer_flist_read_cache(inputname, flist);
    }

    /* change group and permissions */
    flist_chmod(flist, groupname);

    /* free the file list */
    bayer_flist_free(&flist);

    /* free the path parameters */
    bayer_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    bayer_free(&paths);

    /* free the group name */
    bayer_free(&groupname);

    /* free the input file name */
    bayer_free(&inputname);

    /* shut down MPI */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
