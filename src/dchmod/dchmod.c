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
#include <ctype.h>

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "mfu.h"

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dchmod [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input   <file>   - read list from file\n");
    printf("  -u, --owner   <name>   - change owner to specified user name\n");
    printf("  -g, --group   <name>   - change group to specified group name\n");
    printf("  -m, --mode    <string> - change mode\n");
    printf("      --exclude <regex>  - exclude a list of files from command\n");
    printf("      --match   <regex>  - match a list of files from command\n");
    printf("  -n, --name             - exclude a list of files from command\n");
    printf("  -v, --verbose          - verbose output\n");
    printf("  -q, --quiet            - quiet output\n");
    printf("  -h, --help             - print usage\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* parse command line options */
    char* inputname = NULL;
    char* ownername = NULL;
    char* groupname = NULL;
    char* modestr   = NULL;
    char* regex_exp = NULL;
    mfu_perms* head = NULL;
    int walk        = 0;
    int exclude     = 0;
    int name        = 0;

    /* verbose by default */
    mfu_debug_level = MFU_LOG_VERBOSE;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",    1, 0, 'i'},
        {"owner",    1, 0, 'u'},
        {"group",    1, 0, 'g'},
        {"mode",     1, 0, 'm'},
        {"exclude",  1, 0, 'e'},
        {"match",    1, 0, 'a'},
        {"name",     0, 0, 'n'},
        {"help",     0, 0, 'h'},
        {"verbose",  0, 0, 'v'},
        {"quiet",    0, 0, 'q'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:g:m:nlhvq",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = MFU_STRDUP(optarg);
                break;
            case 'u':
                ownername = MFU_STRDUP(optarg);
                break;
            case 'g':
                groupname = MFU_STRDUP(optarg);
                break;
            case 'm':
                modestr = MFU_STRDUP(optarg);
                break;
            case 'e':
                regex_exp = MFU_STRDUP(optarg);
                exclude = 1;
                break;
            case 'a':
                regex_exp = MFU_STRDUP(optarg);
                exclude = 0;
                break;
            case 'n':
                name = 1;
                break;
            case 'h':
                usage = 1;
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = MFU_LOG_NONE;
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
    mfu_param_path* paths = NULL;
    if (optind < argc) {
        /* got a path to walk */
        walk = 1;

        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (mfu_param_path*) MFU_MALLOC((size_t)numpaths * sizeof(mfu_param_path));

        /* process each path */
        const char** argpaths = (const char**)(&argv[optind]);
        mfu_param_path_set_all(numpaths, argpaths, paths);

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

    /* check that our mode string parses correctly */
    if (modestr != NULL) {
        int valid = mfu_perms_parse(modestr, &head);
        if (! valid) {
            usage = 1;
            if (rank == 0) {
                printf("invalid mode string: %s\n", modestr);
            }

            /* free the head of the list */
            mfu_perms_free(&head);
        }
    }

    /* print usage if we need to */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    /* flag used to check if permissions need to be
     * set on the walk */
    if (head != NULL) {
        mfu_perms_need_dir_rx(head, walk_opts);
    }

    /* get our list of files, either by walking or reading an
     * input file */
    if (walk) {
        /* if in octal mode set use_stat=0 to stat each file on walk */
        if (head != NULL && head->octal && ownername == NULL && groupname == NULL) {
            walk_opts->use_stat = 0;
        }
        /* walk list of input paths */
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist);
    }
    else {
        /* read list from file */
        mfu_flist_read_cache(inputname, flist);
    }

    /* assume we'll use the full list */
    mfu_flist srclist = flist;

    /* filter the list if needed */
    mfu_flist filtered_flist = MFU_FLIST_NULL;
    if (regex_exp != NULL) {
        /* filter the list based on regex */
        filtered_flist = mfu_flist_filter_regex(flist, regex_exp, exclude, name);

        /* update our source list to use the filtered list instead of the original */
        srclist = filtered_flist;
    }
   
    /* change group and permissions */
    mfu_flist_chmod(srclist, ownername, groupname, head);
   
    /* free list if it was used */
    if (filtered_flist != MFU_FLIST_NULL){
        /* free the filtered flist (if any) */
        mfu_flist_free(&filtered_flist);
    }

    /* free the file list */
    mfu_flist_free(&flist);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free the owner and group names */
    mfu_free(&ownername);
    mfu_free(&groupname);

    /* free the modestr */
    mfu_free(&modestr);

    /* free the match_pattern if it isn't null */
    if (regex_exp != NULL) {
        mfu_free(&regex_exp);
    }

    /* free the head of the list */
    mfu_perms_free(&head);

    /* free the input file name */
    mfu_free(&inputname);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
