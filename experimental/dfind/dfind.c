#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>

#include "mpi.h"

#include "mfu.h"
#include "common.h"
#include "pred.h"

/* TODO: change globals to struct */
static int walk_stat = 1;
static int dir_perm = 0;

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dfind [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file>                      - read list from file\n");
    printf("  -o, --output <file>                     - write processed list to file\n");
    printf("  -v, --verbose                           - verbose output\n");
    printf("  -h, --help                              - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

static void mfu_flist_pred(mfu_flist flist, pred_item* p)
{
    uint64_t idx;
    uint64_t size = mfu_flist_size(flist);
    for (idx = 0; idx < size; idx++) {
        const char* name = mfu_flist_file_get_name(flist, idx);
        execute(flist, idx, p);
    }
    return;
}

static mfu_flist mfu_flist_filter_pred(mfu_flist flist, pred_item* p)
{
    /* create a new list to copy matching items */
    mfu_flist list = mfu_flist_subset(flist);

    /* get size of input list */
    uint64_t size = mfu_flist_size(flist);

    /* iterate over each item in input list */
    uint64_t idx;
    for (idx = 0; idx < size; idx++) {
        /* get path to this item */
        const char* name = mfu_flist_file_get_name(flist, idx);

        /* run string of predicates against item */
        int ret = execute(flist, idx, p);
        if (ret == 0) {
            /* copy item into new list if all predicates pass */
            mfu_flist_file_copy(flist, idx, list);
        }
    }

    return list;
}

int main (int argc, char** argv)
{
    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    int ch;

    char* inputname  = NULL;
    char* outputname = NULL;
    int walk = 0;
    int text = 1;

    static struct option long_options[] = {
        {"input",     1, 0, 'i'},
        {"output",    1, 0, 'o'},
        {"verbose",   0, 0, 'v'},
        {"help",      0, 0, 'h'},

        { "maxdepth", required_argument, NULL, 'm' },
        { "gid",      required_argument, NULL, 'g' },
        { "group",    required_argument, NULL, 'G' },
        { "uid",      required_argument, NULL, 'u' },
        { "user",     required_argument, NULL, 'U' },
        { "size",     required_argument, NULL, 's' },
        { "name",     required_argument, NULL, 'n' },
        { "newer",    required_argument, NULL, 'N' },
        { "type",     required_argument, NULL, 't' },
        { "print",    no_argument,       NULL, 'p' },
        { "exec",     required_argument, NULL, 'e' },
        { NULL, 0, NULL, 0 },
    };

    options.maxdepth = INT_MAX;
    
    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:o:t:",
                    long_options, NULL
                );

        if (c == -1) {
            break;
        }

        int i;
        int space;
        char* buf;
        mfu_param_path param_path;
    	switch (c) {
    	case 'e':
    	    space = sysconf(_SC_ARG_MAX);
    	    buf = (char *)MFU_MALLOC(space);
    
    	    for (i = optind-1; strcmp(";", argv[i]); i++) {
    	        if (i > argc) {
                    if (rank == 0) {
    	                printf("%s: exec missing terminating ';'\n", argv[0]);
                    }
    	            exit(1);
    	        }
    	        strncat(buf, argv[i], space);
    	        space -= strlen(argv[i]) + 1; /* save room for space or null */
    	        if (space <= 0) {
                    if (rank == 0) {
    	                printf("%s: exec argument list too long.\n", argv[0]);
                    }
    	            mfu_free(&buf);
    	            continue;
    	        }
    	        strcat(buf, " ");
    	        optind++;
    	    }
    	    buf[strlen(buf)] = '\0'; /* clobbers trailing space */
    	    pred_add(pred_exec, buf);
    	    break;
    
    	case 'm':
    	    options.maxdepth = atoi(optarg);
    	    break;

    	case 'g':
            /* TODO: error check argument */
    	    buf = MFU_STRDUP(optarg);
    	    pred_add(pred_gid, (void *)buf);
    	    break;

    	case 'G':
    	    buf = MFU_STRDUP(optarg);
    	    pred_add(pred_group, (void *)buf);
    	    break;

    	case 'u':
            /* TODO: error check argument */
    	    buf = MFU_STRDUP(optarg);
    	    pred_add(pred_uid, (void *)buf);
    	    break;

    	case 'U':
    	    buf = MFU_STRDUP(optarg);
    	    pred_add(pred_user, (void *)buf);
    	    break;

    	case 's':
    	    buf = MFU_STRDUP(optarg);
    	    pred_add(pred_size, (void *)buf);
    	    break;

    	case 'n':
    	    pred_add(pred_name, MFU_STRDUP(optarg));
    	    break;
    
    	case 'N':
            mfu_param_path_set(optarg, &param_path);
            if (! param_path.path_stat_valid) {
                if (rank == 0) {
    	            printf("%s: can't find file %s\n", argv[0], optarg);
                }
    	        exit(1);
    	    }
    	    pred_add(pred_newer, (void *)(param_path.path_stat.st_mtime));
            mfu_param_path_free(&param_path);
    	    break;
    
    	case 'p':
    	    pred_add(pred_print, NULL);
    	    break;
    
    	case 't':
    	    switch (*optarg) {
    	        case 'b': pred_add(pred_type, (void *)S_IFBLK); break;
    	        case 'c': pred_add(pred_type, (void *)S_IFCHR); break;
    	        case 'd': pred_add(pred_type, (void *)S_IFDIR); break;
    	        case 'f': pred_add(pred_type, (void *)S_IFREG); break;
    	        case 'l': pred_add(pred_type, (void *)S_IFLNK); break;
    	        case 'p': pred_add(pred_type, (void *)S_IFIFO); break;
    	        case 's': pred_add(pred_type, (void *)S_IFSOCK); break;
    
    	        default:
                    if (rank == 0) {
    	                printf("%s: unsupported file type %s\n", argv[0], optarg);
                    }
    	            exit(1);
    	            break;
    	    }
    	    break;
    
        case 'i':
            inputname = MFU_STRDUP(optarg);
            break;
        case 'o':
            outputname = MFU_STRDUP(optarg);
            break;
        case 'v':
            mfu_debug_level = MFU_LOG_VERBOSE;
            break;
        case 'h':
            usage = 1;
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
    
    pred_commit();

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
        char** p = &argv[optind];
        mfu_param_path_set_all((uint64_t)numpaths, (const char**)p, paths);
        optind += numpaths;

        /* don't allow user to specify input file with walk */
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
    
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }


    /* create an empty file list */
    mfu_flist flist = mfu_flist_new();

    if (walk) {
        /* walk list of input paths */
        mfu_flist_walk_param_paths(numpaths, paths, walk_stat, dir_perm, flist);
    }
    else {
        /* read data from cache file */
        mfu_flist_read_cache(inputname, flist);
    }

    /* apply predicates to each item in list */
    //mfu_flist_pred(flist, pred_head);
    mfu_flist flist2 = mfu_flist_filter_pred(flist, pred_head);
    mfu_flist_print(flist2);

    /* write data to cache file */
    if (outputname != NULL) {
        if (!text) {
            mfu_flist_write_cache(outputname, flist2);
        } else {
            mfu_flist_write_text(outputname, flist2);
        }
    }

    mfu_flist_free(&flist2);

    /* free users, groups, and files objects */
    mfu_flist_free(&flist);

    /* free memory allocated for options */
    mfu_free(&outputname);
    mfu_free(&inputname);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
