#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>

#include <regex.h>

#include "mpi.h"

#include "mfu.h"
#include "common.h"

int MFU_PRED_EXEC  (mfu_flist flist, uint64_t idx, void* arg);
int MFU_PRED_PRINT (mfu_flist flist, uint64_t idx, void* arg);

int MFU_PRED_EXEC (mfu_flist flist, uint64_t idx, void* arg)
{
    int argmax = 1024*1024;;
    int written = 0;
    int ret;
    char* command = MFU_STRDUP((char*) arg);
    char* cmdline = (char*) MFU_MALLOC(argmax);
    char* subst = strstr(command, "{}");

    if (subst) {
        subst[0] = '\0';
        subst += 2; /* Point to the first char after '{}' */
    }

    const char* name = mfu_flist_file_get_name(flist, idx);

    written = snprintf(cmdline, argmax/sizeof(char), "%s%s%s", command, name, subst);
    if (written > argmax/sizeof(char)) {
        fprintf(stderr, "argument %s to exec too long.\n", cmdline);
        mfu_free(&cmdline);
        mfu_free(&command);
        return -1;
    }

    ret = system(cmdline);

    mfu_free(&cmdline);
    mfu_free(&command);

    return ret ? 0 : 1;
}

int MFU_PRED_PRINT (mfu_flist flist, uint64_t idx, void* arg)
{
    const char* name = mfu_flist_file_get_name(flist, idx);
    printf("%s\n", name);
    return 1;
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dfind [options] <path> EXPRESSIONS...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file>                      - read list from file\n");
    printf("  -o, --output <file>                     - write processed list to file\n");
    printf("  -v, --verbose                           - verbose output\n");
    printf("  -q, --quiet                             - quiet output\n");
    printf("  -h, --help                              - print usage\n");
    printf("\n");
    printf("Tests:\n");
    printf("  --amin N       - last accessed N minutes ago\n");
    printf("  --anewer FILE  - last accessed more recently than FILE modified\n");
    printf("  --atime N      - last accessed N days ago\n");
    printf("  --cmin N       - status last changed N minutes ago\n");
    printf("  --cnewer FILE  - status last changed more recently than FILE modified\n");
    printf("  --ctime N      - status last changed N days ago\n");
    printf("  --mmin N       - data last modified N minutes ago\n");
    printf("  --newer FILE   - modified more recently than FILE\n");
    printf("  --mtime N      - data last modified N days ago\n");
    printf("\n");
    printf("  --gid N        - numeric group ID is N\n");
    printf("  --group NAME   - belongs to group NAME\n");
    printf("  --uid N        - numeric user ID is N\n");
    printf("  --user NAME    - owned by user NAME\n");
    printf("\n");
    printf("  --name PATTERN - base name matches shell pattern PATTERN\n");
    printf("  --path PATTERN - full path matches shell pattern PATTERN\n");
    printf("  --regex REGEX  - full path matches POSIX regex REGEX\n");
    printf("\n");
    printf("  --size N       - size is N bytes.  Supports attached units like KB, MB, GB\n");
    printf("  --type C       - of type C: d=dir, f=file, l=symlink\n");
    printf("\n");
    printf("  Tests with N can use -N (less than N), N (exactly N), +N (more than N)\n");
    printf("\n");
    printf("Actions:\n");
    printf("  --print        - print item name to stdout\n");
    printf("  --exec CMD ;   - execute CMD on item\n");
    printf("\n");
    fflush(stdout);
    return;
}

/* apply predicate tests and actions to matching items in flist */
static void mfu_flist_pred(mfu_flist flist, mfu_pred* p)
{
    uint64_t idx;
    uint64_t size = mfu_flist_size(flist);
    for (idx = 0; idx < size; idx++) {
        mfu_pred_execute(flist, idx, p);
    }
    return;
}

/* look up mtimes for specified file,
 * return secs/nsecs in newly allocated mfu_pred_times struct,
 * return NULL on error */
static mfu_pred_times* get_mtimes(const char* file)
{
    mfu_param_path param_path;
    mfu_param_path_set(file, &param_path);
    if (! param_path.path_stat_valid) {
        return NULL;
    }
    mfu_pred_times* t = (mfu_pred_times*) MFU_MALLOC(sizeof(mfu_pred_times));
    mfu_stat_get_mtimes(&param_path.path_stat, &t->secs, &t->nsecs);
    mfu_param_path_free(&param_path);
    return t;
}

static int add_type(mfu_pred* p, char t)
{
    mode_t* type = (mode_t*) MFU_MALLOC(sizeof(mode_t));
    switch (t) {
    case 'b':
        *type = S_IFBLK;
        break;
    case 'c':
        *type = S_IFCHR;
        break;
    case 'd':
        *type = S_IFDIR;
        break;
    case 'f':
        *type = S_IFREG;
        break;
    case 'l':
        *type = S_IFLNK;
        break;
    case 'p':
        *type = S_IFIFO;
        break;
    case 's':
        *type = S_IFSOCK;
        break;

    default:
        /* unsupported type character */
        mfu_free(&type);
        return -1;
        break;
    }

    /* add check for this type */
    mfu_pred_add(p, MFU_PRED_TYPE, (void *)type);
    return 1;
}

static void pred_commit (mfu_pred* p)
{
    int need_print = 1;

    mfu_pred* cur = p;
    while (cur) {
        if (cur->f == MFU_PRED_PRINT || cur->f == MFU_PRED_EXEC) {
            need_print = 0;
            break;
        }
        cur = cur->next;
    }

    if (need_print) {
//        mfu_pred_add(p, MFU_PRED_PRINT, NULL);
    }
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

    /* pointer to mfu_walk_opts */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* capture current time for any time based queries,
     * to get a consistent value, capture and bcast from rank 0 */
    mfu_pred_times* now_t = mfu_pred_now();

    int ch;

    mfu_pred* pred_head = mfu_pred_new();
    char* inputname  = NULL;
    char* outputname = NULL;
    int walk = 0;
    int text = 0;

    static struct option long_options[] = {
        {"input",     1, 0, 'i'},
        {"output",    1, 0, 'o'},
        {"verbose",   0, 0, 'v'},
        {"quiet",     0, 0, 'q'},
        {"help",      0, 0, 'h'},

        { "maxdepth", required_argument, NULL, 'd' },

        { "amin",     required_argument, NULL, 'a' },
        { "anewer",   required_argument, NULL, 'B' },
        { "atime",    required_argument, NULL, 'A' },
        { "cmin",     required_argument, NULL, 'c' },
        { "cnewer",   required_argument, NULL, 'D' },
        { "ctime",    required_argument, NULL, 'C' },
        { "mmin",     required_argument, NULL, 'm' },
        { "newer",    required_argument, NULL, 'N' },
        { "mtime",    required_argument, NULL, 'M' },

        { "gid",      required_argument, NULL, 'g' },
        { "group",    required_argument, NULL, 'G' },
        { "uid",      required_argument, NULL, 'u' },
        { "user",     required_argument, NULL, 'U' },

        { "name",     required_argument, NULL, 'n' },
        { "path",     required_argument, NULL, 'P' },
        { "regex",    required_argument, NULL, 'r' },
        { "size",     required_argument, NULL, 's' },
        { "type",     required_argument, NULL, 't' },

        { "print",    no_argument,       NULL, 'p' },
        { "exec",     required_argument, NULL, 'e' },
        { NULL, 0, NULL, 0 },
    };

    options.maxdepth = INT_MAX;

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:o:vqh",
                    long_options, NULL
                );

        if (c == -1) {
            break;
        }

        int i;
        int space;
        char* buf;
        mfu_pred_times* t;
        mfu_pred_times_rel* tr;
        regex_t* r;
        int ret;

        /* verbose by default */
        mfu_debug_level = MFU_LOG_VERBOSE;

    	switch (c) {
    	case 'e':
            space = 1024 * 1024;
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
    	    mfu_pred_add(pred_head, MFU_PRED_EXEC, buf);
    	    break;

    	case 'd':
    	    options.maxdepth = atoi(optarg);
    	    break;

    	case 'g':
            /* TODO: error check argument */
    	    buf = MFU_STRDUP(optarg);
    	    mfu_pred_add(pred_head, MFU_PRED_GID, (void *)buf);
    	    break;

    	case 'G':
    	    buf = MFU_STRDUP(optarg);
    	    mfu_pred_add(pred_head, MFU_PRED_GROUP, (void *)buf);
    	    break;

    	case 'u':
            /* TODO: error check argument */
    	    buf = MFU_STRDUP(optarg);
    	    mfu_pred_add(pred_head, MFU_PRED_UID, (void *)buf);
    	    break;

    	case 'U':
    	    buf = MFU_STRDUP(optarg);
    	    mfu_pred_add(pred_head, MFU_PRED_USER, (void *)buf);
    	    break;

    	case 's':
    	    buf = MFU_STRDUP(optarg);
    	    mfu_pred_add(pred_head, MFU_PRED_SIZE, (void *)buf);
    	    break;

    	case 'n':
    	    mfu_pred_add(pred_head, MFU_PRED_NAME, MFU_STRDUP(optarg));
    	    break;
    	case 'P':
    	    mfu_pred_add(pred_head, MFU_PRED_PATH, MFU_STRDUP(optarg));
    	    break;
    	case 'r':
            r = (regex_t*) MFU_MALLOC(sizeof(regex_t));
            ret = regcomp(r, optarg, 0);
            if (ret) {
                MFU_ABORT(-1, "Could not compile regex: `%s' rc=%d\n", optarg, ret);
            }
    	    mfu_pred_add(pred_head, MFU_PRED_REGEX, (void*)r);
    	    break;

    	case 'a':
            tr = mfu_pred_relative(optarg, now_t);
    	    mfu_pred_add(pred_head, MFU_PRED_AMIN, (void *)tr);
    	    break;
    	case 'm':
            tr = mfu_pred_relative(optarg, now_t);
    	    mfu_pred_add(pred_head, MFU_PRED_MMIN, (void *)tr);
    	    break;
    	case 'c':
            tr = mfu_pred_relative(optarg, now_t);
    	    mfu_pred_add(pred_head, MFU_PRED_CMIN, (void *)tr);
    	    break;

    	case 'A':
            tr = mfu_pred_relative(optarg, now_t);
    	    mfu_pred_add(pred_head, MFU_PRED_ATIME, (void *)tr);
    	    break;
    	case 'M':
            tr = mfu_pred_relative(optarg, now_t);
    	    mfu_pred_add(pred_head, MFU_PRED_MTIME, (void *)tr);
    	    break;
    	case 'C':
            tr = mfu_pred_relative(optarg, now_t);
    	    mfu_pred_add(pred_head, MFU_PRED_CTIME, (void *)tr);
    	    break;

    	case 'B':
            t = get_mtimes(optarg);
            if (t == NULL) {
                if (rank == 0) {
    	            printf("%s: can't find file %s\n", argv[0], optarg);
                }
    	        exit(1);
    	    }
    	    mfu_pred_add(pred_head, MFU_PRED_ANEWER, (void *)t);
    	    break;
    	case 'N':
            t = get_mtimes(optarg);
            if (t == NULL) {
                if (rank == 0) {
    	            printf("%s: can't find file %s\n", argv[0], optarg);
                }
    	        exit(1);
    	    }
    	    mfu_pred_add(pred_head, MFU_PRED_MNEWER, (void *)t);
    	    break;
    	case 'D':
            t = get_mtimes(optarg);
            if (t == NULL) {
                if (rank == 0) {
    	            printf("%s: can't find file %s\n", argv[0], optarg);
                }
    	        exit(1);
    	    }
    	    mfu_pred_add(pred_head, MFU_PRED_CNEWER, (void *)t);
    	    break;

    	case 'p':
    	    mfu_pred_add(pred_head, MFU_PRED_PRINT, NULL);
    	    break;

    	case 't':
            ret = add_type(pred_head, *optarg);
            if (ret != 1) {
                if (rank == 0) {
    	            printf("%s: unsupported file type %s\n", argv[0], optarg);
                }
    	        exit(1);
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
        case 'q':
            mfu_debug_level = MFU_LOG_NONE;
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

    pred_commit(pred_head);

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
        mfu_flist_walk_param_paths(numpaths, paths, walk_opts, flist);
    }
    else {
        /* read data from cache file */
        mfu_flist_read_cache(inputname, flist);
    }

    /* apply predicates to each item in list */
    mfu_flist flist2 = mfu_flist_filter_pred(flist, pred_head);

    /* write data to cache file */
    if (outputname != NULL) {
        if (!text) {
            mfu_flist_write_cache(outputname, flist2);
        } else {
            mfu_flist_write_text(outputname, flist2);
        }
    }

    /* free off the filtered list */
    mfu_flist_free(&flist2);

    /* free users, groups, and files objects */
    mfu_flist_free(&flist);

    /* free predicate list */
    mfu_pred_free(&pred_head);

    /* free memory allocated for options */
    mfu_free(&outputname);
    mfu_free(&inputname);

    /* free the path parameters */
    mfu_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    /* free structure holding current time */
    mfu_free(&now_t);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    return 0;
}
