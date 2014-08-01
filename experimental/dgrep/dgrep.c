#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <ctype.h>

#include "dgrep.h"
#include "log.h"

int DGREP_global_rank;
FILE *DGREP_debug_stream;
DGREP_loglevel DGREP_debug_level;

char *DGREP_PATH;
char *DGREP_ARGS;

void
DGREP_start(CIRCLE_handle *handle)
{
    handle->enqueue(DGREP_PATH);
}

void
DGREP_search(CIRCLE_handle *handle)
{
    DIR *current_dir;

    char temp[CIRCLE_MAX_STRING_LEN];
    char stat_temp[CIRCLE_MAX_STRING_LEN];

    struct dirent *current_ent;
    struct stat st;

    /* Pop an item off the queue */
    handle->dequeue(temp);

    /* Try and stat it, checking to see if it is a link */
    if(lstat(temp,&st) != EXIT_SUCCESS)
    {
        LOG(DGREP_LOG_ERR, "Error: Couldn't stat \"%s\"", temp);
        return;
    }
    /* Check to see if it is a directory.  If so, put its children in the queue */
    else if(S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
    {
        current_dir = opendir(temp);

        if(!current_dir)
        {
            LOG(DGREP_LOG_ERR, "Unable to open dir");
        }
        else
        {
            /* Read in each directory entry */
            while((current_ent = readdir(current_dir)) != NULL)
            {
            /* We don't care about . or .. */
            if((strncmp(current_ent->d_name,".",2)) && (strncmp(current_ent->d_name,"..",3)))
                {
                    strcpy(stat_temp,temp);
                    strcat(stat_temp,"/");
                    strcat(stat_temp,current_ent->d_name);

                    handle->enqueue(&stat_temp[0]);
                }
            }
        }
        closedir(current_dir);
    }
    else if(S_ISREG(st.st_mode)) {
        FILE *fp;
        char cmd[CIRCLE_MAX_STRING_LEN + 10];
        char out[1035];

        sprintf(cmd, "grep %s %s", DGREP_ARGS, temp);

        fp = popen(cmd, "r");
        if (fp == NULL) {
            printf("Failed to run command\n" );
        }

        while (fgets(out, sizeof(out) - 1, fp) != NULL) {
            printf("%s", out);
        }

        pclose(fp);
    }
}

void
print_usage(char *prog)
{
    fprintf(stdout, "Usage: %s -p <path> -a <pattern>\n", prog);
}

int
main (int argc, char **argv)
{
    int index;
    int c;

    int args_flag = 0;
    int path_flag = 0;

    DGREP_debug_stream = stdout;
    DGREP_debug_level  = DGREP_LOG_DBG;
     
    opterr = 0;
    while((c = getopt(argc, argv, "p:a:")) != -1)
    {
        switch(c)
        {
            case 'a':
                DGREP_ARGS = optarg;
                args_flag = 1;
                break;
            case 'p':
                DGREP_PATH = realpath(optarg, NULL);
                path_flag = 1;
                break;
            case '?':
                if (optopt == 'p' || optopt == 'a')
                    fprintf(stderr, "Error: Option -%c requires an argument.\n", optopt);
                else if (isprint (optopt))
                    fprintf(stderr, "Error: Unknown option `-%c'.\n", optopt);
                else
                    fprintf(stderr,
                        "Error: Unknown option character `\\x%x'.\n",
                        optopt);

                exit(EXIT_FAILURE);
            default:
                abort();
        }
    }

    for (index = optind; index < argc; index++)
    {
        print_usage(argv[0]);
        fprintf(stdout, "Error: Non-option argument %s\n", argv[index]);

        exit(EXIT_FAILURE);
    }

    if(path_flag == 0)
    {
        print_usage(argv[0]);
        fprintf(stdout, "Error: You must specify a starting path name.\n");

        exit(EXIT_FAILURE);
    }

    if(args_flag == 0)
    {
        print_usage(argv[0]);
        fprintf(stdout, "Error: You must specify a pattern to search on.\n");

        exit(EXIT_FAILURE);
    }

    DGREP_global_rank = CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);

    CIRCLE_cb_create (&DGREP_start);
    CIRCLE_cb_process(&DGREP_search);

    CIRCLE_begin();
    CIRCLE_finalize();

    exit(EXIT_SUCCESS);
}

/* EOF */
