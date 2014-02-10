#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>

#include <libcircle.h>

#include "common.h"
#include "pred.h"
#include "queue.h"

int main(int argc, char** argv)
{
    int ch;

    static struct option longopts[] = {
        { "exec", required_argument, NULL, 'e' },
        { "maxdepth", required_argument, NULL, 'm' },
        { "name", required_argument, NULL, 'n' },
        { "newer", required_argument, NULL, 'N' },
        { "type", required_argument, NULL, 't' },
        { "print", no_argument, NULL, 'p' },
        { NULL, 0, NULL, 0 },
    };

    options.maxdepth = INT_MAX;

    while((ch = getopt_long_only(argc, argv, "t:", longopts, NULL)) != -1) {
        switch(ch) {
            case 'e': {
                int space = sysconf(_SC_ARG_MAX);
                char* buf = (char*)malloc(space);

                for(int i = optind - 1; strcmp(";", argv[i]); i++) {
                    if(i > argc) {
                        printf("%s: exec missing terminating ';'\n", argv[0]);
                        exit(1);
                    }

                    strncat(buf, argv[i], space);
                    space -= strlen(argv[i]) + 1; /* save room for space or null */

                    if(space <= 0) {
                        printf("%s: exec argument list too long.\n", argv[0]);
                        free(buf);
                        continue;
                    }

                    strcat(buf, " ");
                    optind++;
                }

                buf[strlen(buf)] = '\0'; /* clobbers trailing space */
                pred_add(pred_exec, buf);
            }
            break;

            case 'm':
                options.maxdepth = atoi(optarg);
                break;

            case 'n':
                pred_add(name, strdup(optarg));
                break;

            case 'N': {
                struct stat statbuf;

                if((stat(optarg, &statbuf)) != 0) {
                    printf("%s: can't find file %s\n", argv[0], optarg);
                    exit(1);
                }

                pred_add(pred_newer, (void*)(statbuf.st_mtimespec.tv_sec));
            }
            break;

            case 'p':
                pred_add(pred_print, NULL);
                break;

            case 't':
                switch(*optarg) {
                    case 'b':
                        pred_add(pred_type, (void*)S_IFBLK);
                        break;

                    case 'c':
                        pred_add(pred_type, (void*)S_IFCHR);
                        break;

                    case 'd':
                        pred_add(pred_type, (void*)S_IFDIR);
                        break;

                    case 'f':
                        pred_add(pred_type, (void*)S_IFREG);
                        break;

                    case 'l':
                        pred_add(pred_type, (void*)S_IFLNK);
                        break;

                    case 'p':
                        pred_add(pred_type, (void*)S_IFIFO);
                        break;

                    case 's':
                        pred_add(pred_type, (void*)S_IFSOCK);
                        break;

                    default:
                        printf("%s: unsupported file type %s\n", argv[0], optarg);
                        exit(1);
                        break;
                }

                break;

            default:
                printf("option error\n");
                exit(1);
                break;
        }
    }

    pred_commit();

    CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(queue_head);
    CIRCLE_cb_process(dequeue);

    options.root = argv[optind] ? argv[optind] : ".";

    CIRCLE_begin();
    CIRCLE_finalize();

    return 0;
}
