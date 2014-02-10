#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <dirent.h>
#include <limits.h>

#include "common.h"
#include "pred.h"
#include "queue.h"

void handle_dir(char* path, int depth)
{
    DIR* target;
    struct dirent* entry;

    char child[PATH_MAX];

    execute(path, pred_head);

    if(depth >= options.maxdepth)
    { return; }

    target = opendir(path);

    if(!target) {
        fprintf(stderr, "error accessing %s\n", path);
        return;
    }

    while((entry = readdir(target))) {
        dbprintf("read: %s\n", entry->d_name);

        if(!strcmp(entry->d_name, ".."))
        { continue; }

        if(!strcmp(entry->d_name, "."))
        { continue; }

        if((snprintf(child, PATH_MAX, "%s/%s", path, entry->d_name)) >= PATH_MAX) {
            fprintf(stderr, "filename %s too long\n", entry->d_name);
            continue;
        }

        switch(entry->d_type) {
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

void handle_file(char* fname)
{
    dbprintf("ran on %s\n", fname);
    execute(fname, pred_head);
}
