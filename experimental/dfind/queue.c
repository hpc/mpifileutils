#include <stdio.h>
#include <stdlib.h>
#include <libcircle.h>
#include <string.h>

#include "common.h"
#include "handle.h"
#include "queue.h"

void queue_dir(char* dirname, int depth)
{
    char* buf = (char*)malloc(CIRCLE_MAX_STRING_LEN * sizeof(char));
    int len;

    len = snprintf(buf, CIRCLE_MAX_STRING_LEN, "D:%d:%s", depth, dirname);

    if(len >= CIRCLE_MAX_STRING_LEN) {
        fprintf(stderr, "%s: directory name too long\n", dirname);
        return;
    }

    dbprintf("queue %s\n", buf);

    CIRCLE_get_handle()->enqueue(buf);
    return;
}

void queue_file(char* fname)
{
    char* buf = (char*)malloc(CIRCLE_MAX_STRING_LEN * sizeof(char));
    int len;

    len = snprintf(buf, CIRCLE_MAX_STRING_LEN, "F:%s", fname);

    if(len >= CIRCLE_MAX_STRING_LEN) {
        fprintf(stderr, "%s: file name too long\n", fname);
        return;
    }

    dbprintf("queue %s\n", buf);

    CIRCLE_get_handle()->enqueue(buf);
    return;
}

void dequeue(CIRCLE_handle* handle)
{
    char buf[CIRCLE_MAX_STRING_LEN];
    char* p;
    int depth;

    handle->dequeue(buf);

    switch(buf[0]) {
        case 'D':
            depth = atoi(&buf[2]);
            p = strchr(&buf[2], ':');
            handle_dir(p + 1, depth);
            break;

        case 'F':
            handle_file(&buf[2]);
            break;

        default:
            fprintf(stderr, "bad buffer %s\n", buf);
            return;
            break;
    }

    return;
}

void queue_head(CIRCLE_handle* handle)
{
    dbprintf("start: %s\n", options.root);
    queue_dir(options.root, 0);
}
