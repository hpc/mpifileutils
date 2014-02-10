#ifndef _QUEUE_H
#define _QUEUE_H

#include <libcircle.h>

#include "common.h"

void queue_dir(char*, int);
void queue_file(char*);
void queue_head(CIRCLE_handle*);

void dequeue(CIRCLE_handle*);

#endif
