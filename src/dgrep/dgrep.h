#ifndef DGREP_H
#define DGREP_H

#include <libcircle.h>

void DGREP_start(CIRCLE_handle *handle);
void DGREP_search(CIRCLE_handle *handle);

void print_usage(char *prog);

#endif /* DGREP_H */
