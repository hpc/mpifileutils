#ifndef _DPARALLEL_LOG_H
#define _DPARALLEL_LOG_H

#include "dparallel.h"

#include <stdio.h>
#include <stdint.h>
#include <time.h>

typedef enum DPARALLEL_loglevel { DPARALLEL_LOG_FATAL = 1,
                                  DPARALLEL_LOG_ERR   = 2,
                                  DPARALLEL_LOG_WARN  = 3,
                                  DPARALLEL_LOG_INFO  = 4,
                                  DPARALLEL_LOG_DBG   = 5
                                } DPARALLEL_loglevel;

#define LOG(level, ...) do {  \
        if (level <= DPARALLEL_debug_level) { \
            fprintf(DPARALLEL_debug_stream, "%d:%d:%s:%d:", (int)time(NULL), \
                    DPARALLEL_global_rank, __FILE__, __LINE__); \
            fprintf(DPARALLEL_debug_stream, __VA_ARGS__); \
            fprintf(DPARALLEL_debug_stream, "\n"); \
            fflush(DPARALLEL_debug_stream); \
        } \
    } while (0)

extern FILE* DPARALLEL_debug_stream;
extern enum DPARALLEL_loglevel DPARALLEL_debug_level;
extern int32_t DPARALLEL_global_rank;

#endif /* _DPARALLEL_LOG_H */
