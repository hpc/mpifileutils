#ifndef LOG_H
#define LOG_H

#include <stdio.h>

typedef enum
{
    DGREP_LOG_FATAL = 1,
    DGREP_LOG_ERR   = 2,
    DGREP_LOG_WARN  = 3,
    DGREP_LOG_INFO  = 4,
    DGREP_LOG_DBG   = 5
} DGREP_loglevel;

#define LOG(level, ...) do {  \
        if (level <= DGREP_debug_level) { \
            fprintf(DGREP_debug_stream,"%d:%s:%d:", DGREP_global_rank, __FILE__, __LINE__); \
            fprintf(DGREP_debug_stream, __VA_ARGS__); \
            fprintf(DGREP_debug_stream, "\n"); \
            fflush(DGREP_debug_stream); \
        } \
    } while (0)

extern int DGREP_global_rank;
extern FILE *DGREP_debug_stream;
extern DGREP_loglevel DGREP_debug_level;

#endif /* LOG_H */
