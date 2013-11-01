#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <time.h>

typedef enum {
    DCMP_LOG_FATAL = 1,
    DCMP_LOG_ERR = 2,
    DCMP_LOG_WARN = 3,
    DCMP_LOG_INFO = 4,
    DCMP_LOG_DBG = 5
} DCMP_loglevel;

#define LOG(level, ...) do {  \
        if (level <= DCMP_debug_level) { \
            char timestamp[256]; \
            time_t ltime = time(NULL); \
            struct tm *ttime = localtime(&ltime); \
            strftime(timestamp, sizeof(timestamp), \
                     "%Y-%m-%dT%H:%M:%S", ttime); \
            if(level == DCMP_LOG_DBG) { \
                fprintf(DCMP_debug_stream,"[%s] [%d] [%s:%d] ", \
                        timestamp, CIRCLE_global_rank, \
                        __FILE__, __LINE__); \
            } else { \
                fprintf(DCMP_debug_stream,"[%s] [%d] [%s:%d] ", \
                        timestamp, CIRCLE_global_rank, \
                        __FILE__, __LINE__); \
            } \
            fprintf(DCMP_debug_stream, __VA_ARGS__); \
            fprintf(DCMP_debug_stream, "\n"); \
            fflush(DCMP_debug_stream); \
        } \
    } while (0)

extern int CIRCLE_global_rank;
extern FILE* DCMP_debug_stream;
extern DCMP_loglevel DCMP_debug_level;

#endif /* LOG_H */
