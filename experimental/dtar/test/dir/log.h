#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <time.h>

typedef enum {
    DTAR_LOG_FATAL = 1,
    DTAR_LOG_ERR   = 2,
    DTAR_LOG_WARN  = 3,
    DTAR_LOG_INFO  = 4,
    DTAR_LOG_DBG   = 5
} DTAR_loglevel;

//                fprintf(DTAR_debug_stream,"[%s] ", timestamp);
#define LOG(level, ...) do {  \
        if (level <= DTAR_debug_level) { \
            char timestamp[256]; \
            time_t ltime = time(NULL); \
            struct tm *ttime = localtime(&ltime); \
            strftime(timestamp, sizeof(timestamp), \
                     "%Y-%m-%dT%H:%M:%S", ttime); \
            if(level == DTAR_LOG_DBG) { \
                fprintf(DTAR_debug_stream,"[%s] [%d] [%s:%d] ", \
                        timestamp, CIRCLE_global_rank, \
                        __FILE__, __LINE__); \
            } else { \
                fprintf(DTAR_debug_stream,"[%s] [%d] [%s:%d] ", \
                        timestamp, CIRCLE_global_rank, \
                        __FILE__, __LINE__); \
            } \
            fprintf(DTAR_debug_stream, __VA_ARGS__); \
            fprintf(DTAR_debug_stream, "\n"); \
            fflush(DTAR_debug_stream); \
        } \
    } while (0)

extern int CIRCLE_global_rank;
extern FILE* DTAR_debug_stream;
extern DTAR_loglevel DTAR_debug_level;

#endif /* LOG_H */
