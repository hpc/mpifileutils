#include <stdlib.h>

#include "dparallel.h"

/** The debug stream for all logging messages. */
FILE* DPARALLEL_debug_stream;

/** The current log level of library logging output. */
enum DPARALLEL_loglevel DPARALLEL_debug_level;

/** The rank value of the current node. */
int32_t DPARALLEL_global_rank;

char* DPARALLEL_readline()
{
    char* buf = (char*) malloc(sizeof(char) * CIRCLE_MAX_STRING_LEN);

    if(fgets(buf, CIRCLE_MAX_STRING_LEN, stdin) != NULL) {
        return buf;
    }
    else {
        free(buf);
        return 0;
    }
}

void DPARALLEL_process(CIRCLE_handle* handle)
{
    if(DPARALLEL_global_rank == 0) {
        char* new_cmd = DPARALLEL_readline();

        if(new_cmd != 0) {
            LOG(DPARALLEL_LOG_DBG, "Enqueueing command `%s'.", new_cmd);
            handle->enqueue(new_cmd);
            free(new_cmd);
            return;
        }
    }

    char cmd[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(cmd);
    int ret = system(cmd);

    LOG(DPARALLEL_LOG_DBG, "Command `%s' returned `%d'.", cmd, ret);

    return;
}

int main(void)
{
    DPARALLEL_debug_level = DPARALLEL_LOG_DBG;
    DPARALLEL_debug_stream = stderr;

    CIRCLE_init(0, NULL, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_process(&DPARALLEL_process);

    CIRCLE_begin();
    CIRCLE_finalize();
}

/* EOF */
