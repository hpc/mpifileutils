#include "treewalk.h"
#include "dtar.h"

#include <dirent.h>
#include <errno.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/time.h>

/** Options specified by the user. */
extern DTAR_options_t DTAR_user_opts;

void DTAR_do_treewalk(DTAR_operation_t* op, \
                       CIRCLE_handle* handle)
{
    struct stat64 statbuf;

    if(lstat64(op->operand, &statbuf) < 0) {
        LOG(DTAR_LOG_DBG, "Could not get info for `%s'. errno=%d %s", op->operand, errno, strerror(errno));
        DTAR_retry_failed_operation(TREEWALK, handle, op);
        return;
    }

    /* first check that we handle this file type */
    if(! S_ISDIR(statbuf.st_mode) &&
       ! S_ISREG(statbuf.st_mode) &&
       ! S_ISLNK(statbuf.st_mode))
    {
        if (S_ISCHR(statbuf.st_mode)) {
          LOG(DTAR_LOG_ERR, "Encountered an unsupported file type S_ISCHR at `%s'.", op->operand);
        } else if (S_ISBLK(statbuf.st_mode)) {
          LOG(DTAR_LOG_ERR, "Encountered an unsupported file type S_ISBLK at `%s'.", op->operand);
        } else if (S_ISFIFO(statbuf.st_mode)) {
          LOG(DTAR_LOG_ERR, "Encountered an unsupported file type S_ISFIFO at `%s'.", op->operand);
        } else if (S_ISSOCK(statbuf.st_mode)) {
          LOG(DTAR_LOG_ERR, "Encountered an unsupported file type S_ISSOCK at `%s'.", op->operand);
        } else {
          LOG(DTAR_LOG_ERR, "Encountered an unsupported file type %x at `%s'.", statbuf.st_mode, op->operand);
        }
        return;
    }

    if(S_ISDIR(statbuf.st_mode)) {
        /* LOG(DTAR_LOG_DBG, "Stat operation found a directory at `%s'.", op->operand); */
        DTAR_stat_process_dir(op, &statbuf, handle);
    }
    else if(S_ISREG(statbuf.st_mode)) {
        /* LOG(DTAR_LOG_DBG, "Stat operation found a file at `%s'.", op->operand); */
        DTAR_stat_process_file(op, &statbuf, handle);
    }
    else if(S_ISLNK(statbuf.st_mode)) {
        /* LOG(DTAR_LOG_DBG, "Stat operation found a link at `%s'.", op->operand); */
        DTAR_stat_process_link(op, &statbuf, handle);
    }
    else {
        LOG(DTAR_LOG_ERR, "Encountered an unsupported file type %x at `%s'.", statbuf.st_mode, op->operand);
        DTAR_retry_failed_operation(TREEWALK, handle, op);
        return;
    }
}

/**
 * This function copies a link.
 */
void DTAR_stat_process_link(DTAR_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle)
{
    return;
}

inline static write_header( off64_t offset, DTAR_operation_t * op) 
{
    if(lseek64(DTAR_writer->fd_tar, offset, SEEK_SET) < 0) {
       printf("Cannot seek in treewalk.c offset is %d\n", offset); 
       return -1;
    }
    
    DTAR_writer->disk=archive_read_disk_new(); 
    int r=archive_read_disk_open(disk, op->operand);
 
    if( r != ARCHIVE_OK ) {
       printf("Cannot read disk in treewalk.c\n");
       return -1;
    } 
   
    DTAR_writer->entry=archive_entry_new();
    r=archive_read_next_header2(DTAR_writer->disk, DTAR_writer->entry);
   
    if (r != ARCHIVE_OK) {
        printf("Cannot read header in treewalk.c\n");
        return -1;
    }    
 
    archive_write_header(a, entry);

    if( r != ARCHIVE_OK) {
        printf("Cannot write header in treewalk.c\n");
        return -1;
    }    
    
    archive_entry_free(DTAR_writer->entry);
    archive_read_close(DTAR_writer->disk);
    archive_read_free(DTAR_writer->disk);

}

void DTAR_stat_process_file(DTAR_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle)
{
    int64_t file_size = statbuf->st_size;
    int64_t chunk_index;
    int64_t num_chunks = file_size / DTAR_CHUNK_SIZE;
    int64_t buffer[2]
    off64_t     offset;
    MPI_Status  stat;

    buffer[0]=(int64_t)CIRCLE_global_rank;
    buffer[1]=(file_size/512+2)*512;
    
    MPI_Send(buffer, 2,  MPI_LONG_LONG, 0, 0, inter_comm);
    MPI_Recv(&offset, 1, MPI_LONG_LONG, 0, 0, inter_comm, &stat);     
    write_header(offset, op);

    op->source_base_offset=offset;

    for(chunk_index = 0; chunk_index < num_chunks; chunk_index++) {
        char* newop = DTAR_encode_operation(COPY, chunk_index, op->operand, \
                                             op->source_base_offset, \
                                             op->dest_base_appendix, file_size);
        handle->enqueue(newop);
        free(newop);
    }

    /* Encode and enqueue the last partial chunk. */
    if((num_chunks * DTAR_CHUNK_SIZE) < file_size || num_chunks == 0) {
        char* newop = DTAR_encode_operation(COPY, chunk_index, op->operand, \
                                             op->source_base_offset, \
                                             op->dest_base_appendix, file_size);
        handle->enqueue(newop);
        free(newop);
    }
}

void DTAR_stat_process_dir(DTAR_operation_t* op,
                            const struct stat64* statbuf,
                            CIRCLE_handle* handle)
{
    return;
}

/* EOF */
