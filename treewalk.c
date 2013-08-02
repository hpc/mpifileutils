#include "dtar.h"
#include "log.h"
#include <archive.h>
#include <archive_entry.h>

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
#include <mpi.h>


/** Options specified by the user. */
extern DTAR_options_t DTAR_user_opts;
extern DTAR_writer_t  DTAR_writer;
extern MPI_Comm       inter_comm;

static void print_header(char * path) {

	   int status;
	   char cmd[1035];
       FILE *fp;
	    
       /* Open the command for reading. */
	   sprintf(cmd, "hexdump -C test.tar >> %s.log", path);
       fp = popen(cmd, "r");
       if (fp == NULL) {
	       printf("Failed to run command\n" );
		   return;
	   }
	   /* close */
	   pclose(fp);
}


void DTAR_do_treewalk(DTAR_operation_t* op, \
                       CIRCLE_handle* handle)
{
    struct stat64 statbuf;

    if(lstat64(op->operand, &statbuf) < 0) {
        LOG(DTAR_LOG_DBG, "Could not get info for `%s'. errno=%d %s", op->operand, errno, strerror(errno));
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

static struct archive * new_archive()
{
	
        char   compress=DTAR_user_opts.compress;
	struct archive *a=archive_write_new();
	
        switch (compress) {
	case 'j': case 'y':
		archive_write_add_filter_bzip2(a);
		break;
	case 'Z':
		archive_write_add_filter_compress(a);
		break;
	case 'z':
		archive_write_add_filter_gzip(a);
		break;
	default:
		archive_write_add_filter_none(a);
		break;
	}
	archive_write_set_format_ustar(a);
        archive_write_set_bytes_per_block(a, 0);
         
        return a;
}


inline static int write_header( off64_t offset, DTAR_operation_t * op) 
{
  
    struct archive *disk= archive_read_disk_new();
    int    r=archive_read_disk_open(disk, op->operand);
 
    if( r != ARCHIVE_OK ) {
       printf("Cannot read disk in treewalk.c\n");
       return -1;
    } 

    struct archive_entry *entry=archive_entry_new();
    r=archive_read_next_header2(disk, entry);
   
    if (r != ARCHIVE_OK) {
        printf("Cannot read header in treewalk.c\n");
        return -1;
    }
    
    struct archive * a=new_archive();
    archive_write_open_fd(a, DTAR_writer.fd_tar); 
    off64_t cur_pos= lseek64(DTAR_writer.fd_tar, offset, SEEK_SET);      

    if(cur_pos < 0) {
       printf("Cannot seek in treewalk.c offset is %d\n", offset); 
       return -1;
    }
  
    print_header(op->operand);
    printf("rank %d file %s header offset is %x  current pos is %x\n", CIRCLE_global_rank, \
            op->operand, offset, cur_pos);
    archive_write_header(a, entry);

    if( r != ARCHIVE_OK) {
        printf("Cannot write header in treewalk.c\n");
        return -1;
    }    
    
    fsync(DTAR_writer.fd_tar);
    print_header(op->operand);

    archive_entry_free(entry);
    archive_read_close(disk);
    archive_read_free(disk);
    
//    archive_write_close(a);
//    archive_write_free(a);
  
    return 0;

}

void DTAR_stat_process_file(DTAR_operation_t* op, \
                             const struct stat64* statbuf,
                             CIRCLE_handle* handle)
{
    int64_t file_size = statbuf->st_size;
    int64_t chunk_index;
    int64_t num_chunks = file_size / DTAR_CHUNK_SIZE;
    int64_t buffer[2];
    off64_t offset=0;
    MPI_Status  stat;

    buffer[0]=(int64_t)CIRCLE_global_rank;

    int64_t rem=(file_size) % 512;
    if (rem == 0) {
        buffer[1]=file_size + 512;
    }
    else {
        buffer[1]=(file_size/512 + 2)*512;
    }

    MPI_Send(buffer, 2,  MPI_LONG_LONG, 0, 0, inter_comm);
    MPI_Recv(&offset, 1, MPI_LONG_LONG, 0, 0, inter_comm, &stat);    

    write_header(offset, op);

    return ;
    op->offset=offset+512;

    printf("rank %d file %s data:%x entry:%x hex_entry:%x\n" , \
            CIRCLE_global_rank, op->operand, op->offset, offset, offset);
    for(chunk_index = 0; chunk_index < num_chunks; chunk_index++) {
        char* newop = DTAR_encode_operation(COPY, chunk_index, op->operand, \
                                             op->offset, \
                                             file_size);
        handle->enqueue(newop);
        free(newop);
    }

    /* Encode and enqueue the last partial chunk. */
    if((num_chunks * DTAR_CHUNK_SIZE) < file_size || num_chunks == 0) {
        char* newop = DTAR_encode_operation(COPY, chunk_index, op->operand, \
                                             op->offset, \
                                             file_size);
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
