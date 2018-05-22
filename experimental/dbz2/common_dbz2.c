#include "dbz2.h"
#include <inttypes.h>

void DBz2_Enqueue(CIRCLE_handle* handle)
{
    /* To use libcircle, this function creates work for compression.
     * It simply puts the all the block numbers for the file in the queue. */
    char* newop;
    for (int i = 0; i < wave_blocks; i++) {
        newop = (char*)MFU_MALLOC(sizeof(char) * 10);
        int64_t block_no = (int64_t)i + (int64_t)blocks_done;
        if (block_no >= tot_blocks) {
            break;
        }

        sprintf(newop, "%" PRId64, block_no);
        handle->enqueue(newop);
        MFU_LOG(MFU_LOG_INFO, "Blocks queued=%" PRId64 "\n", block_no);
    }
}

/* process each compression block */
void DBz2_Dequeue(CIRCLE_handle* handle)
{
    /* used to check whether memory is full because the number of blocks
    to be processed in a wave have been completed for this wave */
    char newop[10];
    handle->dequeue(newop);

    int64_t block_no;
    sscanf(newop, "%" PRId64, &block_no);

    lseek64(fd, block_no * block_size, SEEK_SET);

    /* read block from input file */
    char* ibuf = (char*)MFU_MALLOC(sizeof(char) * block_size); /*Input buffer*/
    ssize_t inSize = mfu_read(fname, fd, (char*)ibuf, (size_t)block_size);

    /* Guaranteed max output size */
    unsigned int outSize = (unsigned int)((block_size * 1.01) + 600);
    //a[blocks_processed]=malloc(sizeof(char)*outSize);

    /* compress block */
    int ret = BZ2_bzBuffToBuffCompress(a[blocks_processed], &outSize, ibuf, (int)inSize, bc_size, 0, 30);
    MFU_LOG(MFU_LOG_DBG, "After compresssion=%s,size=%u\n", a[blocks_processed], outSize);
    if (ret != 0) {
        MFU_LOG(MFU_LOG_ERR, "Error in compression for rank %d", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* set metadata for the compressed block */
    my_blocks[my_tot_blocks].sno    = block_no;
    my_blocks[my_tot_blocks].length = outSize;
    my_blocks[my_tot_blocks].rank   = rank;

    blocks_processed++;
    my_tot_blocks++;
    MFU_LOG(MFU_LOG_INFO, "Processed block %" PRId64 ",num processed=%" PRId64 ",rank=%d, blocks per wave=%" PRId64 "\n", block_no, blocks_processed, rank, blocks_pn_pw);

    mfu_free(&ibuf);
}

void DBz2_decompEnqueue(CIRCLE_handle* handle)
{
    /* This function creates work for libcircle if we are using decompression
     * The function reads the trailer, which stores the offset of all the
     * blocks and creates a string of the form:
     *    block_no:offset:next_offset(offset of next block.
     * It pushes this string into the queue */

    /* The last 8 bytes of the trailer stores the location or offset of the start of the trailer */
    int64_t trailer_begin;
    int64_t last_offset = (int64_t)lseek64(fd, -8, SEEK_END);
    mfu_read(fname, fd, &trailer_begin, 8);
    MFU_LOG(MFU_LOG_INFO, "trailer begins at:%" PRId64 "\n", trailer_begin);

    int64_t start;
    lseek64(fd, trailer_begin, SEEK_SET);
    mfu_read(fname, fd, &start, 8);

    int64_t block_num = 0;
    int64_t end = -1;
    while (1) {
        /* when the offset of the trailer is read,
         * all block offsets have been read and queued */
        if (end == trailer_begin) {
            break;
        }

        char* newop = (char*)MFU_MALLOC(sizeof(char) * 50);
        mfu_read(fname, fd, &end, 8);

        MFU_LOG(MFU_LOG_DBG, "Start and End at:%" PRId64 "%" PRId64 "\n", start, end);
        sprintf(newop, "%"PRId64":%" PRId64 ":%" PRId64, block_num, start, end);
        MFU_LOG(MFU_LOG_INFO, "The queued item is:%s\n", newop);

        handle->enqueue(newop);
        block_num++;
        MFU_LOG(MFU_LOG_INFO, "Blocks queued=%" PRId64 "\n", block_num);
        start = end;
    }
}

/*function to perfrom libcircle processing in decompression*/
void DBz2_decompDequeue(CIRCLE_handle* handle)
{
    char newop[50];
    handle->dequeue(newop);

    /* parse newop to get block_no, offset and offset of next block */
    int64_t block_num;
    int64_t offset;
    int64_t next_offset;
    char* token = strtok(newop, ":");
    sscanf(token, "%" PRId64, &block_num);
    token = strtok(NULL, ":");
    sscanf(token, "%"PRId64, &offset);
    token = strtok(NULL, ":");
    sscanf(token, "%"PRId64, &next_offset);

    /* find length of compressed block */
    unsigned int length = (unsigned int)(next_offset - offset);
    int64_t in_offset = block_num * block_size;
    unsigned int outSize = (unsigned int)((block_size * 10 * 1.01) + 600);
    MFU_LOG(MFU_LOG_INFO, "Block size=%" PRId64 "\n", block_size);

    /* create input and output buffers */
    char* obuf = MFU_MALLOC(sizeof(char) * outSize);
    char* ibuf = (char*)MFU_MALLOC(sizeof(char) * length);
    MFU_LOG(MFU_LOG_INFO, "Block specs:block_no=%" PRId64 "offset=%" PRId64 "next_offset%" PRId64 "length %u\n", block_num, offset, next_offset, length);


    /* Read from correct position of compressed file */
    lseek64(fd, offset, SEEK_SET);
    ssize_t inSize = mfu_read(fname, fd, (char*)ibuf, length);

    /* Perform decompression */
    MFU_LOG(MFU_LOG_DBG, "The string is=%s\n", ibuf);
    int ret = BZ2_bzBuffToBuffDecompress(obuf, &outSize, ibuf, length, 0, 0);
    MFU_LOG(MFU_LOG_DBG, "After Decompresssion=%s,size=%uret=%d\n", obuf, outSize, ret);
    /*     if(ret!=0)
            {
                    printf("Error in compression for rank %d",rank);
                    MPI_Finalize();
                    exit(1);
            }    */

    /*write result to correct offset in file*/
    lseek64(fd_out, in_offset, SEEK_SET);
    mfu_write(fname_out, fd_out, obuf, outSize);

    mfu_free(&ibuf);
    mfu_free(&obuf);
}
