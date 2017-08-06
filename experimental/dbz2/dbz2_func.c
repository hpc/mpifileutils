#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#define _LARGEFILE64_SOURCE
#include<unistd.h>
#include<stdio.h>
#include<stdlib.h>
#include<sys/sysinfo.h>
#include<string.h>
#include<sys/time.h>
#include<sys/resource.h>
#include "mpi.h"
#include<unistd.h>
#include<stdint.h>
#include <fcntl.h>
#include<sys/stat.h>
#include<utime.h>
#include "dbz2.h"
#include<inttypes.h>

#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
#ifndef O_BINARY
#define O_BINARY 0
#endif

struct block_info* my_blocks;
int64_t blocks_processed = 0;
char** a;
int64_t my_prev_blocks = 0;
int64_t blocks_pn_pw;
int bc_size;
int64_t block_size;
int64_t blocks_done = 0;
int64_t wave_blocks;
int64_t tot_blocks;
int rank;
char fname[50];
int fd;
int fd_out;
int64_t my_tot_blocks = 0;
void find_wave_size(int b_size, int opts_memory)
{
    bc_size = b_size;
    block_size = (int64_t)b_size * 100 * 1024;
    struct sysinfo info;
    int size;
    int64_t mem_limit;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    struct rlimit limit;
    getrlimit(RLIMIT_DATA, &limit);
    MFU_LOG(MFU_LOG_INFO, "The limit is %lld %lld\n", (long long)limit.rlim_cur, (long long)limit.rlim_max);
    sysinfo(&info);
    MFU_LOG(MFU_LOG_INFO, "The free and total ram are:%lu,%lu", info.freeram, info.totalram);
    MFU_LOG(MFU_LOG_INFO, "The block size is:%" PRId64, block_size);
    if ((unsigned long)limit.rlim_cur < info.freeram)
    { mem_limit = (int64_t)limit.rlim_cur; }
    else
    { mem_limit = (int64_t)info.freeram; }
    if (opts_memory * 1024 * 1024 < mem_limit && opts_memory > 0)
    { mem_limit = (int64_t)opts_memory * 1024 * 1024; }
    int64_t wave_size_approx = mem_limit - (int64_t)info.totalram * 2 / 100 - 8 * block_size - 400 * 1024 - 128 - block_size;
    int64_t comp_buff_size = 1.01 * block_size + 600;
    int64_t waves_blocks_approx = wave_size_approx / comp_buff_size;
    int64_t wave_size = wave_size_approx - 2 * tot_blocks * sizeof(struct block_info);
    blocks_pn_pw = (int64_t)(0.4 * wave_size / comp_buff_size);
    if (blocks_pn_pw > 800)
    { blocks_pn_pw = 800; }
    //int blocks_n_w=(int)blocks_pn_pw;

    MPI_Allreduce(&blocks_pn_pw, &wave_blocks, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    MFU_LOG(MFU_LOG_INFO, "The total number of blocks in a wave=%" PRId64 ",The number of blocks in this rank=%" PRId64 "Each wave size on this node=%" PRId64, wave_blocks, blocks_pn_pw, wave_size_approx);
}

void decompress(char* fname, char* fname_out)
{
    int size;
    char size_str[5];
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MFU_LOG(MFU_LOG_INFO, "The file name is:%s\n", fname);
    fd = open(fname, O_RDONLY | O_BINARY | O_LARGEFILE);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for reading rank %d", rank);
        MPI_Finalize();
        exit(1);
    }
    read(fd, (char*)size_str, 4 * sizeof(char));
    /*the 3rd character stored i nthe file gives the block size used for compression*/
    lseek64(fd, 0, SEEK_SET);
    int bc_size = size_str[3] - '0';
    struct stat st;
    stat(fname, &st);
    MFU_LOG(MFU_LOG_INFO, "The block size %d\n", bc_size);

    /*Open file for wrting and change permissions*/
    if (rank == 0) {
        fd_out = open(fname_out, O_CREAT | O_RDWR | O_TRUNC | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Finalize();
            exit(1);
        }
        struct utimbuf uTimBuf;
        uTimBuf.actime = st.st_atime;
        uTimBuf.modtime = st.st_mtime;
        chmod(fname, st.st_mode);
        utime(fname, &uTimBuf);
        chown(fname, st.st_uid, st.st_gid);
    }
    int bret = MPI_Barrier(MPI_COMM_WORLD);
    if (bret != MPI_SUCCESS) {
        MFU_LOG(MFU_LOG_ERR, "Barrier error\n");
        MPI_Finalize();
        exit(1);
    }
    if (rank != 0) {
        fd_out = open(fname_out, O_RDWR | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Finalize();
            exit(1);
        }
    }
    block_size = bc_size * 100 * 1024;

    /*Actual call to libcircle and callback fuction setting*/

    CIRCLE_init(0, NULL, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&DBz2_decompEnqueue);
    CIRCLE_cb_process(&DBz2_decompDequeue);
    CIRCLE_begin();
    CIRCLE_finalize();
}

void dbz2_compress(int b_size, char* fname, int opts_memory)
{
    int size;
    int64_t num_waves;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int rcount[size];
    struct stat st;
    stat(fname, &st);
    block_size = (int64_t)b_size * 100 * 1024;
    if (st.st_size % block_size == 0)
    { tot_blocks = (int64_t)(st.st_size) / block_size; }
    else
    { tot_blocks = (int64_t)(st.st_size) / block_size + 1; }
    find_wave_size(b_size, opts_memory);
    MFU_LOG(MFU_LOG_INFO, "The file name is:%s\n", fname);
    fd = open(fname, O_RDONLY | O_BINARY | O_LARGEFILE);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open file for reading rank %d", rank);
        MPI_Finalize();
        exit(1);
    }
    stat(fname, &st);

    /*The file for output is opened and options set*/

    if (rank == 0) {
        fd_out = open(strcat(fname, ".bz2"), O_CREAT | O_RDWR | O_TRUNC | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Finalize();
            exit(1);
        }
        struct utimbuf uTimBuf;
        uTimBuf.actime = st.st_atime;
        uTimBuf.modtime = st.st_mtime;
        chmod(fname, st.st_mode);
        utime(fname, &uTimBuf);
        chown(fname, st.st_uid, st.st_gid);
    }
    int bret = MPI_Barrier(MPI_COMM_WORLD);
    if (bret != MPI_SUCCESS) {
        MFU_LOG(MFU_LOG_ERR, "Barrier error\n");
        MPI_Finalize();
        exit(1);
    }
    if (rank != 0) {
        fd_out = open(strcat(fname, ".bz2"), O_RDWR | O_BINARY | O_LARGEFILE, FILE_MODE);
        if (fd_out < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open file for writing rank %d", rank);
            MPI_Finalize();
            exit(1);
        }
    }
    blocks_processed = 0;
    my_prev_blocks = 0;
    blocks_done = my_tot_blocks = 0;
    int64_t comp_buff_size = 1.01 * block_size + 600;
    /*Set the number of blocks in the file*/
    if (st.st_size % block_size == 0)
    { tot_blocks = (int64_t)(st.st_size) / block_size; }
    else
    { tot_blocks = (int64_t)(st.st_size) / block_size + 1; }
    MFU_LOG(MFU_LOG_INFO, "size of file=%lu\n", (unsigned long)st.st_size);
    if (tot_blocks % wave_blocks == 0)
    { num_waves = tot_blocks / wave_blocks; }
    else
    { num_waves = tot_blocks / wave_blocks + 1; }
    /*stores metadata of all blocks processed by this process*/
    my_blocks = (struct block_info*)malloc(sizeof(struct block_info) * blocks_pn_pw * num_waves);
    if (my_blocks == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Metadata: memory allocation failed");
        MPI_Finalize();
        exit(1);
    }
    int64_t last_offset = 0;
    struct block_info** this_wave_blocks = (struct block_info**)malloc(sizeof(struct block_info*)*wave_blocks);
    MPI_Datatype metatype, oldtypes[3];
    MPI_Aint offsets[3], extent;
    int blockcounts[3];
    offsets[0] = 0;
    oldtypes[0] = MPI_UNSIGNED;
    blockcounts[0] = 1;
    MPI_Type_extent(MPI_UNSIGNED, &extent);
    offsets[1] = extent;
    oldtypes[1] = MPI_INT;
    blockcounts[1] = 1;
    MPI_Type_extent(MPI_INT, &extent);
    offsets[2] = extent + offsets[1];
    oldtypes[2] = MPI_INT64_T;
    blockcounts[2] = 2;
    MPI_Type_struct(3, blockcounts, offsets, oldtypes, &metatype);
    MPI_Type_commit(&metatype);
    struct block_info rbuf[wave_blocks];
    int64_t actual_wave_blocks;
    /*Call libcircle in a loop to work each wave as a single instance of libcircle*/
    for (blocks_done = 0; blocks_done < tot_blocks; blocks_done += wave_blocks) {
        int blocks_for_wave;
        if ((tot_blocks - blocks_done) < wave_blocks)
        { blocks_for_wave = tot_blocks - blocks_done; }
        else
        { blocks_for_wave = wave_blocks; }
        a = (char**)malloc(sizeof(char*)*blocks_for_wave);
        if (a == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Initial allocation: memory allocation failed");
            MPI_Finalize();
            exit(1);
        }
        for (int i = 0; i < blocks_for_wave; i++) {
            a[i] = (char*)malloc(comp_buff_size * sizeof(char));
            //memset(a[i],1,comp_buff_size*sizeof(char));
            if (a[i] == NULL) {
                MFU_LOG(MFU_LOG_ERR, "Out of memory\n");
                MPI_Finalize();
                exit(1);
            }
        }
        CIRCLE_init(0, NULL, CIRCLE_DEFAULT_FLAGS);
        CIRCLE_cb_create(&DBz2_Enqueue);
        CIRCLE_cb_process(&DBz2_Dequeue);
        CIRCLE_begin();
        CIRCLE_finalize();
        /*gather the number of blocks processed by each process in this wave*/
        MPI_Gather(&blocks_processed, 1, MPI_INT, rcount, 1, MPI_INT, 0, MPI_COMM_WORLD);
        int displs[size];
        displs[0] = 0;
        actual_wave_blocks = 0; /*actual number of blocks processed by all processes in this wave*/
        for (int k = 0; k < size; k++)
        { actual_wave_blocks += (int64_t)rcount[k]; }
        for (int k = 1; k < size; k++)
        { displs[k] = displs[k - 1] + rcount[k - 1]; }
        /*Gather metadata of all blocks processed in this wave*/
        int res = MPI_Gatherv(&my_blocks[my_prev_blocks], blocks_processed, metatype, rbuf, rcount, displs, metatype, 0, MPI_COMM_WORLD);
        if (res != MPI_SUCCESS)
        { MFU_LOG(MFU_LOG_ERR, "Error in gather\n"); }
        //memset(this_wave_blocks,0,sizeof(this_wave_blocks));
        MPI_Barrier(MPI_COMM_WORLD);
        if (bret != MPI_SUCCESS) {
            MFU_LOG(MFU_LOG_ERR, "Barrier error\n");
            MPI_Finalize();
            exit(1);
        }
        /*compute the offset of all blocks processed in current wave*/
        if (rank == 0) {
            for (int k = 0; k < actual_wave_blocks; k++) {
                this_wave_blocks[rbuf[k].sno - blocks_done] = &rbuf[k];
            }
            this_wave_blocks[0]->offset = last_offset;
            for (int k = 1; k < actual_wave_blocks; k++) {
                this_wave_blocks[k]->offset = this_wave_blocks[k - 1]->offset + this_wave_blocks[k - 1]->length;
            }
            last_offset = this_wave_blocks[actual_wave_blocks - 1]->offset + this_wave_blocks[actual_wave_blocks - 1]->length;
        }
        /*provide info about the offset of coressponding blocks to process that processed it*/
        MPI_Scatterv(&rbuf, rcount, displs, metatype, &my_blocks[my_prev_blocks], blocks_processed, metatype, 0, MPI_COMM_WORLD);
        /*Each process writes out the blocks it processed in current wave at the correct offset*/
        for (int k = 0; k < blocks_processed; k++) {
            lseek64(fd_out, my_blocks[my_prev_blocks + k].offset, SEEK_SET);
            MFU_LOG(MFU_LOG_DBG, "Data to be written=%s,%u\n", a[k], my_blocks[my_prev_blocks + k].length);
            write(fd_out, a[k], my_blocks[my_prev_blocks + k].length);
        }
        my_prev_blocks = my_tot_blocks;
        blocks_processed = 0;
        for (int i = 0; i < blocks_for_wave; i++) {
            free(a[i]);
        }
        free(a);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if (bret != MPI_SUCCESS) {
        MFU_LOG(MFU_LOG_ERR, "Barrier error\n");
        MPI_Finalize();
        exit(1);
    }
    /*End of all waves*/
    /*Broadcast offset of start of trailer*/
    MPI_Bcast(&last_offset, 1, MPI_UNSIGNED_LONG, 0, MPI_COMM_WORLD);
    /*Each process writes the offset of all blocks processed by it at corect location in trailer*/
    for (int k = 0; k < my_tot_blocks; k++) {
        lseek64(fd_out, last_offset + my_blocks[k].sno * 8, SEEK_SET);
        int64_t this_offset = (int64_t)my_blocks[k].offset;
        write(fd_out, &this_offset, 8);
    }
    /*root writes the locaion of trailer start to last 8 bytes of the file*/
    if (rank == 0) {
        lseek64(fd_out, last_offset + tot_blocks * 8, SEEK_SET);
        int64_t trailer_offset = (int64_t)last_offset;
        write(fd_out, &trailer_offset, 8);
    }
    free(my_blocks);
    close(fd);
    close(fd_out);
}
/*int main(int argc , char ** argv)
{
    char fname[50];
    strncpy(fname,argv[3],50);
    if(strcmp(argv[2],"c")==0)
    {
        int b_size=atoi(argv[1]);
        MPI_Init(&argc, &argv);
        compress(b_size,fname);
        MPI_Finalize();
    }
    else
    {
        decompress(fname);
        printf("Decompression\n");
    }
}*/
