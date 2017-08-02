
#ifndef DBZ2_H_
#define DBZ2_H_

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
#include <libcircle.h>
#include <bzlib.h>
#include "mfu.h"

/*Struct to store metadata of each compressed block. This is passed to the root at the end of each wave*/

struct block_info{
                unsigned int length;
               	int rank; 
		int64_t offset;
                int64_t sno;
        };
extern struct block_info* my_blocks; /*stores the metadata of blocks processed by the process over all waves*/
extern int64_t blocks_processed;   /*the number of blocks processed till now in current wave by the process*/
extern char **a;               /*The array that stores the compressed blocks for a process for current wave*/
extern int64_t my_prev_blocks;   /*number of blocks processed by the process at the end of previous wave*/
extern int64_t blocks_pn_pw;     /*Number of blocks that the process can work on in each wave*/
extern int bc_size;            /*Block size for compression in 100kB*/
extern int64_t block_size;    /*Block size for compressin in bytes*/
extern int64_t blocks_done;    /*Total number of blocks processed across previous waves by all processes*/
extern int64_t wave_blocks;   /*Number of blocks that can be processed by all processes across a single wave*/
extern int64_t tot_blocks;   /*Total number of blocks in the file*/
extern char fname[50];       /*Name of input file*/
extern int fd;                /*Input file descriptor*/
extern int fd_out;           /*Output file descriptor*/
extern int64_t my_tot_blocks;  /*The total number of blocks processed by me across all waves and upto the current point in thcurrent wave*/

extern int rank;

void DBz2_Enqueue(CIRCLE_handle* handle); /*create callback function for compress*/ 
void DBz2_Dequeue(CIRCLE_handle* handle);  /*process callback function for compress*/
void DBz2_decompEnqueue(CIRCLE_handle* handle); /*create callback function for decompress*/
void DBz2_decompDequeue(CIRCLE_handle* handle);  /*process callback function for decompress*/
void dbz2_compress(int b_size, char *fname,int opts_memory);
void decompress(char *fname,char *fname_out );
#endif 
