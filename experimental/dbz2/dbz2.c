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
#include <glib.h>
#include "mfu.h"

#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
#ifndef O_BINARY
#define O_BINARY 0
#endif

/*All the options available*/

static gboolean opts_decompress = FALSE;
static gboolean opts_compress = FALSE;
static gboolean opts_keep = FALSE;
static gboolean opts_force = FALSE;
static gint opts_memory= -1;
static gint     opts_blocksize = 9;
static gboolean opts_verbose;
static gboolean opts_debug;
static GOptionEntry entries[] = {
        {"decompress", 'd', 0, G_OPTION_ARG_NONE, &opts_decompress, "Decompress file", NULL  },
        {"compress", 'z', 0, G_OPTION_ARG_NONE, &opts_compress, "Compress file", NULL },
        {"keep", 'k', 0, G_OPTION_ARG_NONE, &opts_keep, "Keep existing input file", NULL },
        {"force", 'f', 0, G_OPTION_ARG_NONE, &opts_force, "Overwrite output file", NULL },
        {"blocksize",'b', 0, G_OPTION_ARG_INT, &opts_blocksize, "Block size", NULL},
       	{"memory limit", 'm', 0 , G_OPTION_ARG_INT, &opts_memory, "Memory limit in MB", NULL},
	{"verbose", 'v', 0, G_OPTION_ARG_NONE, &opts_verbose, "Verbose output", NULL },
        {"debug", 'd', 0, G_OPTION_ARG_NONE, &opts_debug, "Debug output", NULL},	
	{ NULL }
};

int main(int argc , char ** argv)
{
       	GError *error = NULL;
    	GOptionContext *context = NULL;
    	context = g_option_context_new(" [sources ... ] [destination file]");
    	g_option_context_add_main_entries(context, entries, NULL);
    	if (!g_option_context_parse(context, &argc, &argv, &error)) 
	{
        	g_option_context_get_help(context, TRUE, NULL);
        	exit(1);

    	}
	/*Set the log level to control the messages that will be printed*/	
	if (opts_debug)
	        mfu_debug_level = MFU_LOG_DBG;
    	else if (opts_verbose)
        	mfu_debug_level = MFU_LOG_INFO;
    	else
        	mfu_debug_level = MFU_LOG_ERR;
 
	char fname[50]; /*stores input file name*/
       	char fname_out[50];/*stores output file name*/
	strncpy(fname_out,argv[1],50); 
	strncpy(fname,argv[1],50);
	if(opts_compress)
        {
                int b_size=(int)opts_blocksize;
		struct stat st;	
		/*If file exists and we are not supposed to overwrite*/	
		if((stat(strcat(argv[1],".bz2"),&st)==0) && (!opts_force))
		{ 
			MFU_LOG(MFU_LOG_ERR,"Output file already exists\n");
			exit(0);	
		}	
		MPI_Init(&argc, &argv);
                dbz2_compress(b_size,fname_out,opts_memory);
                MPI_Finalize();
       		/*If we are to remove the input file*/	
		if(!opts_keep)
			remove(fname); 
	}
        else if(opts_decompress)
        {
                int len=strlen(fname_out);
		fname_out[len-4]='\0'; /*removes the trailing .bz2 from the string*/
		MFU_LOG(MFU_LOG_INFO,"The file name is:%s %s %d",fname, fname_out,len);	
		struct stat st;
		/*If file exists and we are not supposed to overwrite*/	
		if((stat(fname_out,&st)==0) && (!opts_force))
		{
			MFU_LOG(MFU_LOG_ERR,"Output file already exisis\n");
			exit(0);
		}	
		MPI_Init(&argc, &argv);
		decompress(fname,fname_out);
               	MPI_Finalize();
		/*If we are to remove the input file*/	
		if(!opts_keep)
			remove(fname); 
        }
	else
		MFU_LOG(MFU_LOG_ERR,"Must use either compression or decompression\n");
}

