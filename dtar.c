#include <sys/types.h>
#include <sys/stat.h>

#include <archive.h>
#include <archive_entry.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dtar.h"

static void create(const char *filename, char compress, int opt_index, const char **argv);


static int verbose=0;

DTAR_options_t DTAR_user_opts;
DTAR_writer_t  DTAR_writer;

int 
main(int argc, const char **argv)
{
	const char *filename = NULL;
	char  compress, mode, opt;
        int flags;
        int opt_index=0;

        MPI_Init(&argc, &argv);        

	mode = 'x';
	verbose = 0;
	compress = '\0';
	flags = ARCHIVE_EXTRACT_TIME;

	/* Among other sins, getopt(3) pulls in printf(3). */
	while (*++argv != NULL && **argv == '-') {
		const char *p = *argv + 1;
                opt_index++;               

		while ((opt = *p++) != '\0') {
			switch (opt) {
			case 'c':
				mode = opt;
				break;
			case 'f':
				if (*p != '\0')
					filename = p;
				else
					filename = *++argv;
				p += strlen(p);
				break;
			case 'j':
				compress = opt;
				break;
			case 'p':
				flags |= ARCHIVE_EXTRACT_PERM;
				flags |= ARCHIVE_EXTRACT_ACL;
				flags |= ARCHIVE_EXTRACT_FFLAGS;
				break;
			case 't':
				mode = opt;
				break;
			case 'v':
				verbose++;
				break;
			case 'x':
				mode = opt;
				break;
			case 'y':
				compress = opt;
				break;
			case 'Z':
				compress = opt;
				break;
			case 'z':
				compress = opt;
				break;
			default:
				usage();
			}
		}
	}

	switch (mode) {
	case 'c':
		create(filename, compress, opt_index, argv);
		break;
	case 't':
//		extract(filename, 0, flags);
		break;
	case 'x':
//		extract(filename, 1, flags);
		break;
	}


        MPI_Finalize();
	return (0);
}

static void
create(const char *filename, char compress, int opt_index, const char **argv)
{
        const char ** argv_beg=argv - opt_index - 1;

        CIRCLE_global_rank = CIRCLE_init(argc, argv_beg, CIRCLE_DEFAULT_FLAGS);
        CIRCLE_cb_create(&DTAR_add_objects);
        CIRCLE_cb_process(&DTAR_process_objects);

        DTAR_parse_path_args(filename, compress, argv);
   
        
        DTAR_jump_table[TREEWALK] = DTAR_do_treewalk;
        DTAR_jump_table[COPY]     = DTAR_do_copy;
        DTAR_jump_table[CLEANUP]  = DTAR_do_cleanup;
    //  DTAR_jump_table[COMPARE]  = DTAR_do_compare;

        CIRCLE_begin();
        CIRCLE_finalize();

}
