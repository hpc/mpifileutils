/*
 * Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
 *   Produced at the Lawrence Livermore National Laboratory
 *   CODE-673838
 *
 * Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
 *   (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)
 *
 * Copyright (2013-2015) UT-Battelle, LLC under Contract No.
 *   DE-AC05-00OR22725 with the Department of Energy.
 *
 * Copyright (c) 2015, DataDirect Networks, Inc.
 * 
 * All rights reserved.
 *
 * This file is part of mpiFileUtils.
 * For details, see https://github.com/hpc/fileutils.
 * Please also read the LICENSE file.
*/

#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h> /* asctime / localtime */
#include <ctype.h>
#include <regex.h>

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"

/* TODO: change globals to struct */
static int walk_stat = 1;

struct perms {
        int octal;
        long mode_octal;
        int usr;
        int group;
        int all;
        int plus;           
        int read;
        int write;
        int execute;
        int capital_execute;
        struct perms* next;
};

/*****************************
 * Driver functions
 ****************************/
static int lookup_gid(const char* name, gid_t* gid)
{
    uint64_t values[2];

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* have rank 0 lookup the group id */
    if (rank == 0) {
        /* lookup specified group name */
        errno = 0;
        struct group* gr = getgrnam(name);
        if (gr != NULL) {
            /* lookup succeeded */
            values[0] = 1;
            values[1] = (uint64_t) gr->gr_gid;
        } else {
            /* indicate that lookup failed */
            values[0] = 0;

            /* print error message if we can */
            if (errno != 0) {
                BAYER_LOG(BAYER_LOG_ERR, "Failed to find entry for group %s errno=%d %s",
                    name, errno, strerror(errno)
                   );
            }
        }
    }

    /* broadcast result from lookup */
    MPI_Bcast(values, 2, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* set the group id */
    int rc = (int) values[0];
    if (values[0] == 1) {
        *gid = (gid_t) values[1];
    }

    return rc;
}

static * valid_modebits(const char* modestr) {
 	int i;
        regex_t regex;
        int regex_return;
        int* mode_type = (int*)malloc(3 * sizeof(int));
        mode_type[0] = mode_type[1] = mode_type[2] = 0; 

        /* mode_type[0]: octal = 1, symbolic = 0
         * mode_type[1]: octal = 0, symbolic = 1
         * mode_type[2]: octal_valid or symbolic_valid 
         * depends on mode_type[0] or mode_type[1], i.e
         * if it is octal & valid then mode_type[0] and mode_type[2]
         * will both be 1 */
        
	if (modestr != NULL) {
            /* if it is octal then assume it will start with a digit */
            if (strlen(modestr) <= 4 && isdigit(modestr[0])) { 
		for (i = 0; i <= strlen(modestr) - 1; i++) {
			/* check if a digit and in the range 0 - 7 */
                        if (isdigit(modestr[i])) {
				if (modestr[i] < '0' || modestr[i] > '7') {
                                        mode_type[2] = 0;
                                        break;                        
				}
				else {
                                        mode_type[2] = 1;
				}
			}
			else {
                                mode_type[2] = 0;
                                break;
			}	
                }
                mode_type[0]= 1;
           }
           /* TODO: if it is symbolic do checks later add = */
           else {
                //compile regular expression 
                mode_type[1] = 1; 
                regex_return = regcomp(&regex, "[u*g*a*][-+][r*w*x*X*]", 0);
                
                if (regex_return) {
                        printf(stderr, "could not compile regex\n");
                }
                /* execute regular expression */
                regex_return = regexec(&regex, modestr, 0, NULL, 0);
                 
                if (!regex_return) {
                        //there was a match!
                        mode_type[2] = 1;  
                }
	   }
        }
        return mode_type;	
}

static int parse_rwx(const char* str, struct perms* p) {
    int rc = 1;
    p->read = 0;
    p->write = 0;
    p->execute = 0;
    p->capital_execute = 0;
    while (str[0] == 'r' || str[0] == 'w' || str[0] == 'x' || str[0] == 'X') {
        if (str[0] == 'r') {
            p->read = 1;
        } else if (str[0] == 'w') {
            p->write = 1;
        } else if (str[0] == 'x') {
            p->execute = 1;
        } else if (str[0] == 'X') {
            p->capital_execute = 1;
        } else if (rc != 1) {
            rc = 0;
        }
        str++;
    }
    return rc;
}

static int parse_plusminus(const char* str, struct perms* p) {
    int rc = 1;
    p->plus = 0;
    if (str[0] == '+') {
        p->plus = 1;
        str++;
        rc = parse_rwx(str, p);
    } else if (str[0] == '-') {
        p->plus = 0;
        str++;
        rc = parse_rwx(str, p);
    } else if (rc != 1) {
        rc = 0;
    }
    return rc;
}

static int parse_uga(const char* str, struct perms* p) {
    int rc = 1;
    p->usr = 0;
    p->group = 0;
    p->all = 0;
    while (str[0] == 'u' || str[0] == 'g' || str[0] == 'a'){
        if (str[0] == 'u') {
            p->usr = 1;
        } else if (str[0] == 'g') {
            p->group = 1;
        } else if (str[0] == 'a') {
            p->all = 1;
        } else if (rc != 1) {
            rc = 0;
        }
        str++; 
    }
    rc = parse_plusminus(str, p);
    return rc;       
}

static void parse_modebits(char* modestr, struct perms** p_head) {
        int * check_mode = valid_modebits(modestr);
        if (check_mode[0] == 1 && check_mode[2] == 1 && check_mode[1] != 1) {
                struct perms* p = BAYER_MALLOC(sizeof(struct perms));
                p->next = NULL;
                p->octal = 1;
                p->mode_octal = strtol(modestr, NULL, 8);
                *p_head = p;
       }
       if (check_mode[1] == 1 && check_mode[2] == 1 && check_mode[0] != 1) {
             struct perms* tail = NULL;
             for(char* token = strtok(modestr, ","); token != NULL; token = strtok(NULL, ",")) {
                struct perms* p = malloc(sizeof(struct perms));
                p->next = NULL;
                p->octal = 0;
                parse_uga(token, p);
                if (tail != NULL) {
                    tail->next = p;
                }
                if(*p_head == NULL) {
                    *p_head = p;
                }
                tail = p;
             }
       }
       free(check_mode);
}

static void set_modebits(struct perms* head, mode_t old_mode, mode_t* mode, bayer_filetype type) {
        if (head->octal) {
                *mode = (mode_t)0;
	        mode_t permbits[12] = {S_ISUID, S_ISGID, S_ISVTX, 
				       S_IRUSR, S_IWUSR, S_IXUSR, 
				       S_IRGRP, S_IWGRP, S_IXGRP, 
				       S_IROTH, S_IWOTH, S_IXOTH};
		long mask = 1 << 11;
		for (int i = 0; i < 12; i++) {
			if (mask & head->mode_octal) {
				*mode |= permbits[i];
			}
			mask >>= 1;
		}
       }
        else {
                   struct perms *p = head;
                   *mode = old_mode;
              while (p != NULL) {
                   if (p->usr) {
                           if (p->plus) {
                                   if (p->read) {
                                           *mode |= S_IRUSR;
                                   }
                                   if (p->write) {
                                           *mode |= S_IWUSR;
                                   }
                                   if (p->execute) {
                                           *mode |= S_IXUSR;
                                   } 
                           } else {
                                   if (p->read) {
                                           *mode &= ~S_IRUSR;
                                   }
                                   if (p->write) {
                                           *mode &= ~S_IWUSR;
                                   }
                                   if (p->execute) {
                                           *mode &= ~S_IXUSR;
                                   } 
                           }
                   } 
                   if (p->group) {
                           if (p->plus) {
                                   if (p->read) {
                                           *mode |= S_IRGRP;
                                   }
                                   if (p->write) {
                                           *mode |= S_IWGRP;
                                   }
                                   if (p->execute) {
                                           *mode |= S_IXGRP;
                                   } 
                                   if (p->capital_execute) {
                                       if (*mode & S_IXUSR || type == BAYER_TYPE_DIR) {
                                           *mode |= S_IXGRP;
                                       }
                                   }  
                           } else {
                                   if (p->read) {
                                           *mode &= ~S_IRGRP;
                                   }
                                   if (p->write) {
                                           *mode &= ~S_IWGRP;
                                   }
                                   if (p->execute) {
                                           *mode &= ~S_IXGRP;
                                   } 
                                   if (p->capital_execute) {
                                               *mode &= ~S_IXGRP;
                                   }  
                           }
                   } 
                   if (p->all) {
                           if (p->plus) {
                                   if (p->read) {
                                           *mode |= S_IROTH;
                                   }
                                   if (p->write) {
                                           *mode |= S_IWOTH;
                                   }
                                   if (p->execute) {
                                           *mode |= S_IXOTH;
                                   }
                                   if (p->capital_execute) {
                                       if (*mode & S_IXUSR || type == BAYER_TYPE_DIR) {
                                           *mode |= S_IXOTH;
                                       }
                                   }  
                           } else {
                                   if (p->read) {
                                           *mode &= ~S_IROTH;
                                   }
                                   if (p->write) {
                                           *mode &= ~S_IWOTH;
                                   }
                                   if (p->execute) {
                                           *mode &= ~S_IXOTH;
                                   }
                                   if (p->capital_execute) {
                                               *mode &= ~S_IXOTH;
                                   }         
                           }
                   }
                   p = p->next;      
               }        
              
       }
}

static void flist_chmod(
    bayer_flist flist,
    const char* grname, struct perms* head)
{
    /* lookup groupid if set, bail out if not */
    gid_t gid;
    if (grname != NULL) {
        int lookup_rc = lookup_gid(grname, &gid);
        if (lookup_rc == 0) {
            /* failed to get group id, bail out */
            return;
        }
    }
	
    /* each process directly removes its elements */
    uint64_t idx;
    uint64_t size = bayer_flist_size(flist);
    for (idx = 0; idx < size; idx++) {
        /* get file name */
        const char* dest_path = bayer_flist_file_get_name(flist, idx);

        /* update group if user gave a group name */
        if (grname != NULL) {
            /* get user id and group id of file */
            uid_t uid = (uid_t) bayer_flist_file_get_uid(flist, idx);
            gid_t oldgid = (gid_t) bayer_flist_file_get_gid(flist, idx);

            /* only bother to change group if it's different */
            if (oldgid != gid) {
                /* note that we use lchown to change ownership of link itself, it path happens to be a link */
                if(bayer_lchown(dest_path, uid, gid) != 0) {
                    /* TODO: are there other EPERM conditions we do want to report? */

                    /* since the user running dcp may not be the owner of the
                     * file, we could hit an EPERM error here, and the file
                     * will be left with the effective uid and gid of the dcp
                     * process, don't bother reporting an error for that case */
                    if (errno != EPERM) {
                        BAYER_LOG(BAYER_LOG_ERR, "Failed to change ownership on %s lchown() errno=%d %s",
                            dest_path, errno, strerror(errno)
                           );
                    }
                }
            }
        }
	if (head != NULL) {

        	/* get mode and type */
        	bayer_filetype type = bayer_flist_file_get_type(flist, idx);
        	mode_t mode = (mode_t) bayer_flist_file_get_mode(flist, idx);

        	/* change mode, unless item is a link */
        	if(type != BAYER_TYPE_LINK) {
                        mode_t new_mode; 
    	                set_modebits(head, mode, &new_mode, type);
            		/* set the mode on the file */
                	if(bayer_chmod(dest_path, new_mode) != 0) {
                		BAYER_LOG(BAYER_LOG_ERR, "Failed to change permissions on %s chmod() errno=%d %s",
                        	dest_path, errno, strerror(errno)
                   		);
            		}
        	}
	}
    }

    return;
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dchmod [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input <file>  - read list from file\n");
    printf("  -g, --group <name>  - change group to specified group name\n");
    printf("  -m, --mode <string> - change mode\n");    
    printf("  -v, --verbose       - verbose output\n");
    printf("  -h, --help          - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
    int i;

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    bayer_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* parse command line options */
    char* inputname = NULL;
    char* groupname = NULL;
    char* modestr = NULL;
    char* mode_type = NULL;
    struct perms* head = NULL;
    int walk = 0;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",    1, 0, 'i'},
        {"group",    1, 0, 'g'},
	{"mode",     1, 0, 'm'},
        {"help",     0, 0, 'h'},
        {"verbose",  0, 0, 'v'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:g:m:lhv",
                    long_options, &option_index
                );

        if (c == -1) {
            break;
        }

        switch (c) {
            case 'i':
                inputname = BAYER_STRDUP(optarg);
                break;
            case 'g':
                groupname = BAYER_STRDUP(optarg);
                break;
	    case 'm':
	    	modestr = BAYER_STRDUP(optarg);
		break;
            case 'h':
                usage = 1;
                break;
            case 'v':
                bayer_debug_level = BAYER_LOG_VERBOSE;
                break;
            case '?':
                usage = 1;
                break;
            default:
                if (rank == 0) {
                    printf("?? getopt returned character code 0%o ??\n", c);
                }
        }
    }

    /* paths to walk come after the options */
    int numpaths = 0;
    bayer_param_path* paths = NULL;
    if (optind < argc) {
        /* got a path to walk */
        walk = 1;

        /* determine number of paths specified by user */
        numpaths = argc - optind;

        /* allocate space for each path */
        paths = (bayer_param_path*) BAYER_MALLOC((size_t)numpaths * sizeof(bayer_param_path));

        /* process each path */
        char** argpaths = &argv[optind];
        bayer_param_path_set_all(numpaths, argpaths, paths);

        /* advance to next set of options */
        optind += numpaths;

        /* don't allow input file and walk */
        if (inputname != NULL) {
            usage = 1;
        }
    }
    else {
        /* if we're not walking, we must be reading,
         * and for that we need a file */
        if (inputname == NULL) {
            usage = 1;
        }
    }
    if (modestr != NULL) {
    	mode_t mode; 
    	int* valid = valid_modebits(modestr);
    		if (valid[2] == 0) {
			usage = 1;
			if (rank == 0) {
				printf("invalid mode string: %s\n", modestr);
			}
    		} 
    }
    /* print usage if we need to */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        bayer_finalize();
        MPI_Finalize();
        return 1;
    }

    parse_modebits(modestr, &head);

    /* create an empty file list */
    bayer_flist flist = bayer_flist_new();

    /* get our list of files, either by walking or reading an
     * input file */
    if (walk) {
        /* walk list of input paths */
        bayer_param_path_walk(numpaths, paths, walk_stat, flist);
    }
    else {
        /* read list from file */
        bayer_flist_read_cache(inputname, flist);
    }

    /* change group and permissions */
    flist_chmod(flist, groupname, head);

    /* free the file list */
    bayer_flist_free(&flist);

    /* free the path parameters */
    bayer_param_path_free_all(numpaths, paths);

    /* free memory allocated to hold params */
    bayer_free(&paths);

    /* free the group name */
    bayer_free(&groupname);

    /* free the modestr */
    bayer_free(&modestr);

    struct perms* tmp;
    struct perms* current = head;
    while (current != NULL) {
        tmp = current;
        bayer_free(&current);
        current = tmp->next;
    }

    /* free the input file name */
    bayer_free(&inputname);

    /* shut down MPI */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
