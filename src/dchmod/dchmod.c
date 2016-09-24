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

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "bayer.h"

/* whether to do directory walk with stat of every ite */
static int walk_stat = 1;

/* we parse the mode string given by the user and build a linked list of
 * permissions operations, this defines one element in that list.  This
 * enables the user to specify a sequence of operations separated with
 * commas like "u+r,g+x" */
struct perms {
    int octal;           /* set to 1 if mode_octal is valid */
    long mode_octal;     /* records octal mode (converted to an integer) */
    int usr;             /* set to 1 if user (owner) bits should be set (e.g. u+r) */
    int group;           /* set to 1 if group bits should be set (e.g. g+r) */
    int other;           /* set to 1 if other bits should be set (e.g. o+r) */
    int all;             /* set to 1 if all bits should be set (e.g. a+r) */
    int plus;            /* set to 1 if mode has plus, set to 0 for minus */
    int read;            /* set to 1 if 'r' is given */
    int write;           /* set to 1 if 'w' is given */
    int execute;         /* set to 1 if 'x' is given */
    int capital_execute; /* set to 1 if 'X' is given */
    int assignment;      /* set to 1 if operation is an assignment (e.g. g=u) */
    char source;         /* records source of target: 'u', 'g', 'a' */
    struct perms* next;  /* pointer to next perms struct in linked list */
};

/* free the linked list, given a pointer to the head */
static void free_list(struct perms** p_head)
{
    struct perms* tmp;
    struct perms* head = *p_head;
    struct perms* current = head;

    /* free the memory for the linked list of structs */
    while (current != NULL) {
        tmp = current;
        bayer_free(&current);
        current = tmp->next;
    }

    /* set the head pointer to NULL to indicate list has been freed */
    *p_head = NULL;
}

/* given a group name, lookup and return the group id in gid,
 * the return code is 0 if gid is valid (group was found), 1 otherwise */
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
        }
        else {
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

/* in an expression like g=u, g is the target, and u is the source,
 * parse out and record the source in our perms struct */
static int parse_source(const char* str, struct perms* p)
{
    /* assume the parse will succeed */
    int rc = 1;

    /* initialize our source field */
    p->source = '\0';

    /* only allow one character at this point */
    if (strlen(str) == 1) {
        /* we've got one source character, now check that
         * it's valid, source can be only one of (u, g, or o),
         * keep a copy in p->source */
        if (str[0] == 'u') {
            p->source = 'u';
        }
        else if (str[0] == 'g') {
            p->source = 'g';
        }
        else if (str[0] == 'o') {
            p->source = 'o';
        }
        else {
            /* source character was not u, g, or o */
            rc = 0;
        }
    }
    else {
        /* string did not have exactly one character */
        rc = 0;
    }

    return rc;
}

/* for a string like u+rwX, parse and record the rwX portion */
static int parse_rwx(const char* str, struct perms* p)
{
    /* assume the parse will succeed */
    int rc = 1;

    /* intialize our fields */
    p->read = 0;
    p->write = 0;
    p->execute = 0;
    p->capital_execute = 0;

    /* set all of the r, w, x, and X flags if valid characters */
    do {
        /* set flag based on current character */
        if (str[0] == 'r') {
            p->read = 1;
        }
        else if (str[0] == 'w') {
            p->write = 1;
        }
        else if (str[0] == 'x') {
            p->execute = 1;
        }
        else if (str[0] == 'X') {
            p->capital_execute = 1;
        }
        else if (str[0] == '\0') {
            break;
        }        
        else {
            /* found an invalid character so set rc=0 */
            rc = 0;
            break;
        }

        /* go to next character in string */
        str++;
    } while (1);

    return rc;
}

/* for a string like g-w, parse the +/-/= character and record
 * what we found */
static int parse_plusminus(const char* str, struct perms* p)
{
    /* assume the parse will succeed */
    int rc = 1;

    /* initialize our flags */
    p->plus = 0;
    p->assignment = 0;

    /* set the plus, minus, or equal flags */
    if (str[0] == '+') {
        /* got a plus, go to next character and parse symbolic bits */
        p->plus = 1;
        str++;
        rc = parse_rwx(str, p);
    }
    else if (str[0] == '-') {
        /* got a minus, go to next character and parse symbolic bits */
        p->plus = 0;
        str++;
        rc = parse_rwx(str, p);
    }
    else if (str[0] == '=') {
        /* this is an assignment, go to next character and parse the source */
        p->assignment = 1;
        str++;
        rc = parse_source(str, p);
    }
    else {
        /* parse error: found a character that is something other than a +, -, or = sign */
        rc = 0;
    }

    /* return our parse return code */
    return rc;
}

/* parse target of user, group, or other */
static int parse_uga(const char* str, struct perms* p)
{
    /* assume the parse will succeed */
    int rc = 1;

    /* if no letter is given then assume "all" 
     * is being set (e.g +rw would be a+rw) */
    int assume_all = 1;

    /* intialize our fields */
    p->usr   = 0;
    p->group = 0;
    p->other = 0;
    p->all   = 0;

    /* set the user, group, other, and all flags */
    do {
        if (str[0] == 'u') {
            p->usr = 1;
            assume_all = 0;
        }
        else if (str[0] == 'g') {
            p->group = 1;
            assume_all = 0;
        }
        else if (str[0] == 'o') {
            p->other = 1;
            assume_all = 0;
        } 
        else if (str[0] == 'a') {
            p->all = 1;
        } 
        else {
            /* found an invalid character */
            break;
        }

        /* go to next character */
        ++str;
    } while  (1);

    /* if assume_all is set then no character
     * was given use p->all */
    if (assume_all) {
        p->all = 1;
    }

    /* parse the remainder of the string */
    rc = parse_plusminus(str, p);

    /* return whether parse succeeded */
    return rc;
}

/* given a mode string like "u+r,g-x", fill in a linked list of permission
 * struct pointers */
static int parse_modebits(char* modestr, struct perms** p_head)
{
    int rc = 0;
    if (modestr != NULL) {
        rc = 1;

        /* check whether we're in octal mode */
        int octal = 0;
        if (strlen(modestr) <= 4) {
            /* got 4 or fewer characters, assume we're in octal mode for now */
            octal = 1;

            /* make sure we only have digits and is in the range 0-7 */
            for (int i = 0; i <= strlen(modestr) - 1; i++) {
                if (modestr[i] < '0' || modestr[i] > '7') {
                    /* found a character out of octal range, can't be in octal */
                    octal = 0;
                    break;
                }
            }
        }

        /* parse the modestring and create our list of permissions structures */
        if (octal) {
            /* in octal mode, just create one node in our list */
            rc = 1;
            struct perms* p = BAYER_MALLOC(sizeof(struct perms));
            p->next = NULL;
            p->octal = 1;
            p->mode_octal = strtol(modestr, NULL, 8);
            *p_head = p;

        }
        else {
            /* if it is not in octal mode assume we are in symbolic mode */
            struct perms* tail = NULL;

            /* make a copy of the input string since strtok will clobber it */
            char* tmpstr = BAYER_STRDUP(modestr);

            /* create a linked list of structs that gets broken up based on the comma syntax
             * i.e. u+r,g+x */
            for (char* token = strtok(tmpstr, ","); token != NULL; token = strtok(NULL, ",")) {
                /* allocate memory for a new struct and set the next pointer to null also
                 * turn octal mode off */
                struct perms* p = malloc(sizeof(struct perms));
                p->next = NULL;
                p->octal = 0;

                /* start parsing this 'token' of the input string */
                rc = parse_uga(token, p);

                /* if the tail is not null then point the tail at the latest struct/token */
                if (tail != NULL) {
                    tail->next = p;
                }

                /* if head is not pointing at anything then this token is the head of the list */
                if (*p_head == NULL) {
                    *p_head = p;
                }

                /* have the tail point at the current/last struct */
                tail = p;

                /* if there was an error parsing the string then free the memory of the list */
                if (rc != 1) {
                    free_list(p_head);
                    break;
                }
            }

            /* free the duplicated string */
            bayer_free(&tmpstr);
        }
    }

    return rc;
}

/* given a linked list of permissions structures, check whether user has given us
 * something like "u+rx", "u+rX", or "u+r,u+X" since we need to set bits on
 * directories during the walk in this case. Also, check for turning on read and
 * execute for the "all" bits as well because those can also turn on the user's
 * read and execute bits */
static void check_usr_input_perms(struct perms* head, int* dir_perms)
{
    /* flag to check if the usr read & execute bits are being turned on,
     * assume they are not */
    *dir_perms = 0;

    /* extra flags to check if usr read and execute are being turned on */
    int usr_r = 0;
    int usr_x = 0;

    if (head->octal) {
        /* in octal mode, se we can check bits directly,
         * use a mask to check if the usr_read and usr_execute bits are being turned on */
        long usr_r_mask = 1 << 8;
        long usr_x_mask = 1 << 6;
        if (usr_r_mask & head->mode_octal) {
            usr_r = 1;
        }
        if (usr_x_mask & head->mode_octal) {
            usr_x = 1;
        }
    }
    else {
        /* in symbolic mode, loop through the linked list of structs to check for u+rx in input */
        struct perms* p = head;
        while (p != NULL) {
            /* check if the execute and read are being turned on for each element of linked linked so
             * that if say someone does something like u+r,u+x (so dir_perms=1) or u+rwx,u-rx (dir_perms=0)
             * it will still give the correct result */
            if (p->usr || p->all) {
                if (p->plus) {
                    if (p->read) {
                        /* got something like u+r or a+r, turn read on */
                        usr_r = 1;
                    }
                    if (p->execute || p->capital_execute) {
                        /* got something like u+x, u+X, a+x, or a+X, turn execute on */
                        usr_x = 1;
                    }
                } else {
                    if  (p->read) {
                        /* got something like u-r or a-r, turn read off */
                        usr_r = 0;
                    }
                    if (p->execute || p->capital_execute) {
                        /* got something like u-x, u-X, a-x, or a-X, turn execute off */
                        usr_x = 0;
                    }
                }
            }

            /* update pointer to next element of linked list */
            p = p->next;
        }
    }

    /* only set the dir_perms flag if both the user execute and user read flags are on */
    if (usr_r && usr_x) {
        *dir_perms = 1;
    }

    return;
}

/* when running in assignment mode, we need to read the read/write/execute bits of the source in p */
static void read_source_bits(const struct perms* p, mode_t mode, int* read, int* write, int* execute)
{
    /* assume all bits on the source are off */
    *read = 0;
    *write = 0;
    *execute = 0;

    /* based on the source (u, g, or a) then check which bits were on for each one (r, w, or x) */

    /* got something like g=u, so user is the source */
    if (p->source == 'u') {
        if (mode & S_IRUSR) {
            *read = 1;
        }
        if (mode & S_IWUSR) {
            *write = 1;
        }
        if (mode & S_IXUSR) {
            *execute = 1;
        }
    }

    /* got something like a=g, so group is the source */
    if (p->source == 'g') {
        if (mode & S_IRGRP) {
            *read = 1;
        }
        if (mode & S_IWGRP) {
            *write = 1;
        }
        if (mode & S_IXGRP) {
            *execute = 1;
        }
    }

    /* got something like a=o, so other is the source */
    if (p->source == 'o') {
        if (mode & S_IROTH) {
            *read = 1;
        }
        if (mode & S_IWOTH) {
            *write = 1;
        }
        if (mode & S_IXOTH) {
            *execute = 1;
        }
    }

    return;
}

/* update target bits in mode based on read/write/execute flags */
static void set_target_bits(const struct perms* p, int read, int write, int execute, mode_t* mode)
{
    /* set the r, w, x bits on usr, group, and execute based on if the flags were set with parsing
     * the input string */

    /* got something like u=g, so user is the target */
    if (p->usr || p->all) {
        if (read) {
            *mode |= S_IRUSR;
        }
        else {
            *mode &= ~S_IRUSR;
        }
        if (write) {
            *mode |= S_IWUSR;
        }
        else {
            *mode &= ~S_IWUSR;
        }
        if (execute) {
            *mode |= S_IXUSR;
        }
        else {
            *mode &= ~S_IXUSR;
        }
    }

    /* got something like g=u, so group is the target */
    if (p->group || p->all) {
        if (read) {
            *mode |= S_IRGRP;
        }
        else {
            *mode &= ~S_IRGRP;
        }
        if (write) {
            *mode |= S_IWGRP;
        }
        else {
            *mode &= ~S_IWGRP;
        }
        if (execute) {
            *mode |= S_IXGRP;
        }
        else {
            *mode &= ~S_IXGRP;
        }
    }

    /* got something like o=u, so other is the target */
    if (p->other || p->all) {
        if (read) {
            *mode |= S_IROTH;
        }
        else {
            *mode &= ~S_IROTH;
        }
        if (write) {
            *mode |= S_IWOTH;
        }
        else {
            *mode &= ~S_IWOTH;
        }
        if (execute) {
            *mode |= S_IXOTH;
        }
        else {
            *mode &= ~S_IXOTH;
        }
    }

    return;
}

/* given a pointer to a permissions struct, set mode bits according to symbolic
 * strings like "ug+rX" */
static void set_symbolic_bits(const struct perms* p, bayer_filetype type, mode_t* mode)
{
    /* set the bits based on flags set when parsing input string */

    /* this will handle things like u+r */
    if (p->usr || p->all) {
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
            if (p->capital_execute) {
                /* If it is a directory then always turn on the user execute
                 * bit. This is also how chmod u+X behaves in the case of a 
                 * directory. */
                if (type == BAYER_TYPE_DIR) {
                    *mode |= S_IXUSR;
                }
            }
        }
        else {
            if (p->read) {
                *mode &= ~S_IRUSR;
            }
            if (p->write) {
                *mode &= ~S_IWUSR;
            }
            if (p->execute) {
                *mode &= ~S_IXUSR;
            }
            if (p->capital_execute) {
                /* If it is a directory then always turn off the user execute
                 * bits. This is also how chmod u-X behaves in the case of a 
                 * directory */
                if (type == BAYER_TYPE_DIR) {
                    *mode &= ~S_IXUSR;
                }
            }
        }
    }

    /* all & group check the capital_execute flag, so if there is a
    * capital X in the input string i.e g+X then if the usr execute
    * bit is set the group execute bit will be set to on. If the usr
    * execute bit is not on, then it will be left alone. If the usr
    * says something like ug+X then the usr bit in the input string
    * will be ignored unless it is a directory. In the case of something
    * like g-X, then it will ALWAYS turn off the group or all execute bit.
    * This is slightly different behavior then the +X, but it is intentional
    * and how chmod also works. */

    if (p->group || p->all) {
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
                /* g+X: enable group execute if user execute is set, or if item is directory */
                if (*mode & S_IXUSR || type == BAYER_TYPE_DIR) {
                    *mode |= S_IXGRP;
                }
            }
        }
        else {
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
                /* g-X: always disable group execute */
                *mode &= ~S_IXGRP;
            }
        }
    }

    if (p->other || p->all) {
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
                /* o+X: enable other execute if user execute is set, or if item is directory */
                if (*mode & S_IXUSR || type == BAYER_TYPE_DIR) {
                    *mode |= S_IXOTH;
                }
            }
        }
        else {
            if (p->read) {
                *mode &= ~S_IROTH;
            }
            if (p->write) {
                *mode &= ~S_IWOTH;
            }
            if (p->execute) {
                *mode &= ~S_IXOTH;
            }
            /* o-X: always disable other execute */
            if (p->capital_execute) {
                *mode &= ~S_IXOTH;
            }
        }
    }

    return;
}

/* given our list of permission ops, the type, and the current mode,
 * compute what the new mode should be */
static void set_modebits(struct perms* head, bayer_filetype type, mode_t old_mode, mode_t* mode)
{
    /* if in octal mode then loop through and check which ones are on based on the mask and
     * the current octal mode bits */
    if (head->octal) {
        /* first turn off all bits */
        *mode = (mode_t) 0;

        /* array of constants to check which mode bits are on or off */
        mode_t permbits[12] = {S_ISUID, S_ISGID, S_ISVTX,
                               S_IRUSR, S_IWUSR, S_IXUSR,
                               S_IRGRP, S_IWGRP, S_IXGRP,
                               S_IROTH, S_IWOTH, S_IXOTH
                              };

        /* start with the bit all the way to the left (of 12 bits) on */
        long mask = 1 << 11;
        for (int i = 0; i < 12; i++) {
            /* use mask to check which bits are on and loop through
             * each element in the array of constants, and if it is
             * on (the mode bits pass in as input) then update the
             * current mode */
            if (mask & head->mode_octal) {
                *mode |= permbits[i];
            }

            /* move the 'on' bit to the right one each time through the loop */
            mask >>= 1;
        }
    }
    else {
        /* initialize new mode to current mode */
        *mode = old_mode;

        /* in symbolic mode, loop through the linked list of structs */
        struct perms* p = head;
        while (p != NULL) {
            /* if the assignment flag is set (equals was found when parsing the input string)
             * then break it up into source & target i.e u=g, then the the taret is u and the source
             * is g. So, all of u's bits will be set the same as g's bits */
            if (p->assignment) {
                /* find the source bits with read_source_bits, only can be one at a time (u, g, or a), and
                 * set appropriate read, write, execute flags */
                int read, write, execute;
                read_source_bits(p, *mode, &read, &write, &execute);

                /* if usr, group, or other were on when parsing the input string then they are considered
                 * a target and the new mode is changed accordingly in set_target_bits */
                set_target_bits(p, read, write, execute, mode);
            }
            else {
                /* if the assignment flag is not set then just use
                 * regular symbolic notation to check if usr, group, or all is set, then
                 * plus/minus, and change new mode accordingly */
                set_symbolic_bits(p, type, mode);
            }

            /* update pointer to next element of linked list */
            p = p->next;
        }
    }
}

static void dchmod_level(bayer_flist list, uint64_t* dchmod_count, const char* grname, struct perms* head, gid_t gid)
{
    /* each process directly changes permissions on its elements for each level */
    uint64_t idx;
    uint64_t size = bayer_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        /* get file name */
        const char* dest_path = bayer_flist_file_get_name(list, idx);

        /* update group if user gave a group name */
        if (grname != NULL) {
            /* get user id and group id of file */
            uid_t uid = (uid_t) bayer_flist_file_get_uid(list, idx);
            gid_t oldgid = (gid_t) bayer_flist_file_get_gid(list, idx);

            /* only bother to change group if it's different */
            if (oldgid != gid) {
                /* note that we use lchown to change ownership of link itself, it path happens to be a link */
                if (bayer_lchown(dest_path, uid, gid) != 0) {
                    /* are there other EPERM conditions we do want to report? */

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

        /* update permissions if we have a list of structs */
        if (head != NULL) {
            /* get mode and type */
            bayer_filetype type = bayer_flist_file_get_type(list, idx);

            /* if in octal mode skip this call */
            mode_t mode = 0;
            if (! head->octal) {
                mode = (mode_t) bayer_flist_file_get_mode(list, idx);
            }

            /* change mode, unless item is a link */
            if (type != BAYER_TYPE_LINK) {
                /* given our list of permission ops, the type, and the current mode,
                 * compute what the new mode should be */
                mode_t new_mode;
                set_modebits(head, type, mode, &new_mode);

                /* as an optimization here, we could avoid setting the mode if the new mode
                 * matches the old mode */

                /* set the mode on the file */
                if (bayer_chmod(dest_path, new_mode) != 0) {
                    BAYER_LOG(BAYER_LOG_ERR, "Failed to change permissions on %s chmod() errno=%d %s",
                              dest_path, errno, strerror(errno)
                             );
                }
            }
        }
    }

    /* report number of permissions changed */
    *dchmod_count = size;

    return;
}

static void flist_chmod(bayer_flist flist, const char* grname, struct perms* head)
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

    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double start_dchmod = MPI_Wtime();

    /* split files into separate lists by directory depth */
    int levels, minlevel;
    bayer_flist* lists;
    bayer_flist_array_by_depth(flist, &levels, &minlevel, &lists);

    /* set bits on items starting at the deepest level, this is so we still get child items
     * in the case that we're disabling bits on the parent items */
    for (int level = levels - 1; level >= 0; level--) {
        double start = MPI_Wtime();

        /* get list of items for this level */
        bayer_flist list = lists[level];

        /* do a dchmod on each element in the list for this level & pass it the size */
        uint64_t size = 0;
        dchmod_level(list, &size, grname, head, gid);

        /* wait for all processes to finish before we start with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);

        double end = MPI_Wtime();

        if (bayer_debug_level >= BAYER_LOG_VERBOSE) {
            uint64_t min, max, sum;
            MPI_Allreduce(&size, &min, 1, MPI_UINT64_T, MPI_MIN, MPI_COMM_WORLD);
            MPI_Allreduce(&size, &max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
            MPI_Allreduce(&size, &sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            double rate = 0.0;
            if (end - start > 0.0) {
                rate = (double)sum / (end - start);
            }
            double time_diff = end - start;
            if (bayer_rank == 0) {
                printf("level=%d min=%lu max=%lu sum=%lu rate=%f secs=%f\n",
                       (minlevel + level), (unsigned long)min, (unsigned long)max, (unsigned long)sum, rate, time_diff
                      );
                fflush(stdout);
            }
        }
    }

    /* free the array of lists */
    bayer_flist_array_free(levels, &lists);

    /* wait for all tasks & end timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double end_dchmod = MPI_Wtime();

    /* report remove count, time, and rate */
    if (bayer_debug_level >= BAYER_LOG_VERBOSE && bayer_rank == 0) {
        uint64_t all_count = bayer_flist_global_size(flist);
        double time_diff = end_dchmod - start_dchmod;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        printf("dchmod %lu items in %f seconds (%f items/sec)\n",
               all_count, time_diff, rate
              );
    }

    return;
}

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dchmod [options] <path> ...\n");
    printf("\n");
    printf("Options:\n");
    printf("  -i, --input   <file>   - read list from file\n");
    printf("  -g, --group   <name>   - change group to specified group name\n");
    printf("  -m, --mode    <string> - change mode\n");
    printf("  -e, --exclude <regex>  - exclude a list of files from command\n");
    printf("  -a, --match   <regex>  - match a list of files from command\n");
    printf("  -n, --name             - exclude a list of files from command\n");
    printf("  -v, --verbose          - verbose output\n");
    printf("  -h, --help             - print usage\n");
    printf("\n");
    fflush(stdout);
    return;
}

int main(int argc, char** argv)
{
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
    char* modestr   = NULL;
    char* regex_exp = NULL;
    struct perms* head = NULL;
    int walk        = 0;
    int exclude     = 0;
    int name        = 0;

    int option_index = 0;
    static struct option long_options[] = {
        {"input",    1, 0, 'i'},
        {"group",    1, 0, 'g'},
        {"mode",     1, 0, 'm'},
        {"exclude",  1, 0, 'e'},
        {"match",    1, 0, 'a'},
        {"name",     0, 0, 'n'},
        {"help",     0, 0, 'h'},
        {"verbose",  0, 0, 'v'},
        {0, 0, 0, 0}
    };

    int usage = 0;
    while (1) {
        int c = getopt_long(
                    argc, argv, "i:g:m:nlhv",
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
            case 'e':
                regex_exp = BAYER_STRDUP(optarg);
                exclude = 1;
                break;
            case 'a':
                regex_exp = BAYER_STRDUP(optarg);
                exclude = 0;
                break;
            case 'n':
                name = 1;
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

    /* check that our mode string parses correctly */
    if (modestr != NULL) {
        int valid = parse_modebits(modestr, &head);
        if (! valid) {
            usage = 1;
            if (rank == 0) {
                printf("invalid mode string: %s\n", modestr);
            }

            /* free the head of the list */
            free_list(&head);
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

    /* create an empty file list */
    bayer_flist flist = bayer_flist_new();

    /* flag used to check if permissions need to be
     * set on the walk */
    int dir_perms = 0;
    check_usr_input_perms(head, &dir_perms);

    /* get our list of files, either by walking or reading an
     * input file */
    if (walk) {
        /* if in octal mode set walk_stat=0 */
        if (head->octal && groupname == NULL) {
            walk_stat = 0;
        }
        /* walk list of input paths */
        bayer_param_path_walk(numpaths, paths, walk_stat, flist, dir_perms);
    }
    else {
        /* read list from file */
        bayer_flist_read_cache(inputname, flist);
    }

    /* assume we'll use the full list */
    bayer_flist srclist = flist;

    /* filter the list if needed */
    bayer_flist filtered_flist = BAYER_FLIST_NULL;
    if (regex_exp != NULL) {
        /* filter the list based on regex */
        filtered_flist = bayer_flist_filter_regex(flist, regex_exp, exclude, name);

        /* update our source list to use the filtered list instead of the original */
        srclist = filtered_flist;
    }

    /* change group and permissions */
    flist_chmod(srclist, groupname, head);
   
    /* free list if it was used */
    if (filtered_flist != BAYER_FLIST_NULL){
        /* free the filtered flist (if any) */
        bayer_flist_free(&filtered_flist);
    }

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

    /* free the match_pattern if it isn't null */
    if (regex_exp != NULL) {
        bayer_free(&regex_exp);
    }

    /* free the head of the list */
    free_list(&head);

    /* free the input file name */
    bayer_free(&inputname);

    /* shut down MPI */
    bayer_finalize();
    MPI_Finalize();

    return 0;
}
