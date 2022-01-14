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

/* with libcap, we can check whether process has capability to change
 * file properties even when the user is not the owner */
#ifdef HAVE_LIBCAP
#include <sys/capability.h>
#endif

#include "libcircle.h"
#include "mfu.h"

/* holds current and total number of items for progress messages */
uint64_t chmod_count;
uint64_t chmod_count_total;

/* progress message state */
mfu_progress* chmod_prog;

/* prints progress messages while updating items */
static void chmod_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute percentage of items */
    double percent = 0.0;
    if (chmod_count_total > 0) {
        percent = 100.0 * (double)vals[0] / (double)chmod_count_total;
    }

    /* compute average rate */
    double rate = 0.0;
    if (secs > 0.0) {
        rate = (double)vals[0] / secs;
    }

    /* compute estimated time remaining */
    double secs_remaining = -1.0;
    if (rate > 0.0) {
        secs_remaining = (double)(chmod_count_total - vals[0]) / rate;
    }

    /* print progress message */
    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Processed %llu items (%.2f%%) in %.3lf secs (%.3lf items/sec) %d secs remaining ...",
            vals[0], percent, secs, rate, (int)secs_remaining);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Processed %llu items (%.2f%%) in %.3lf secs (%.3lf items/sec)",
            vals[0], percent, secs, rate);
    }
}

/* free the linked list, given a pointer to the head */
void mfu_perms_free(mfu_perms** p_head)
{
    if (p_head != NULL) {
        /* free the memory for the linked list of structs */
        mfu_perms* current = *p_head;
        while (current != NULL) {
            /* record poiner to next element in list */
            mfu_perms* tmp = current->next;
 
            /* free current element */
            mfu_free(&current);

            /* update to next element in list */
            current = tmp;
        }

        /* set the head pointer to NULL to indicate list has been freed */
        *p_head = NULL;
    }
}

/* given a user name, lookup and return the user id in uid,
 * the return code is 1 if uid is valid (user name was found), 0 otherwise */
static int lookup_uid(const char* name, uid_t* uid)
{
    /* first check whether we have a numeric uid */
    int all_digits = 1;
    const char* ptr;
    for (ptr = name; *ptr != '\0'; ptr++) {
        if (! isdigit(*ptr)) {
            /* found a character that is not a digit,
             * so don't treat this as a uid */
            all_digits = 0;
            break;
        }
    }

    /* got a string of digits, consider it a valid uid */
    if (all_digits) {
        *uid = (uid_t) atoi(name);
        return 1;
    }

    /* have rank 0 lookup uid for username, bcast result to others,
     * the first entry will be a flag indicating whether the lookup
     * succeeded (1) or not (0), if successful, the uid will be
     * stored in the second entry */
    uint64_t values[2];

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* have rank 0 do the lookup */
    if (rank == 0) {
        /* lookup specified user name */
        errno = 0;
        struct passwd* pw = getpwnam(name);
        if (pw != NULL) {
            /* lookup succeeded, copy the uid */
            values[0] = 1;
            values[1] = (uint64_t) pw->pw_uid;
        }
        else {
            /* indicate that lookup failed */
            values[0] = 0;

            /* print error message if we can */
            if (errno != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to find entry for name `%s' (errno=%d %s)",
                          name, errno, strerror(errno));
            }
        }
    }

    /* broadcast result from lookup */
    MPI_Bcast(values, 2, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* copy user id to return value if lookup was successful */
    int rc = (int) values[0];
    if (values[0] == 1) {
        *uid = (uid_t) values[1];
    }

    return rc;
}

/* given a group name, lookup and return the group id in gid,
 * the return code is 1 if gid is valid (group was found), 0 otherwise */
static int lookup_gid(const char* name, gid_t* gid)
{
    /* first check whether we have a numeric gid */
    int all_digits = 1;
    const char* ptr;
    for (ptr = name; *ptr != '\0'; ptr++) {
        if (! isdigit(*ptr)) {
            /* found a character that is not a digit,
             * so don't treat this as a gid */
            all_digits = 0;
            break;
        }
    }

    /* got a string of digits, consider it a valid gid */
    if (all_digits) {
        *gid = (gid_t) atoi(name);
        return 1;
    }

    /* the first entry will be a flag indicating whether the lookup
     * succeeded (1) or not (0), if successful, the gid will be
     * stored in the second entry */
    uint64_t values[2];

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* have rank 0 do the lookup */
    if (rank == 0) {
        /* lookup specified group name */
        errno = 0;
        struct group* gr = getgrnam(name);
        if (gr != NULL) {
            /* lookup succeeded, copy the gid */
            values[0] = 1;
            values[1] = (uint64_t) gr->gr_gid;
        }
        else {
            /* indicate that lookup failed */
            values[0] = 0;

            /* print error message if we can */
            if (errno != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to find entry for group `%s' (errno=%d %s)",
                          name, errno, strerror(errno));
            }
        }
    }

    /* broadcast result from lookup */
    MPI_Bcast(values, 2, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* copy group id to return value if lookup was successful */
    int rc = (int) values[0];
    if (values[0] == 1) {
        *gid = (gid_t) values[1];
    }

    return rc;
}

/* in an expression like g=u, g is the target, and u is the source,
 * parse out and record the source in our perms struct,
 * return 1 if we parse string successfully, 0 otherwise */
static int parse_source(const char* str, mfu_perms* p)
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

/* for a string like u+rwX, parse and record the rwX portion,
 * return 1 if we parse string successfully, 0 otherwise */
static int parse_rwx(const char* str, mfu_perms* p)
{
    /* assume the parse will succeed */
    int rc = 1;

    /* intialize our fields */
    p->read            = 0;
    p->write           = 0;
    p->execute         = 0;
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
 * operator, return 1 if we parse string successfully, 0 otherwise */
static int parse_plusminus(const char* str, mfu_perms* p)
{
    /* assume the parse will succeed */
    int rc = 1;

    /* initialize our flags */
    p->plus       = 0;
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

/* parse target of user, group, other, all, or implicit all,
 * return 1 if we parse string successfully, 0 otherwise */
static int parse_ugoa(const char* str, mfu_perms* p)
{
    /* assume the parse will succeed */
    int rc = 1;

    /* if no letter is given then mode update applies to all,
     * this syntax also considers the user's umask
     * so we need to track that no letter was provided */
    p->assume_all = 1;

    /* intialize our fields */
    p->usr   = 0;
    p->group = 0;
    p->other = 0;
    p->all   = 0;

    /* set the user, group, other, and all flags */
    do {
        if (str[0] == 'u') {
            p->usr = 1;
            p->assume_all = 0;
        }
        else if (str[0] == 'g') {
            p->group = 1;
            p->assume_all = 0;
        }
        else if (str[0] == 'o') {
            p->other = 1;
            p->assume_all = 0;
        } 
        else if (str[0] == 'a') {
            p->all = 1;
            p->assume_all = 0;
        } 
        else {
            /* found an invalid character */
            break;
        }

        /* go to next character */
        ++str;
    } while  (1);

    /* if assume_all is set then no character
     * was given so apply settings to all */
    if (p->assume_all) {
        p->all = 1;
    }

    /* parse the remainder of the string */
    rc = parse_plusminus(str, p);

    /* return whether parse succeeded */
    return rc;
}

/* given a mode string like "u+r,g-x", fill in a linked list of permission
 * struct pointers */
int mfu_perms_parse(const char* modestr, mfu_perms** p_head)
{
    /* initialize output parameter */
    *p_head = NULL;

    /* bail out if we didn't get a mode string to parse */
    if (modestr == NULL) {
        return 0;
    }

    /* assume we'll succeed in parsing */
    int rc = 1;

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
        /* we're in octal mode, allocate a new node for our list */
        mfu_perms* p = MFU_MALLOC(sizeof(mfu_perms));
        p->next = NULL;

        /* initialize node as octal and
         * convert octal string to a mode value */
        p->octal      = 1;
        p->mode_octal = strtol(modestr, NULL, 8);

        /* return list to caller */
        *p_head = p;
    }
    else {
        /* if we're not in octal mode, assume we are in symbolic mode,
         * we'll build a linked list for each entry in a comma-delimited
         * list like u+rX,g+r */

        /* define struct to keep track of end of list */
        mfu_perms* tail = NULL;

        /* make a copy of the input string since strtok will clobber it */
        char* tmpstr = MFU_STRDUP(modestr);

        /* create a linked list of structs that gets broken up based on the comma syntax
         * i.e. u+r,g+x */
        for (char* token = strtok(tmpstr, ",");
             token != NULL;
             token = strtok(NULL, ","))
        {
            /* allocate memory for a new struct and set the next pointer to null also
             * turn octal mode off */
            mfu_perms* p = malloc(sizeof(mfu_perms));
            p->next  = NULL;
            p->octal = 0;

            /* set item as head of list if this is the first item */
            if (*p_head == NULL) {
                *p_head = p;
            }

            /* attach item to end of list */
            if (tail != NULL) {
                tail->next = p;
            }
            tail = p;

            /* parse this token of the input string */
            rc = parse_ugoa(token, p);

            /* if there was an error parsing the string then free the memory of the list */
            if (rc != 1) {
                mfu_perms_free(p_head);
                break;
            }
        }

        /* free the duplicated string */
        mfu_free(&tmpstr);
    }

    return rc;
}

/* given a linked list of permissions structures, check whether user has given us
 * something like "u+rx", "u+rX", or "u+r,u+X" since we need to set bits on
 * directories during the walk in this case. Also, check for turning on read and
 * execute for the "all" bits as well because those can also turn on the user's
 * read and execute bits */
void mfu_perms_need_dir_rx(const mfu_perms* head, mfu_walk_opts_t* walk_opts)
{
    /* flag to check if the usr read & execute bits are being turned on,
     * assume they are not */
    walk_opts->dir_perms = 0;

    /* extra flags to check if usr read and execute are being turned on */
    int usr_r = 0;
    int usr_x = 0;

    if (head->octal) {
        /* in octal mode, se we can check bits directly with a mask,
         * check if usr read and execute bits are being turned on,
         * r-x------ */
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
        const mfu_perms* p = head;
        while (p != NULL) {
            /* check if the execute and read are being turned on for each element of linked linked so
             * that if say someone does something like u+r,u+x (so dir_perms=1) or u+rwx,u-rx (dir_perms=0)
             * it will still give the correct result */
            if (p->usr || p->all) {
                if (p->plus) {
                    /* plus, turning bits on */
                    if (p->read) {
                        /* got something like u+r or a+r, turn read on */
                        usr_r = 1;
                    }
                    if (p->execute || p->capital_execute) {
                        /* got something like u+x, u+X, a+x, or a+X, turn execute on */
                        usr_x = 1;
                    }
                } else {
                    /* minus, turning bits off */
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
        walk_opts->dir_perms = 1;
    }

    return;
}

/* when running in assignment mode, we need to read the read/write/execute bits of the source in p */
static void read_source_bits(const mfu_perms* p, mode_t mode, int* read, int* write, int* execute)
{
    /* assume all bits on the source are off */
    *read    = 0;
    *write   = 0;
    *execute = 0;

    /* based on the source (u, g, or a) then check which bits
     * are on for each one (r, w, or x) */

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
static void set_target_bits(const mfu_perms* p, int read, int write, int execute, mode_t* mode)
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
static void set_symbolic_bits(const mfu_perms* p, mfu_filetype type, mode_t mask, mode_t* mode)
{
    /* save the old mode in case assume_all flag is on */
    mode_t old_mode = *mode;

    /* set the bits based on flags set when parsing input string */
    /* this will handle things like u+r */
    if (p->usr || p->all) {
        if (p->plus) {
            /* plus, turn bits on */
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
                if (type == MFU_TYPE_DIR) {
                    *mode |= S_IXUSR;
                }
            }
        }
        else {
            /* minus, turn bits off */
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
                if (type == MFU_TYPE_DIR) {
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
            /* plus, turn bits on */
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
                if (*mode & S_IXUSR || type == MFU_TYPE_DIR) {
                    *mode |= S_IXGRP;
                }
            }
        }
        else {
            /* minus, turn bits off */
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
            /* plus, turn bits on */
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
                if (*mode & S_IXUSR || type == MFU_TYPE_DIR) {
                    *mode |= S_IXOTH;
                }
            }
        }
        else {
            /* minus, turn bits off */
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

    /* if assume_all flag is on then calculate the mode 
     * based on the umask value */
    if (p->assume_all) {
        /* get set of bits in current mode that we won't change 
         * because of umask */
        old_mode &= mask;

        /* mask out any bits of new mode that we shouldn't change 
         * due to umask */
        *mode &= ~mask;

        /* merge in bits from previous mode that had been masked */
        *mode |= old_mode;
    }

    return;
}

/* given our list of permission ops, the type, and the current mode,
 * compute what the new mode should be */
static void set_modebits(const mfu_perms* head, mfu_filetype type, mode_t old_mode, mode_t mask, mode_t* mode)
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
        const mfu_perms* p = head;
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
                 * regular symbolic notation to check if usr, group, other, and/or all is being set, then
                 * plus/minus, and change new mode accordingly */
                set_symbolic_bits(p, type, mask, mode);
            }

            /* update pointer to next element of linked list */
            p = p->next;
        }
    }
}

static enum {
  ITEM_COUNT = 0,
  CHMOD_SUCCESS,
  CHMOD_FAILURE,
  CHMOD_SKIPPED,
  CHOWN_SUCCESS,
  CHOWN_FAILURE,
  CHOWN_SKIPPED
} chmod_stat;

static int chmod_list(
    mfu_flist list,
    uint64_t* stats,
    const mfu_perms* head,
    const char* usrname,
    const char* grname,
    mfu_chmod_opts_t* opts)
{
    /* assume we'll succeed, set this to FAILURE on any error */
    int rc = MFU_SUCCESS;

    /* initialize stats to report back what was changed */
    stats[ITEM_COUNT] = 0;
    stats[CHMOD_SUCCESS] = 0;
    stats[CHMOD_FAILURE] = 0;
    stats[CHMOD_SKIPPED] = 0;
    stats[CHOWN_SUCCESS] = 0;
    stats[CHOWN_FAILURE] = 0;
    stats[CHOWN_SKIPPED] = 0;

    /* each process directly changes permissions on its elements for each level */
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        /* get file name */
        const char* dest_path = mfu_flist_file_get_name(list, idx);

        /* update owner/group if user gave an owner/group name */
        if (usrname != NULL || grname != NULL) {
            /* compute new user id, assume it doesn't change,
             * calling chown with uid/gid = -1 will not change */
            uid_t newuid = -1;
            if (usrname != NULL) {
                /* user gave us a uid value, so the uid may have changed */
                newuid = opts->uid;
            }

            /* compute new group id, assume it doesn't change */
            gid_t newgid = -1;
            if (grname != NULL) {
                /* user gave us a gid value, so the gid may have changed */
                newgid = opts->gid;
            }

            /* assume we'll attempt to change owner/group */
            int change = 1;

            /* if we have current owner/group of items, we can skip calling
             * chown on items that already have the new owner/group and items
             * where the user doesn't have permission to change them */
            if (mfu_flist_have_detail(list)) {
                /* get user id and group id of file */
                uid_t olduid = (uid_t) mfu_flist_file_get_uid(list, idx);
                gid_t oldgid = (gid_t) mfu_flist_file_get_gid(list, idx);

                /* only need to change if new owner/group are
                 * different from current owner/group */
                if ((usrname == NULL || olduid == newuid) &&
                    (grname  == NULL || oldgid == newgid))
                {
                    /* new owner/group are same as current owner/group,
                     * so no need to change it */
                    change = 0;
                }

                /* only attempt to change group if effective user id of
                 * the process is the owner of the item or process has
                 * CAP_CHWON capability */
                if (grname != NULL && opts->geteuid != olduid && !opts->capchown) {
                    /* want to change group, but effective uid is not the
                     * owner, linux prevents normal users from doing this */
                    change = 0;
                }
            }

            /* if user threw force option, always try to change */
            if (opts->force) {
                change = 1;
            }

            /* only bother to change owner or group if they are different,
             * or if force options is enabled */
            if (change) {
                /* note that we use lchown to change ownership of link itself,
                 * if path happens to be a link */
                if (mfu_lchown(dest_path, newuid, newgid) == 0) {
                    /* succeeded in changing the owner/group of this item */
                    stats[CHOWN_SUCCESS] += 1;
                } else {
                    /* hit an error changing the owner/group of this item */
                    stats[CHOWN_FAILURE] += 1;

                    /* since the user running dchmod may not be the owner of the
                     * file, we could hit an EPERM error here, allow the silence
                     * option to avoid printing errors in that case */
                    if (errno != EPERM || !opts->silence) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to change ownership on `%s' lchown() (errno=%d %s)",
                            dest_path, errno, strerror(errno));
                    }

                    /* hit an error */
                    rc = MFU_FAILURE;
                }
            } else {
                /* skip this item */
                stats[CHOWN_SKIPPED] += 1;
            }
        }

        /* update permissions if we have a list of structs */
        if (head != NULL) {
            /* get type of item */
            mfu_filetype type = mfu_flist_file_get_type(list, idx);

            /* get the current permissions on the item,
             * if in octal mode, we may not have the mode for each file */
            mode_t mode = 0;
            if (mfu_flist_have_detail(list)) {
                mode = (mode_t) mfu_flist_file_get_mode(list, idx);
            }

            /* given our list of permission ops, the type, the current mode,
             * and the umask, compute what the new mode should be */
            mode_t new_mode;
            set_modebits(head, type, mode, opts->umask, &new_mode);

            /* assume we'll attempt to change permissions */
            int change = 1;

            /* can check existing permissions and whether user has
             * access to change permissions if we have existing bits
             * and file owner information */
            if (mfu_flist_have_detail(list)) {
                /* don't bother changing permissions if they already match,
                 * since mode from stat also contains file type bits,
                 * we mask those off before comparing */
                mode_t bitmask = S_ISUID | S_ISGID | S_ISVTX | S_IRWXU | S_IRWXG | S_IRWXO;
                mode_t newbits = (new_mode & bitmask);
                mode_t oldbits = (mode     & bitmask);
                if (newbits == oldbits) {
                    /* new bits match old bits, no need to do anything */
                    change = 0;
                }

                /* don't bother changing permissions on files we don't own,
                 * unless process has CAP_FOWNER capability */
                uid_t owner = (uid_t) mfu_flist_file_get_uid(list, idx);
                if (opts->geteuid != owner && !opts->capfowner) {
                    /* don't attempt to change files we don't own */
                    change = 0;
                }
            }

            /* always try to change permissions if force option is set */
            if (opts->force) {
                change = 1;
            }

            /* we never attempt to change permissions on symlinks, even with force */
            if (type == MFU_TYPE_LINK) {
                change = 0;
            }

            /* finally attempt to change permissions if needed */
            if (change) {
                /* set the mode on the file */
                if (mfu_chmod(dest_path, new_mode) == 0) {
                    /* succeeded in changing the permission bits of this item */
                    stats[CHMOD_SUCCESS] += 1;
                } else {
                    /* hit an error changing the permission bits of this item */
                    stats[CHMOD_FAILURE] += 1;

                    /* since the user running dchmod may not be the owner of the
                     * file, we could hit an EPERM error here, allow the silence
                     * option to avoid printing errors in that case */
                    if (errno != EPERM || !opts->silence) {
                        MFU_LOG(MFU_LOG_ERR, "Failed to change permissions on `%s' chmod() (errno=%d %s)",
                            dest_path, errno, strerror(errno));
                    }

                    /* hit an error */
                    rc = MFU_FAILURE;
                }
            } else {
                /* skip this item */
                stats[CHMOD_SKIPPED] += 1;
            }
        }

        /* update our count for progress messages */
        chmod_count++;
        mfu_progress_update(&chmod_count, chmod_prog);
    }

    /* report number of items considered */
    stats[ITEM_COUNT] = size;

    return rc;
}

/* given an input flist, set permissions on items according to perms list,
 * optionally, if usrname != NULL, change owner, or if grname != NULL, change group */
void mfu_flist_chmod(
    mfu_flist flist,
    const char* usrname,
    const char* grname,
    const mfu_perms* head,
    mfu_chmod_opts_t* opts)
{
    /* get global size of list */
    uint64_t all_count = mfu_flist_global_size(flist);

    /* report remove count, time, and rate */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Changing %lu items", all_count);
    }

    /* lookup current umask, we consider umask when not given a
     * leading letter to specify a target as in "+rX" vs "a+rX",
     * in that case bits set in umask are not affected */
    opts->umask = umask(S_IWGRP | S_IWOTH);
    umask(opts->umask);

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* lookup user id if given a user name */
    if (usrname != NULL) {
        int lookup_rc = lookup_uid(usrname, &opts->uid);
        if (lookup_rc == 0) {
            /* failed to get user id, bail out */
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to find uid for user name `%s'", usrname);
            }
            return;
        }
    }

    /* lookup group id if given a group name */
    if (grname != NULL) {
        int lookup_rc = lookup_gid(grname, &opts->gid);
        if (lookup_rc == 0) {
            /* failed to get group id, bail out */
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to find gid for group name `%s'", grname);
            }
            return;
        }
    }

    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double start_dchmod = MPI_Wtime();

    /* initialize progress messages */
    chmod_count = 0;
    chmod_count_total = all_count;
    chmod_prog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, chmod_progress_fn);

    /* variable to track total stats across levels */
    uint64_t total_stats[7] = {0};

    /* split files into separate lists by directory depth */
    int levels, minlevel;
    mfu_flist* lists;
    mfu_flist_array_by_depth(flist, &levels, &minlevel, &lists);

    /* set bits on items starting at the deepest level, this is so we still get child items
     * in the case that we're disabling bits on the parent items */
    for (int level = levels - 1; level >= 0; level--) {
        /* spread items for this level evenly over all procs */
        mfu_flist list = mfu_flist_spread(lists[level]);

        /* do a dchmod on each element in the list for this level & pass it the size */
        uint64_t stats[7] = {0};
        chmod_list(list, stats, head, usrname, grname, opts);

        /* tally up stats for above operation into running totals */
        total_stats[ITEM_COUNT]    += stats[ITEM_COUNT];
        total_stats[CHOWN_SUCCESS] += stats[CHOWN_SUCCESS];
        total_stats[CHOWN_FAILURE] += stats[CHOWN_FAILURE];
        total_stats[CHOWN_SKIPPED] += stats[CHOWN_SKIPPED];
        total_stats[CHMOD_SUCCESS] += stats[CHMOD_SUCCESS];
        total_stats[CHMOD_FAILURE] += stats[CHMOD_FAILURE];
        total_stats[CHMOD_SKIPPED] += stats[CHMOD_SKIPPED];

        /* free list of spread items */
        mfu_flist_free(&list);

        /* wait for all processes to finish before we start with files at next level */
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /* finalize our progress messages */
    mfu_progress_complete(&chmod_count, &chmod_prog);

    /* free the array of lists */
    mfu_flist_array_free(levels, &lists);

    /* compute global totals */
    uint64_t global_stats[7] = {0};
    MPI_Allreduce(total_stats, global_stats, 7, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* wait for all tasks and end timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double end_dchmod = MPI_Wtime();

    /* report remove count, time, and rate */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        double time_diff = end_dchmod - start_dchmod;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = ((double)all_count) / time_diff;
        }
        MFU_LOG(MFU_LOG_INFO, "Processed %lu items in %.3lf seconds (%.3lf items/sec) skipped/success/error chown=(%lu/%lu/%lu) chmod=(%lu/%lu/%lu)",
               all_count, time_diff, rate,
               (unsigned long)global_stats[CHOWN_SKIPPED], (unsigned long)global_stats[CHOWN_SUCCESS], (unsigned long)global_stats[CHOWN_FAILURE],
               (unsigned long)global_stats[CHMOD_SKIPPED], (unsigned long)global_stats[CHMOD_SUCCESS], (unsigned long)global_stats[CHMOD_FAILURE]);
    }

    return;
}

/* return a newly allocated chmod structure, set default values on its fields */
mfu_chmod_opts_t* mfu_chmod_opts_new(void)
{
    mfu_chmod_opts_t* opts = (mfu_chmod_opts_t*) MFU_MALLOC(sizeof(mfu_chmod_opts_t));

    /* cache current real user id */
    opts->getuid = getuid();

    /* cache current effective user id,
     * determines uid when considering owner ID of files */
    opts->geteuid = geteuid();

    /* chown with uid==-1 preserves the same owner,
     * default to keeping the owner the same */ 
    opts->uid = -1;

    /* chown with gid==-1 preserves the same group,
     * default to keeping the group the same */ 
    opts->gid = -1;

    /* umask to apply when setting permissions with
     * implied all in symbolic mode, like "+rX" */
    opts->umask = 0;

    /* whether process is running with CAP_CHOWN, allowing
     * changes to uid/gid of file even when effective id
     * of the process does not match the file */
    opts->capchown = false;
#ifdef HAVE_LIBCAP
    int cap_rc = cap_get_bound(CAP_CHOWN);
    if (cap_rc > 0) {
        /* process is running with CAP_CHOWN capability */
        opts->capchown = true;
    }
#endif

    /* whether process is running with CAP_FOWNER, allowing
     * changes to permissions of file even when effective user id
     * of the process does not match the owner of the file */
    opts->capfowner = false;
#ifdef HAVE_LIBCAP
    cap_rc = cap_get_bound(CAP_FOWNER);
    if (cap_rc > 0) {
        /* process is running with CAP_FOWNER capability */
        opts->capfowner = true;
    }
#endif

    /* avoid calling chmod/chown on all items,
     * if this is set to true, call on every item */
    opts->force = false;

    /* when someone is using --force on a directory of
     * files they don't own, they'll get lots of EPERM
     * errors, this option is to disable those */
    opts->silence = false;

    return opts;
}

/* free chmod options allocated from mfu_chmod_opts_new */
void mfu_chmod_opts_delete(mfu_chmod_opts_t** popts)
{
  if (popts != NULL) {
    mfu_free(popts);
  }
}
