/* Implements logic to deal with translation between user id / username and group id / groupname */

#define _GNU_SOURCE
#include <dirent.h>
#include <fcntl.h>
#include <sys/syscall.h>

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
#include <regex.h>

/* These headers are needed to query the Lustre MDS for stat
 * information.  This information may be incomplete, but it
 * is faster than a normal stat, which requires communication
 * with the MDS plus every OST a file is striped across. */
//#define LUSTRE_STAT
#ifdef LUSTRE_STAT
#include <sys/ioctl.h>
#include <lustre/lustre_user.h>
#endif /* LUSTRE_STAT */

#include <pwd.h> /* for getpwent */
#include <grp.h> /* for getgrent */
#include <errno.h>
#include <string.h>

#include <libgen.h> /* dirname */

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"
#include "mfu_flist_internal.h"
#include "strmap.h"

/****************************************
 * Functions on types
 ***************************************/

static void buft_init(buf_t* items)
{
    items->buf     = NULL;
    items->bufsize = 0;
    items->count   = 0;
    items->chars   = 0;
    items->dt      = MPI_DATATYPE_NULL;
}

static void buft_copy(buf_t* src, buf_t* dst)
{
    dst->bufsize = src->bufsize;
    dst->buf = MFU_MALLOC(dst->bufsize);
    memcpy(dst->buf, src->buf, dst->bufsize);

    dst->count = src->count;
    dst->chars = src->chars;

    if (src->dt != MPI_DATATYPE_NULL) {
        MPI_Type_dup(src->dt, &dst->dt);
    }
    else {
        dst->dt = MPI_DATATYPE_NULL;
    }
}

static void buft_free(buf_t* items)
{
    mfu_free(&items->buf);
    items->bufsize = 0;

    if (items->dt != MPI_DATATYPE_NULL) {
        MPI_Type_free(&(items->dt));
    }

    items->count = 0;
    items->chars = 0;

    return;
}

/* build a name-to-id map and an id-to-name map */
void mfu_flist_usrgrp_create_map(const buf_t* items, strmap* id2name)
{
    uint64_t i;
    const char* ptr = (const char*)items->buf;
    for (i = 0; i < items->count; i++) {
        const char* name = ptr;
        ptr += items->chars;

        uint64_t id;
        mfu_unpack_uint64(&ptr, &id);

        /* convert id number to string */
        char id_str[32];
        int len_int = snprintf(id_str, sizeof(id_str), "%llu", (unsigned long long) id);
        if (len_int < 0) {
            /* TODO: ERROR! */
            MFU_LOG(MFU_LOG_ERR, "Failed to convert id %llu to string, ret=%d", (unsigned long long) id, len_int);
        }

        size_t len = (size_t) len_int;
        if (len > (sizeof(id_str) - 1)) {
            /* TODO: ERROR! */
            MFU_LOG(MFU_LOG_ERR, "Converting id %llu to string needs buffer of at least %d chars", (unsigned long long) id, len_int+1);
        }

        strmap_set(id2name, id_str, name);
    }
    return;
}

/* given an id, lookup its corresponding name, returns id converted
 * to a string if no matching name is found */
const char* mfu_flist_usrgrp_get_name_from_id(strmap* id2name, uint64_t id)
{
    /* convert id number to string representation */
    char id_str[32];
    int len_int = snprintf(id_str, sizeof(id_str), "%llu", (unsigned long long) id);
    if (len_int < 0) {
        /* TODO: ERROR! */
        MFU_LOG(MFU_LOG_ERR, "Failed to convert id %llu to string, ret=%d", (unsigned long long) id, len_int);
    }

    size_t len = (size_t) len_int;
    if (len > (sizeof(id_str) - 1)) {
        /* TODO: ERROR! */
        MFU_LOG(MFU_LOG_ERR, "Converting id %llu to string needs buffer of at least %d chars", (unsigned long long) id, len_int+1);
    }

    /* lookup name by id */
    const char* name = strmap_get(id2name, id_str);

    /* if not found, store id as name and return that */
    if (name == NULL) {
        strmap_set(id2name, id_str, id_str);
        name = strmap_get(id2name, id_str);
    }

    return name;
}

/****************************************
 * Functions to read user and group info
 ***************************************/

/* create a type consisting of chars number of characters
 * immediately followed by a uint32_t */
void mfu_flist_usrgrp_create_stridtype(int chars, MPI_Datatype* dt)
{
    /* build type for string */
    MPI_Datatype dt_str;
    MPI_Type_contiguous(chars, MPI_CHAR, &dt_str);

    /* build keysat type */
    MPI_Datatype types[2];
    types[0] = dt_str;       /* file name */
    types[1] = MPI_UINT64_T; /* id */
    DTCMP_Type_create_series(2, types, dt);

    MPI_Type_free(&dt_str);
    return;
}

/* element for a linked list of name/id pairs */
typedef struct strid {
    char* name;
    uint64_t id;
    struct strid* next;
} strid_t;

/* insert specified name and id into linked list given by
 * head, tail, and count, also increase maxchars if needed */
static void strid_insert(
    const char* name,
    uint64_t id,
    strid_t** head,
    strid_t** tail,
    int* count,
    int* maxchars)
{
    /* allocate and fill in new element */
    strid_t* elem = (strid_t*) malloc(sizeof(strid_t));
    elem->name = MFU_STRDUP(name);
    elem->id   = id;
    elem->next = NULL;

    /* insert element into linked list */
    if (*head == NULL) {
        *head = elem;
    }
    if (*tail != NULL) {
        (*tail)->next = elem;
    }
    *tail = elem;
    (*count)++;

    /* increase maximum name if we need to */
    size_t len = strlen(name) + 1;
    if (*maxchars < (int)len) {
        /* round up to nearest multiple of 8 */
        size_t len8 = len / 8;
        if (len8 * 8 < len) {
            len8++;
        }
        len8 *= 8;

        *maxchars = (int)len8;
    }

    return;
}

/* copy data from linked list to array */
static void strid_serialize(strid_t* head, int chars, void* buf)
{
    char* ptr = (char*)buf;
    strid_t* current = head;
    while (current != NULL) {
        char* name  = current->name;
        uint64_t id = current->id;

        strcpy(ptr, name);
        ptr += chars;

        mfu_pack_uint64(&ptr, id);

        current = current->next;
    }
    return;
}

/* delete linked list and reset head, tail, and count values */
static void strid_delete(strid_t** head, strid_t** tail, int* count)
{
    /* free memory allocated in linked list */
    strid_t* current = *head;
    while (current != NULL) {
        strid_t* next = current->next;
        mfu_free(&current->name);
        mfu_free(&current);
        current = next;
    }

    /* set list data structure values back to NULL */
    *head  = NULL;
    *tail  = NULL;
    *count = 0;

    return;
}

/* read user array from file system using getpwent() */
void mfu_flist_usrgrp_get_users(flist_t* flist)
{
    /* get pointer to users buf_t */
    buf_t* items = &flist->users;

    /* initialize output parameters */
    buft_init(items);

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* rank 0 iterates over users with getpwent */
    strid_t* head = NULL;
    strid_t* tail = NULL;
    int count = 0;
    int chars = 0;
    if (rank == 0) {
        struct passwd* p;
        while (1) {
            /* get next user, this function can fail so we retry a few times */
            int retries = 3;
retry:
            p = getpwent();
            if (p == NULL) {
                if (errno == EIO) {
                    retries--;
                }
                else {
                    /* TODO: ERROR! */
                    retries = 0;
                }
                if (retries > 0) {
                    goto retry;
                }
            }

            if (p != NULL) {
                /*
                printf("User=%s Pass=%s UID=%d GID=%d Name=%s Dir=%s Shell=%s\n",
                  p->pw_name, p->pw_passwd, p->pw_uid, p->pw_gid, p->pw_gecos, p->pw_dir, p->pw_shell
                );
                printf("User=%s UID=%d GID=%d\n",
                  p->pw_name, p->pw_uid, p->pw_gid
                );
                */
                char* name  = p->pw_name;
                uint64_t id = p->pw_uid;
                strid_insert(name, id, &head, &tail, &count, &chars);
            }
            else {
                /* hit the end of the user list */
                endpwent();
                break;
            }
        }

        //    printf("Max username %d, count %d\n", (int)chars, count);
    }

    /* bcast count and number of chars */
    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&chars, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* create datatype to represent a username/id pair */
    MPI_Datatype dt;
    mfu_flist_usrgrp_create_stridtype(chars, &dt);

    /* get extent of type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* allocate an array to hold all user names and ids */
    size_t bufsize = (size_t)count * (size_t)extent;
    char* buf = (char*) MFU_MALLOC(bufsize);

    /* copy items from list into array */
    if (rank == 0) {
        strid_serialize(head, chars, buf);
    }

    /* broadcast the array of usernames and ids */
    MPI_Bcast(buf, count, dt, 0, MPI_COMM_WORLD);

    /* set output parameters */
    items->buf     = buf;
    items->bufsize = bufsize;
    items->count   = (uint64_t) count;
    items->chars   = (uint64_t) chars;
    items->dt      = dt;

    /* delete the linked list */
    if (rank == 0) {
        strid_delete(&head, &tail, &count);
    }

    /* create map of user id to user name */
    mfu_flist_usrgrp_create_map(items, flist->user_id2name);
    flist->have_users = 1;

    return;
}

/* read group array from file system using getgrent() */
void mfu_flist_usrgrp_get_groups(flist_t* flist)
{
    /* get pointer to users buf_t */
    buf_t* items = &flist->groups;

    /* initialize output parameters */
    buft_init(items);

    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* rank 0 iterates over users with getpwent */
    strid_t* head = NULL;
    strid_t* tail = NULL;
    int count = 0;
    int chars = 0;
    if (rank == 0) {
        struct group* p;
        while (1) {
            /* get next user, this function can fail so we retry a few times */
            int retries = 3;
retry:
            p = getgrent();
            if (p == NULL) {
                if (errno == EIO || errno == EINTR) {
                    retries--;
                }
                else {
                    /* TODO: ERROR! */
                    retries = 0;
                }
                if (retries > 0) {
                    goto retry;
                }
            }

            if (p != NULL) {
                /*
                        printf("Group=%s GID=%d\n",
                          p->gr_name, p->gr_gid
                        );
                */
                char* name  = p->gr_name;
                uint64_t id = p->gr_gid;
                strid_insert(name, id, &head, &tail, &count, &chars);
            }
            else {
                /* hit the end of the group list */
                endgrent();
                break;
            }
        }

        //    printf("Max groupname %d, count %d\n", chars, count);
    }

    /* bcast count and number of chars */
    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&chars, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* create datatype to represent a username/id pair */
    MPI_Datatype dt;
    mfu_flist_usrgrp_create_stridtype(chars, &dt);

    /* get extent of type */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(dt, &lb, &extent);

    /* allocate an array to hold all user names and ids */
    size_t bufsize = (size_t)count * (size_t)extent;
    char* buf = (char*) MFU_MALLOC(bufsize);

    /* copy items from list into array */
    if (rank == 0) {
        strid_serialize(head, chars, buf);
    }

    /* broadcast the array of usernames and ids */
    MPI_Bcast(buf, count, dt, 0, MPI_COMM_WORLD);

    /* set output parameters */
    items->buf     = buf;
    items->bufsize = bufsize;
    items->count   = (uint64_t) count;
    items->chars   = (uint64_t) chars;
    items->dt      = dt;

    /* delete the linked list */
    if (rank == 0) {
        strid_delete(&head, &tail, &count);
    }

    /* create map of user id to user name */
    mfu_flist_usrgrp_create_map(items, flist->group_id2name);
    flist->have_groups = 1;

    return;
}

/* initialize structures for user and group names and id-to-name maps */
void mfu_flist_usrgrp_init(flist_t* flist)
{
    /* initialize user, group, and file buffers */
    buft_init(&flist->users);
    buft_init(&flist->groups);

    /* allocate memory for maps */
    flist->have_users  = 0;
    flist->have_groups = 0;
    flist->user_id2name  = strmap_new();
    flist->group_id2name = strmap_new();

    return;
}

/* free user and group structures */
void mfu_flist_usrgrp_free(flist_t* flist)
{
    buft_free(&flist->users);
    buft_free(&flist->groups);

    strmap_delete(&flist->user_id2name);
    strmap_delete(&flist->group_id2name);

    return;
}

/* copy user and group structures from srclist to flist */
void mfu_flist_usrgrp_copy(flist_t* srclist, flist_t* flist)
{
    buft_copy(&srclist->users, &flist->users);
    buft_copy(&srclist->groups, &flist->groups);
    strmap_merge(flist->user_id2name, srclist->user_id2name);
    strmap_merge(flist->group_id2name, srclist->group_id2name);
    flist->have_users  = 1;
    flist->have_groups = 1;

    return; 
}
