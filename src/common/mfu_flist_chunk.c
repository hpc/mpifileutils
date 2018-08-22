#include <stdio.h>
#include <stdint.h>
#include <unistd.h>

#include <string.h>

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"

/****************************************
 * Functions to divide flist into linked list of file sections
 ***************************************/

/* given a file offset, the rank of the last process to hold an
 * extra chunk, and the number of chunks per rank, compute
 * and return the rank of the chunk at the given offset */
static int map_chunk_to_rank(uint64_t offset, uint64_t cutoff, uint64_t chunks_per_rank)
{
    /* total number of chunks held by ranks below cutoff */
    uint64_t cutoff_coverage = cutoff * (chunks_per_rank + 1);

    /* given an offset of a chunk, identify which rank will
     * be responsible */
    int rank;
    if (offset < cutoff_coverage) {
        rank = (int) (offset / (chunks_per_rank + 1));
    } else {
        rank = (int) (cutoff + (offset - cutoff_coverage) / chunks_per_rank);
    }

    return rank;
}

/* This is a long routine, but the idea is simple.  All tasks sum up
 * the number of file chunks they have, and those are then evenly
 * distributed amongst the processes.  */
mfu_file_chunk* mfu_file_chunk_list_alloc(mfu_flist list, uint64_t chunk_size)
{
    /* get our rank and number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* total up number of file chunks for all files in our list */
    uint64_t count = 0;
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        /* get type of item */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);

        /* if we have a file, add up its chunks */
        if (type == MFU_TYPE_FILE) {
            /* get size of file */
            uint64_t file_size = mfu_flist_file_get_size(list, idx);

            /* compute number of chunks to copy for this file */
            uint64_t chunks = file_size / chunk_size;
            if (chunks * chunk_size < file_size || file_size == 0) {
                /* this accounts for the last chunk, which may be
                 * partial or it adds a chunk for 0-size files */
                chunks++;
            }

            /* include these chunks in our total */
            count += chunks;
        }
    }

    /* compute total number of chunks across procs */
    uint64_t total;
    MPI_Allreduce(&count, &total, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* get global offset of our first chunk */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }

    /* compute number of chunks per task, ranks below cutoff will
     * be responsible for (chunks_per_rank+1) and ranks at cutoff
     * and above are responsible for chunks_per_rank */
    uint64_t chunks_per_rank = total / (uint64_t) ranks;
    uint64_t coverage = chunks_per_rank * (uint64_t) ranks;
    uint64_t cutoff = total - coverage;

    /* TODO: replace this with DSDE */

    /* allocate an array of integers to use in alltoall,
     * we'll set a flag to 1 if we have data for that rank
     * and set it to 0 otherwise, then we'll exchange flags
     * with an alltoall */
    int* sendlist = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recvlist = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));

    /* assume we won't send to any ranks,
     * so initialize all ranks to 0 */
    int i;
    for (i = 0; i < ranks; i++) {
        sendlist[i] = 0;
    }

    /* if we have some chunks, figure out the number of ranks
     * we'll send to and the range of rank ids, set flags to 1 */
    int send_ranks = 0;
    int first_send_rank, last_send_rank;
    if (count > 0) {
        /* compute first rank we'll send data to */
        first_send_rank = map_chunk_to_rank(offset, cutoff, chunks_per_rank);

        /* compute last rank we'll send to */
        uint64_t last_offset = offset + count - 1;
        last_send_rank  = map_chunk_to_rank(last_offset, cutoff, chunks_per_rank);

        /* set flag for each process we'll send data to */
        for (i = first_send_rank; i <= last_send_rank; i++) {
            sendlist[i] = 1;
        }

        /* compute total number of destinations we'll send to */
        send_ranks = last_send_rank - first_send_rank + 1;
    }

    /* allocate a linked list for each process we'll send to */
    mfu_file_chunk** heads = (mfu_file_chunk**) MFU_MALLOC((size_t)send_ranks * sizeof(mfu_file_chunk*));
    mfu_file_chunk** tails = (mfu_file_chunk**) MFU_MALLOC((size_t)send_ranks * sizeof(mfu_file_chunk*));
    uint64_t* counts  = (uint64_t*)   MFU_MALLOC((size_t)send_ranks * sizeof(uint64_t));
    uint64_t* bytes   = (uint64_t*)   MFU_MALLOC((size_t)send_ranks * sizeof(uint64_t));
    char** sendbufs   = (char**)      MFU_MALLOC((size_t)send_ranks * sizeof(char*));

    /* initialize values */
    for (i = 0; i < send_ranks; i++) {
        heads[i]    = NULL;
        tails[i]    = NULL;
        counts[i]   = 0;
        bytes[i]    = 0;
        sendbufs[i] = NULL;
    }

    /* now iterate through files and build up list of chunks we'll
     * send to each task, as an optimization, we encode consecutive
     * chunks of the same file into a single unit */
    uint64_t current_offset = offset;
    for (idx = 0; idx < size; idx++) {
        /* get type of item */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);

        /* if we have a file, add up its chunks */
        if (type == MFU_TYPE_FILE) {
            /* get size of file */
            uint64_t file_size = mfu_flist_file_get_size(list, idx);

            /* compute number of chunks to copy for this file */
            uint64_t chunks = file_size / chunk_size;
            if (chunks * chunk_size < file_size || file_size == 0) {
                chunks++;
            }

            /* iterate over each chunk of this file and determine the
             * rank we should send it to */
            int prev_rank = MPI_PROC_NULL;
            uint64_t chunk_id;
            for (chunk_id = 0; chunk_id < chunks; chunk_id++) {
                /* determine which rank we should map this chunk to */
                int current_rank = map_chunk_to_rank(current_offset, cutoff, chunks_per_rank);

                /* compute index into our send_ranks arrays */
                int rank_index = current_rank - first_send_rank;

                /* if this chunk goes to a rank we've already created
                 * an element for, just update that element, otherwise
                 * create a new element */
                if (current_rank == prev_rank) {
                    /* we've already got an element started for this
                     * file and rank, just update its count field to
                     * append this element */
                    mfu_file_chunk* elem = tails[rank_index];
                    elem->length += chunk_size;

                    /* adjusting length in case chunk is a partial chunk */
                    uint64_t remainder = file_size - elem->offset;
                    if (remainder < elem->length) {
                        elem->length = remainder;
                    }
                } else {
                    /* we're sending to a new rank or have the start
                     * of a new file, either way allocate a new element */
                    mfu_file_chunk* elem = (mfu_file_chunk*) MFU_MALLOC(sizeof(mfu_file_chunk));
                    elem->name             = mfu_flist_file_get_name(list, idx);
                    elem->offset           = chunk_id * chunk_size;
                    elem->length           = chunk_size;
                    elem->file_size        = file_size;
                    elem->rank_of_owner    = rank;
                    elem->index_of_owner   = idx;
                    elem->next             = NULL;

                    /* adjusting length in case chunk is a partial chunk */
                    uint64_t remainder = file_size - elem->offset;
                    if (remainder < elem->length) {
                        elem->length = remainder;
                    }

                    /* compute bytes needed to pack this item,
                     * full name NUL-terminated, chunk id,
                     * number of chunks, and file size */
                    size_t pack_size = strlen(elem->name) + 1;
                    pack_size += 5 * 8;

                    /* append element to list */
                    if (heads[rank_index] == NULL) {
                        heads[rank_index] = elem;
                    }
                    if (tails[rank_index] != NULL) {
                        tails[rank_index]->next = elem;
                    }
                    tails[rank_index] = elem;
                    counts[rank_index]++;
                    bytes[rank_index] += pack_size;

                    /* remember which rank we're sending to */
                    prev_rank = current_rank;
                }

                /* go on to our next chunk */
                current_offset++;
            }
        }
    }

    /* exchange flags with ranks so everyone knows who they'll
     * receive data from */
    MPI_Alltoall(sendlist, 1, MPI_INT, recvlist, 1, MPI_INT, MPI_COMM_WORLD);

    /* determine number of ranks that will send to us */
    int first_recv_rank = MPI_PROC_NULL;
    int recv_ranks = 0;
    for (i = 0; i < ranks; i++) {
        if (recvlist[i]) {
            /* record the first rank we'll receive from */
            if (first_recv_rank == MPI_PROC_NULL) {
                first_recv_rank = i;
            }

            /* increase our count of procs to send to us */
            recv_ranks++;
        }
    }
    
    /* build the list of ranks to receive from */
    int *recvranklist = (int *)MFU_MALLOC(sizeof(int) * recv_ranks);
    int recv_count = 0;
    for (i = 0; i < ranks; i++) {
        if (recvlist[i]) {
            recvranklist[recv_count] = i;
            recv_count++;
        }
    }
 
    /* determine number of messages we'll have outstanding */
    int msgs = send_ranks + recv_ranks;
    MPI_Request* request = (MPI_Request*) MFU_MALLOC((size_t)msgs * sizeof(MPI_Request));
    MPI_Status*  status  = (MPI_Status*)  MFU_MALLOC((size_t)msgs * sizeof(MPI_Status));

    /* create storage to hold byte counts that we'll send
     * and receive, it would be best to use uint64_t here
     * but for that, we'd need to create a datatypen,
     * with an int, we should be careful we don't overflow */
    int* send_counts = (int*) MFU_MALLOC((size_t)send_ranks * sizeof(int));
    int* recv_counts = (int*) MFU_MALLOC((size_t)recv_ranks * sizeof(int));

    /* initialize our send counts */
    for (i = 0; i < send_ranks; i++) {
        /* TODO: check that we don't overflow here */

        send_counts[i] = (int) bytes[i];
    }

    /* post irecv to get sizes */
    for (i = 0; i < recv_ranks; i++) {
        int recv_rank = recvranklist[i];
        MPI_Irecv(&recv_counts[i], 1, MPI_INT, recv_rank, 0, MPI_COMM_WORLD, &request[i]);
    }

    /* post isend to send sizes */
    for (i = 0; i < send_ranks; i++) {
        int req_id = recv_ranks + i;
        int send_rank = first_send_rank + i;
        MPI_Isend(&send_counts[i], 1, MPI_INT, send_rank, 0, MPI_COMM_WORLD, &request[req_id]);
    }

    /* wait for sizes to come in */
    MPI_Waitall(msgs, request, status);

    /* allocate memory and encode lists for sending */
    for (i = 0; i < send_ranks; i++) {
        /* allocate buffer for this destination */
        size_t sendbuf_size = (size_t) bytes[i];
        sendbufs[i] = (char*) MFU_MALLOC(sendbuf_size);

        /* pack data into buffer */
        char* sendptr = sendbufs[i];
        mfu_file_chunk* elem = heads[i];
        while (elem != NULL) {
            /* pack file name */
            strcpy(sendptr, elem->name);
            sendptr += strlen(elem->name) + 1;

            /* pack chunk id, count, and file size */
            mfu_pack_uint64(&sendptr, elem->offset);
            mfu_pack_uint64(&sendptr, elem->length);
            mfu_pack_uint64(&sendptr, elem->file_size);
            mfu_pack_uint64(&sendptr, elem->rank_of_owner);
            mfu_pack_uint64(&sendptr, elem->index_of_owner);

            /* go to next element */
            elem = elem->next;
        }
    }

    /* sum up total bytes that we'll receive */
    size_t recvbuf_size = 0;
    for (i = 0; i < recv_ranks; i++) {
        recvbuf_size += (size_t) recv_counts[i];
    }

    /* allocate memory for recvs */
    char* recvbuf = (char*) MFU_MALLOC(recvbuf_size);

    /* post irecv for incoming data */
    char* recvptr = recvbuf;
    for (i = 0; i < recv_ranks; i++) {
        int recv_count = recv_counts[i];
        int recv_rank = recvranklist[i];
        MPI_Irecv(recvptr, recv_count, MPI_BYTE, recv_rank, 0, MPI_COMM_WORLD, &request[i]);
        recvptr += recv_count;
    }

    /* post isend to send outgoing data */
    for (i = 0; i < send_ranks; i++) {
        int req_id = recv_ranks + i;
        int send_rank = first_send_rank + i;
        int send_count = send_counts[i];
        MPI_Isend(sendbufs[i], send_count, MPI_BYTE, send_rank, 0, MPI_COMM_WORLD, &request[req_id]);
    }

    /* waitall */
    MPI_Waitall(msgs, request, status);

    mfu_file_chunk* head = NULL;
    mfu_file_chunk* tail = NULL;

    /* iterate over all received data */
    const char* packptr = recvbuf;
    char* recvbuf_end = recvbuf + recvbuf_size;
    while (packptr < recvbuf_end) {
        /* unpack file name */
        const char* name = packptr;
        packptr += strlen(name) + 1;

        /* unpack chunk offset, count, and file size */
        uint64_t offset, length, file_size, rank_of_owner, index_of_owner;
        mfu_unpack_uint64(&packptr, &offset);
        mfu_unpack_uint64(&packptr, &length);
        mfu_unpack_uint64(&packptr, &file_size);
        mfu_unpack_uint64(&packptr, &rank_of_owner);
        mfu_unpack_uint64(&packptr, &index_of_owner);

        /* allocate memory for new struct and set next pointer to null */
        mfu_file_chunk* p = malloc(sizeof(mfu_file_chunk));
        p->next = NULL;

        /* set the fields of the struct */
        p->name = strdup(name);
        p->offset = offset;
        p->length = length;
        p->file_size = file_size;
        p->rank_of_owner = rank_of_owner;
        p->index_of_owner = index_of_owner;

        /* if the tail is not null then point the tail at the latest struct */
        if (tail != NULL) {
            tail->next = p;
        }
        
        /* if head is not pointing at anything then this struct is head of list */
        if (head == NULL) {
            head = p;
        }

        /* have tail point at the current/last struct */
        tail = p;
    }

    return head;
}

/* free the linked list of structs (copy elem's) */
void mfu_file_chunk_list_free(mfu_file_chunk** phead)
{
    /* check whether we were given a pointer */
    if (phead != NULL) {
        /* free the linked list of structs (mfu_file_chunk) */
        mfu_file_chunk* tmp;
        mfu_file_chunk* current = *phead;
        while (current != NULL) {
            /* get pointer to current element and advance current */
            tmp = current;
            current = current->next;

            /* free the name string we had strdup'd */
            mfu_free(&tmp->name);

            /* free the element */
            mfu_free(&tmp);
        }

        /* set caller's pointer to NULL to indicate it's freed */
        *phead = NULL;
    }

    return;
}
