#include "dtar.h"
#include "log.h"
#include "helper.h"
#include <archive.h>
#include <archive_entry.h>

#include <dirent.h>
#include <errno.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/time.h>
#include <mpi.h>

/** Options specified by the user. */
extern DTAR_options_t DTAR_user_opts;
extern DTAR_writer_t DTAR_writer;
extern MPI_Comm inter_comm;
extern int xattr_flag;

int HDR_LENGTH = 1536;

inline static struct archive * new_archive() {

    char compress = DTAR_user_opts.compress;
    struct archive *a = archive_write_new();

    switch (compress) {
    case 'j':
    case 'y':
        archive_write_add_filter_bzip2(a);
        break;
    case 'Z':
        archive_write_add_filter_compress(a);
        break;
    case 'z':
        archive_write_add_filter_gzip(a);
        break;
    default:
        archive_write_add_filter_none(a);
        break;
    }

    int r = archive_write_set_format_pax(a);

    if ( r != ARCHIVE_OK ) {
        LOG(DTAR_LOG_ERR, archive_error_string(a));
        return NULL;
    }

    archive_write_set_bytes_per_block(a, 0);

    return a;
}

inline static int write_header(off64_t offset, DTAR_operation_t * op) {

    char* bname = op->operand;
    char* dname = op->dir;
    char dir[PATH_MAX];
    struct stat st;
    int fd, ret;
    struct archive *ard;

    if (getcwd(dir, PATH_MAX) == NULL) {
        printf("can not getcwd in write_header\n");
        return -1;
    }

    LOG(DTAR_LOG_INFO, "op = [%s], dir = [%s], base = [%s], pwd = [%s]\n",
            op->operand, dname, bname, dir);

    ret = chdir(dname);
    if (ret != 0) {
        printf("can not chdir in write_header\n");
        return -1;
    }

    struct archive_entry *entry = archive_entry_new();
    archive_entry_copy_pathname(entry, bname);

    if (xattr_flag) {
        fd = open(bname, O_RDONLY); // Error check?
        ard = archive_read_disk_new();
        archive_read_disk_set_standard_lookup(ard);
        archive_read_disk_entry_from_file(ard, entry, fd, NULL);
        archive_read_free(ard);

    } else {
        stat(bname, &st); // Error check?
        archive_entry_copy_stat(entry, &st);
        archive_entry_set_uname(entry, userNameFromId(st.st_uid));
        archive_entry_set_gname(entry, groupNameFromId(st.st_gid));

    }

    struct archive * a = new_archive();
    archive_write_open_fd(a, DTAR_writer.fd_tar);
    off64_t cur_pos = lseek64(DTAR_writer.fd_tar, offset, SEEK_SET);

    if (cur_pos < 0) {
        printf("Cannot seek in treewalk.c offset is %d\n", offset);
        return -1;
    }

    printf("rank %d file %s header offset is %x  current pos is %x\n",
            CIRCLE_global_rank, op->operand, offset, cur_pos);

    archive_write_header(a, entry);

    // TODO: this may be not necessary
    fsync(DTAR_writer.fd_tar);

    // free resources, why you can't free archive?
    archive_entry_free(entry);
    // archive_write_free(a);

    ret = chdir(dir);
    return 0;
}

void DTAR_do_treewalk(DTAR_operation_t* op, CIRCLE_handle* handle) {
    struct stat64 statbuf;

    char path[PATH_MAX];
    strcpy(path, op->dir);
    strcat(path, "/");
    strcat(path, op->operand);

    printf("rank %d, path is %s\n", CIRCLE_global_rank, path);

    if (lstat64(path, &statbuf) < 0) {
        LOG(DTAR_LOG_DBG, "Could not get info for `%s'. errno=%d %s",
                op->operand, errno, strerror(errno));
        return;
    }

    /* first check that we handle this file type */
    if (! S_ISDIR(statbuf.st_mode) && ! S_ISREG(statbuf.st_mode)
            && ! S_ISLNK(statbuf.st_mode)) {
        if (S_ISCHR(statbuf.st_mode)) {
            LOG(DTAR_LOG_ERR,
                    "Encountered an unsupported file type S_ISCHR at `%s'.",
                    op->operand);
        } else if (S_ISBLK(statbuf.st_mode)) {
            LOG(DTAR_LOG_ERR,
                    "Encountered an unsupported file type S_ISBLK at `%s'.",
                    op->operand);
        } else if (S_ISFIFO(statbuf.st_mode)) {
            LOG(DTAR_LOG_ERR,
                    "Encountered an unsupported file type S_ISFIFO at `%s'.",
                    op->operand);
        } else if (S_ISSOCK(statbuf.st_mode)) {
            LOG(DTAR_LOG_ERR,
                    "Encountered an unsupported file type S_ISSOCK at `%s'.",
                    op->operand);
        } else {
            LOG(DTAR_LOG_ERR,
                    "Encountered an unsupported file type %x at `%s'.",
                    statbuf.st_mode, op->operand);
        }
        return;
    }

    if (S_ISDIR(statbuf.st_mode)) {
        /* LOG(DTAR_LOG_DBG, "Stat operation found a directory at `%s'.", op->operand); */
        DTAR_stat_process_dir(op, &statbuf, handle);
    } else if (S_ISREG(statbuf.st_mode)) {
        /* LOG(DTAR_LOG_DBG, "Stat operation found a file at `%s'.", op->operand); */
        DTAR_stat_process_file(op, &statbuf, handle);
    } else if (S_ISLNK(statbuf.st_mode)) {
        /* LOG(DTAR_LOG_DBG, "Stat operation found a link at `%s'.", op->operand); */
        DTAR_stat_process_link(op, &statbuf, handle);
    } else {
        LOG(DTAR_LOG_ERR, "Encountered an unsupported file type %x at `%s'.",
                statbuf.st_mode, op->operand);
        return;
    }
}

/**
 * This function copies a link.
 */
void DTAR_stat_process_link(DTAR_operation_t* op, const struct stat64* statbuf,
        CIRCLE_handle* handle) {

    int64_t buffer[2];
    off64_t offset = 0;
    MPI_Status stat;
    buffer[0] = (int64_t) CIRCLE_global_rank;
    buffer[1] = HDR_LENGTH;

    MPI_Send(buffer, 2, MPI_LONG_LONG, 0, 0, inter_comm);
    MPI_Recv(&offset, 1, MPI_LONG_LONG, 0, 0, inter_comm, &stat);

    printf("rank %d, offset is %d\n", CIRCLE_global_rank, offset);
    write_header(offset, op);
}

void DTAR_stat_process_file(DTAR_operation_t* op, const struct stat64* statbuf,
        CIRCLE_handle* handle) {
    int64_t file_size = statbuf->st_size;
    int64_t chunk_index;
    int64_t num_chunks = file_size / DTAR_CHUNK_SIZE;
    int64_t buffer[2];
    off64_t offset = 0;
    MPI_Status stat;

    buffer[0] = (int64_t) CIRCLE_global_rank;

    int64_t rem = (file_size) % 512;
    if (rem == 0) {
        buffer[1] = file_size + HDR_LENGTH;
    } else {
        buffer[1] = (file_size / 512  + 4) * 512;
    }

    MPI_Send(buffer, 2, MPI_LONG_LONG, 0, 0, inter_comm);
    MPI_Recv(&offset, 1, MPI_LONG_LONG, 0, 0, inter_comm, &stat);

    printf("rank %d, offset is %d\n", CIRCLE_global_rank, offset);

    write_header(offset, op);

    op->offset = offset + HDR_LENGTH;

    printf("rank %d file %s data:%x entry:%x hex_entry:%x\n",
            CIRCLE_global_rank, op->operand, op->offset, offset, offset);
    for (chunk_index = 0; chunk_index < num_chunks; chunk_index++) {
        char* newop = DTAR_encode_operation(COPY, chunk_index, op->operand,
                op->offset, file_size, op->dir);
        handle->enqueue(newop);
        free(newop);
    }

    /* Encode and enqueue the last partial chunk. */
    if ((num_chunks * DTAR_CHUNK_SIZE) < file_size || num_chunks == 0) {
        char* newop = DTAR_encode_operation(COPY, chunk_index, op->operand,
                op->offset, file_size, op->dir);
        handle->enqueue(newop);
        free(newop);
    }
}

void DTAR_stat_process_dir(DTAR_operation_t* op, const struct stat64* statbuf,
        CIRCLE_handle* handle) {
    int64_t buffer[2];
    off64_t offset = 0;
    MPI_Status stat;

    buffer[0] = (int64_t) CIRCLE_global_rank;
    buffer[1] = HDR_LENGTH;
    MPI_Send(buffer, 2, MPI_LONG_LONG, 0, 0, inter_comm);
    MPI_Recv(&offset, 1, MPI_LONG_LONG, 0, 0, inter_comm, &stat);
    write_header(offset, op);

    DIR* curr_dir;
    char* curr_dir_name;
    char* newop;

    struct dirent* curr_ent;
    char newop_path[PATH_MAX];

    char path[PATH_MAX];
    strcpy(path, op->dir);
    strcat(path, "/");
    strcat(path, op->operand);

    curr_dir = opendir(path);

    if (curr_dir == NULL) {
        LOG(DTAR_LOG_ERR, "Unable to open dir `%s'. errno=%d %s", op->operand,
                errno, strerror(errno));
        return;
    } else {
        while ((curr_ent = readdir(curr_dir)) != NULL) {
            curr_dir_name = curr_ent->d_name;

            if ((strncmp(curr_dir_name, ".", 2))
                    && (strncmp(curr_dir_name, "..", 3))) {
                sprintf(newop_path, "%s/%s", op->operand, curr_dir_name);
                LOG(DTAR_LOG_DBG, "Stat operation is enqueueing `%s'",
                        newop_path);
                newop = DTAR_encode_operation(TREEWALK, 0, newop_path, 0, 0,
                        op->dir);
                handle->enqueue(newop);
                free(newop);
            }
        }
    }

    closedir(curr_dir);

    return;
}

/* EOF */
