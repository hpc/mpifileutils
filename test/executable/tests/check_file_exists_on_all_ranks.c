/*
 * Checks whether file exists at a path for all ranks, and optionally
 * compares file contents with a source file
 *
 * Usage: check_file_exists_on_all_ranks <dest_path> [src_path]
 *
 * Intended as a test for the dbcast executable. Returns 0 if the file exists
 * on every rank (and matches source if provided), or returns 1 otherwise.
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#define BUFFER_SIZE (1024 * 1024)

int compare_files(const char *file1, const char *file2, int rank)
{
    int fd1 = -1, fd2 = -1;
    int result = 0;
    char *buf1 = NULL;
    char *buf2 = NULL;

    /* check file sizes match */
    struct stat st1, st2;
    if (stat(file1, &st1) != 0) {
        fprintf(stderr, "Rank %d: Failed to stat %s (errno=%d %s)\n",
                rank, file1, errno, strerror(errno));
        return 0;
    }
    if (stat(file2, &st2) != 0) {
        fprintf(stderr, "Rank %d: Failed to stat %s (errno=%d %s)\n",
                rank, file2, errno, strerror(errno));
        return 0;
    }

    if (st1.st_size != st2.st_size) {
        printf("Rank %d: File sizes differ (%ld vs %ld)\n",
               rank, (long)st1.st_size, (long)st2.st_size);
        return 0;
    }

    /* open both files */
    fd1 = open(file1, O_RDONLY);
    if (fd1 < 0) {
        fprintf(stderr, "Rank %d: Failed to open %s (errno=%d %s)\n",
                rank, file1, errno, strerror(errno));
        return 0;
    }

    fd2 = open(file2, O_RDONLY);
    if (fd2 < 0) {
        fprintf(stderr, "Rank %d: Failed to open %s (errno=%d %s)\n",
                rank, file2, errno, strerror(errno));
        close(fd1);
        return 0;
    }

    /* compare file contents in buffers */
    buf1 = malloc(BUFFER_SIZE);
    buf2 = malloc(BUFFER_SIZE);
    if (!buf1 || !buf2) {
        fprintf(stderr, "Rank %d: Failed to allocate memory\n", rank);
        goto cleanup;
    }

    off_t total_read = 0;
    off_t file_size = st1.st_size;
    result = 1;

    while (total_read < file_size) {
        size_t to_read = BUFFER_SIZE;
        if (file_size - total_read < BUFFER_SIZE) {
            to_read = file_size - total_read;
        }

        ssize_t n1 = read(fd1, buf1, to_read);
        ssize_t n2 = read(fd2, buf2, to_read);

        if (n1 < 0 || n2 < 0) {
            fprintf(stderr, "Rank %d: Read error at offset %ld (errno=%d %s)\n",
                    rank, (long)total_read, errno, strerror(errno));
            result = 0;
            break;
        }

        if (n1 != n2) {
            printf("Rank %d: Read different amounts at offset %ld (%ld vs %ld)\n",
                   rank, (long)total_read, (long)n1, (long)n2);
            result = 0;
            break;
        }

        /* compare the buffers */
        if (memcmp(buf1, buf2, n1) != 0) {
            printf("Rank %d: File contents differ at offset %ld\n",
                   rank, (long)total_read);
            result = 0;
            break;
        }

        total_read += n1;

        /* check for unexpected EOF */
        if (n1 == 0 && total_read < file_size) {
            fprintf(stderr, "Rank %d: Unexpected EOF at offset %ld\n",
                    rank, (long)total_read);
            result = 0;
            break;
        }
    }

cleanup:
    if (buf1) free(buf1);
    if (buf2) free(buf2);
    if (fd1 >= 0) close(fd1);
    if (fd2 >= 0) close(fd2);

    return result;
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 2) {
        if (rank == 0) {
            fprintf(stderr, "Usage: %s <dest_path> [src_path]\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    const char *dest_path = argv[1];
    const char *src_path = (argc > 2) ? argv[2] : NULL;

    int exists = (access(dest_path, F_OK) == 0);
    int contents_match = 1;

    /* compare files if source path was given */
    if (src_path != NULL && exists) {
        contents_match = compare_files(dest_path, src_path, rank);
    }

    int local_result = exists && contents_match;

    int *results = NULL;

    if (rank == 0) {
        results = malloc(size * sizeof(*results));
        if (results == NULL) {
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Gather(&local_result, 1, MPI_INT, results, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int return_code = 0;

    if (rank == 0) {
        for (int i = 0; i < size; i++) {
            if (!results[i]) {
                return_code = 1;
            }
        }

        free(results);
    }

    MPI_Bcast(&return_code, 1, MPI_INT, 0, MPI_COMM_WORLD);

    MPI_Finalize();

    return return_code;
}
