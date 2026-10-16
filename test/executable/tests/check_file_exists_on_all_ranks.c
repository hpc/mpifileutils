/*
 * Checks whether file exists at a path for all ranks
 *
 * Intented as a test for the dbcast executable. Returns 0 if the file exists
 * on every rank, or returns 1 otherwise.
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    const char *path = argv[1];
    int exists = (access(path, F_OK) == 0);

    int *results = NULL;

    if (rank == 0) {
        results = malloc(size * sizeof(*results));
        if (results == NULL) {
            MPI_Abort(MPI_COMM_WORLD, 2);
        }
    }

    MPI_Gather(&exists, 1, MPI_INT, results, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int return_code = 0;

    if (rank == 0) {
        for (int i = 0; i < size; i++) {
            printf("Rank %d: %s\n", i, results[i] ? "file exists" : "file missing");

            if (!results[i]) {
                return_code = 1;
            }
        }

        printf("File exists on every rank: %s\n", return_code == 0 ? "yes" : "no");

        free(results);
    }

    MPI_Bcast(&return_code, 1, MPI_INT, 0, MPI_COMM_WORLD);

    MPI_Finalize();

    return return_code;
}
