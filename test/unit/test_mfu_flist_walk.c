#include "common.h"

static const char* test_tmpdir = NULL;

static int test_mfu_flist_walk_paths(int rank, int ranks)
{
    int failures = 0;

    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();
    mfu_file_t* mfu_file = mfu_file_new();

    /* Check that walking a single path fills flist correctly */
    {
        TreeNode* tree_root = NULL;
        char* path_1 = NULL;

        /* create file tree */
        tree_root = tree_scenario_1("test_mfu_flist_walk_paths_single");
        if (rank == 0) {
            create_empty_test_tree(tree_root, test_tmpdir, 0700, &path_1);
        }

        /* broadcast path to all ranks */
        broadcast_path(rank, &path_1);

        const char* paths[] = {path_1};

        /* run mfu_flist_walk_paths */
        mfu_flist flist = mfu_flist_new();
        int rc = mfu_flist_walk_paths(1, paths, walk_opts, flist, mfu_file);
        failures += mpi_check_int(rank, "Walk was successful", rc, 0);

        /* verify that flist across ranks matches the tree */
        TreeNode* tree_roots[] = {tree_root};
        char* parent_paths[] = {(char*)test_tmpdir};
        failures += mpi_check_int(
            rank,
            "flist matches the file tree",
            verify_flist_match(rank, flist, 1, tree_roots, parent_paths),
            1
        );

        mfu_flist_free(&flist);
        free(path_1);
        free_tree(tree_root);
    }

    /* Check that walking multiple paths fills flist correctly */
    {
        TreeNode* tree_root_1 = NULL;
        TreeNode* tree_root_2 = NULL;
        TreeNode* tree_root_3 = NULL;
        char* path_1 = NULL;
        char* path_2 = NULL;
        char* path_3 = NULL;

        /* create file trees */
        tree_root_1 = tree_scenario_2("test_mfu_flist_walk_paths_multiple_1");
        tree_root_2 = tree_scenario_3("test_mfu_flist_walk_paths_multiple_2");
        tree_root_3 = tree_scenario_4("test_mfu_flist_walk_paths_multiple_3");
        if (rank == 0) {
            create_empty_test_tree(tree_root_1, test_tmpdir, 0700, &path_1);
            create_empty_test_tree(tree_root_2, test_tmpdir, 0700, &path_2);
            create_empty_test_tree(tree_root_3, test_tmpdir, 0700, &path_3);
        }

        /* broadcast paths to all ranks */
        broadcast_path(rank, &path_1);
        broadcast_path(rank, &path_2);
        broadcast_path(rank, &path_3);

        const char* paths[] = {path_1, path_2, path_3};

        /* run mfu_flist_walk_paths */
        mfu_flist flist = mfu_flist_new();
        int rc = mfu_flist_walk_paths(3, paths, walk_opts, flist, mfu_file);
        failures += mpi_check_int(
            rank,
            "Walk was successful (multiple paths)",
            rc,
            0
        );

        /* verify that flist across ranks matches all trees */
        TreeNode* tree_roots[] = {tree_root_1, tree_root_2, tree_root_3};
        char* parent_paths[] = {(char*)test_tmpdir, (char*)test_tmpdir,
                                (char*)test_tmpdir};
        failures += mpi_check_int(
            rank,
            "flist matches all file trees",
            verify_flist_match(rank, flist, 3, tree_roots, parent_paths),
            1
        );

        mfu_flist_free(&flist);
        free(path_1);
        free(path_2);
        free(path_3);
        free_tree(tree_root_1);
        free_tree(tree_root_2);
        free_tree(tree_root_3);
    }

    /* Check the dereference walk option works for symlink values */
    {
        TreeNode* tree_root = NULL;
        TreeNode* test_root = NULL;
        char* path_1 = NULL;
        char* path_2 = NULL;

        /* create file tree */
        tree_root = tree_scenario_5("test_mfu_flist_walk_paths_dereference");
        if (rank == 0) {
            create_empty_test_tree(tree_root, test_tmpdir, 0700, &path_1);
        }

        /* broadcast paths to all ranks */
        broadcast_path(rank, &path_1);

        const char* paths[] = {path_1};

        walk_opts->dereference = 1;

        /* run mfu_flist_walk_paths with dereference */
        mfu_flist flist = mfu_flist_new();
        int rc = mfu_flist_walk_paths(1, paths, walk_opts, flist, mfu_file);
        failures += mpi_check_int(rank, "Walk was successful (dereference)", rc,
                                  0);

        walk_opts->dereference = 0;

        /* verify symlink is not included in the flist */
        TreeNode* tree_roots_with_symlink[] = {tree_root};
        char* parent_paths_with_symlink[] = {(char*)test_tmpdir};
        failures += mpi_check_int(
            rank,
            "flist does not match tree with symlink",
            verify_flist_match(rank, flist, 1, tree_roots_with_symlink, 
                               parent_paths_with_symlink),
            0
        );
        
        /* dereference symlinks */
        if (rank == 0) {
            test_tree_dereference_symlinks(tree_root, test_tmpdir);
        }
        MPI_Barrier(MPI_COMM_WORLD);

        /* verify that flist across ranks matches tree */
        failures += mpi_check_int(
            rank,
            "flist matches tree with dereferenced symlinks",
            verify_flist_match(rank, flist, 1, tree_roots_with_symlink, 
                               parent_paths_with_symlink),
            1
        );

        mfu_flist_free(&flist);
        free(path_1);
        free_tree(tree_root);
    }

    /* Check that walking a path fails when permission is denied */
    {
        TreeNode* tree_root = NULL;
        char* path_1 = NULL;

        /* create file tree */
        tree_root = tree_scenario_1(
            "test_mfu_flist_walk_paths_single_no_permissions"
        );
        if (rank == 0) {
            create_empty_test_tree(tree_root, test_tmpdir, 0700, &path_1);
        }

        /* broadcast path to all ranks */
        broadcast_path(rank, &path_1);

        /* prepare subdirectory path for walking */
        char child_path[4096];
        snprintf(child_path, sizeof(child_path), "%s/subdir1", path_1);
        const char* paths[] = {child_path};

        /* remove execute permission from parent directory */
        if (rank == 0) {
            chmod(path_1, 0600);
        }

        /* run mfu_flist_walk_paths, expect failure */
        mfu_flist flist = mfu_flist_new();
        int rc = mfu_flist_walk_paths(1, paths, walk_opts, flist, mfu_file);
        failures += mpi_check_int(rank, "Walk failed due to stat error", rc, 
                                  -1);

        /* restore permissions */
        if (rank == 0) {
            chmod(path_1, 0700);
        }

        mfu_flist_free(&flist);
        free(path_1);
        free_tree(tree_root);
    }

    mfu_file_delete(&mfu_file);
    mfu_walk_opts_delete(&walk_opts);

    return failures;
}

static int test_mfu_flist_stat(int rank, int ranks)
{
    int failures = 0;

    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();
    mfu_file_t* mfu_file = mfu_file_new();

    /* Check that calling function over an flist with no stat information
     * gives an flist with stat information */
    {
        TreeNode* tree_root = NULL;
        char* path_1 = NULL;

        /* create file tree */
        tree_root = tree_scenario_1("test_mfu_flist_stat");
        if (rank == 0) {
            create_empty_test_tree(tree_root, test_tmpdir, 0700, &path_1);
        }

        /* broadcast path to all ranks */
        broadcast_path(rank, &path_1);

        const char* paths[] = {path_1};

        /* walk the path without stat information */
        walk_opts->use_stat = 0;
        mfu_flist in_flist = mfu_flist_new();
        int rc = mfu_flist_walk_paths(1, paths, walk_opts, in_flist, mfu_file);
        failures += mpi_check_int(rank, "Walk was successful", rc, 0);

        /* verify flist lacks stat details */
        failures += mpi_check_int(
            rank,
            "in_flist does not have stat details",
            mfu_flist_have_detail(in_flist),
            0
        );

        /* run mfu_flist_stat */
        mfu_flist flist = mfu_flist_new();
        mfu_flist_stat(in_flist, flist, NULL, NULL, walk_opts->dereference,
                       mfu_file);
        mfu_flist_free(&in_flist);

        /* verify the flist contains correct stat information */
        TreeNode* tree_roots[] = {tree_root};
        char* parent_paths[] = {(char*)test_tmpdir};
        failures += mpi_check_int(
            rank,
            "flist contains correct stat information",
            verify_flist_match(rank, flist, 1, tree_roots, parent_paths),
            1
        );

        mfu_flist_free(&flist);
        free(path_1);
        free_tree(tree_root);
    }

    mfu_file_delete(&mfu_file);
    mfu_walk_opts_delete(&walk_opts);

    return failures;
}

int main(int argc, char** argv)
{
    test_tmpdir = getenv("TEST_TMPDIR");
    if (!test_tmpdir) {
        fprintf(stderr, "TEST_TMPDIR environment variable not set\n");
        return EXIT_FAILURE;
    }

    /* initialize MPI */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and the size of comm_world */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    int failures = 0;

    if (rank == 0 ) {
        printf("\nRunning tests for mfu_flist_walk_paths()...\n");
    }
    failures += test_mfu_flist_walk_paths(rank, ranks);


    if (rank == 0 ) {
        printf("\nRunning tests for mfu_flist_stat()...\n");
    }
    failures += test_mfu_flist_stat(rank, ranks);

    /* combine failures from all ranks */
    int total_failures = 0;
    MPI_Allreduce(&failures, &total_failures, 1, MPI_INT, MPI_SUM,
                  MPI_COMM_WORLD);
    failures = total_failures;

    /* print results from rank 0 only */
    if (rank == 0) {
        if (!failures) {
            printf("\nAll mfu_flist_walk.c tests passed.\n");
        } else {
            printf("\n%d mfu_flist_walk.c test(s) FAILED.\n", failures);
        }
    }

    /* shut down MPI */
    mfu_finalize();
    MPI_Finalize();

    if (!failures) {
        return EXIT_SUCCESS;
    }
    return EXIT_FAILURE; 
}
