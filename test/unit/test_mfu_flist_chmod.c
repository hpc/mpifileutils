#include "common.h"

static const char* test_tmpdir = NULL;

static int is_usr_plus_read(const mfu_perms* perms)
{
    return perms != NULL
        && perms->octal == 0
        && perms->usr == 1
        && perms->group == 0
        && perms->other == 0
        && perms->all == 0
        && perms->assume_all == 0
        && perms->plus == 1
        && perms->read == 1
        && perms->write == 0
        && perms->execute == 0
        && perms->capital_execute == 0
        && perms->assignment == 0;
}

static int is_group_minus_execute(const mfu_perms* perms)
{
    return perms != NULL
        && perms->octal == 0
        && perms->usr == 0
        && perms->group == 1
        && perms->other == 0
        && perms->all == 0
        && perms->assume_all == 0
        && perms->plus == 0
        && perms->read == 0
        && perms->write == 0
        && perms->execute == 1
        && perms->capital_execute == 0
        && perms->assignment == 0;
}

static int test_mfu_perms_parse(void)
{
    int failures = 0;

    /* Check that mode string is expectedly considered valid and that the
     * permission struct linked list was filled out as expected. */
    {
        mfu_perms* head = NULL;
        const char modestr[] = "u+r,g-x";
        int valid = mfu_perms_parse(modestr, &head);
        failures += check_int("valid mode string", valid, 1);

        const mfu_perms* second = head != NULL ? head->next : NULL;
        failures += check_int("u+r permission", is_usr_plus_read(head), 1);
        failures += check_int(
            "g-x permission",
            is_group_minus_execute(second),
            1
        );
        failures += check_int(
            "permission list length",
            second != NULL && second->next == NULL,
            1
        );

        mfu_perms_free(&head);
        failures += check_int("permission list freed", head == NULL, 1);
    }

    /* Check that mode string is expectedly considered invalid. */
    {
        mfu_perms* head = NULL;
        const char modestr[] = "u+ry";
        int valid = mfu_perms_parse(modestr, &head);
        failures += check_int("invalid mode string", valid, 0);
        failures += check_int(
            "uninitialized permission struct linked list",
            head == NULL,
            1
        );
    }

    /* Check that mode octal is expectedly considered valid and that the
     * permission struct linked list was filled out as expected. */
    {
        mfu_perms* head = NULL;
        const char modestr[] = "741";
        int valid = mfu_perms_parse(modestr, &head);
        failures += check_int("valid mode octal", valid, 1);

        failures += check_int("using octal mode", head->octal, 1);
        failures += check_int("permission mode 741", head->mode_octal,
                              0741 & 0777);
        failures += check_int("permission list length", head->next == NULL, 1);

        mfu_perms_free(&head);
        failures += check_int("permission list freed", head == NULL, 1);
    }

    /* Check that mode octal is expectedly considered invalid. */
    {
        mfu_perms* head = NULL;
        const char modestr[] = "080";
        int valid = mfu_perms_parse(modestr, &head);
        failures += check_int("invalid mode octal", valid, 0);
        failures += check_int(
            "uninitialized permission struct linked list",
            head == NULL,
            1
        );
    }

    return failures;
}

static int test_run_mfu_perms_need_dir_rx_with_modestr(const char* modestr,
                                                       const int expected_flag)
{
    int failures = 0;
    char check_str_1[strlen(modestr) + sizeof(": expected dir_perms flag set")];
    
    mfu_perms* head = NULL;
    int valid = mfu_perms_parse(modestr, &head);  
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    mfu_perms_need_dir_rx(head, walk_opts);
    snprintf(check_str_1, sizeof(check_str_1),
             "%s: expected dir_perms flag set", modestr);
    failures += check_int(check_str_1, walk_opts->dir_perms, expected_flag);

    mfu_perms_free(&head);
    failures += check_int("permission list freed", head == NULL, 1);
    mfu_walk_opts_delete(&walk_opts);
    failures += check_int("walk opts freed", walk_opts == NULL, 1);

    return failures;
}

static int successful_mfu_perms_need_dir_rx_with_modestr(const char* modestr)
{
    return test_run_mfu_perms_need_dir_rx_with_modestr(modestr, 1);
}

static int failed_mfu_perms_need_dir_rx_with_modestr(const char* modestr)
{
    return test_run_mfu_perms_need_dir_rx_with_modestr(modestr, 0);
}

static int test_mfu_perms_need_dir_rx(void)
{
    int failures = 0;
    
    /* Check that mode u+rx means that directory permissions do not need to be
     * updated during walk. */
    {
        const char modestr[] = "u+rx";
        failures += successful_mfu_perms_need_dir_rx_with_modestr(modestr);
    }

    /* Check that mode a+rx means that directory permissions do not need to be
     * updated during walk. */
    {
        const char modestr[] = "a+rx";
        failures += successful_mfu_perms_need_dir_rx_with_modestr(modestr);
    }

    /* Check that mode g+rx means that directory permissions need to be updated
     * during walk. */
    {
        const char modestr[] = "g+rx";
        failures += failed_mfu_perms_need_dir_rx_with_modestr(modestr);
    }

    /* Check that mode u+rx,a-x means that directory permissions need to be
     * updated during walk. */
    {
        const char modestr[] = "u+rx,a-x";
        failures += failed_mfu_perms_need_dir_rx_with_modestr(modestr);
    }

    /* Check that mode 500 means that directory permissions do not need to be
     * updated during walk. */
    {
        const char modestr[] = "500";
        failures += successful_mfu_perms_need_dir_rx_with_modestr(modestr);
    }
    
    /* Check that mode 055 means that directory permissions do need to be
     * updated during walk. */
    {
        const char modestr[] = "055";
        failures += failed_mfu_perms_need_dir_rx_with_modestr(modestr);
    }

    return failures;
}

static int test_mfu_flist_chmod(int rank, int ranks)
{
    int failures = 0;

    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();
    mfu_file_t* mfu_file = mfu_file_new();
    mfu_chmod_opts_t* chmod_opts = mfu_chmod_opts_new();

    /* Check that setting octal permissions on an flist updates permissions on 
     * the filesystem */
    {
        TreeNode* tree_root = NULL;
        char* tree_path = NULL;

        /* create file tree */
        tree_root = tree_scenario_1("test_mfu_flist_chmod_octal");
        if (rank == 0) {
            create_empty_test_tree(tree_root, test_tmpdir, 0700, &tree_path);
        }

        /* broadcast tree_path to all ranks */
        broadcast_path(rank, &tree_path);

        const char* tree_paths[] = {tree_path};

        /* walk the tree_path */
        mfu_flist in_flist = mfu_flist_new();
        int rc = mfu_flist_walk_paths(1, tree_paths, walk_opts, in_flist, mfu_file);
        failures += mpi_check_int(rank, "Walk was successful", rc, 0);
        
        /* check that mode string parses correctly */
        mfu_perms* head = NULL;
        const char modestr[] = "761";
        int valid = mfu_perms_parse(modestr, &head);
        failures += mpi_check_int(rank, "valid octal mode string", valid, 1);

        /* run mfu_flist_chmod */
        mfu_flist_chmod(in_flist, NULL, NULL, head, chmod_opts);
        
        /* verify that the tree_path permissions have been changed */
        if (rank == 0) {
            failures += check_int(
                "permission on tree updated",
                verify_test_tree_permissions(tree_root, test_tmpdir, 0761),
                1
            );
        }

        mfu_perms_free(&head);
        mfu_flist_free(&in_flist);
        free(tree_path);
        free_tree(tree_root);
    }

    /* Check that setting target bit permissions on an flist updates permissions
     * on the filesystem */
    {
        TreeNode* tree_root = NULL;
        char* tree_path = NULL;

        /* create file tree */
        tree_root = tree_scenario_2("test_mfu_flist_chmod_target");
        if (rank == 0) {
            create_empty_test_tree(tree_root, test_tmpdir, 0700, &tree_path);
        }

        /* broadcast tree_path to all ranks */
        broadcast_path(rank, &tree_path);

        const char* tree_paths[] = {tree_path};

        /* walk the tree_path */
        mfu_flist in_flist = mfu_flist_new();
        int rc = mfu_flist_walk_paths(1, tree_paths, walk_opts, in_flist, mfu_file);
        failures += mpi_check_int(rank, "Walk was successful", rc, 0);

        /* check that mode string parses correctly */
        mfu_perms* head = NULL;
        const char modestr[] = "g=u,o=u";
        int valid = mfu_perms_parse(modestr, &head);
        failures += mpi_check_int(rank, "valid target mode string", valid, 1);

        /* run mfu_flist_chmod */
        mfu_flist_chmod(in_flist, NULL, NULL, head, chmod_opts);

        /* verify that the tree_path permissions have been changed */
        if (rank == 0) {
            failures += check_int(
                "permission on tree updated",
                verify_test_tree_permissions(tree_root, test_tmpdir, 0777),
                1
            );
        }

        mfu_perms_free(&head);
        mfu_flist_free(&in_flist);
        free(tree_path);
        free_tree(tree_root);
    }

    /* Check that setting symbolic bit permissions on an flist updates
     * permissions on the filesystem */
    {
        TreeNode* tree_root = NULL;
        char* tree_path = NULL;

        /* create file tree */
        tree_root = tree_scenario_3("test_mfu_flist_chmod_symbolic");
        if (rank == 0) {
            create_empty_test_tree(tree_root, test_tmpdir, 0700, &tree_path);
        }

        /* broadcast tree_path to all ranks */
        broadcast_path(rank, &tree_path);

        const char* tree_paths[] = {tree_path};

        /* walk the tree_path, dereferencing symlinks to target actual files */
        mfu_walk_opts_t* walk_opts_deref = mfu_walk_opts_new();
        walk_opts_deref->dereference = 1;

        mfu_flist in_flist = mfu_flist_new();
        int rc = mfu_flist_walk_paths(1, tree_paths, walk_opts_deref, in_flist, mfu_file);
        failures += mpi_check_int(rank, "Walk with dereference was successful", rc, 0);

        /* check that mode string parses correctly */
        mfu_perms* head = NULL;
        const char modestr[] = "g+rw,o+x";
        int valid = mfu_perms_parse(modestr, &head);
        failures += mpi_check_int(rank, "valid symbolic mode string", valid, 1);

        /* run mfu_flist_chmod */
        mfu_flist_chmod(in_flist, NULL, NULL, head, chmod_opts);

        /* verify that the target permissions have been changed */
        if (rank == 0) {
            failures += check_int(
                "permission on tree updated",
                verify_test_tree_permissions(tree_root, test_tmpdir, 0761),
                1
            );
        }

        mfu_perms_free(&head);
        mfu_flist_free(&in_flist);
        mfu_walk_opts_delete(&walk_opts_deref);
        free(tree_path);
        free_tree(tree_root);
    }

    mfu_chmod_opts_delete(&chmod_opts);
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
        printf("\nRunning tests for mfu_perms_parse()...\n");
        failures += test_mfu_perms_parse();

        printf("\nRunning tests for mfu_perms_need_dir_rx()...\n");
        failures += test_mfu_perms_need_dir_rx();

        printf("\nRunning tests for mfu_flist_chmod()...\n");
    }
    failures += test_mfu_flist_chmod(rank, ranks);

    /* combine failures from all ranks */
    int total_failures = 0;
    MPI_Allreduce(&failures, &total_failures, 1, MPI_INT, MPI_SUM,
                  MPI_COMM_WORLD);
    failures = total_failures;

    /* print results from rank 0 only */
    if (rank == 0) {
        if (!failures) {
            printf("\nAll mfu_flist_chmod.c tests passed.\n");
        } else {
           printf("\n%d mfu_flist_chmod.c test(s) FAILED.\n", failures);
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
