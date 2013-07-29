









int main(int argc, char** argv)
{
    int c;
    int option_index = 0;

    MPI_Init(&argc, &argv);

    /* Initialize our processing library and related callbacks. */
    /* This is a bit of chicken-and-egg problem, because we'd like
     * to have our rank to filter output messages below but we might
     * also want to set different libcircle flags based on command line
     * options -- for now just pass in the default flags */
    CIRCLE_global_rank = CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&DCOPY_add_objects);
    CIRCLE_cb_process(&DCOPY_process_objects);

    DCOPY_debug_stream = stdout;

    /* By default, don't perform a conditional copy. */
    DCOPY_user_opts.conditional = false;

    /* By default, don't skip the compare option. */
    DCOPY_user_opts.skip_compare = false;

    /* By default, show info log messages. */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_INFO;
    DCOPY_debug_level = DCOPY_LOG_INFO;

    /* By default, don't unlink destination files if an open() fails. */
    DCOPY_user_opts.force = false;

    /* By default, don't bother to preserve all attributes. */
    DCOPY_user_opts.preserve = false;

    /* By default, don't attempt any type of recursion. */
    DCOPY_user_opts.recursive = false;
    DCOPY_user_opts.recursive_unspecified = false;

    /* By default, assume the filesystem is reliable (exit on errors). */
    DCOPY_user_opts.reliable_filesystem = true;

    static struct option long_options[] = {
        {"conditional"          , no_argument      , 0, 'c'},
        {"skip-compare"         , no_argument      , 0, 'C'},
        {"debug"                , required_argument, 0, 'd'},
        {"force"                , no_argument      , 0, 'f'},
        {"help"                 , no_argument      , 0, 'h'},
        {"preserve"             , no_argument      , 0, 'p'},
        {"recursive"            , no_argument      , 0, 'R'},
        {"recursive-unspecified", no_argument      , 0, 'r'},
        {"unreliable-filesystem", no_argument      , 0, 'U'},
        {"version"              , no_argument      , 0, 'v'},
        {0                      , 0                , 0, 0  }
    };

    /* Parse options */
    while((c = getopt_long(argc, argv, "cCd:fhpRrUv", \
                           long_options, &option_index)) != -1) {
        switch(c) {

            case 'c':
                DCOPY_user_opts.conditional = true;

                if(CIRCLE_global_rank == 0) {
                    LOG(DCOPY_LOG_INFO, "Performing a conditional copy.");
                }

                break;

            case 'C':
                DCOPY_user_opts.skip_compare = true;

                if(CIRCLE_global_rank == 0) {
                    LOG(DCOPY_LOG_INFO, "Skipping the comparison stage " \
                        "(may result in corruption).");
                }

                break;

            case 'd':

                if(strncmp(optarg, "fatal", 5) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_FATAL;
                    DCOPY_debug_level = DCOPY_LOG_FATAL;

                    if(CIRCLE_global_rank == 0) {
                        LOG(DCOPY_LOG_INFO, "Debug level set to: fatal");
                    }

                }
                else if(strncmp(optarg, "err", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_ERR;
                    DCOPY_debug_level = DCOPY_LOG_ERR;

                    if(CIRCLE_global_rank == 0) {
                        LOG(DCOPY_LOG_INFO, "Debug level set to: errors");
                    }

                }
                else if(strncmp(optarg, "warn", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_WARN;
                    DCOPY_debug_level = DCOPY_LOG_WARN;

                    if(CIRCLE_global_rank == 0) {
                        LOG(DCOPY_LOG_INFO, "Debug level set to: warnings");
                    }

                }
                else if(strncmp(optarg, "info", 4) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_INFO;
                    DCOPY_debug_level = DCOPY_LOG_INFO;

                    if(CIRCLE_global_rank == 0) {
                        LOG(DCOPY_LOG_INFO, "Debug level set to: info");
                    }

                }
                else if(strncmp(optarg, "dbg", 3) == 0) {
                    CIRCLE_debug = CIRCLE_LOG_DBG;
                    DCOPY_debug_level = DCOPY_LOG_DBG;

                    if(CIRCLE_global_rank == 0) {
                        LOG(DCOPY_LOG_INFO, "Debug level set to: debug");
                    }

                }
                else {
                    if(CIRCLE_global_rank == 0) {
                        LOG(DCOPY_LOG_INFO, "Debug level `%s' not recognized. " \
                            "Defaulting to `info'.", optarg);
                    }
                }

                break;

            case 'f':
                DCOPY_user_opts.force = true;

                if(CIRCLE_global_rank == 0) {
                    LOG(DCOPY_LOG_INFO, "Deleting destination on errors.");
                }

                break;

            case 'h':

                if(CIRCLE_global_rank == 0) {
                    DCOPY_print_usage(argv);
                }

                DCOPY_exit(EXIT_SUCCESS);
                break;

            case 'p':
                DCOPY_user_opts.preserve = true;

                if(CIRCLE_global_rank == 0) {
                    LOG(DCOPY_LOG_INFO, "Preserving file attributes.");
                }

                break;

            case 'R':
                DCOPY_user_opts.recursive = true;

                if(CIRCLE_global_rank == 0) {
                    LOG(DCOPY_LOG_INFO, "Performing correct recursion.");
                    LOG(DCOPY_LOG_WARN, "Warning, only files and directories are implemented.");
                }

                break;

            case 'r':
                DCOPY_user_opts.recursive_unspecified = true;

                if(CIRCLE_global_rank == 0) {
                    LOG(DCOPY_LOG_INFO, "Performing recursion. " \
                        "Ignoring special files.");
                }

                break;

            case 'U':
                DCOPY_user_opts.reliable_filesystem = false;

                if(CIRCLE_global_rank == 0) {
                    LOG(DCOPY_LOG_INFO, "Unreliable filesystem specified. " \
                        "Retry mode enabled.");
                }

                break;

            case 'v':

                if(CIRCLE_global_rank == 0) {
                    DCOPY_print_version();
                }

                DCOPY_exit(EXIT_SUCCESS);
                break;

            case '?':
            default:

                if(CIRCLE_global_rank == 0) {
                    if(optopt == 'd') {
                        DCOPY_print_usage(argv);
                        fprintf(stderr, "Option -%c requires an argument.\n", \
                                optopt);
                    }
                    else if(isprint(optopt)) {
                        DCOPY_print_usage(argv);
                        fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                    }
                    else {
                        DCOPY_print_usage(argv);
                        fprintf(stderr,
                                "Unknown option character `\\x%x'.\n",
                                optopt);
                    }
                }

                DCOPY_exit(EXIT_FAILURE);
                break;
        }
    }

    /** Parse the source and destination paths. */
    DCOPY_parse_path_args(argv, optind, argc);

    /* initialize linked list of stat objects */
    DCOPY_list_head  = NULL;
    DCOPY_list_tail  = NULL;

    /* Initialize our jump table for core operations. */
    DCOPY_jump_table[TREEWALK] = DCOPY_do_treewalk;
    DCOPY_jump_table[COPY]     = DCOPY_do_copy;
    DCOPY_jump_table[CLEANUP]  = DCOPY_do_cleanup;
    DCOPY_jump_table[COMPARE]  = DCOPY_do_compare;

    /* Set the log level for the processing library. */
    CIRCLE_enable_logging(CIRCLE_debug);

    /* Grab a relative and actual start time for the epilogue. */
    time(&(DCOPY_statistics.time_started));
    DCOPY_statistics.wtime_started = CIRCLE_wtime();

    /* Perform the actual file copy. */
    CIRCLE_begin();

    /* Determine the actual and relative end time for the epilogue. */
    DCOPY_statistics.wtime_ended = CIRCLE_wtime();
    time(&(DCOPY_statistics.time_ended));

    /* Let the processing library cleanup. */
    CIRCLE_finalize();

    /* set permissions, ownership, and timestamps if needed */
    DCOPY_set_metadata();

    /* free list of stat objects */
    DCOPY_stat_elem_t* current = DCOPY_list_head;
    while (current != NULL) {
        DCOPY_stat_elem_t* next = current->next;
        free(current->file);
        free(current->sb);
        free(current);
        current = next;
    }

    /* Print the results to the user. */
    DCOPY_epilogue();

    DCOPY_exit(EXIT_SUCCESS);
}

/* EOF */
