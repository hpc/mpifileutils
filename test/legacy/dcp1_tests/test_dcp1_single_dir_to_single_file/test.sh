#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dcp1 will copy a single directory to a single file.
#
# Expected behavior:
#
#   An error code should be displayed if this action is attempted.
#
# Reminder:
#
#   Lines that echo to the terminal will only be available if DEBUG is enabled
#   in the test runner (test_all.sh).
##############################################################################

# Turn on verbose output
#set -x

# Print out the basic paths we'll be using.
echo "Using dcp1 binary at: $DCP_TEST_BIN"
echo "Using tmp directory at: $DCP_TEST_TMP"

##############################################################################
# Generate the paths for:
#   * A directory.
#   * A file with zero length.
#   * A file which contains random data.
PATH_A_DIRECTORY="$DCP_TEST_TMP/dcp1_test_single_dir_to_single_file.$RANDOM.tmp"
PATH_B_EMPTY="$DCP_TEST_TMP/dcp1_test_single_dir_to_single_file.$RANDOM.tmp"
PATH_C_RANDOM="$DCP_TEST_TMP/dcp1_test_single_dir_to_single_file.$RANDOM.tmp"

# Print out the generated paths to make debugging easier.
echo "A_DIRECTORY path at: $PATH_A_DIRECTORY"
echo "B_EMPTY     path at: $PATH_B_EMPTY"
echo "C_RANDOM    path at: $PATH_C_RANDOM"

# Create empty file.
touch $PATH_B_EMPTY

# Create the random file.
dd if=/dev/urandom of=$PATH_C_RANDOM bs=4M count=5

##############################################################################
# Test copying the directory to an empty file. The result should be an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_A_DIRECTORY $PATH_B_EMPTY
if [[ $? -eq 0 ]]; then
    echo "Unexpectedd success when copying a directory to an empty file. (A -> B)."
    exit 1;
fi

##############################################################################
# Test copying a directory to a random file. The result should be an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_A_DIRECTORY $PATH_C_RANDOM
if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying a directory to a random file (A -> C)."
    exit 1;
fi

##############################################################################
# Since we didn't find any problems, exit with success.

exit 0

# EOF
