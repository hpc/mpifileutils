#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dcp1 will copy a single directory to a single directory.
#
# Expected behavior:
#
#   The source directory should be copied to the destination directory.
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
#   * Two directories.
#   * One file which contain random data.
PATH_A_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"
PATH_B_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"
PATH_C_RANDOM="$DCP_TEST_TMP/$(basename $PATH_A_DIRECTORY)/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"

# Print out the generated paths to make debugging easier.
echo "A_DIRECTORY path at: $PATH_A_DIRECTORY"
echo "B_DIRECTORY path at: $PATH_B_DIRECTORY"
echo "C_RANDOM    path at: $PATH_C_RANDOM"

# Create the directories.
mkdir $PATH_A_DIRECTORY
mkdir $PATH_B_DIRECTORY

# Create the random file.
dd if=/dev/urandom of=$PATH_C_RANDOM bs=3M count=2

##############################################################################
# Test copying a directory to a directory. The result should be the directory
# placed inside the directory.

mpirun -n 3 $DCP_TEST_BIN $PATH_A_DIRECTORY $PATH_B_DIRECTORY
if [[ $? -ne 0 ]]; then
    echo "Error returned when copying a directory to a directory (A -> B)."
    exit 1;
fi

$DCP_CMP_BIN "$PATH_B_DIRECTORY/$(basename $PATH_A_DIRECTORY)/$(basename $PATH_C_RANDOM)" $PATH_C_RANDOM
if [[ $? -ne 0 ]]; then
    echo "CMP mismatch when copying a directory to a directory (A -> B/A/C)."
    exit 1
fi

##############################################################################
# Since we didn't find any problems, exit with success.

exit 0

# EOF
