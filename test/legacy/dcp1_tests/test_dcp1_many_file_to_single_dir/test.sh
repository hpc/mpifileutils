#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dcp1 will copy many files to a single directory.
#
# Expected behavior:
#
#   All of the source files should be copied to the destination directory.
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
#   * Two files with zero length.
#   * Two files which contain random data.
#   * A directory.
PATH_A_EMPTY="$DCP_TEST_TMP/dcp1_test_many_file_to_single_dir.$RANDOM.tmp"
PATH_B_EMPTY="$DCP_TEST_TMP/dcp1_test_many_file_to_single_dir.$RANDOM.tmp"
PATH_C_RANDOM="$DCP_TEST_TMP/dcp1_test_many_file_to_single_dir.$RANDOM.tmp"
PATH_D_RANDOM="$DCP_TEST_TMP/dcp1_test_many_file_to_single_dir.$RANDOM.tmp"
PATH_E_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_file_to_single_dir.$RANDOM.tmp"

# Print out the generated paths to make debugging easier.
echo "A_EMPTY     path at: $PATH_A_EMPTY"
echo "B_EMPTY     path at: $PATH_B_EMPTY"
echo "C_RANDOM    path at: $PATH_C_RANDOM"
echo "D_RANDOM    path at: $PATH_D_RANDOM"
echo "E_DIRECTORY path at: $PATH_E_DIRECTORY"

# Create empty files.
touch $PATH_A_EMPTY
touch $PATH_B_EMPTY

# Create the random files.
dd if=/dev/urandom of=$PATH_C_RANDOM bs=2M count=4
dd if=/dev/urandom of=$PATH_D_RANDOM bs=3M count=3

# Create the directory.
mkdir $PATH_E_DIRECTORY

##############################################################################
# Test copying several files to a directory. The result should be the files
# placed inside the directory.

mpirun -n 3 $DCP_TEST_BIN $PATH_A_EMPTY $PATH_C_RANDOM $PATH_B_EMPTY $PATH_D_RANDOM $PATH_E_DIRECTORY
if [[ $? -ne 0 ]]; then
    echo "Error returned when copying an several files to a directory (A,B,C,D -> E)."
    exit 1;
fi

$DCP_CMP_BIN "$PATH_E_DIRECTORY/$(basename $PATH_A_EMPTY)" $PATH_A_EMPTY
if [[ $? -ne 0 ]]; then
    echo "CMP mismatch when copying empty file to a directory (A -> E/B)."
    exit 1
fi

$DCP_CMP_BIN "$PATH_E_DIRECTORY/$(basename $PATH_B_EMPTY)" $PATH_B_EMPTY
if [[ $? -ne 0 ]]; then
    echo "CMP mismatch when copying empty file to a directory (B -> E/B)."
    exit 1
fi

$DCP_CMP_BIN "$PATH_E_DIRECTORY/$(basename $PATH_C_RANDOM)" $PATH_C_RANDOM
if [[ $? -ne 0 ]]; then
    echo "CMP mismatch when copying empty file to a directory (C -> E/C)."
    exit 1
fi

$DCP_CMP_BIN "$PATH_E_DIRECTORY/$(basename $PATH_D_RANDOM)" $PATH_D_RANDOM
if [[ $? -ne 0 ]]; then
    echo "CMP mismatch when copying empty file to a directory (D -> E/D)."
    exit 1
fi

##############################################################################
# Since we didn't find any problems, exit with success.

exit 0

# EOF
