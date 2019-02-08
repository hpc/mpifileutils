#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dcp1 will copy a single file to a directory.
#
# Expected behavior:
#
#   The file should be placed inside the directory.
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
echo "Using cmp binary at: $DCP_CMP_BIN"
echo "Using tmp directory at: $DCP_TEST_TMP"

##############################################################################
# Generate the paths for:
#   * A file that doesn't exist at all.
#   * A file with zero length.
#   * A file which contains random data.
PATH_A_NOEXIST="$DCP_TEST_TMP/dcp1_test_single_file_to_single_dir.$RANDOM.tmp"
PATH_B_EMPTY="$DCP_TEST_TMP/dcp1_test_single_file_to_single_dir.$RANDOM.tmp"
PATH_C_RANDOM="$DCP_TEST_TMP/dcp1_test_single_file_to_single_dir.$RANDOM.tmp"
PATH_D_DIRECTORY="$DCP_TEST_TMP/dcp1_test_single_file_to_single_dir.$RANDOM.tmp"

# Print out the generated paths to make debugging easier.
echo "A_NOEXIST   path at: $PATH_A_NOEXIST"
echo "B_EMPTY     path at: $PATH_B_EMPTY"
echo "C_RANDOM    path at: $PATH_C_RANDOM"
echo "D_DIRECTORY path at: $PATH_D_DIRECTORY"

# Create the empty file.
touch $PATH_B_EMPTY

# Create the random file.
dd if=/dev/urandom of=$PATH_C_RANDOM bs=4M count=5

# Create the directory.
mkdir $PATH_D_DIRECTORY

##############################################################################
# Test copying a no-exist file to a directory. The result should be an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_A_NOEXIST $PATH_D_DIRECTORY

if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying a no-exist file to a directory (A -> D)."
    exit 1;
fi

##############################################################################
# Test copying an empty file to a directory. The result should be the empty
# file placed inside the directory.

mpirun -n 3 $DCP_TEST_BIN $PATH_B_EMPTY $PATH_D_DIRECTORY
if [[ $? -ne 0 ]]; then
    echo "Error returned when copying an empty file to a directory (B -> D)."
    exit 1;
fi

$DCP_CMP_BIN "$PATH_D_DIRECTORY/$(basename $PATH_B_EMPTY)" $PATH_B_EMPTY
if [[ $? -ne 0 ]]; then
    echo "CMP mismatch when copying empty file to a directory (B -> D/B)."
    exit 1
fi

##############################################################################
# Test copying an empty file to a directory. The result should be the empty
# file placed inside the directory.

mpirun -n 3 $DCP_TEST_BIN $PATH_C_RANDOM $PATH_D_DIRECTORY
if [[ $? -ne 0 ]]; then
    echo "Error returned when copying a random file to a directory (C -> D)."
    exit 1;
fi

$DCP_CMP_BIN "$PATH_D_DIRECTORY/$(basename $PATH_C_RANDOM)" $PATH_C_RANDOM
if [[ $? -ne 0 ]]; then
    echo "CMP mismatch when copying a random file to a directory (C -> D/C)."
    exit 1
fi

##############################################################################
# Since we didn't find any problems, exit with success.

exit 0

# EOF
