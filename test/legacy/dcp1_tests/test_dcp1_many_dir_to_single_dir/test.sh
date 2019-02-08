#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dcp1 will copy many directories to a single directory.
#
# Expected behavior:
#
#   All of the source directories should be copied to the destination directory.
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
#   * Four directories.
#   * Three files which contain random data.
PATH_A_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"
PATH_B_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"
PATH_C_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"
PATH_D_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"
PATH_E_RANDOM="$DCP_TEST_TMP/$(basename $PATH_A_DIRECTORY)/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"
PATH_F_RANDOM="$DCP_TEST_TMP/$(basename $PATH_B_DIRECTORY)/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"
PATH_G_RANDOM="$DCP_TEST_TMP/$(basename $PATH_C_DIRECTORY)/dcp1_test_many_dir_to_single_dir.$RANDOM.tmp"

# Print out the generated paths to make debugging easier.
echo "A_DIRECTORY path at: $PATH_A_DIRECTORY"
echo "B_DIRECTORY path at: $PATH_B_DIRECTORY"
echo "C_DIRECTORY path at: $PATH_C_DIRECTORY"
echo "D_DIRECTORY path at: $PATH_D_DIRECTORY"
echo "E_RANDOM    path at: $PATH_E_RANDOM"
echo "F_RANDOM    path at: $PATH_F_RANDOM"
echo "G_RANDOM    path at: $PATH_G_RANDOM"

# Create the directories.
mkdir $PATH_A_DIRECTORY
mkdir $PATH_B_DIRECTORY
mkdir $PATH_C_DIRECTORY
mkdir $PATH_D_DIRECTORY

# Create the random files.
dd if=/dev/urandom of=$PATH_E_RANDOM bs=2M count=1
dd if=/dev/urandom of=$PATH_F_RANDOM bs=3M count=3
dd if=/dev/urandom of=$PATH_G_RANDOM bs=3M count=2

##############################################################################
# Test copying several directories to a directory. The result should be the directories
# placed inside the directory.

mpirun -n 3 $DCP_TEST_BIN $PATH_A_DIRECTORY $PATH_B_DIRECTORY $PATH_C_DIRECTORY $PATH_D_DIRECTORY
if [[ $? -ne 0 ]]; then
    echo "Error returned when copying an several directories to a directory (A,B,C -> D)."
    exit 1;
fi

echo "comparing $PATH_D_DIRECTORY/$(basename $PATH_A_DIRECTORY)/$(basename $PATH_E_RANDOM) to $PATH_E_RANDOM"

$DCP_CMP_BIN "$PATH_D_DIRECTORY/$(basename $PATH_A_DIRECTORY)/$(basename $PATH_E_RANDOM)" $PATH_E_RANDOM
if [[ $? -ne 0 ]]; then
    echo "CMP mismatch when copying a directory to a directory (A -> D/A/E)."
    exit 1
fi

##############################################################################
# Since we didn't find any problems, exit with success.

exit 0

# EOF
