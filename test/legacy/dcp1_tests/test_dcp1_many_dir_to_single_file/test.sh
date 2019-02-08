#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dcp1 will copy many directories to a single file.
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
#   * A file with zero length.
#   * A file which contains random data.
#   * A file which doesn't exist.
#   * Four directories.
PATH_A_EMPTY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_file.$RANDOM.tmp"
PATH_B_RANDOM="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_file.$RANDOM.tmp"
PATH_C_NOEXIST="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_file.$RANDOM.tmp"
PATH_D_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_file.$RANDOM.tmp"
PATH_E_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_file.$RANDOM.tmp"
PATH_F_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_file.$RANDOM.tmp"
PATH_G_DIRECTORY="$DCP_TEST_TMP/dcp1_test_many_dir_to_single_file.$RANDOM.tmp"

# Print out the generated paths to make debugging easier.
echo "A_EMPTY     path at: $PATH_A_EMPTY"
echo "B_RANDOM    path at: $PATH_B_RANDOM"
echo "C_NOEXIST   path at: $PATH_C_NOEXIST"
echo "D_DIRECTORY path at: $PATH_D_DIRECTORY"
echo "E_DIRECTORY path at: $PATH_E_DIRECTORY"
echo "F_DIRECTORY path at: $PATH_F_DIRECTORY"
echo "G_DIRECTORY path at: $PATH_G_DIRECTORY"

# Create an empty file.
touch $PATH_A_EMPTY

# Create the random file.
dd if=/dev/urandom of=$PATH_B_RANDOM bs=2M count=4

# Create the directories.
mkdir $PATH_D_DIRECTORY
mkdir $PATH_E_DIRECTORY
mkdir $PATH_F_DIRECTORY
mkdir $PATH_G_DIRECTORY

##############################################################################
# Test copying several directories into an empty file. This should result in
# an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_D_DIRECTORY $PATH_E_DIRECTORY $PATH_F_DIRECTORY $PATH_A_EMPTY
if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying several directories to an empty file. (D,E,F -> A)."
    exit 1;
fi

##############################################################################
# Test copying several directories into a random file. This should result in
# an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_D_DIRECTORY $PATH_E_DIRECTORY $PATH_F_DIRECTORY $PATH_B_RANDOM
if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying several directories to a random file. (D,E,F -> B)."
    exit 1;
fi

##############################################################################
# Test copying several directories into a no-exist file. This should result in
# an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_D_DIRECTORY $PATH_E_DIRECTORY $PATH_F_DIRECTORY $PATH_C_NOEXIST
if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying several directories to a no-exist file. (D,E,F -> C)."
    exit 1;
fi

##############################################################################
# Since we didn't find any problems, exit with success.

exit 0

# EOF
