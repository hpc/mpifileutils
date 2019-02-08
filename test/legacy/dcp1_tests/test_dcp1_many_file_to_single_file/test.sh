#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dcp1 will copy many files to a single file.
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
#   * Four files with zero length.
#   * Four files which contain random data.
#   * A file that doesn't exist.
PATH_A_EMPTY="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"
PATH_B_EMPTY="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"
PATH_C_EMPTY="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"
PATH_D_EMPTY="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"
PATH_E_RANDOM="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"
PATH_F_RANDOM="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"
PATH_G_RANDOM="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"
PATH_H_RANDOM="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"
PATH_I_NOEXIST="$DCP_TEST_TMP/dcp1_test_many_file_to_single_file.$RANDOM.tmp"

# Print out the generated paths to make debugging easier.
echo "A_EMPTY  path at: $PATH_A_EMPTY"
echo "B_EMPTY  path at: $PATH_B_EMPTY"
echo "C_EMPTY  path at: $PATH_C_EMPTY"
echo "D_EMPTY  path at: $PATH_D_EMPTY"
echo "E_RANDOM path at: $PATH_E_RANDOM"
echo "F_RANDOM path at: $PATH_F_RANDOM"
echo "G_RANDOM path at: $PATH_G_RANDOM"
echo "H_RANDOM path at: $PATH_H_RANDOM"

# Create empty files.
touch $PATH_A_EMPTY
touch $PATH_B_EMPTY
touch $PATH_C_EMPTY
touch $PATH_D_EMPTY

# Create the random files.
dd if=/dev/urandom of=$PATH_E_RANDOM bs=2M count=4
dd if=/dev/urandom of=$PATH_F_RANDOM bs=3M count=3
dd if=/dev/urandom of=$PATH_G_RANDOM bs=1M count=1
dd if=/dev/urandom of=$PATH_H_RANDOM bs=6M count=2

##############################################################################
# Test copying several empty files into an empty file. This should result in
# an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_A_EMPTY $PATH_B_EMPTY $PATH_C_EMPTY $PATH_D_EMPTY
if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying empty files to an empty file. (A,B,C -> D)."
    exit 1;
fi

##############################################################################
# Test copying several empty files into a random file. This should result in
# an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_A_EMPTY $PATH_B_EMPTY $PATH_C_EMPTY $PATH_E_EMPTY
if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying empty files to a random file (A,B,C -> E)."
    exit 1;
fi

##############################################################################
# Test copying several random files into an empty file. This should result in
# an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_E_RANDOM $PATH_F_RANDOM $PATH_G_RANDOM $PATH_A_EMPTY
if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying random files to an empty file (E,F,G -> A)."
    exit 1;
fi

##############################################################################
# Test copying several random files into a file that doesn't exist. This
# should result in an error.

mpirun -n 3 $DCP_TEST_BIN $PATH_E_RANDOM $PATH_F_RANDOM $PATH_G_RANDOM $PATH_I_NOEXIST
if [[ $? -eq 0 ]]; then
    echo "Unexpected success when copying random files to a no-exist file (E,F,G -> I)."
    exit 1;
fi

##############################################################################
# Since we didn't find any problems, exit with success.

exit 0

# EOF
