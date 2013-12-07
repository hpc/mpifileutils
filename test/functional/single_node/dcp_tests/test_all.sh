#!/bin/bash

###############################################################################
#
#                     A simple test runner for dcp.
#
###############################################################################

# Turn this on if you want output from each test printed out.
DEBUG=1

# A temporary directory that all tests can use for scratch files.
TEST_TMP_DIR=../tmp

# The dcp binary path to use. This must be relative to the test_all.sh script.
TEST_DCP_BIN=../src/dcp

# The mpirun binary to use.
TEST_MPIRUN_BIN=/usr/bin/mpirun

# The cmp binary to use.
TEST_CMP_BIN=/usr/bin/cmp

# Basic counters for summary output
TESTS_RUN=0
TESTS_FAILED=0
TESTS_PASSED=0

# Determine where the test directory is
TESTS_DIR=$(dirname ${BASH_SOURCE[0]})

# If we don't find any tests, just don't run anything.
shopt -s nullglob

# Make sure we're in the same directory as the tests.
pushd $TESTS_DIR > /dev/null

# Make a temp dir for tests.
mkdir $TEST_TMP_DIR

echo "# =============================================================================="
echo "# Running ALL tests for DCP."
echo "# =============================================================================="
echo "# Tests started at: $(date)"
echo "# =============================================================================="

# Fix up the tmp and bin paths for subshells.
export DCP_TEST_BIN=$(readlink -f $TEST_DCP_BIN)
export DCP_TEST_TMP=$(readlink -f $TEST_TMP_DIR)
export DCP_MPIRUN_BIN=$(readlink -f $TEST_MPIRUN_BIN)
export DCP_CMP_BIN=$(readlink -f $TEST_CMP_BIN)

# Tell the tests what mode we're in
export DEBUG

# Find and run all of the tests.
for TEST in ./*
do
    if [[ -d "$TEST" ]]; then
        TEST_OUT=$($TEST"/test.sh"); RETVAL=$?;

        if [[ $DEBUG -eq 1 ]]; then
            echo "$TEST_OUT"
        fi

        if [[ $RETVAL -eq 0 ]]; then
            echo "SUCCESS $(echo "$TEST" | sed 's/[^a-zA-Z0-9_]//g')";
            TESTS_PASSED=`expr $TESTS_PASSED + 1`;
        fi

        if [[ $RETVAL -ne 0 ]]; then
            echo "FAILED $(echo "$TEST" | sed 's/[^a-zA-Z0-9_]//g')";
            TESTS_FAILED=`expr $TESTS_FAILED + 1`;
        fi

        TESTS_RUN=`expr $TESTS_RUN + 1`;
    fi
done

echo "# =============================================================================="
echo "# DCP Test Summary:"
echo "#     Passed:         $TESTS_PASSED"
echo "#     Failed:         $TESTS_FAILED"
echo "# =============================================================================="
echo "#     Tests Run:      $TESTS_RUN"
echo "#     Percent Passed: $(echo "scale=2; ($TESTS_PASSED*100) / $TESTS_RUN" | bc)%"
echo "# =============================================================================="
echo "# Tests ended at: $(date)"
echo "# =============================================================================="

# Return to the original directory where this script was run.
popd > /dev/null

# Return failure if any tests failed.
exit $TESTS_FAILED;

# EOF
