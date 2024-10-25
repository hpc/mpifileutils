#!/bin/bash

##############################################################################
# Description:
#
#   Verify dsync handles data
#     - file data is identical after a copy
#     - file with differing data is copied if --contents arg is used
#     - file with differing data is not copied if --contents arg is not used and metadata match
#
# Notes:
#   - does not test whether data copies are spread across nodes/tasks evenly
#
##############################################################################

# Turn on verbose output
#set -x

DSYNC_TEST_BIN=${DSYNC_TEST_BIN:-${1}}
DSYNC_SRC_DIR=${DSYNC_SRC_DIR:-${2}}
DSYNC_DEST_DIR=${DSYNC_DEST_DIR:-${3}}
DSYNC_TMP_FILE=${DSYNC_TMP_FILE:-${4}}

echo "Using dsync binary at: $DSYNC_TEST_BIN"
echo "Using src directory at: $DSYNC_SRC_DIR"
echo "Using dest directory at: $DSYNC_DEST_DIR"
echo "Using directory tree: $DSYNC_TMP_FILE"

# tests not yet implemented

exit 0
