#!/bin/bash

##############################################################################
# Description:
#
#   Verify dsync handles metdata
#     - file with differing metadata is copied
#     - file metadata is identical after a copy
#     - directory metadata is identical after a sync
#
# Notes:
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
