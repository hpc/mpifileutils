#!/bin/bash

. utility/set_funcs.sh

##############################################################################
# Description:
#
#   Verify dsync fails when it should
#      - source does not exist
#      - parent of dest does not exist
#
# Notes:
#
##############################################################################

# Turn on verbose output
#set -x

MFU_TEST_BIN=${MFU_TEST_BIN:-${1}}
DSYNC_SRC_BASE=${DSYNC_SRC_BASE:-${2}}
DSYNC_DEST_BASE=${DSYNC_DEST_BASE:-${3}}
DSYNC_TREE_NAME=${DSYNC_TREE_NAME:-${4}}

echo "Using MFU binaries at: $MFU_TEST_BIN"
echo "Using src parent directory at: $DSYNC_SRC_BASE"
echo "Using dest parent directory at: $DSYNC_DEST_BASE"

DSYNC_SRC_DIR=$(mktemp --directory ${DSYNC_SRC_BASE}/${DSYNC_TREE_NAME}.XXXXX)
DSYNC_DEST_DIR=$(mktemp --directory ${DSYNC_DEST_BASE}/${DSYNC_TREE_NAME}.XXXXX)

function sync_and_verify()
{
	local srcdir=$1
	local destdir=$2
	local name=$3
	local expectation=$4

	local result=0
	local dest_type="unknown"

	quiet_opt="--quiet"

	${MFU_TEST_BIN}/dsync $quiet_opt $delete_opt $srcdir $destdir
	rc=$?
	echo "dsync failed with rc $rc"

	if [[ "$rc" -eq 0 ]]; then
		echo "FAILED verify of option $name for $destdir type $dest_type"
		result=1

	else
		echo "PASSED verify of option $name for $destdir type $dest_type"
		result=0
	fi

	return $result
}

# source does not exist
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_DEST_DIR/stuff
sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff source_missing expect_dsync_fail

# parent of dest does not exist
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff/childdir dest_parent_missing expect_dsync_fail

# clean up
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff

exit 0
