#!/bin/bash

. utility/set_funcs.sh

##############################################################################
# Description:
#
#   Check sequential syncs for unexpected changes
#     - after dsync between src and dest, a second dsync copies nothing
#     - after rsync between src and dest, a dsync copies nothing
#     - after dsync between src and dest, a rsync copies nothing
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

rsync=$(which rsync 2>/dev/null)
if [[ -z $rsync ]]; then
	echo "test_rsync.sh: unable to find rsync in path, exiting"
	exit 1
fi

mpirun=$(which mpirun 2>/dev/null)
mopts=""
if [[ -n $mpirun ]]; then
	procs=$(( $(nproc ) / 8 ))
	if [[ $procs -gt 16 ]]; then
		procs=16
	fi
	mopts="-c $procs"

	echo "Using mpirun: $mpirun $mopts"
fi

echo "Using MFU binaries at: $MFU_TEST_BIN"
echo "Using src parent directory at: $DSYNC_SRC_BASE"
echo "Using dest parent directory at: $DSYNC_DEST_BASE"
echo "Using rsync at: $rsync"

DSYNC_SRC=$(mktemp --directory ${DSYNC_SRC_BASE}/${DSYNC_TREE_NAME}.XXXXX)
DSYNC_DEST=$(mktemp --directory ${DSYNC_DEST_BASE}/${DSYNC_TREE_NAME}.XXXXX)

function fs_type()
{
	fname=$1
	df -T ${fname} | awk '$1 != "Filesystem" {print $2}'
}

function list_all_files()
{
	find $1 -printf '%P\n' | sort | grep -v '^$'
}

function dsync_and_verify()
{
	local src=$1
	local dest=$2
	local name=$3
	local expectation=$4

	local rc=0
	local result=0
	local dest_type=""

	dsync_output=$(mktemp /tmp/test_multiple_syncs.dsync_output.XXXXX)
	if [[ -n $mpirun ]]; then
		$mpirun $mopts ${MFU_TEST_BIN}/dsync --delete $src $dest > $dsync_output 2>&1
	else
		${MFU_TEST_BIN}/dsync --delete $src $dest > $dsync_output 2>&1
	fi
	rc=$?

	if [[ $rc -ne 0 ]]; then
		echo "dsync failed with rc $rc"
		result=1
	fi

	if [[ $expectation = "initial_sync" ]]; then
		rm $dsync_output
		return $result
	fi

	unexpected_changes=$(mktemp /tmp/test_multiple_syncs.unexpected.XXXXX)
	if [[ $rc -eq 0 ]]; then
		grep -E -e "Creating [0-9][0-9]* (files|directories)" \
			-e "Copy data:" \
			-e "Updated [0-9][0-9]* items" \
			$dsync_output > $unexpected_changes
		if [[ $? -eq 0 ]]; then
			result=1
		fi
	fi

	if [ "$result" -eq 0 ]; then
		echo "PASSED verify of option $name for $dest type $dest_type"
	else
		echo "FAILED verify of option $name for $dest type $dest_type"
		echo =======================
		echo "unexpected changes:"
		cat $unexpected_changes
		echo =======================
	fi

	rm $dsync_output $unexpected_changes

	return $result
}

function rsync_and_verify()
{
	local src=$1
	local dest=$2
	local name=$3
	local expectation=$4

	local rc=0
	local result=0
	local dest_type=""

	rsync_output=$(mktemp /tmp/test_multiple_syncs.rsync_output.XXXXX)
	$rsync -av -HAX $src $dest > $rsync_output 2>&1
	rc=$?

	if [[ $rc -ne 0 ]]; then
		echo "rsync failed with rc $rc"
		result=1
	fi

	if [[ $expectation = "initial_sync" ]]; then
		rm $rsync_output
		return $result
	fi

	unexpected_changes=$(mktemp /tmp/test_multiple_syncs.unexpected.XXXXX)
	if [[ $rc -eq 0 ]]; then
		grep -v -e "^sending incremental" \
			-e "^sent [1-9][0-9,]* bytes" \
			-e "^total size is" \
			-e "^$" \
			$rsync_output > $unexpected_changes
		if [[ $? -eq 0 ]]; then
			result=1
		fi
	fi

	if [ "$result" -eq 0 ]; then
		echo "PASSED verify of option $name for $dest type $dest_type"
	else
		echo "FAILED verify of option $name for $dest type $dest_type"
		echo =======================
		echo "unexpected changes:"
		cat $unexpected_changes
		echo =======================
	fi

	rm $rsync_output $unexpected_changes

	return $result
}

# generate test data
# dfilemaker options: -d/--depth , -s/--size, -n/--nitems
rm -fr $DSYNC_SRC/stuff
mkdir $DSYNC_SRC/stuff
${MFU_TEST_BIN}/dfilemaker -d 5-10 -n 100-300 -s 1MB-10MB $DSYNC_SRC/stuff

# after dsync between src and dest, a second dsync copies nothing
rm -fr $DSYNC_DEST/stuff
dsync_and_verify  $DSYNC_SRC/stuff $DSYNC_DEST/stuff multi-dsync initial_sync
dsync_and_verify  $DSYNC_SRC/stuff $DSYNC_DEST/stuff multi-dsync no_change

# after rsync between src and dest, a dsync copies nothing
rm -fr $DSYNC_DEST/stuff
rsync_and_verify  $DSYNC_SRC/stuff/ $DSYNC_DEST/stuff rsync_then_dsync initial_sync
dsync_and_verify  $DSYNC_SRC/stuff $DSYNC_DEST/stuff rsync_then_dsync no_change

# after dsync between src and dest, a rsync copies nothing
rm -fr $DSYNC_DEST/stuff
dsync_and_verify  $DSYNC_SRC/stuff $DSYNC_DEST/stuff dsync_then_rsync initial_sync
rsync_and_verify  $DSYNC_SRC/stuff/ $DSYNC_DEST/stuff dsync_then_rsync no_change

# clean up
rm -fr $DSYNC_SRC/stuff
rm -fr $DSYNC_DEST/stuff

exit 0
