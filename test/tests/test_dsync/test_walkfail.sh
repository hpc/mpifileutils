#!/bin/bash

. utility/set_funcs.sh

##############################################################################
# Description:
#
#   Verify dsync handles an error during source or destination walk correctly
#     - source walk fails without --delete arg
#     - source walk fails with --delete arg
#     - destination walk fails without --delete arg
#     - destination walk fails with --delete arg
#
# Notes:
#   Need to figure out what correct behavior is for all the above cases, and
#   document what we decide on.  One source of ideas is the rsync man page.
#
##############################################################################

# Turn on verbose output
#set -x

DSYNC_TEST_BIN=${DSYNC_TEST_BIN:-${1}}
DSYNC_SRC_BASE=${DSYNC_SRC_BASE:-${2}}
DSYNC_DEST_BASE=${DSYNC_DEST_BASE:-${3}}
DSYNC_TREE_NAME=${DSYNC_TREE_NAME:-${4}}

DSYNC_TREE_DATA=/usr/include/c++

mpirun=$(which mpirun 2>/dev/null)
mpirun_opts=""
if [[ -n $mpirun ]]; then
	procs=$(( $(nproc ) / 8 ))
	if [[ $procs -gt 16 ]]; then
		procs=16
	fi
	mpirun_opts="-c $procs"

	echo "Using mpirun: $mpirun $mpirun_opts"
fi

echo "Using dsync binary at: $DSYNC_TEST_BIN"
echo "Using src parent directory at: $DSYNC_SRC_BASE"
echo "Using dest parent directory at: $DSYNC_DEST_BASE"
echo "Using test data from: $DSYNC_TREE_DATA"

DSYNC_SRC_DIR=$(mktemp --directory ${DSYNC_SRC_BASE}/${DSYNC_TREE_NAME}.XXXXX)
DSYNC_DEST_DIR=$(mktemp --directory ${DSYNC_DEST_BASE}/${DSYNC_TREE_NAME}.XXXXX)

function fs_type()
{
	fname=$1
	df -T ${fname} | awk '$1 != "Filesystem" {print $2}'
}

function list_all_files()
{
	find $1 -printf '%P\n' | sort | grep -v '^$'
}

function sync_and_verify()
{
	local srcdir=$1
	local destdir=$2
	local name=$3
	local expectation=$4

	local result=0
	local dest_type=""

	src_list=$(mktemp /tmp/sync_and_verify.src.XXXXX)
	list_all_files $srcdir > $src_list

	dest_list=$(mktemp /tmp/sync_and_verify.dest.XXXXX)

	# dest may not exist, but its parent must exist
	if [[ -d $destdir ]]; then
		dest_type=$(fs_type $destdir)
		list_all_files $destdir > $dest_list
	else
		parent=$(dirname $destdir)
		if [[ ! -d $parent ]]; then
			echo "sync_and_verify: destdir $destdir and its parent do not exist"
			exit 1
		fi
		dest_type=$(fs_type $parent)
	fi

	quiet_opt="--quiet"
	delete_opt=""
	if [[ $name = "delete" ]]; then
		delete_opt="--delete"
	fi

	if [[ -n $mpirun ]]; then
		$mpirun $mpirun_opts $DSYNC_TEST_BIN $quiet_opt $delete_opt $srcdir $destdir
	else
		$DSYNC_TEST_BIN $quiet_opt $delete_opt $srcdir $destdir
	fi
	rc=$?

	if [[ $rc -ne 0 ]]; then
		echo "dsync failed with rc $rc"
		result=1
	fi

	if [[ $expectation = "union" && $name = "delete" ]]; then
		# dsync will have copied the "000" perms on destination, which would prevent us
		# from walking to get true "after list".  Change that, our test is just verifying
		# presence or absence of files, not metadata set on destination.
		chmod 755 $DSYNC_DEST_DIR/stuff/on/both/trees
	fi

	if [[ $result -eq 0 ]]; then
		after_list=$(mktemp /tmp/sync_and_verify.after.XXXXX)
		list_all_files $destdir > $after_list

		expected_list=$(mktemp /tmp/sync_and_verify.expected.XXXXX)

		case $expectation in
		  "union")
			union $src_list $dest_list > $expected_list
			;;
		  "src_exactly")
			cat $src_list > $expected_list
			;;
		esac

		sets_equal $after_list $expected_list
		result=$?
	fi

	if [ "$result" -eq 0 ]; then
		echo "PASSED verify of option $name for $destdir type $dest_type"
	else
		echo "FAILED verify of option $name for $destdir type $dest_type"
		echo =======================
		echo "before: src_list"
		cat $src_list
		echo
		echo "before: dest_list"
		cat $dest_list
		echo
		echo "after: after_list:"
		cat $after_list
		echo
		echo "expected:"
		cat $expected_list
		echo =======================
	fi

	rm $src_list $dest_list $after_list $expected_list

	return $result
}

# source walk fails without --delete arg
# not implemented

# destination walk fails without --delete arg
# not implemented

# destination walk fails with --delete arg
# not implemented

# source walk fails with --delete arg
# no files or directories on destination are deleted
# subset of source files discovered before walk failure are copied to test

rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
mkdir $DSYNC_DEST_DIR/stuff

mkdir -p  $DSYNC_SRC_DIR/stuff/on/both/trees/deepdir
mkdir     $DSYNC_SRC_DIR/stuff/on/both/shallow
mkdir -p $DSYNC_DEST_DIR/stuff/on/both/trees/deepdir
mkdir    $DSYNC_DEST_DIR/stuff/on/both/shallow

# not reachable during walk due to permissions
chmod 000 $DSYNC_SRC_DIR/stuff/on/both/trees

# delete should be disabled, so no files deleted
# non-walkable src will not be copied, but also not in "$src_list"
# union is of walkable part of src dir and dest dir (all walkable)
sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff delete union

# clean up
#chmod 755 $DSYNC_SRC_DIR/stuff/on/both/trees
#chmod 755 $DSYNC_DEST_DIR/stuff/on/both/trees
#rm -fr $DSYNC_SRC_DIR/stuff
#rm -fr $DSYNC_DEST_DIR/stuff

exit 0
