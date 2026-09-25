#!/bin/bash

. utility/set_funcs.sh

##############################################################################
# Description:
#
#   Verify dsync handles presence or absence
#     - empty source, but non-empty destination
#     - non-empty source, but empty destination
#     - directories on source, but not on destination, are copied
#     - files on source, but not on destination, are copied
#     - directories on destination, but not source, are undisturbed without --delete
#     - files on destination, but not source, are undisturbed without --delete
#     - directories on destination, but not source, are removed with --delete
#     - files on destination, but not source, are removed with --delete
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

echo "Using MFU binaries at: $MFU_TEST_BIN"
echo "Using src parent directory at: $DSYNC_SRC_BASE"
echo "Using dest parent directory at: $DSYNC_DEST_BASE"

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
		$mpirun $mpirun_opts ${MFU_TEST_BIN}/dsync $quiet_opt $delete_opt $srcdir $destdir
	else
		${MFU_TEST_BIN}/dsync $quiet_opt $delete_opt $srcdir $destdir
	fi
	rc=$?

	if [[ $rc -ne 0 ]]; then
		echo "dsync failed with rc $rc"
		result=1
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

# empty source, but non-empty destination
# directories on destination, but not source, are undisturbed without --delete
# files on destination, but not source, are undisturbed without --delete
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
mkdir $DSYNC_DEST_DIR/stuff
${MFU_TEST_BIN}/dfilemaker --depth 5-10 --nitems 100-300 --size 1MB-10MB $DSYNC_DEST_DIR/stuff
sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff empty_source union

# non-empty source, but empty destination
# files on source, but not on destination, are copied
# directories on source, but not on destination, are copied
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
${MFU_TEST_BIN}/dfilemaker --depth 5-10 --nitems 100-300 --size 1MB-10MB $DSYNC_SRC_DIR/stuff
sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff empty_destination union

# directories on destination, but not source, are removed with --delete
# files on destination, but not source, are removed with --delete
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
mkdir $DSYNC_DEST_DIR/stuff
${MFU_TEST_BIN}/dfilemaker --depth 5-10 --nitems 100-300 --size 1MB-10MB $DSYNC_SRC_DIR/stuff
mkdir $DSYNC_DEST_DIR/stuff/destdir
touch $DSYNC_DEST_DIR/stuff/destfile
sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff delete src_exactly

# clean up
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff

exit 0
