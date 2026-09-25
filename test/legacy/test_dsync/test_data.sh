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

#
# In resulting file, field 1 is sum, field 2 is filename
#
function sum_all_files()
{
	pushd $1 >/dev/null
	find . -type f -print0 | xargs --no-run-if-empty -0 md5sum | sort -k2
	popd >/dev/null
}

function sync_and_verify()
{
	local srcdir=$1
	local destdir=$2
	local name=$3
	local expectation=$4

	local result=0
	local dest_type=""

	src_sum=$(mktemp /tmp/sync_and_verify.src.XXXXX)
	sum_all_files $srcdir > $src_sum

	dest_sum=$(mktemp /tmp/sync_and_verify.dest.XXXXX)
	sum_all_files $destdir > $dest_sum

	dest_type=$(fs_type $destdir)

	quiet_opt="--quiet"
	contents_opt=""

	if [[ $name = "with_contents" ]]; then
		contents_opt="--contents"
	fi

	if [[ -n $mpirun ]]; then
		$mpirun $mpirun_opts ${MFU_TEST_BIN}/dsync $quiet_opt $contents_opt $srcdir $destdir
	else
		${MFU_TEST_BIN}/dsync $quiet_opt $contents_opt $srcdir $destdir
	fi
	rc=$?

	if [[ $rc -ne 0 ]]; then
		echo "dsync failed with rc $rc"
		result=1
	fi

	if [[ $result -eq 0 ]]; then
		after_sum=$(mktemp /tmp/sync_and_verify.after.XXXXX)
		sum_all_files $destdir > $after_sum

		expected_sum=$(mktemp /tmp/sync_and_verify.expected.XXXXX)

		case $expectation in
		  "union")
		        # need to keep the md5sum from dest for files that existed there but not src, before sync
			cat $src_sum > $expected_sum
			cat $dest_sum | while read csum fname; do
				if ! grep -w "$fname" $expected_sum >/dev/null 2>&1; then
					echo "$csum  $fname" >> $expected_sum
				fi
			done
			;;
		  "src_exactly")
			cat $src_sum > $expected_sum
			;;
		  "dest_exactly")
			cat $dest_sum > $expected_sum
			;;
		esac

		diff $after_sum $expected_sum
		result=$?
	fi

	if [ "$result" -eq 0 ]; then
		echo "PASSED verify of option $name for $destdir type $dest_type"
	else
		echo "FAILED verify of option $name for $destdir type $dest_type - sets differ"
		echo =======================
		echo "before: src_sum"
		cat $src_sum
		echo
		echo "before: dest_sum"
		cat $dest_sum
		echo
		echo "after: after_sum:"
		cat $after_sum
		echo
		echo "expected:"
		cat $expected_sum
		echo =======================
	fi

	rm $src_sum $dest_sum $after_sum $expected_sum

	return $result
}

# verify file data is identical after a copy
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
mkdir $DSYNC_DEST_DIR/stuff
$MFU_TEST_BIN/dfilemaker --nitems 1000-2000 --depth 5-6 --size 1MB-25MB $DSYNC_SRC_DIR/stuff

sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff checksums_verified union

# verify file with same type, size, owner, mtime, but differing data is NOT copied by default
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff
mkdir $DSYNC_SRC_DIR/stuff
mkdir $DSYNC_DEST_DIR/stuff

$MFU_TEST_BIN/dfilemaker --nitems 50-100 --depth 2-3 --size 1MB-25MB $DSYNC_SRC_DIR/stuff
cp -a $DSYNC_SRC_DIR/stuff/* $DSYNC_DEST_DIR/stuff
find $DSYNC_DEST_DIR/stuff -type f -print | while read fname; do
	dd if=/dev/urandom of=$fname bs=1K count=1 conv=notrunc seek=$((RANDOM % 64))
done
find $DSYNC_SRC_DIR/stuff -type f -print0 | xargs -0 touch --date="2004-02-29 16:21:42"
find $DSYNC_DEST_DIR/stuff -type f -print0 | xargs -0 touch --date="2004-02-29 16:21:42"

sync_and_verify $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff no_contents dest_exactly

# verify file with same type, size, owner, mtime, but differing data IS copied if --contents arg is used
sync_and_verify  $DSYNC_SRC_DIR/stuff $DSYNC_DEST_DIR/stuff with_contents src_exactly

# clean up
rm -fr $DSYNC_SRC_DIR/stuff
rm -fr $DSYNC_DEST_DIR/stuff

exit 0
