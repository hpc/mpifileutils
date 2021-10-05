#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dsync properly copies xattrs
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
echo "Using temp file name: $DSYNC_TMP_FILE"

set -a lustre_xattr_names
lustre_xattr_names=("lustre.lov" "trusted.som" "trusted.lov" "trusted.lma" "trusted.lmv" "trusted.dmv" "trusted.link" "trusted.fid" "trusted.version" "trusted.hsm" "trusted.lfsck_bitmap" "trusted.dummy")

set -a other_xattr_names
other_xattr_names=("xattr1" "456" "testing" "sync_and_verify_test")

function fs_type()
{
	fname=$1
	df -T ${fname} | awk '$1 != "Filesystem" {print $2}'
}

# set all xattrs in other_xattr_names on file
# no need for equivalent for lustre xattrs, because they cannot be set from userspace
function set_other_xattrs()
{
	fname=$1

	set -e
	for attrname in ${other_xattr_names[*]}; do
		attr -s $attrname -V "$attrname:1234567890" $fname
	done
	set +e
}

function list_all_xattrs()
{
	fname=$1

	echo Listing xattrs on $fname
	attr -l $fname
}

function compare_xattr_lists()
{
	fname0=$1
	fname1=$2

	for attrname in ${lustre_xattr_names[*]} ${other_xattr_names[*]}; do
		in0=$(attr -g -q $attrname $fname0 2>/dev/null && echo 1)
		in1=$(attr -g -q $attrname $fname1 2>/dev/null && echo 1)
		if [ "$in0" -eq 1 -o "$in1" -eq 1 ]; then
			echo "$attrname $in0 $in1"
		fi
	done
}

function sync_and_verify()
{
	local srcdir=$1
	local destdir=$2
	local name=$3
	local opt=$4

	local result=0
	local dest_type=$(fs_type $destdir)

	if [ $opt = "non-lustre" ]; then
		echo "SKIPPED verify of option $opt"
		return 0
	fi

	if [ $opt = "libattr" -a $(id -u) -ne 0 ]; then
		echo "SKIPPED verify of option $opt, need root to test"
		return 0
	fi

	set -e
	rm -f $destdir/$name

	if [ $opt = "libattr" ]; then
		echo "user.sync_and_verify_test  skip" >> /etc/xattr.conf
	fi

	$DSYNC_TEST_BIN --quiet --xattrs=$opt $srcdir $destdir

	if [ $opt = "libattr" ]; then
		sed --in-place "/^user.sync_and_verify_test/d" /etc/xattr.conf
	fi

	srclog=$(mktemp /tmp/sync_and_verify.src.XXXXX)
	destlog=$(mktemp /tmp/sync_and_verify.dest.XXXXX)

	list_all_xattrs  $srcdir/$name  | awk '!/Listing xattrs on/ {print $1,$2,$3,$4,$5}'  > $srclog
	list_all_xattrs  $destdir/$name | awk '!/Listing xattrs on/ {print $1,$2,$3,$4,$5}'  > $destlog

	case $opt in
	  "none")
		result=$(wc -c < $destlog)
		;;
	  "all")
		diff $srclog $destlog
		result=$?
		;;
	  "libattr")
		diff <(grep -v -w sync_and_verify_test $srclog) $destlog
		result=$?
		;;
	esac

	set +e

	if [ "$result" -eq 0 ]; then
		echo "PASSED verify of option $opt for $destdir type $dest_type"
	else
		echo "FAILED verify of option $opt for $destdir type $dest_type"
		echo =======================
		echo "dest:"
		cat $destlog
		echo =======================
		exit 1
	fi

	rm $srclog $destlog

	return $result
}

# Create the source
echo Preparing Source
touch $DSYNC_SRC_DIR/aaa
set_other_xattrs $DSYNC_SRC_DIR/aaa

# Make sure the short option is accepted; rest of tests use long option
set -e
$DSYNC_TEST_BIN --quiet -X all $DSYNC_SRC_DIR $DSYNC_DEST_DIR
set +e

# Sync and verify
echo
echo Testing dsync
sync_and_verify  $DSYNC_SRC_DIR $DSYNC_DEST_DIR aaa none
sync_and_verify  $DSYNC_SRC_DIR $DSYNC_DEST_DIR aaa all
sync_and_verify  $DSYNC_SRC_DIR $DSYNC_DEST_DIR aaa non-lustre
sync_and_verify  $DSYNC_SRC_DIR $DSYNC_DEST_DIR aaa libattr

# Clean up
rm $DSYNC_SRC_DIR/aaa

exit 0
