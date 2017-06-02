#!/bin/bash

##############################################################################
# Description:
#
#   A test to check if dcp works well with FIEMAP.
#
##############################################################################

# Turn on verbose output
#set -x

DCP_TEST_BIN=${DCP_TEST_BIN:-${1}}
DCP_MPIRUN_BIN=${DCP_MPIRUN_BIN:-${2}}
DCP_CMP_BIN=${DCP_CMP_BIN:-${3}}
DCP_SRC_DIR=${DCP_SRC_DIR:-${4}}
DCP_DEST_DIR=${DCP_DEST_DIR:-${5}}
DCP_TMP_FILE=${DCP_TMP_FILE:-${6}}

echo "Using dcp1 binary at: $DCP_TEST_BIN"
echo "Using mpirun binary at: $DCP_MPIRUN_BIN"
echo "Using cmp binary at: $DCP_CMP_BIN"
echo "Using src directory at: $DCP_SRC_DIR"
echo "Using dest directory at: $DCP_DEST_DIR"

#build checkfiemap if not found
CHECKFIEMAP=${CHECKFIEMAP:-"`dirname $0`/checkfiemap"}
if [ ! -f "$CHECKFIEMAP" ]; then
	cc `dirname $0`/checkfiemap.c -o `dirname $0`/checkfiemap
	if [[ $? -ne 0 ]]; then
		echo "Failed to build `dirname $0`/checkfiemap.c"
		exit 1;
	fi
	CHECKFIEMAP=`dirname $0`/checkfiemap
fi

# Create the two empty files.
touch $DCP_SRC_DIR/aaa
touch $DCP_DEST_DIR/aaa

$CHECKFIEMAP --test $DCP_SRC_DIR/aaa
if [[ $? -ne 0 ]]; then
	echo "Source filesystem $DCP_SRC_DIR does not support FIEMAP, skip testing"
	exit 0
fi

NEED_CHECK_FIEMAP=true

$CHECKFIEMAP --test $DCP_DEST_DIR/aaa
if [[ $? -ne 0 ]]; then
	echo "Destination filesystem $DCP_DEST_DIR does not support FIEMAP, won't check fiemap"
	NEED_CHECK_FIEMAP=false
fi

rm -f $DCP_SRC_DIR/aaa
rm -f $DCP_DEST_DIR/aaa

rm -f $DCP_SRC_DIR/$DCP_TMP_FILE
rm -f $DCP_DEST_DIR/$DCP_TMP_FILE

function test_fiemap {
	$CHECKFIEMAP $DCP_SRC_DIR/$DCP_TMP_FILE $1
	if [[ $? -ne 0 ]]; then
		echo "Fiemap mismatch: $DCP_SRC_DIR/$DCP_TMP_FILE"
		rm -f $DCP_SRC_DIR/$DCP_TMP_FILE
		rm -f $DCP_DEST_DIR/$DCP_TMP_FILE
		exit 1
	fi

	$DCP_MPIRUN_BIN -np 3 $DCP_TEST_BIN -S $DCP_SRC_DIR/$DCP_TMP_FILE $DCP_DEST_DIR
	if [[ $? -ne 0 ]]; then
		echo "Fialed to run cmd: $DCP_MPIRUN_BIN -np 3 $DCP_TEST_BIN -S $DCP_SRC_DIR/$DCP_TMP_FILE $DCP_DEST_DIR"
		rm -f $DCP_SRC_DIR/$DCP_TMP_FILE
		rm -f $DCP_DEST_DIR/$DCP_TMP_FILE
		exit 1
	fi

	# Stat the two random files.
	stat $DCP_SRC_DIR/$DCP_TMP_FILE
	if [[ $? -ne 0 ]]; then
		echo "Failed to stat $DCP_SRC_DIR/$DCP_TMP_FILE"
		rm -f $DCP_SRC_DIR/$DCP_TMP_FILE
		rm -f $DCP_DEST_DIR/$DCP_TMP_FILE
		exit 1
	fi
	stat $DCP_DEST_DIR/$DCP_TMP_FILE
	if [[ $? -ne 0 ]]; then
		echo "Failed to stat $DCP_DEST_DIR/$DCP_TMP_FILE"
		rm -f $DCP_SRC_DIR/$DCP_TMP_FILE
		rm -f $DCP_DEST_DIR/$DCP_TMP_FILE
		exit 1
	fi

	$DCP_CMP_BIN $DCP_SRC_DIR/$DCP_TMP_FILE $DCP_DEST_DIR/$DCP_TMP_FILE
	if [[ $? -ne 0 ]]; then
	    echo "CMP mismatch: $DCP_SRC_DIR/$DCP_TMP_FILE $DCP_DEST_DIR/$DCP_TMP_FILE."
	    exit 1
	fi
	if [ $NEED_CHECK_FIEMAP == true ]; then
		$CHECKFIEMAP $DCP_DEST_DIR/$DCP_TMP_FILE $1
		if [[ $? -ne 0 ]]; then
			echo "Fiemap mismatch: $DCP_DEST_DIR/$DCP_TMP_FILE"
			rm -f $DCP_SRC_DIR/$DCP_TMP_FILE
			rm -f $DCP_DEST_DIR/$DCP_TMP_FILE
			exit 1
		fi
	fi

	rm -f $DCP_SRC_DIR/$DCP_TMP_FILE
	rm -f $DCP_DEST_DIR/$DCP_TMP_FILE
}

echo "Subtest 1, no holes."
# Create source file, no holes.
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=4M count=1
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=4M seek=1 count=1

test_fiemap 8388608

echo "Subtest 2, front 4K hole."
# Create source file, front 4K hole.
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=4K seek=1 count=1
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=4k seek=2 count=1

test_fiemap 8192

echo "Subtest 3, middle 1G hole."
# Create source file, middle 1G hole.
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=1M count=1
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=1M seek=1025 count=1

test_fiemap 2097152

echo "Subtest 4, end 1G hole."
# Create source file, middle 1G hole.
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=1M count=1
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=1M seek=1025 count=0

test_fiemap 1048576

echo "Subtest 5, front 4M, middle 1G, end 1G hole."
# Create source file, middle 1G hole.
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=4M seek=1 count=1
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=4M seek=258 count=1
dd if=/dev/urandom of=$DCP_SRC_DIR/$DCP_TMP_FILE bs=4M seek=515 count=0

test_fiemap 8388608

exit 0
