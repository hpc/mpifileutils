#!/bin/bash

NUM_FILES=20
NUM_PROC=5
FILE_SIZE=1M

TMPDIR=`pwd`/tmp
TMPCHCK=`pwd`/tmp/check
TARGET=$TMPDIR/test.tar

FILES=""



###Check options
while [[ $# -gt 1 ]]; do
	if [[ "$1" == "-S" ]]; then
	        FILE_SIZE=$2
       	        shift
		shift
 	fi
	if [[ "$1" == "-np" ]]; then
		NUM_PROC=$2
		shift
		shift
	fi
	if [[ "$1" == "-c" ]]; then
		NUM_FILES=$2
		shift
		shift
	fi
done	

###Create temporary directories
mkdir -p $TMPDIR
mkdir -p $TMPCHCK

###Write random files in base64 for checking
for i in $(seq 1 $NUM_FILES); do
	echo -ne "\rwriting file $i"
	FILES="$FILES file${i}.tmp"
	head -c $FILE_SIZE /dev/urandom | base64 > $TMPDIR/file$i.tmp
done

echo
echo "creating parallel tar file..."
cd $TMPDIR

###Create the tarfile
mpirun -np $NUM_PROC -machinefile ../machines ../dtar -c -f $TARGET $FILES

echo extracting tar file...

###Extract the resulting tarfile with GNU tar
tar -xf $TARGET -C $TMPCHCK

###Check extracted files against the originals
GOOD=1
for i in $(seq 1 $NUM_FILES); do
	echo checking file$i...
	DIFF=$(diff $TMPDIR/file${i}.tmp $TMPCHCK/file${i}.tmp)
	if [[ $DIFF -ne 0 ]]; then
		echo file $i does not match
		GOOD=0
	fi
done
if [[ $GOOD -eq 1 ]]; then
	echo
	echo "tar file is good"
fi

rm -rf $TMPDIR

