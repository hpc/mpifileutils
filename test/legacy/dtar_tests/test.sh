#!/bin/bash

NUM_FILES=10
NUM_PROC=5
FILE_SIZE=1K

TMPDIR=`pwd`/tmp
TMPCHCK=`pwd`/check
TARGET=`pwd`/test.tar

D=0
L=0
OPTIONS='-s -np -c -d -l'

###Check options
while [[ $# -gt 0 ]]; do
	if [[ "$1" == "-s" ]]; then
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
	if [[ "$1" == "-d" ]]; then
		D=1
		shift
	fi
	if [[ "$1" == "-l" ]]; then
		L=1
		shift
	fi
	if [[ "$1" == "--DEBUG" ]]; then
		DEBUG=1
		shift
	fi
done	

###Create temporary directories
mkdir -p $TMPDIR
mkdir -p $TMPCHCK


###Write random files and direcotories in base64 for checking
cd $TMPDIR
function write {
	for i in $(seq 1 $1); do
		if [ $(($RANDOM % 100 )) -lt 20 ] && [ $D -eq 1 ]; then
			mkdir dir${i}
		        cd dir${i}
			write $(( $1/2 ))	
		else
			echo -ne "\rwriting files..."
			head -c $FILE_SIZE /dev/urandom | base64 > file$i.tmp
		fi
	done
	cd ..
}
write $NUM_FILES

###Insert symbolic links
if [[ $L -eq 1 ]]; then
	ln -s .$0 $TMPDIR/filelink.lnk
	ln -s /usr/bin $TMPDIR/dirlink
fi

echo

###Create the tarfile
echo "creating parallel tar file..."
mpirun -np $NUM_PROC ./dtar -cf $TARGET $TMPDIR
###Extract the resulting tarfile with GNU tar
echo extracting tar file...
tar -xf $TARGET -C $TMPCHCK

###Check extracted files against the originals
GOOD=1
echo checking files...
diff -q -r $TMPDIR $TMPCHCK/tmp
DIFF=$?
if [[ $DEBUG -eq 1 ]]; then
	echo "diff: $DIFF"
fi
echo
if [[ $DIFF -ne 0 ]]; then
	if [[ "$DIFF" == *".lnk"* ]]; then
		echo "symbolic link failure"
	else
		echo "resulting archive is invalid"
	fi
else
	echo "tar file is good"
fi
if [[ $DEBUG -ne 1 ]]; then
	rm -rf $TMPDIR $TMPCHCK $TARGET
fi
