#!/bin/bash
export SAVE_PWD=${SAVE_PWD:-$PWD}
export FAIL_ON_ERROR=${FAIL_ON_ERROR:-yes}
export LUSTRE_ENABLE=${LUSTRE_ENABLE:-no}

log ()
{
	echo $*
}

pass() {
	$TEST_FAILED && echo -n "FAIL " || echo -n "PASS "
	echo $@
}

error_noexit()
{
	log "== ${TESTSUITE} ${TESTNAME} failed: $@ == `date +%H:%M:%S`"
	TEST_FAILED=true
}

error()
{
	error_noexit "$@"
	[ "$FAIL_ON_ERROR" = "yes" ] && exit 1 || true
}

error_exit() {
	error_noexit "$@"
	exit 1
}

cleanup_dir()
{
	local DIR=$1
	if [ "$DIR" = "" ]; then
		echo "no directory is given"
		exit 1;
	fi
	rm $DIR/* -fr
}

basetest() {
	if [[ $1 = [a-z]* ]]; then
		echo $1
	else
		echo ${1%%[a-z]*}
	fi
}

run_one() {
	testnum=$1
	message=$2
	export tfile=f${testnum}
	export tdir=d${testnum}

	local SAVE_UMASK=`umask`
	umask 0022

	local BEFORE=`date +%s`
	echo
	log "== test $testnum: $message == `date +%H:%M:%S`"

	export TESTNAME=test_$testnum
	TEST_FAILED=false
	test_${testnum} || error "test_$testnum failed with $?"

	cd $SAVE_PWD

	pass "($((`date +%s` - $BEFORE))s)"
	TEST_FAILED=false
	unset TESTNAME
	unset tdir
	umask $SAVE_UMASK
}

run_test()
{
	cleanup_dir $TEST_SRC
	cleanup_dir $TEST_DST
	run_one $1 "$2"
	RET=$?

	cleanup_dir $TEST_SRC
	cleanup_dir $TEST_DST
	return $RET
}

DCMP=install/bin/dcmp
DWALK=install/bin/dwalk
TEST_DIR=/tmp/dcmp_test
TEST_SRC=$TEST_DIR/src
TEST_DST=$TEST_DIR/dst
OUTPUT_FILE=/tmp/file
IOR=iortest/ior
CBIF=iortest/cbif

if [ ! -e $TEST_SRC ]; then
	mkdir $TEST_SRC -p
fi

if [ ! -e $TEST_DST ]; then
	mkdir $TEST_DST -p
fi


if [ ! -d $TEST_SRC ]; then
	error_exit "$TEST_SRC is not a directory"
fi

if [ ! -d $TEST_DST ]; then
	error_exit "$TEST_DST is not a directory"
fi

in_flist()
{
	OUTPUT_FILE=$1
	FILE_NAME=$2
	if [ "$OUTPUT_FILE" = "" ] ; then
		error_exit "output file is not given"
	fi

	if [ "$FILE_NAME" = "" ] ; then
		error_exit "output file is not given"
	fi

	FILE=$($DWALK --print --input $OUTPUT_FILE | grep $FILE_NAME | \
		awk -F File= '{print $2}')
	if [ "$FILE_NAME" = "$FILE" ]; then
		return 0;
	fi
	return 1;
}

test_0()
{
	touch $TEST_SRC/$tfile
	mkdir $TEST_SRC/$tdir
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o EXIST=ONLY_SRC:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		|| error "$TEST_SRC/$tfile not in file"
	in_flist $OUTPUT_FILE $TEST_SRC/$tdir \
		&& error "$TEST_SRC/$tdir is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		&& error "$TEST_DST/$tdir is printed"
	return 0
}
run_test 0 "check EXIST = ONLY_SRC"

test_1()
{
	touch $TEST_SRC/$tfile
	touch $TEST_DST/$tfile
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o EXIST=ONLY_DEST:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		&& error "$TEST_SRC/$tfile is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tfile \
		&& error "$TEST_DST/$tfile is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		|| error "$TEST_DST/$tfile is not printed"
	return 0
}
run_test 1 "check EXIST = ONLY_DEST"

test_2()
{
	touch $TEST_SRC/$tfile
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o EXIST=DIFFER:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		|| error "$TEST_SRC/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		|| error "$TEST_DST/$tfile is not printed"
	return 0
}
run_test 2 "check EXIST = DIFFER"

test_3()
{
	touch $TEST_SRC/$tfile
	touch $TEST_DST/$tfile
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o EXIST=COMMON:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		&& error "$TEST_DST/$tdir is printed"
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		|| error "$TEST_SRC/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tfile \
		|| error "$TEST_DST/$tfile is not printed"
	return 0
}
run_test 3 "check EXIST = COMMON"

test_4()
{
	touch $TEST_SRC/$tfile
	mkdir $TEST_SRC/$tdir
	mkdir $TEST_DST/$tfile
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o TYPE=DIFFER:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		|| error "$TEST_SRC/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_SRC/$tdir \
		&& error "$TEST_SRC/$tdir is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tfile \
		|| error "$TEST_DST/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		&& error "$TEST_DST/$tdir is printed"
	return 0
}
run_test 4 "check TYPE = DIFFER"

test_5()
{
	touch $TEST_SRC/$tfile
	mkdir $TEST_SRC/$tdir
	mkdir $TEST_DST/$tfile
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o EXIST=COMMON@TYPE=DIFFER:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		|| error "$TEST_SRC/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_SRC/$tdir \
		&& error "$TEST_SRC/$tdir is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tfile \
		|| error "$TEST_DST/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		&& error "$TEST_DST/$tdir is printed"
	return 0
}
run_test 5 "check (EXIST = COMMON) && (TYPE = DIFFER)"

test_6()
{
	touch $TEST_SRC/$tfile
	mkdir $TEST_SRC/$tdir
	mkdir $TEST_DST/$tfile
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o TYPE=DIFFER@EXIST=COMMON:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		|| error "$TEST_SRC/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_SRC/$tdir \
		&& error "$TEST_SRC/$tdir is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tfile \
		|| error "$TEST_DST/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		&& error "$TEST_DST/$tdir is printed"
	return 0
}
run_test 6 "check (TYPE = DIFFER) && (EXIST = COMMON)"

test_7()
{
	touch $TEST_SRC/$tfile
	mkdir $TEST_SRC/$tdir
	mkdir $TEST_DST/$tfile
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o TYPE=DIFFER@EXIST=DIFFER:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		&& error "$TEST_SRC/$tfile is printed"
	in_flist $OUTPUT_FILE $TEST_SRC/$tdir \
		&& error "$TEST_SRC/$tdir is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tfile \
		&& error "$TEST_DST/$tfile is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		&& error "$TEST_DST/$tdir is printed"
	return 0
}
run_test 7 "check (TYPE = DIFFER) && (EXIST = DIFFER)"

test_8()
{
	touch $TEST_SRC/$tfile
	mkdir $TEST_SRC/$tdir
	mkdir $TEST_DST/$tfile
	mkdir $TEST_DST/$tdir
	$DCMP $TEST_DST $TEST_SRC -o EXIST=DIFFER@TYPE=DIFFER:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		&& error "$TEST_SRC/$tfile is printed"
	in_flist $OUTPUT_FILE $TEST_SRC/$tdir \
		&& error "$TEST_SRC/$tdir is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tfile \
		&& error "$TEST_DST/$tfile is printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tdir \
		&& error "$TEST_DST/$tdir is printed"
	return 0
}
run_test 8 "check (EXIST = DIFFER) && (TYPE = DIFFER)"

test_9()
{
	touch $TEST_SRC/$tfile
	mkdir $TEST_SRC/$tdir
	mkdir $TEST_DST/$tfile
	$DCMP $TEST_DST $TEST_SRC -o TYPE=DIFFER,EXIST=DIFFER:$OUTPUT_FILE
	in_flist $OUTPUT_FILE $TEST_SRC/$tfile \
		|| error "$TEST_SRC/$tfile is not printed"
	in_flist $OUTPUT_FILE $TEST_SRC/$tdir \
		|| error "$TEST_SRC/$tdir is not printed"
	in_flist $OUTPUT_FILE $TEST_DST/$tfile \
		|| error "$TEST_DST/$tfile is not printed"
	return 0
}
run_test 9 "check (TYPE = DIFFER) || (EXIST = DIFFER)"


test_10()
{
	# make direcotry in lscratchd & create extra file
	# to check it is counting correctly
	touch extrafile.txt
	cp extrafile.txt ${TEST_SRC}/
	rm -rf extrafile.txt

	if [ $LUSTRE_ENABLE == 'yes' ];then
		# set the default striping on the directories
		lfs setstripe -c -1 $TEST_SRC
	fi

	# create a decent size file with IOR to test with 10GB 
	srun -p pdebug -n 10 \
	$IOR -k -a POSIX -b1g -t1m -o${TEST_SRC}/tmptest10 

	# after files are created copy them into different directories
	$DCP ${TEST_SRC}/tmptest10 ${TEST_DST}/

	# create some tmp files to work with for results
	touch tmpfile.txt
	touch output.txt

	# put dcmp results in a tmpfile 
	$DCMP ${TEST_SRC} ${TEST_DST} > tmpfile.txt

	# read the results to a tmpfile
	dcmp_results=$(grep -inr "[0-9]*/[0-9]*" tmpfile.txt >> output.txt)

	# remove extra text just keep line number and count
	remove_text=$(sed -ri 's/[^0-9|^:|^/]*//g' output.txt)

	# for this one file contents should be the same
	# and there should be more files in one of the directories
	SAME_RESULTS=""

	# variables for expected count results
	# these results mean line 5 and the 2/2 are
	# the actual directory counts, etc
	file_count_same="5::2/2"
	file_count_extras="2::1/0"

	SAME_COUNT=0
	EXTRAS_COUNT=0
	# loop through results line by line
	while read RESULTS
	do
		if [ "$RESULTS" == "$file_count_same" ]
		then
			$((SAME_COUNT++))
		fi
		if [ "$RESULTS" == "$file_count_extras" ]
		then 

			$((EXTRAS_COUNT++))
		fi
	done < output.txt

	if [ $SAME_COUNT == 0 ]; then
		error "Found same file contents: $file_count_same; FAIL"
	else
		echo "Found same file contents: $file_count_same; PASS"
	fi

	if [ $EXTRAS_COUNT == 0 ]; then
		error "Found files that dont exist in both directories: $file_count_extras; FAIL"
	else
		echo "Found files that dont exist in both directories: $file_count_extras; PASS"
	fi
	# reset tmp files
	truncate -s 0 tmpfile.txt
	truncate -s 0 output.txt
	 
	# change byte 0 of the file & see if dcmp detects change
	$CBIF ${TEST_SRC}/tmptest10 0 57

	# put dcmp results in a tmpfile 
	$DCMP ${TEST_SRC} ${TEST_DST} > tmpfile.txt

	# read the results to a tmpfile
	dcmp_results=$(grep -inr "[0-9]*/[0-9]*" tmpfile.txt >> output.txt)

	# remove extra text just keep line number and count
	remove_text=$(sed -ri 's/[^0-9|^:|^/]*//g' output.txt)

	# for this one file contents should be different
	DIFFERENT_RESULTS=""

	# expected result variables
	file_count_diff="6::1/1"

	DIFF_COUNT=0
	# loop over results line by line
	while read DIFFERENT_RESULTS
	do
		if [ "$DIFFERENT_RESULTS" == "$file_count_diff" ]
		then

			$((DIFF_COUNT++))
		fi
	done < output.txt

	if [ $DIFF_COUNT == 0 ]; then
		error "Found different file contents: $file_count_diff; FAIL"
	else
		echo "Found different file contents: $file_count_diff; PASS"
	fi
	# remove the tmp files and tmp directories 
	rm -rf tmpfile.txt
	rm -rf output.txt
	return 0
}

run_test 10 "Same, extras and diff comparison"
