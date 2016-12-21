#!/bin/bash

# some simple test cases to quickly verify dcmp is working

# variable for test directories
TESTDIR1=/p/lscratchd/sikich1/testdcmp1
TESTDIR2=/p/lscratchd/sikich1/testdcmp2

# make direcotry in lscratchd & create extra file
# to check it is counting correctly
touch extrafile.txt
mkdir -p ${TESTDIR1}
mkdir -p ${TESTDIR2}
cp extrafile.txt ${TESTDIR1}/
rm -rf extrafile.txt

# set the default striping on the directories
lfs setstripe -c -1 $TESTDIR1

# create a decent size file with IOR to test with 10GB 
srun -p pdebug -n 10 \
iortest/IOR -k -a POSIX -b1g -t1m -o${TESTDIR1}/tmptest10 

# after files are created copy them into different directories
install/bin/dcp2 ${TESTDIR1}/tmptest10 ${TESTDIR2}/

# create some tmp files to work with for results
touch tmpfile.txt
touch output.txt

# put dcmp results in a tmpfile 
install/bin/dcmp ${TESTDIR1} ${TESTDIR2} > tmpfile.txt

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

PASS_COUNT=0

# loop through results line by line
while read RESULTS
do
        if [ "$RESULTS" == "$file_count_same" ]
        then
                echo "Found same file contents: $file_count_same; PASS"
                $((PASS_COUNT++))
        fi
        if [ "$RESULTS" == "$file_count_extras" ]
        then 
                echo "Found files that dont exist in both directories: $file_count_extras; PASS"
                $((PASS_COUNT++))
        fi
done < output.txt

# reset tmp files
truncate -s 0 tmpfile.txt
truncate -s 0 output.txt
 
# change byte 0 of the file & see if dcmp detects change
iortest/cbif ${TESTDIR1}/tmptest10 0 57

# put dcmp results in a tmpfile 
install/bin/dcmp ${TESTDIR1} ${TESTDIR2} > tmpfile.txt

# read the results to a tmpfile
dcmp_results=$(grep -inr "[0-9]*/[0-9]*" tmpfile.txt >> output.txt)

# remove extra text just keep line number and count
remove_text=$(sed -ri 's/[^0-9|^:|^/]*//g' output.txt)

# for this one file contents should be different
DIFFERENT_RESULTS=""

# expected result variables
file_count_diff="6::1/1"

# loop over results line by line
while read DIFFERENT_RESULTS
do
        if [ "$DIFFERENT_RESULTS" == "$file_count_diff" ]
        then
                echo "Found different file contents: $file_count_diff; PASS"
                $((PASS_COUNT++))
        fi
done < output.txt

echo $PASS_COUNT
echo "TESTS PASSED: $PASS_COUNT/3"

# remove the tmp files and tmp directories 
rm -rf tmpfile.txt
rm -rf output.txt
install/bin/drm ${TESTDIR1} ${TESTDIR2}
