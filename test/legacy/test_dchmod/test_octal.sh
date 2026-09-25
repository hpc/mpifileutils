#!/bin/bash

# test to make sure all permissions are being set in octal mode on all files recurseively

# TODO: need to figure out how to check all of the expected results when user read and write
# bits are turned off because find gets a permission denied

# generate random number between 700 - 777 to use for octal
OCTAL="$(shuf -i700-777 -n1)"
echo "OCTAL value generated is: $OCTAL"

# TODO: directories to test, maybe I should use more files/directories???
# TODO: generate longer file trees here eventually
mkdir -p ~/tmp0/tmp1/tmp2

# get directory where dchmod command is, set default to home directory if nothing passed in
if [ "$#" -eq 0 ]; then
        echo "provide path to dchmod" 
else 
        DCHMOD=$1
fi

# turn on permissions, then check all of the files/directories are set 
$DCHMOD -v -m $OCTAL ~/tmp0

EXPECTED_RESULT=$OCTAL

# total count, and passed count variables to check that they match
TOTAL_FILE_COUNT=0
PASSED_FILE_COUNT=0

for file in $(find ~/tmp0); do
        RESULT="$(stat -c '%a' $file)"
        TOTAL_FILE_COUNT=$((TOTAL_FILE_COUNT+1))
        if [ ${RESULT} == $EXPECTED_RESULT ]; then
                PASSED_FILE_COUNT=$((PASSED_FILE_COUNT+1))
                echo "$file : PASS"
                echo "$file: ${RESULT} matches expected: $EXPECTED_RESULT" 
        else 
                echo "FAILED TO SET PERMISSION ON: $file"
                echo "$file: ${RESULT} doesn't match expected: $EXPECTED_RESULT" 
        fi
done

echo "--------------------------------"
echo "TOTAL_FILE_COUNT: $TOTAL_FILE_COUNT"
echo "PASSED_FILE_COUNT: $PASSED_FILE_COUNT"
echo "--------------------------------"

rm -rf ~/tmp0
