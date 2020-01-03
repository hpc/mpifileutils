#!/bin/bash
DFIND_TEST_VERBOSE=0
####################
# Author: Robert E. Novak
# email: novak5@llnl.gov
#
# This test is designed to show the differences between Gnu find and dfind
# in processing spaces " " in filenames for the exec command.
#
# The challenge with this script is the large number of definitions that all
# look similar.  In theory as we develop more tests, very litle in the
# base version of this file has to change.
####################
# Define all of the hpc FIND command variables that come in from the 
# command line and will eventually be passed to FIEMAP for parallel testing
#
# Just to make this (hopefully) a little less confusing
#
# dfind ........ the binary under test = DFIND_TEST_BIN
# mpirun|srun .. the command to execute the dfind on multiple machines
#                DFIND_MPIRUN_BIN
# diff ......... the command to compare the difference in output between
#                dfind and find (GNU find since we want to emulate
#                the semantics
#                DFIND_CMP_BIN
# ../test/src .. the directory relative to mpifileutils where source
#                test data is place
#                DFIND_SRC_DIR
# ../test/dest . The directory relative to mpifileutils where test
#                output is placed
#                DFIND_DEST_DIR
# dfind.test.sh  The shell script generated inside this script to perform
#                the testing.
#                DFIND_TMP_FILE
# ../test/bin .. the directory in which executable files (including this
#                script and the generated script) are placed to avoid
#                cluttering up the project ../install/bin
#                DFIND_TESTING_BIN_DIR
####################
####################
# A final word about the .. relativity to the project parent directory.
# The build process for mpifileutils installs the following:
# ../install/bin
# ../install/include
# ../install/lib
# ../install/lib64
# ../install/share
#
# In order to avoid cluttering the installation directory further, the
# testing src, dest and bin directories are placed in 
# ../test
#
# To facilitate finding the root of the project from within the directory
# where you run the tests (the above was done to keep from cluttering this
# directory as well), there is a short script "getgitroot" which determines
# the project root directory by walking up the tree to find the .git
# directory:
# Find and return the parent directory that contains ".git"
#
# That is the parent directory that is the "root" of all evil so that we can
# execute binaries located in ../install/bin
####################
# while [ ! -d .git ]
# do
# 	cd ..
# done
# pwd
####################
DFIND_TEST_BIN=${DFIND_TEST_BIN:-${1}}
DFIND_MPIRUN_BIN=${DFIND_MPIRUN_BIN:=${2}}
DFIND_CMP_BIN=${DFIND_CMP_BIN:-${3}}
DFIND_SRC_DIR=${DFIND_SRC_DIR:-${4}}
DFIND_DEST_DIR=${DFIND_DEST_DIR:-${5}}
DFIND_TMP_FILE=${DFIND_TMP_FILE:-${6}}
DFIND_TESTING_BIN_DIR=${DFIND_TESTING_BIN_DIR:-${7}}
echo "\${DFIND_TESTING_BIN_DIR} = ${DFIND_TESTING_BIN_DIR}"
####################
# Define the Test Number
####################
DFIND_TEST_NUMBER=01
TEST_PREFIX="TEST_${DFIND_TEST_NUMBER}"
####################
# Make the directories REAL
####################
DFIND_TEST_BIN=$(realpath ${DFIND_TEST_BIN})
DFIND_SRC_DIR=$(realpath ${DFIND_SRC_DIR})
DFIND_DEST_DIR=$(realpath ${DFIND_DEST_DIR})
DFIND_TESTING_BIN_DIR=$(realpath ${DFIND_TESTING_BIN_DIR})
DFIND_CMP_RESULT=${DFIND_DEST_DIR}/${TEST_PREFIX}_cmp_result.txt

####################
# Where we will put the output in the DEST_DIR
####################
DFIND_FIND_OUT=${DFIND_DEST_DIR}/${TEST_PREFIX}_dfind_find_out.txt

####################
# Define the GNU programs that we are testing against.
####################
GNU_FIND_BIN=$(which find)
GNU_FIND_OUT=${DFIND_DEST_DIR}/${TEST_PREFIX}_gnu_find_out.txt

####################
# The verbose name of this test.  We had to wait until here so that
# the binaries are named.
####################
DFIND_TEST_NAME="Test ${DFIND_TEST_NUMBER} --> ${DFIND_TEST_BIN} vs. ${GNU_FIND_BIN} \r\n
Test ${DFIND_TEST_NUMBER} --> $(basename ${DFIND_TEST_BIN}) vs. $(basename ${GNU_FIND_BIN})\r\n
for dfind --exec {} substitution of\r\n
file names containing spaces\r\n"

####################
# Confirm that we were passed the right information
####################
if [ $DFIND_TEST_VERBOSE -gt 0 ]
then
	echo -e "Using dfind binary at:\t\t${DFIND_TEST_BIN}"
	echo -e "Using mpirun binary at:\t\t${DFIND_MPIRUN_BIN}"
	echo -e "Using cmp binary at:\t\t${DFIND_CMP_BIN}"

	####################
	# This helps to confirm that the comparison test is from GNU
	####################
	strings ${DFIND_CMP_BIN} | egrep '^GNU'
	echo -e "Using src directory at:\t\t${DFIND_SRC_DIR}"
	echo -e "Using dest directory at:\t${DFIND_DEST_DIR}"
	echo -e "Using tmp file at:\t\t${DFIND_TMP_FILE}"
	echo -e "Using testing bin dir at:\t${DFIND_TESTING_BIN_DIR}"
	echo -e "Comparison test against:\t${GNU_FIND_BIN}"
	echo -e "Test Prefix:\t\t${TEST_PREFIX}"
	echo -e "Test Name: ${DFIND_TEST_NAME}"
	####################
	# This helps to confirm that the comparison is against the GNU version
	####################
	strings ${GNU_FIND_BIN} | egrep "^GNU"
fi

####################
# Construct the test case
# Create a file name with no spaces
####################
echo "A file with no spaces in the name" > ${DFIND_SRC_DIR}/A_file_with_no_spaces_in_the_name
echo "A file with spaces in the name" > "${DFIND_SRC_DIR}/A_file_with spaces_in_the_name"

####################
# Test the GNU find in single mode
####################
if [ ${DFIND_TEST_VERBOSE} -gt 0 ]
then
	echo "find ${DFIND_SRC_DIR} -exec ls {}a 2>> ${GNU_FIND_OUT}.err ';' > ${GNU_FIND_OUT}"
fi
find ${DFIND_SRC_DIR} -exec ls {}a 2>> ${GNU_FIND_OUT}.err ';' > ${GNU_FIND_OUT}
####################
# Test the HPC find in single mode
####################
if [ ${DFIND_TEST_VERBOSE} -gt 0 ]
then
#	echo "${DFIND_MPIRUN_BIN} -n 1 ${DFIND_TEST_BIN} ${DFIND_SRC_DIR} --exec ls {} ';' > ${DFIND_FIND_OUT}"
	echo "#!/bin/bash > ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}"
	echo "${DFIND_TEST_BIN} ${DFIND_SRC_DIR} --exec ls {}a 2>> ${DFIND_FIND_OUT}.err ';' 2>&1 >> ${DFIND_FIND_OUT} > ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}"
	echo "chmod +x ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}"
	echo "# ${DFIND_MPIRUN_BIN} -n 1 ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}"
fi
echo "#!/bin/bash" > ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}
echo "${DFIND_TEST_BIN} ${DFIND_SRC_DIR} --exec ls {}a 2>> ${DFIND_FIND_OUT}.err ';' 2>&1 >> ${DFIND_FIND_OUT}" > ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}
chmod +x ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}

# ${DFIND_MPIRUN_BIN} -n 1 ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}
/bin/bash ${DFIND_TESTING_BIN_DIR}/${DFIND_TMP_FILE}

####################
# Compare the result files.
####################
if [ ${DFIND_TEST_VERBOSE} -gt 0 ]
then
	echo "${DFIND_CMP_BIN} ${GNU_FIND_OUT} ${DFIND_FIND_OUT} 2>&1 > ${DFIND_CMP_RESULT}"
fi
${DFIND_CMP_BIN} ${GNU_FIND_OUT} ${DFIND_FIND_OUT} 2>&1 > ${DFIND_CMP_RESULT}
if [ $? -ne 0 ]
then
	echo -e "FAILURE: $DFIND_TEST_NAME"
	echo "RESULT:"
	cat ${DFIND_CMP_RESULT} ${GNU_FIND_OUT}.err ${DFIND_FIND_OUT}.err
else
	####################
	# Only report success in verbose mode
	####################
	if [ $DFIND_TEST_VERBOSE -gt 0 ]
	then
		echo -e "SUCCESS: $FIND_TEST_NAME"
		echo "RESULT:"
		echo ${DFIND_CMP_RESULT}
	fi
fi
