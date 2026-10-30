#!/bin/bash
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
DFIND_TEST_NUMBER=${DFIND_TEST_NUMBER:-${8}}
GNU_FIND_BIN=${GNU_FIND_BIN:-${9}}

####################
# To make the rest of the script more legible
####################
DFIND=${DFIND_TEST_BIN}
MPIRUN=${DFIND_MPIRUN_BIN}
CMP=${DFIND_CMP_BIN}
SRCDIR=${DFIND_SRC_DIR}
DESTDIR=${DFIND_DEST_DIR}
TMPFILE=${DFIND_TMP_FILE}
TESTBINDIR=${DFIND_TESTING_BIN_DIR}

####################
# Define the Test Number
####################
TEST_PREFIX="TEST_${DFIND_TEST_NUMBER}"

####################
# Make the directories REAL
####################
DFIND=$(realpath ${DFIND})
SRCDIR=$(realpath ${SRCDIR})
DESTDIR=$(realpath ${DESTDIR})
TESTBINDIR=$(realpath ${TESTBINDIR})

####################
# Define the GNU programs that we are testing against.
####################
GFIND=$(which ${GNU_FIND_BIN})

####################
# Where we will put the output data
####################
DFIND_OUT=${DESTDIR}/${TEST_PREFIX}_dfind_find_out.txt
GFIND_OUT=${DESTDIR}/${TEST_PREFIX}_gnu_find_out.txt
DFIND_CMP_RESULT=${DESTDIR}/${TEST_PREFIX}_cmp_result.txt

####################
# The verbose name of this test.  We had to wait until here so that
# the binaries are named.
####################
DFIND_TEST_NAME="Test ${DFIND_TEST_NUMBER} --> ${DFIND} vs. ${GFIND} \r\n
Test ${DFIND_TEST_NUMBER} --> $(basename ${DFIND}) vs. $(basename ${GFIND})\r\n
for dfind --exec {} substitution of file names containing spaces\r\n"

####################
# Confirm that we were passed the right information
####################
if [ $DFIND_TEST_VERBOSE -gt 0 ]
then
	echo -e "Using dfind binary at:\t\t${DFIND}"
	echo -e "Using mpirun binary at:\t\t${MPIRUN}"
	echo -e "Using cmp binary at:\t\t${CMP}"

	####################
	# This helps to confirm that the comparison test is from GNU
	####################
	strings ${CMP} | egrep '^GNU '
	echo -e "Using src directory at:\t\t${SRCDIR}"
	echo -e "Using dest directory at:\t${DESTDIR}"
	echo -e "Using tmp file at:\t\t${TMPFILE}"
	echo -e "Using testing bin dir at:\t${TESTBINDIR}"
	echo -e "Comparison test against:\t${GFIND}"
	echo -e "Test Prefix:\t\t${TEST_PREFIX}"
	echo -e "Test Name: ${DFIND_TEST_NAME}"
	####################
	# This helps to confirm that the comparison is against the GNU version
	####################
	strings ${GFIND} | egrep '^GNU '
fi

################################################################################
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
#
# Everything up to here has been a preamble.  Here is where we construct the
# test data to perform the test.
#
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
################################################################################
####################
# Construct the test case
# Create a file name with no spaces
####################
echo "A file with no spaces in the name" > ${SRCDIR}/A_file_with_no_spaces_in_the_name
echo "A file with spaces in the name" > "${SRCDIR}/A_file_with spaces_in_the_name"

####################
# Test the GNU find in single mode
####################
if [ ${DFIND_TEST_VERBOSE} -gt 0 ]
then
	echo "${GFIND} ${SRCDIR} -exec ls {}a 2>> ${GFIND_OUT}.err ';' > ${GFIND_OUT}"
fi
${GFIND} ${SRCDIR} -exec ls {}a 2>> ${GFIND_OUT}.err ';' > ${GFIND_OUT}

####################
# Test the HPC find in single mode
####################
if [ ${DFIND_TEST_VERBOSE} -gt 0 ]
then
#	echo "${MPIRUN} -n 1 ${DFIND} ${SRCDIR} --exec ls {} ';' > ${DFIND_OUT}"
	echo "#!/bin/bash > ${TESTBINDIR}/${TMPFILE}"
	echo "${DFIND} ${SRCDIR} --exec ls {}a 2>> ${DFIND_OUT}.err ';' 2>&1 >> ${DFIND_OUT} > ${TESTBINDIR}/${TMPFILE}"
	echo "chmod +x ${TESTBINDIR}/${TMPFILE}"
	echo "# ${MPIRUN} -n 1 ${TESTBINDIR}/${TMPFILE}"
fi
echo "#!/bin/bash" > ${TESTBINDIR}/${TMPFILE}
echo "${DFIND} ${SRCDIR} --exec ls {}a 2>> ${DFIND_OUT}.err ';' 2>&1 >> ${DFIND_OUT}" > ${TESTBINDIR}/${TMPFILE}
chmod +x ${TESTBINDIR}/${TMPFILE}

# ${MPIRUN} -n 1 ${TESTBINDIR}/${TMPFILE}
/bin/bash ${TESTBINDIR}/${TMPFILE}

####################
# Compare the result files.
####################
if [ ${DFIND_TEST_VERBOSE} -gt 0 ]
then
	echo "${CMP} ${GFIND_OUT} ${DFIND_OUT} 2>&1 > ${DFIND_CMP_RESULT}"
fi
${CMP} ${GFIND_OUT} ${DFIND_OUT} 2>&1 > ${DFIND_CMP_RESULT}
if [ $? -ne 0 ]
then
	echo -e "FAILURE: $DFIND_TEST_NAME"
	echo "RESULT:"
	cat ${DFIND_CMP_RESULT} ${GFIND_OUT}.err ${DFIND_OUT}.err
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
