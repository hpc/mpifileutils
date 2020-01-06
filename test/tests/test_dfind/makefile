SHELL=/bin/bash
.sh:
	cp $? `basename $? .sh`
	chmod +x `basename $? .sh`
.ONESHELL
####################
# Perform testing of the dfind command
# Tests will be numbered and will report both
# the command name and the number as part of the
# diagnostic output.
####################
PROJECTROOT=$(shell git rev-parse --git-dir)/..

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
# dfind.test.sh  The shell script installed inside this script to perform
#                the testing. These scripts are found in the same directory
#                as the make file and are copied into the ../test/bin
#                directory to provide an actual test script.
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
# directory as well), we use the "git rev-parse --git-dir" to find the .git
# root directory:
####################
DFIND_INSTALL_DIR?=$(PROJECTROOT)/../install/bin
DFIND_TEST_BIN?=$(DFIND_INSTALL_DIR)/dfind
DFIND_MPIRUN_BIN?=$(shell which srun)
DFIND_CMP_BIN?=$(shell which diff)
DFIND_SRC_DIR?=$(PROJECTROOT)/../test/src
DFIND_DEST_DIR?=$(PROJECTROOT)/../test/dest
DFIND_TMP_FILE?=dfind_test.sh
DFIND_TESTING_BIN_DIR?=$(PROJECTROOT)/../test/bin
GNU_FIND?=find
VERBOSE?=1

####################
# At first blush it may seem redundant to pass in the test name...
# until you have to re-order the testing sequence
####################
TEST-01 := test_dfind_spaces
TEST-01_Number := 01
DFIND_TEST_NUM-01 := $(DFIND_TESTING_BIN_DIR)/$(TEST-01)

TEST-02 := test_dfind_exec_stderr_redirect
TEST-02_Number := 02
DFIND_TEST_NUM-02 := $(DFIND_TESTING_BIN_DIR)/$(TEST-02)

TESTS = $(TEST-01) $(TEST-02)

.PHONY: all clean install uninstall run run1 run2

install: $(TESTS)
	mkdir -p $(DFIND_TESTING_BIN_DIR)
	install -m711 -g linuxdev -o $(USER) -C $? $(DFIND_TESTING_BIN_DIR)
clean:
	@for script in $(TESTS) ; \
	do \
		echo "rm -f $$script" ; \
		rm -f $$script ; \
	done
	rm -rf $(PROJECTROOT)/../test
uninstall:
	@for script in $(TESTS) ; \
	do \
		echo "rm -f $(DFIND_TESTING_BIN_DIR)/$$script" ; \
		rm f $(DFIND_TESTING_BIN_DIR)/$$script ; \
	done
run: run1 run2
	echo testing complete
# run:  $(DFIND_TEST_NUM-01)
run1:  install $(DFIND_TEST_NUM-01) $(DFIND_TEST_BIN)
	rm -rf ${DFIND_SRC_DIR} ${DFIND_DEST_DIR}
	mkdir -p $(DFIND_SRC_DIR) $(DFIND_DEST_DIR) $(DFIND_TESTING_BIN_DIR)
	DFIND_TEST_VERBOSE=$(VERBOSE) $(DFIND_TEST_NUM-01) $(DFIND_TEST_BIN) \
										 $(DFIND_MPIRUN_BIN) $(DFIND_CMP_BIN) $(DFIND_SRC_DIR) \
										 $(DFIND_DEST_DIR) $(DFIND_TMP_FILE) \
										 $(DFIND_TESTING_BIN_DIR) $(TEST-01_Number) $(GNU_FIND)
run2:  install $(DFIND_TEST_NUM-02) $(DFIND_TEST_BIN)
	rm -rf ${DFIND_SRC_DIR} ${DFIND_DEST_DIR}
	mkdir -p $(DFIND_SRC_DIR) $(DFIND_DEST_DIR) $(DFIND_TESTING_BIN_DIR)
	DFIND_TEST_VERBOSE=$(VERBOSE) $(DFIND_TEST_NUM-02) $(DFIND_TEST_BIN) \
										 $(DFIND_MPIRUN_BIN) $(DFIND_CMP_BIN) $(DFIND_SRC_DIR) \
										 $(DFIND_DEST_DIR) $(DFIND_TMP_FILE) \
										 $(DFIND_TESTING_BIN_DIR) $(TEST-02_Number)  $(GNU_FIND)
# vim: set syntax=makefile,sw=4,t=4
