SHELL=/bin/bash
.sh:
	cp $? `basename $? .sh`
	chmod +x `basename $? .sh`

####################
# Perform testing of the dfind command
# Tests will be numbered and will report both
# the command name and the number as part of the
# diagnostic output.
####################
PROJECTROOT := $(shell bash ./getgitroot.sh)

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
DFIND_INSTALL_DIR := $(PROJECTROOT)/../install/bin
DFIND_TEST_BIN := $(DFIND_INSTALL_DIR)/dfind
DFIND_MPIRUN_BIN := $(shell which srun)
DFIND_CMP_BIN := $(shell which diff)
DFIND_SRC_DIR := $(PROJECTROOT)/../test/src
DFIND_DEST_DIR := $(PROJECTROOT)/../test/dest
DFIND_TMP_FILE := dfind_test.sh
DFIND_TESTING_BIN_DIR := $(PROJECTROOT)/../test/bin

TEST-01 := test_dfind_spaces
TEST-02 := test_dfind_exec_stderr_redirect
DFIND_TEST_NUM-01 := $(DFIND_TESTING_BIN_DIR)/$(TEST-01)
DFIND_TEST_NUM-02 := $(DFIND_TESTING_BIN_DIR)/$(TEST-02)

TESTS = getgitroot $(TEST-01) $(TEST-02)

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
	$(DFIND_TEST_NUM-01) $(DFIND_TEST_BIN) $(DFIND_MPIRUN_BIN) $(DFIND_CMP_BIN) \
							$(DFIND_SRC_DIR) $(DFIND_DEST_DIR) $(DFIND_TMP_FILE) $(DFIND_TESTING_BIN_DIR)
run2:  install $(DFIND_TEST_NUM-02) $(DFIND_TEST_BIN)
	rm -rf ${DFIND_SRC_DIR} ${DFIND_DEST_DIR}
	mkdir -p $(DFIND_SRC_DIR) $(DFIND_DEST_DIR) $(DFIND_TESTING_BIN_DIR)
	$(DFIND_TEST_NUM-02) $(DFIND_TEST_BIN) $(DFIND_MPIRUN_BIN) $(DFIND_CMP_BIN) \
							$(DFIND_SRC_DIR) $(DFIND_DEST_DIR) $(DFIND_TMP_FILE) $(DFIND_TESTING_BIN_DIR)
# vim: set syntax=makefile,sw=4,t=4
