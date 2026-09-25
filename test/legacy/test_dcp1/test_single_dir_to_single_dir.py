#!/usr/bin/env python2
from subprocess import call

def test_dcp1_single_dir_to_single_dir():
        rc = call("~/mpifileutils/test/legacy/dcp1_tests/test_dcp1_single_dir_to_single_dir/test.sh", shell=True)
