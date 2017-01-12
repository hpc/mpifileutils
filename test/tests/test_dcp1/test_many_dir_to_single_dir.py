#!/usr/bin/env python2
from subprocess import call

def test_dcp_many_dir_to_single_dir():
        rc = call("~/fileutils.git/test/legacy/dcp_tests/test_dcp_many_dir_to_single_dir/test.sh", shell=True)
