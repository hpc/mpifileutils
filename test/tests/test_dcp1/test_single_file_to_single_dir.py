#!/usr/bin/env python2
from subprocess import call

def test_dcp_single_file_to_single_dir():
        rc = call("~/fileutils.git/test/legacy/dcp_tests/test_dcp_single_file_to_single_dir/test.sh", shell=True)
