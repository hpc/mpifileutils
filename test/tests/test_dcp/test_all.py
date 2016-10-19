#!/usr/bin/env python2
from subprocess import call

def test_all():
        rc = call("~/fileutils.git/test/legacy/dcp_tests/test_all.sh", shell=True)
