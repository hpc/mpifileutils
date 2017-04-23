#!/usr/bin/env python2
from subprocess import call

def test_fiemap():
        rc = call("~/mpifileutils/test/tests/test_dcp/test_fiemap.sh", shell=True)
