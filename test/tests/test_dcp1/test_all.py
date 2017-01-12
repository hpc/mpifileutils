#!/usr/bin/env python2
from subprocess import call

def test_all():
        rc = call("~/mpifileutils/test/legacy/dcp1_tests/test_all.sh", shell=True)
