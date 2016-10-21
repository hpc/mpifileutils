#!/usr/bin/env python2
from subprocess import call

def test_octal():
        rc = call("~/fileutils.git/test/tests/test_dchmod/test_octal.sh", shell=True)
