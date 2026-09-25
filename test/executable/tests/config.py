#!/usr/bin/env python3

import os

run_cmd =[os.environ["MPIEXEC_EXECUTABLE"], os.environ["MPIEXEC_NUMPROC_FLAG"], os.environ["MPIEXEC_MAX_NUMPROCS"]]
test_dir = os.environ["TEST_TMPDIR"]
