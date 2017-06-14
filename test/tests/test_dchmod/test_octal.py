#!/usr/bin/env python2
import subprocess 

# chane your path here as necessary  
mpifu_path  = "~/mpifileutils/test/tests/test_dchmod/test_octal.sh"
dchmod_path = "~/mpifileutils/install/bin/dchmod" 

def test_octal():
        p = subprocess.Popen(["%s %s" % (mpifu_path, dchmod_path)], shell=True, executable="/bin/bash").communicate()
