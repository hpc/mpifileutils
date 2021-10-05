#!/usr/bin/env python2
import subprocess

# change paths here for bash script as necessary
mpifu_path     = "~/mpifileutils/test/tests/test_dsync/test_xattr.sh"

# vars in bash script
dsync_test_bin   = "/root/mpifileutils/install/bin/dsync"
dsync_src_dir    = "/mnt/lustre"
dsync_dest_dir   = "/mnt/lustre2"
dsync_test_file  = "file_test_xattr_XXX"

def test_xattr():
        p = subprocess.Popen(["%s %s %s %s %s %s %s" % (dsync_test_bin,
          dsync_src_dir, dsync_dest_dir, dsync_test_file)], shell=True,
          executable="/bin/bash").communicate()
