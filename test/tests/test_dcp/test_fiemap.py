#!/usr/bin/env python2
import subprocess 

# change paths here for bash script as necessary
mpifu_path     = "~/mpifileutils/test/tests/test_dcp/test_fiemap.sh" 

# vars in bash script
dcp_test_bin   = "/root/mpifileutils/install/bin/dcp"
dcp_mpirun_bin = "mpirun"
dcp_cmp_bin    = "diff"
dcp_src_dir    = "/mnt/lustre"
dcp_dest_dir   = "/mnt/lustre2"
dcmp_tmp_file  = "file_test_fiemap_XXX"

def test_fiemap():
        p = subprocess.Popen(["%s %s %s %s %s %s %s" % (mpifu_path, dcp_test_bin, dcp_mpirun_bin, 
          dcp_cmp_bin, dcp_src_dir, dcp_dest_dir, dcmp_tmp_file)], shell=True, executable="/bin/bash").communicate()
