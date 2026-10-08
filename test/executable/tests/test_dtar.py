#!/usr/bin/env python3

import os
import unittest

from tests import common, config


class TestDTAR(common.TestCase):
    """Tests for dtar"""

    def setUp(self):
        # set up unique directory information for this test
        self.dtar_base_dirpath = os.path.join(
            config.test_dir, self.__class__.__name__
        )
        self.dtar_src_dirname = f"src_{self._testMethodName}"
        self.dtar_archivename = f"archive_{self._testMethodName}.tar"

        self.src_dirpath = os.path.join(
            self.dtar_base_dirpath, self.dtar_src_dirname
        )
        self.src_tempfilepath = os.path.join(self.src_dirpath, "tempfile")
        self.src_tempfilepath_symlink = os.path.join(
            self.src_dirpath, "symlink"
        )

        self.dest_dirpath = os.path.join(
            self.dtar_base_dirpath, f"dest_{self._testMethodName}"
        )
        self.dest_src_tempfilepath = os.path.join(
            self.dest_dirpath, self.dtar_src_dirname, "tempfile"
        )
        self.dest_src_tempfilepath_symlink = os.path.join(
            self.dest_dirpath, self.dtar_src_dirname, "symlink"
        )
        self.dest_archivepath = os.path.join(
            self.dest_dirpath, self.dtar_archivename
        )

        self.data_input = b"\x00dtar test data\xff" * 1024

    def test_create_tarfile(self):
        """Create a tar archive"""

        # set up directories/files to test on
        common.write_bytes(self.src_tempfilepath, self.data_input)
        common.create_empty_directories(self.dest_dirpath)

        # Skip test on NFS filesystem
        if common.filesystem_type(self.src_tempfilepath) == "nfs":
            self.skipTest("One should not create an archive file on NFS.")

        # run dtar to create an archive
        self.run_distributed_successfully(
            "dtar",
            "-C",
            self.dtar_base_dirpath,
            "-c",
            "-f",
            self.dest_archivepath,
            self.dtar_src_dirname,
        )

        # CHECK: archive was created
        self.assertTrue(
            os.path.exists(self.dest_archivepath),
            msg="Archive was not created.",
        )

        # untar
        self.run_local_successfully(
            "tar", "-C", self.dest_dirpath, "-xf", self.dest_archivepath
        )

        # CHECK: file contents are the same
        self.assertEqual(
            common.read_bytes(self.dest_src_tempfilepath),
            self.data_input,
            msg="File data differs.",
        )

    def test_extract_tarfile(self):
        """Extract a tar archive"""

        # set up directories/files to test on
        common.write_bytes(self.src_tempfilepath, self.data_input)
        common.create_empty_directories(self.dest_dirpath)

        # run tar to create an archive
        self.run_local_successfully(
            "tar",
            "-C", self.dtar_base_dirpath,
            "-cf",
            self.dest_archivepath,
            self.dtar_src_dirname,
        )

        # CHECK: archive was created
        self.assertTrue(
            os.path.exists(self.dest_archivepath),
            msg="Archive was not created.",
        )

        # run dtar to untar
        self.run_distributed_successfully(
            "dtar",
            "-C", self.dest_dirpath,
            "-x",
            "-f",
            self.dtar_archivename,
        )

        # CHECK: file contents are the same
        self.assertEqual(
            common.read_bytes(self.dest_src_tempfilepath),
            self.data_input,
            msg="File data differs.",
        )

    def test_symlink_preservation(self):
        """Symlinks should be preserved"""

        # set up directories/files to test on
        common.write_bytes(self.src_tempfilepath, self.data_input)
        common.create_empty_directories(self.dest_dirpath)

        # create a symlink in src
        os.symlink(self.src_tempfilepath, self.src_tempfilepath_symlink)

        # Skip test on NFS filesystem
        if common.filesystem_type(self.src_tempfilepath) == "nfs":
            self.skipTest("One should not create an archive file on NFS.")

        # run dtar to create an archive
        self.run_distributed_successfully(
            "dtar",
            "-C", self.dtar_base_dirpath,
            "-c",
            "-f",
            self.dest_archivepath,
            self.dtar_src_dirname,
        )

        # CHECK: archive was created
        self.assertTrue(
            os.path.exists(self.dest_archivepath),
            msg="Archive was not created.",
        )

        # run dtar to untar
        self.run_distributed_successfully(
            "dtar",
            "-C", self.dest_dirpath,
            "-x",
            "-f",
            self.dtar_archivename,
        )

        # CHECK: file contents are the same and symlink was preserved
        self.assertEqual(
            common.read_bytes(self.dest_src_tempfilepath),
            self.data_input,
            msg="File data differs.",
        )
        self.assertTrue(
            os.path.islink(self.dest_src_tempfilepath_symlink)
            and os.readlink(self.dest_src_tempfilepath_symlink)
            == self.src_tempfilepath,
            msg="Symlink was not preserved.",
        )


if __name__ == "__main__":
    unittest.main()
