#!/usr/bin/env python3

import os
import random
import time
import unittest

from tests import common, config, dcp_dsync_common_tests


class TestDCP(dcp_dsync_common_tests.CommonTests):
    """Tests for dcp"""

    def setUp(self):
        # set up unique directory information for this test
        super().setUp()

        # for common tests
        self.cp_cmd = "dcp"

    def test_preserve_copy(self):
        """Copy file while preserving permissions, groups, and timestamps"""

        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath, self.dest_dirpath)
        common.create_empty_files(self.src_tempfilepath)

        # set permissions to a random valid octal mode between 700 - 777
        octal = "7" + str(random.randrange(0, 8)) + str(random.randrange(0, 8))
        os.chmod(self.src_tempfilepath, int(octal, 8))

        # wait 1 second for timestamps to change if preserve is not working
        time.sleep(1)

        # run dcp, with preserve option
        self.run_distributed_successfully(
            "dcp", "--preserve", self.src_tempfilepath, self.dest_dirpath
        )

        # CHECK: permissions, group, and timestamps are the same
        src_stat = os.stat(self.src_tempfilepath)
        dest_stat = os.stat(self.dest_tempfilepath)
        self.assertEqual(
            f"{dest_stat.st_mode & 0o777:o}",
            octal,
            msg="Permissions not preserved in copied file.",
        )
        self.assertEqual(
            dest_stat.st_gid,
            src_stat.st_gid,
            msg="Group ID not preserved in copied file.",
        )
        self.assertEqual(
            dest_stat.st_atime_ns,
            src_stat.st_atime_ns,
            msg="Access timestamp not preserved in copied file.",
        )
        self.assertEqual(
            dest_stat.st_mtime_ns,
            src_stat.st_mtime_ns,
            msg="Modification timestamp not preserved in copied file.",
        )


if __name__ == "__main__":
    unittest.main()
