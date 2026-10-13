#!/usr/bin/env python3

import os
import unittest

from tests import common, config


class TestDBCAST(common.TestCase):
    """Tests for dbcast"""

    def setUp(self):
        # set up unique directory information for this test
        self.path = os.path.join(
            config.test_dir,
            self.__class__.__name__,
            f"{self._testMethodName}",
        )
        self.src_path = os.path.join(self.path, f"{self._testMethodName}_file")

    def test_broadcast(self):
        """Broadcast a 16GiB file to each node"""

        # write 16GiB to source file
        common.create_empty_directories(self.path)
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_path}",
            "bs=1M",
            "count=16384",
        )

        dest_path = os.path.join(
            f"{os.sep}tmp", f"{self._testMethodName}_file"
        )

        # run dbcast
        self.run_distributed_successfully("dbcast", self.src_path, dest_path)

        # run check_file_exists_on_all_ranks
        result = self.run_distributed_successfully(
            "check_file_exists_on_all_ranks", dest_path, self.src_path
        )

        # CHECK: file exists and matches source on all ranks
        self.assertEqual(
            result.returncode,
            0,
            msg="File could not be accessed or data differs on some ranks.",
        )

        # clean up file on nodes
        self.run_distributed_successfully("rm", "-f", dest_path)


if __name__ == "__main__":
    unittest.main()
