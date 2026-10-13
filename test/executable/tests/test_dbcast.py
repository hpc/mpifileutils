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
        common.create_empty_files(self.src_path)

    def test_broadcast(self):
        """Broadcast a file to /tmp on each node"""

        dest_path = os.path.join(
            f"{os.sep}tmp", f"{self._testMethodName}_file"
        )

        # run dbcast
        self.run_distributed_successfully("dbcast", self.src_path, dest_path)

        # run check_file_exists_on_all_ranks
        result = self.run_distributed_successfully(
            "check_file_exists_on_all_ranks", dest_path
        )

        # CHECK: file exists on all ranks
        self.assertEqual(
            result.returncode,
            0,
            msg="File could not be accessed on all ranks.",
        )


if __name__ == "__main__":
    unittest.main()
