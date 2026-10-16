#!/usr/bin/env python3

import os
import shutil
import unittest

from tests import common, config


class TestDDUP(common.TestCase):
    """Tests for ddup"""

    def setUp(self):
        # set up unique directory information for this test
        self.path = os.path.join(
            config.test_dir,
            self.__class__.__name__,
            f"{self._testMethodName}",
        )

    def test_report_duplicates(self):
        """Report path names to files having identical content"""

        # set up directories/files to test on
        file1_path = os.path.join(self.path, "file1")
        file2_path = os.path.join(self.path, "file2")
        file3_path = os.path.join(self.path, "file3")
        common.create_random_files(file1_path, file2_path)
        shutil.copy(file1_path, file3_path)

        # run ddup
        ddup_result = self.run_distributed_successfully("ddup", self.path)

        # CHECK: file1 and file3 reported
        self.assertIn("file1", ddup_result.stdout, msg="File not reported.")
        self.assertIn("file3", ddup_result.stdout, msg="File not reported.")

        # CHECK: file2 not reported
        self.assertNotIn(
            "file2", ddup_result.stdout, msg="File reported unexpectedly."
        )


if __name__ == "__main__":
    unittest.main()
