#!/usr/bin/env python3

import os
import time
import unittest

from tests import common, config


class TestDFIND(common.TestCase):
    """Tests for dfind"""

    def setUp(self):
        # set up unique directory information for this test
        self.path = os.path.join(
            config.test_dir,
            self.__class__.__name__,
            f"{self._testMethodName}",
        )
        self.file1_path = os.path.join(self.path, "file1")
        self.file2_path = os.path.join(self.path, "file2")
        self.file3_path = os.path.join(self.path, "file3")
        self.output_filepath = os.path.join(self.path, "output")

    def test_find_by_size(self):
        """Find all entries less than 1MB"""

        # set up directories/files to test on
        common.create_empty_directories(self.path)
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.file1_path}",
            "bs=1K",
            "count=1",
        )
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.file2_path}",
            "bs=1M",
            "count=1",
        )
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.file3_path}",
            "bs=1K",
            "count=1",
        )

        # run dfind
        dfind_result = self.run_distributed_successfully(
            "dfind",
            "--text",
            "--output", self.output_filepath,
            "--size", "-1MB",
            self.path,
        )

        # CHECK: only file2 not reported
        output = common.get_output_file_list(self.output_filepath)
        self.assertIn(self.path, output, msg="Expected file was not reported.")
        self.assertIn(
            self.file1_path, output, msg="Expected file was not reported."
        )
        self.assertIn(
            self.file3_path, output, msg="Expected file was not reported."
        )
        self.assertNotIn(
            self.file2_path, output, msg="Unexpected file was reported."
        )

    def test_find_by_modification_time(self):
        """Find all files not changed in the past 30 days"""

        # set up directories/files to test on
        common.create_empty_files(
            self.file1_path, self.file2_path, self.file3_path
        )
        now_ns = time.time_ns()
        os.utime(self.file1_path, ns=(now_ns, now_ns - days_to_ns(40)))
        os.utime(self.file2_path, ns=(now_ns, now_ns - days_to_ns(20)))
        os.utime(self.file3_path, ns=(now_ns, now_ns - days_to_ns(31)))

        # run dfind
        dfind_result = self.run_distributed_successfully(
            "dfind",
            "--text",
            "--output", self.output_filepath,
            "--type", "f",
            "--mtime", "+30",
            self.path,
        )

        # CHECK: file1 and file3 reported
        output = common.get_output_file_list(self.output_filepath)
        self.assertIn(
            self.file1_path, output, msg="Expected file was not reported."
        )
        self.assertIn(
            self.file3_path, output, msg="Expected file was not reported."
        )
        self.assertNotIn(
            self.path, output, msg="Unexpected file was reported."
        )
        self.assertNotIn(
            self.file2_path, output, msg="Unexpected file was reported."
        )

    def test_find_by_access_time(self):
        """Find all files accessed after another file was modified"""

        # set up directories/files to test on
        common.create_empty_files(
            self.file1_path, self.file2_path, self.file3_path
        )
        now_ns = time.time_ns()
        os.utime(
            self.file1_path,
            ns=(now_ns - days_to_ns(8), now_ns - days_to_ns(10)),
        )
        os.utime(self.file2_path, ns=(now_ns - days_to_ns(9), now_ns))
        os.utime(self.file3_path, ns=(now_ns - days_to_ns(11), now_ns))

        # run dfind
        dfind_result = self.run_distributed_successfully(
            "dfind",
            "--text",
            "--output", self.output_filepath,
            "--type", "f",
            "--anewer", self.file1_path,
            self.path,
        )

        # CHECK: file1 and file2 reported
        output = common.get_output_file_list(self.output_filepath)
        self.assertIn(
            self.file1_path, output, msg="Expected file was not reported."
        )
        self.assertIn(
            self.file2_path, output, msg="Expected file was not reported."
        )
        self.assertNotIn(
            self.path, output, msg="Unexpected file was reported."
        )
        self.assertNotIn(
            self.file3_path, output, msg="Unexpected file was reported."
        )


def days_to_ns(days):
    return days * 24 * 60 * 60 * 1_000_000_000


if __name__ == "__main__":
    unittest.main()
