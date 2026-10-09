#!/usr/bin/env python3

import os
import unittest

from tests import common, config


class TestDRM(common.TestCase):
    """Tests for drm"""

    def setUp(self):
        # set up unique directory information for this test
        self.basepath = os.path.join(
            config.test_dir, self.__class__.__name__, self._testMethodName
        )
        self.basepath_file1 = os.path.join(self.basepath, "file1")
        self.basepath_dir1 = os.path.join(self.basepath, "dir1")
        self.basepath_dir2 = os.path.join(self.basepath, "dir2")
        self.basepath_dir2_file1 = os.path.join(self.basepath, "dir2", "file1")

        common.create_empty_directories(self.basepath_dir1)
        common.create_empty_files(
            self.basepath_file1, self.basepath_dir2_file1
        )

    def test_delete_directory(self):
        """Delete a directory and its contents"""

        # run drm
        self.run_distributed_successfully("drm", self.basepath)

        # CHECK: directory does not exist
        self.assertFalse(os.path.exists(self.basepath))

    def test_match_delete(self):
        """Delete all items ending with a pattern from directory tree"""

        # run drm
        self.run_distributed_successfully(
            "drm", "--match", "1$", self.basepath
        )

        # CHECK: only matching items were deleted
        self.assertTrue(
            os.path.exists(self.basepath_dir2),
            msg="Entry deleted unexpectly.",
        )
        paths = [
            self.basepath_file1,
            self.basepath_dir1,
            self.basepath_dir2_file1,
        ]
        for path in paths:
            self.assertFalse(
                os.path.exists(path), msg="Entry was not deleted."
            )

    def test_exclude_delete(self):
        """Delete all items from directory tree except for a single entry"""

        # run drm
        self.run_distributed_successfully(
            "drm", "--name", "--exclude", "dir2", self.basepath
        )

        # CHECK: only matching item was kept
        self.assertTrue(
            os.path.exists(self.basepath_dir2),
            msg="Entry deleted unexpectly.",
        )
        paths = [
            self.basepath_file1,
            self.basepath_dir1,
            self.basepath_dir2_file1,
        ]
        for path in paths:
            self.assertFalse(
                os.path.exists(path), msg="Entry was not deleted."
            )

    def test_dryrun(self):
        """List item that would be deleted"""

        # run drm
        drm_result = self.run_distributed_successfully(
            "drm", "--dryrun", "--name", "--match", "dir1", self.basepath
        )

        # CHECK: item was listed
        self.assertIn(
            self.basepath_dir1,
            drm_result.stdout,
            msg="Entry was not listed.",
        )

        # CHECK: item was not deleted
        self.assertTrue(
            os.path.exists(self.basepath_dir1),
            msg="Entry deleted unexpectly.",
        )


if __name__ == "__main__":
    unittest.main()
