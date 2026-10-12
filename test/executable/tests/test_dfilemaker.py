#!/usr/bin/env python3

import os
import unittest

from tests import common, config


class TestDFILEMAKER(common.TestCase):
    """Tests for dfilemaker"""

    def setUp(self):
        # set up unique directory information for this test
        self.destination_path = os.path.join(
            config.test_dir,
            self.__class__.__name__,
            f"dest_{self._testMethodName}",
        )

    def test_create_random_directory_tree(self):
        """Generate a directory tree with random depth and file sizes"""

        # set up directories/files to test on
        common.create_empty_directories(self.destination_path)

        # run dfilemaker
        self.run_distributed_successfully(
            "dfilemaker",
            "--depth=5-10",
            "--size=1MB-5MB",
            self.destination_path,
        )

        # CHECK: generated tree depth is within the requested range
        tree_depth = get_tree_depth(self.destination_path)
        self.assertGreaterEqual(
            tree_depth, 5, msg="Minimum tree depth generated is incorrect."
        )
        self.assertLessEqual(
            tree_depth, 10, msg="Maximum tree depth generated is incorrect."
        )

        # CHECK: generated regular file sizes are within the requested range
        file_sizes = get_regular_file_sizes(self.destination_path)
        self.assertTrue(
            file_sizes, msg="dfilemaker did not generate any regular files"
        )
        self.assertGreaterEqual(
            min(file_sizes),
            1 * 1024 * 1024,
            msg="Mimimum file size generated is incorrect.",
        )
        self.assertLessEqual(
            max(file_sizes),
            5 * 1024 * 1024,
            msg="Maximum file size generated is incorrect.",
        )


def get_tree_depth(root_dir):
    max_depth = 0
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for name in dirnames + filenames:
            path = os.path.join(dirpath, name)
            relative_path = os.path.relpath(path, root_dir)
            depth = len(relative_path.split(os.sep))
            max_depth = max(max_depth, depth)
    return max_depth


def get_regular_file_sizes(root_dir):
    file_sizes = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            if not os.path.islink(path) and os.path.isfile(path):
                file_sizes.append(os.path.getsize(path))
    return file_sizes


if __name__ == "__main__":
    unittest.main()
