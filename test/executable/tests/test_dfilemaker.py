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
            "--size=2MB-6MB",
            "--nitems=200-300",
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
            2 * 1024 * 1024,
            msg="Mimimum file size generated is incorrect.",
        )
        self.assertLessEqual(
            max(file_sizes),
            6 * 1024 * 1024,
            msg="Maximum file size generated is incorrect.",
        )

        # CHECK: the number of items (files, dirs, links) generated is within the requested range
        total_items = count_all_items(self.destination_path)
        self.assertGreaterEqual(
            total_items,
            200,
            msg="Minimum number of items generated is incorrect.",
        )
        self.assertLessEqual(
            total_items,
            300,
            msg="Maximum number of items generated is incorrect.",
        )
    
    def test_invalid_range_parameters(self):
        """Using invalid range parameters result in failure"""

        # set up directories/files to test on
        common.create_empty_directories(self.destination_path)

        # run dfilemaker, with invalid ranges
        self.run_distributed_with_failure(
            "dfilemaker",
            "--depth=10-5",
            self.destination_path,
        )

        self.run_distributed_with_failure(
            "dfilemaker",
            "--size=6MB-2MB",
            self.destination_path,
        )

        # directory should be empty
        self.assertEqual(
            get_tree_depth(self.destination_path),
            0,
            msg="Destination should be empty"
        )

    def test_create_exact_depth_and_size(self):
        """Generate a directory tree with exact depth, file sizes, and number of items"""

        # set up directories/files to test on
        common.create_empty_directories(self.destination_path)

        # run dfilemaker with exact values
        self.run_distributed_successfully(
            "dfilemaker",
            "--depth=5-5",
            "--size=4MB-4MB",
            "--nitems=15-15",
            self.destination_path,
        )

        # CHECK: generated tree depth matches the exact requested value
        tree_depth = get_tree_depth(self.destination_path)
        self.assertEqual(
            tree_depth, 5, msg="Tree depth should match exact value."
        )

        # CHECK: generated regular file sizes match the exact requested value
        file_sizes = get_regular_file_sizes(self.destination_path)
        self.assertTrue(
            file_sizes, msg="dfilemaker did not generate any regular files"
        )
        for size in file_sizes:
            self.assertEqual(
                size,
                4 * 1024 * 1024,
                msg=f"File size should match exact value."
            )

        # CHECK: generated number of items matches the exact requested value
        total_items = count_all_items(self.destination_path)
        self.assertEqual(
            total_items,
            15,
            msg="Total number of items should match exact value."
        )

    def test_same_seed_produces_identical_trees(self):
        """The same seed produces the same files."""

        tree_1 = os.path.join(self.destination_path, "tree1")
        tree_2 = os.path.join(self.destination_path, "tree2")
        common.create_empty_directories(tree_1, tree_2)

        shared_opts = [
            "--depth=3-4",
            "--size=5K-10K",
            "--nitems=10-15",
            "--seed=53471",
        ]
        self.run_distributed_successfully(
            "dfilemaker",
            *shared_opts,
            tree_1
        )
        self.run_distributed_successfully(
            "dfilemaker",
            *shared_opts,
            tree_2
        )

        # CHECK: the two trees are identical
        result = common.run_local(
            "rsync", "-cnr", "--delete", "--no-links", tree_1, tree_2
        )
        self.assertEqual(result.returncode, 0, msg="File trees do not match")


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


def count_all_items(root_dir):
    total_count = 0
    for _, dirnames, filenames in os.walk(root_dir):
        total_count += len(dirnames) + len(filenames)
    return total_count


if __name__ == "__main__":
    unittest.main()
