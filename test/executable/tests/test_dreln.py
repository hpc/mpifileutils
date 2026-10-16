#!/usr/bin/env python3

import os
import unittest

from tests import common, config


class TestDRELN(common.TestCase):
    """Tests for dreln"""

    def setUp(self):
        # set up unique directory information for this test
        base = os.path.join(config.test_dir, self.__class__.__name__)
        self.old_path = os.path.join(base, f"old_{self._testMethodName}")
        self.symlink_path = os.path.join(
            base, f"symlink_{self._testMethodName}"
        )
        self.new_path = os.path.join(base, f"new_{self._testMethodName}")

        self.files = ["file1", "file2", "file3"]
        for path in [self.old_path, self.new_path]:
            common.create_empty_files(
                *[os.path.join(path, file) for file in self.files]
            )

        common.create_empty_directories(self.symlink_path)
        for file in self.files:
            os.symlink(
                os.path.join(self.old_path, file),
                os.path.join(self.symlink_path, file),
            )

    def test_update_symlinks(self):
        """Update symlinks from old_path to point to new_path instead."""

        # run dreln
        self.run_distributed_successfully(
            "dreln", self.old_path, self.new_path, self.symlink_path
        )

        # CHECK: all symlinks point to new_path
        for file in self.files:
            self.assertEqual(
                os.readlink(os.path.join(self.symlink_path, file)),
                os.path.join(self.new_path, file),
                msg="Symlink did not change.",
            )

    def test_skip_symlinks(self):
        """Skip links whose targets are not absolute paths to old_path."""

        # create a symlink that points elsewhere
        unchanged_symlink1 = os.path.join(self.symlink_path, "file4")
        os.symlink(f"{os.sep}tmp", unchanged_symlink1)

        # create a symlink that uses a relative path to old_path
        unchanged_symlink2 = os.path.join(self.symlink_path, "file5")
        os.symlink(
            os.path.join("..", f"old_{self._testMethodName}", "file1"),
            unchanged_symlink2,
        )

        # run dreln
        self.run_distributed_successfully(
            "dreln", self.old_path, self.new_path, self.symlink_path
        )

        # CHECK: symlinks do not point to new_path
        self.assertNotIn(
            f"new_{self._testMethodName}",
            os.readlink(unchanged_symlink1),
            msg="Symlink changed unexpectedly.",
        )
        self.assertNotIn(
            f"new_{self._testMethodName}",
            os.readlink(unchanged_symlink2),
            msg="Symlink changed unexpectedly.",
        )

    def test_relative_symlinks(self):
        """Update symlinks with relative paths"""

        # run dreln
        self.run_distributed_successfully(
            "dreln",
            "--relative",
            self.old_path,
            self.new_path,
            self.symlink_path,
        )

        # CHECK: all symlinks point to new_path with a relative link
        for file in self.files:
            self.assertEqual(
                os.readlink(os.path.join(self.symlink_path, file)),
                os.path.join("..", f"new_{self._testMethodName}", file),
                msg="Symlink does not use relative link.",
            )

    def test_preserve_modification_times(self):
        """Update symlinks without changing the modification time"""

        # set modification times on symlinks
        for file in self.files:
            os.utime(
                os.path.join(self.symlink_path, file),
                ns=(1_000_000_000, 1_000_000_000),
                follow_symlinks=False,
            )

        # run dreln
        self.run_distributed_successfully(
            "dreln",
            "--preserve",
            self.old_path,
            self.new_path,
            self.symlink_path,
        )

        # CHECK: modification times did not change
        for file in self.files:
            self.assertEqual(
                os.stat(
                    os.path.join(self.symlink_path, file),
                    follow_symlinks=False,
                ).st_mtime_ns,
                1_000_000_000,
                msg="Modification time changed unexpectedly.",
            )

    def test_multipath_symlinks(self):
        """Update symlinks using multiple paths"""

        # set up another directory with symlinks
        self.symlink_path2 = os.path.join(
            config.test_dir,
            self.__class__.__name__,
            f"symlink_{self._testMethodName}_2",
        )

        common.create_empty_directories(self.symlink_path2)
        for file in self.files:
            os.symlink(
                os.path.join(self.old_path, file),
                os.path.join(self.symlink_path2, file),
            )

        # run dreln
        self.run_distributed_successfully(
            "dreln",
            self.old_path,
            self.new_path,
            self.symlink_path,
            self.symlink_path2,
        )

        # CHECK: all symlinks point to new_path
        for file in self.files:
            self.assertEqual(
                os.readlink(os.path.join(self.symlink_path, file)),
                os.path.join(self.new_path, file),
                msg="Symlink did not change.",
            )
            self.assertEqual(
                os.readlink(os.path.join(self.symlink_path2, file)),
                os.path.join(self.new_path, file),
                msg="Symlink did not change.",
            )


if __name__ == "__main__":
    unittest.main()
