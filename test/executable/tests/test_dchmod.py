#!/usr/bin/env python3

import os
import random
import unittest

from tests import common, config


class TestDCHMOD(common.TestCase):
    """Tests for dchmod"""

    def setUp(self):
        # Set up the file tree to test on.
        self.tmp_tree_base = os.path.join(
            config.test_dir,
            self.__class__.__name__,
            f"tmp_{self._testMethodName}",
        )

        common.create_empty_directories(
            os.path.join(self.tmp_tree_base, "tmp0", "tmp1", "tmp2")
        )
        common.create_empty_files(
            os.path.join(self.tmp_tree_base, "tmp0", "tmp0_file"),
            os.path.join(self.tmp_tree_base, "tmp0", "tmp1", "tmp1_file"),
        )

    def test_octal_mode(self):
        """Use octal mode to change permissions"""

        # Run dchmod with a random valid octal mode between 700 and 777.
        octal = "7" + str(random.randrange(0, 8)) + str(random.randrange(0, 8))
        self.run_distributed_successfully(
            "dchmod", "-v", "--mode", octal, self.tmp_tree_base
        )

        # Check that all permissions were set correctly.
        permissions_map = common.get_tree_permissions(self.tmp_tree_base)
        incorrect_permissions = [
            path
            for path, permission in permissions_map.items()
            if permission != octal
        ]
        self.assertFalse(
            incorrect_permissions,
            msg=f"Permission not set on {incorrect_permissions}",
        )

    def test_exclude(self):
        """Change permissions except for paths matching the exclusion regex."""

        # Run dchmod with symbolic syntax equivalent to 761.
        # NOTE: 761 is not a permission used as a default on most systems.
        self.run_distributed_successfully(
            "dchmod",
            "--exclude", f".*{os.sep}tmp1{os.sep}.*",
            "--mode", "u+rwx,g+rw,g-x,o+x,o-rw",
            self.tmp_tree_base,
        )

        # Check that all permissions are correct and the regex is respected.
        permissions_map = common.get_tree_permissions(self.tmp_tree_base)
        incorrect_permissions = [
            path
            for path, permission in permissions_map.items()
            if (f"{os.sep}tmp1{os.sep}" in path and permission == "761")
            or (f"{os.sep}tmp1{os.sep}" not in path and permission != "761")
        ]
        self.assertFalse(
            incorrect_permissions,
            msg=f"Permission incorrectly set on {incorrect_permissions}",
        )

    def test_match(self):
        """Change permissions on items whose name match regex"""

        # Run dchmod with symbolic syntax equivalent to 761.
        # NOTE: 761 is not a permission used as a default on most systems.
        self.run_distributed_successfully(
            "dchmod",
            "--name",
            "--match", ".*_file$",
            "--mode", "u+rwx,g+rw,g-x,o+x,o-rw",
            self.tmp_tree_base,
        )

        # Check that all permissions are correct and the regex is respected.
        permissions_map = common.get_tree_permissions(self.tmp_tree_base)
        incorrect_permissions = [
            path
            for path, permission in permissions_map.items()
            if (path.endswith("_file") and permission != "761")
            or (not path.endswith("_file") and permission == "761")
        ]
        self.assertFalse(
            incorrect_permissions,
            msg=f"Permission incorrectly set on {incorrect_permissions}",
        )


if __name__ == "__main__":
    unittest.main()
