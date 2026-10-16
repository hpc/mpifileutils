#!/usr/bin/env python3

import os
import unittest

from tests import common, config


class TestDSTRIPE(common.TestCase):
    """Tests for dstripe"""

    def setUp(self):
        # set up unique directory information for this test
        self.path = os.path.join(
            config.test_dir,
            self.__class__.__name__,
            f"{self._testMethodName}",
        )
        self.file1_path = os.path.join(self.path, "file1")
        self.file2_path = os.path.join(self.path, "file2")

        common.create_empty_files(self.file1_path, self.file2_path)

        # Skip test if not on Lustre
        if (
            common.filesystem_type(self.path) != "lustre"
            or os.environ["MFU_ENABLE_LUSTRE"] != "ON"
        ):
            self.skipTest(
                "Path is not on Lustre and/or not compiled with "
                "ENABLE_LUSTRE=ON"
            )

    def test_stripe_on_all(self):
        """Stripe files on all storage devices using a 2MB stripe size"""

        # run dstripe
        self.run_distributed_successfully(
            "dstripe", "--size", "2MB", self.path
        )

        for path in [self.file1_path, self.file2_path]:
            # CHECK: count uses all osts
            self.assertEqual(
                get_stripe_count(path),
                get_total_osts(path),
                msg="Stripe count is incorrect.",
            )

            # CHECK: stripe size is correct
            self.assertEqual(
                get_stripe_size(path),
                2 * 1024 * 1024,
                msg="Stripe size is incorrect.",
            )

    def test_stripe_on_half(self):
        """Stripe a file on half the devices using a 1 GB stripe size."""

        devices = get_total_osts(self.path) // 2

        # run dstripe
        self.run_distributed_successfully(
            "dstripe", "--count", devices, "--size", "1GB", self.file1_path
        )

        # CHECK: count uses all osts
        self.assertEqual(
            get_stripe_count(self.file1_path),
            devices,
            msg="Stripe count is incorrect.",
        )

        # CHECK: stripe size is correct
        self.assertEqual(
            get_stripe_size(self.file1_path),
            1024 * 1024 * 1024,
            msg="Stripe size is incorrect.",
        )

    def test_restripe_on_min_size(self):
        """Restripe all files that are at least 1MB in size"""

        # set up files for this test
        common.write_bytes(self.file1_path, b"\x00" * 1024)
        common.write_bytes(self.file2_path, b"\x00" * 1024 * 1024)

        # run dstripe
        self.run_distributed_successfully(
            "dstripe", "--minsize", "1MB", "--size", "11MB", self.path
        )

        # CHECK: stripe size set on file2, but not file1
        self.assertEqual(
            get_stripe_size(self.file2_path),
            11 * 1024 * 1024,
            msg="Stripe size is incorrect.",
        )
        self.assertNotEqual(
            get_stripe_size(self.file1_path),
            11 * 1024 * 1024,
            msg="Stripe size set unexpectedly.",
        )

    def test_report(self):
        """Display the current stripe count and stripe size of all files"""

        # set up files for this test
        total_devices = get_total_osts(self.path)
        common.write_bytes(self.file1_path, b"\x00" * 1024)
        common.write_bytes(self.file2_path, b"\x00" * 1024 * 1024)
        self.run_distributed_successfully(
            "dstripe",
            "--count", total_devices // 2,
            "--size", "5MB",
            self.file1_path,
        )
        self.run_distributed_successfully(
            "dstripe",
            "--count", total_devices,
            "--size", "1GB",
            self.file2_path,
        )

        # run dstripe
        dstrip_result = self.run_distributed_successfully(
            "dstripe", "--report", self.path
        )

        # CHECK: output is correct
        self.assertRegex(
            dstrip_result.stdout,
            rf"1.00 KiB\s+{total_devices // 2}\s+5.00 MiB\s+"
            rf"{self.file1_path}",
            msg="Incorrect report.",
        )
        self.assertRegex(
            dstrip_result.stdout,
            rf"1.00 MiB\s+{total_devices}\s+1.00 GiB\s+{self.file2_path}",
            msg="Incorrect report.",
        )


def get_total_osts(path):
    result = common.run_local("lfs", "osts", path)
    return result.stdout.count("ACTIVE")


def get_stripe_count(path):
    result = common.run_local("lfs", "getstripe", path)
    for line in result.stdout.split("\n"):
        if "lmm_stripe_count" in line:
            return int(line.split(":")[1].strip())
    return None


def get_stripe_size(path):
    result = common.run_local("lfs", "getstripe", path)
    for line in result.stdout.split("\n"):
        if "lmm_stripe_size" in line:
            return int(line.split(":")[1].strip())
    return None


if __name__ == "__main__":
    unittest.main()
