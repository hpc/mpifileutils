#!/usr/bin/env python3

import os
import unittest

from tests import common, config


class TestDBZ2(common.TestCase):
    """Tests for dbz2"""

    def setUp(self):
        # set up unique directory information for this test
        self.path = os.path.join(
            config.test_dir,
            self.__class__.__name__,
            f"{self._testMethodName}",
        )
        self.filepath = os.path.join(self.path, "file")
        self.compressed_filepath = os.path.join(self.path, "file.dbz2")

    def test_compress(self):
        """Compress a file"""

        # set up directories/files to test on
        common.create_random_files(self.filepath)

        # run dbz2
        self.run_distributed_successfully("dbz2", "--compress", self.filepath)

        # CHECK: compressed file was created
        self.assertTrue(
            os.path.exists(self.compressed_filepath),
            msg="Compressed file was not created.",
        )

        # CHECK: input file was removed
        self.assertFalse(
            os.path.exists(self.filepath), msg="Input file was not removed."
        )

        # CHECK: compressed file is valid
        self.run_local_successfully("bzip2", "-t", self.compressed_filepath)

    def test_compress_keep_input(self):
        """Compress a file and keep the input file"""

        # set up directories/files to test on
        common.create_random_files(self.filepath)

        # run dbz2
        self.run_distributed_successfully(
            "dbz2", "--keep", "--compress", self.filepath
        )

        # CHECK: compressed file was created
        self.assertTrue(
            os.path.exists(self.compressed_filepath),
            msg="Compressed file was not created.",
        )

        # CHECK: input file was not removed
        self.assertTrue(
            os.path.exists(self.filepath), msg="Input file was not removed."
        )

        # CHECK: compressed file is valid
        self.run_local_successfully("bzip2", "-t", self.compressed_filepath)

    def test_compress_overwrite(self):
        """Compress a file and overwrite any existing output file"""

        # set up directories/files to test on
        common.create_random_files(self.filepath, self.compressed_filepath)

        # run dbz2 without --force, expect failure
        self.run_distributed_with_failure("dbz2", "--compress", self.filepath)

        # run dbz2 with --force
        self.run_distributed_successfully(
            "dbz2", "--force", "--compress", self.filepath
        )

        # CHECK: compressed file is valid
        self.run_local_successfully("bzip2", "-t", self.compressed_filepath)

    def test_decompress(self):
        """Decompress a file"""

        # set up directories/files to test on
        common.write_bytes(self.filepath, b"\x00test data\xff")

        # run dbz2 to compress a file
        self.run_distributed_successfully("dbz2", "--compress", self.filepath)

        # CHECK: compressed file was created
        self.assertTrue(
            os.path.exists(self.compressed_filepath),
            msg="Compressed file was not created.",
        )

        # CHECK: input file was removed
        self.assertFalse(
            os.path.exists(self.filepath), msg="Input file was not removed."
        )

        # run dbz2 to decompress the file
        self.run_distributed_successfully(
            "dbz2", "--decompress", self.compressed_filepath
        )

        # CHECK: decompressed file was created
        self.assertTrue(
            os.path.exists(self.filepath),
            msg="Decompressed file was not created.",
        )

        # CHECK: data was not changed
        self.assertEqual(
            common.read_bytes(self.filepath), b"\x00test data\xff"
        )


if __name__ == "__main__":
    unittest.main()
