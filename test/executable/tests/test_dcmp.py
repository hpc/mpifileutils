#!/usr/bin/env python3

import os
import shutil
import unittest

from tests import common, config


class TestDCMP(common.TestCase):
    """Tests for dcmp"""

    def setUp(self):
        # set up unique directory and output file information for this test
        base = os.path.join(config.test_dir, self.__class__.__name__)
        self.src_dirpath = os.path.join(base, f"src_{self._testMethodName}")
        self.dest_dirpath = os.path.join(base, f"dest_{self._testMethodName}")
        self.output_filepath = os.path.join(
            base,
            f"output_{self._testMethodName}",
        )

        self.src_dirpath_tempdir_path = os.path.join(
            self.src_dirpath, "tempdir"
        )
        self.src_dirpath_tempfile_path = os.path.join(
            self.src_dirpath, "tempfile"
        )
        self.dest_dirpath_tempdir_path = os.path.join(
            self.dest_dirpath, "tempdir"
        )
        self.dest_dirpath_tempfile_path = os.path.join(
            self.dest_dirpath, "tempfile"
        )

    def test_expression_0(self):
        """check EXIST=ONLY_SRC with binary formatted output"""

        # set up directories/files to test on
        common.create_empty_directories(
            self.src_dirpath_tempdir_path, self.dest_dirpath_tempdir_path
        )
        common.create_empty_files(self.src_dirpath_tempfile_path)

        # run dcmp, output entry that exists only in source path
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-o", f"EXIST=ONLY_SRC:{self.output_filepath}",
        )

        # run dwalk over output
        dwalk_result = self.run_distributed_successfully(
            "dwalk", "--print", "--input", self.output_filepath
        )

        # CHECK: src/tempfile reported
        self.assertIn(
            self.src_dirpath_tempfile_path,
            dwalk_result.stdout,
            msg=dwalk_result.stderr,
        )

        # CHECK: src/tempdir not reported
        self.assertNotIn(
            self.src_dirpath_tempdir_path,
            dwalk_result.stdout,
            msg=dwalk_result.stderr,
        )

        # CHECK: dest/tempdir not reported
        self.assertNotIn(
            self.dest_dirpath_tempdir_path,
            dwalk_result.stdout,
            msg=dwalk_result.stderr,
        )

    def test_expression_1(self):
        """check EXIST=ONLY_DEST"""

        # set up directories/files to test on
        common.create_empty_directories(self.dest_dirpath_tempdir_path)
        common.create_empty_files(
            self.src_dirpath_tempfile_path,
            self.dest_dirpath_tempfile_path,
        )

        # run dcmp, output entry that exists only in destination path
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"EXIST=ONLY_DEST:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: src/tempfile not reported
        self.assertNotIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempfile not reported
        self.assertNotIn(self.dest_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempdir is reported
        self.assertIn(self.dest_dirpath_tempdir_path, output, msg=output)

    def test_expression_2(self):
        """check EXIST=DIFFER"""

        # set up directories/files to test on
        common.create_empty_files(self.src_dirpath_tempfile_path)
        common.create_empty_directories(self.dest_dirpath_tempdir_path)

        # run dcmp, output entries that differ in existence
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"EXIST=DIFFER:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: src/tempfile is reported
        self.assertIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempdir is reported
        self.assertIn(self.dest_dirpath_tempdir_path, output, msg=output)

    def test_expression_3(self):
        """check EXIST=COMMON"""

        # set up directories/files to test on
        common.create_empty_directories(self.dest_dirpath_tempdir_path)
        common.create_empty_files(
            self.src_dirpath_tempfile_path,
            self.dest_dirpath_tempfile_path,
        )

        # run dcmp, output entries that exist in both paths
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"EXIST=COMMON:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: dest/tempdir not reported
        self.assertNotIn(self.dest_dirpath_tempdir_path, output, msg=output)

        # CHECK: src/tempfile is reported
        self.assertIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempfile is reported
        self.assertIn(self.dest_dirpath_tempfile_path, output, msg=output)

    def test_expression_4(self):
        """check TYPE=DIFFER"""

        # set up directories/files to test on
        common.create_empty_directories(
            self.src_dirpath_tempdir_path,
            self.dest_dirpath_tempdir_path,
            self.dest_dirpath_tempfile_path,
        )
        common.create_empty_files(self.src_dirpath_tempfile_path)

        # run dcmp, output entries with different types
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"TYPE=DIFFER:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: src/tempfile is reported
        self.assertIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: src/tempdir not reported
        self.assertNotIn(self.src_dirpath_tempdir_path, output, msg=output)

        # CHECK: dest/tempfile is reported
        self.assertIn(self.dest_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempdir not reported
        self.assertNotIn(self.dest_dirpath_tempdir_path, output, msg=output)

    def test_expression_5(self):
        """check (EXIST=COMMON) && (TYPE=DIFFER)"""

        # set up directories/files to test on
        common.create_empty_directories(
            self.src_dirpath_tempdir_path,
            self.dest_dirpath_tempdir_path,
            self.dest_dirpath_tempfile_path,
        )
        common.create_empty_files(self.src_dirpath_tempfile_path)

        # run dcmp, output entries that exist in both but have different types
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"EXIST=COMMON@TYPE=DIFFER:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: src/tempfile is reported
        self.assertIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: src/tempdir not reported
        self.assertNotIn(self.src_dirpath_tempdir_path, output, msg=output)

        # CHECK: dest/tempfile is reported
        self.assertIn(self.dest_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempdir not reported
        self.assertNotIn(self.dest_dirpath_tempdir_path, output, msg=output)

    def test_expression_6(self):
        """check (TYPE=DIFFER) && (EXIST=COMMON)"""

        # set up directories/files to test on
        common.create_empty_directories(
            self.src_dirpath_tempdir_path,
            self.dest_dirpath_tempdir_path,
            self.dest_dirpath_tempfile_path,
        )
        common.create_empty_files(self.src_dirpath_tempfile_path)

        # run dcmp, output entries with different types that exist in both
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"TYPE=DIFFER@EXIST=COMMON:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: src/tempfile is reported
        self.assertIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: src/tempdir not reported
        self.assertNotIn(self.src_dirpath_tempdir_path, output, msg=output)

        # CHECK: dest/tempfile is reported
        self.assertIn(self.dest_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempdir not reported
        self.assertNotIn(self.dest_dirpath_tempdir_path, output, msg=output)

    def test_expression_7(self):
        """check (TYPE=DIFFER) && (EXIST=DIFFER)"""

        # set up directories/files to test on
        common.create_empty_directories(
            self.src_dirpath_tempdir_path,
            self.dest_dirpath_tempdir_path,
            self.dest_dirpath_tempfile_path,
        )
        common.create_empty_files(self.src_dirpath_tempfile_path)

        # Run dcmp for entries with different types and existence. None match.
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"TYPE=DIFFER@EXIST=DIFFER:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: src/tempfile not reported
        self.assertNotIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: src/tempdir not reported
        self.assertNotIn(self.src_dirpath_tempdir_path, output, msg=output)

        # CHECK: dest/tempfile not reported
        self.assertNotIn(self.dest_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempdir not reported
        self.assertNotIn(self.dest_dirpath_tempdir_path, output, msg=output)

    def test_expression_8(self):
        """check (EXIST=DIFFER) && (TYPE=DIFFER)"""

        # set up directories/files to test on
        common.create_empty_directories(
            self.src_dirpath_tempdir_path,
            self.dest_dirpath_tempdir_path,
            self.dest_dirpath_tempfile_path,
        )
        common.create_empty_files(self.src_dirpath_tempfile_path)

        # Run dcmp for entries with different existence and types. None match.
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"EXIST=DIFFER@TYPE=DIFFER:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: src/tempfile not reported
        self.assertNotIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: src/tempdir not reported
        self.assertNotIn(self.src_dirpath_tempdir_path, output, msg=output)

        # CHECK: dest/tempfile not reported
        self.assertNotIn(self.dest_dirpath_tempfile_path, output, msg=output)

        # CHECK: dest/tempdir not reported
        self.assertNotIn(self.dest_dirpath_tempdir_path, output, msg=output)

    def test_expression_9(self):
        """check (TYPE=DIFFER) || (EXIST=DIFFER)"""

        # set up directories/files to test on
        common.create_empty_directories(
            self.src_dirpath_tempdir_path,
            self.dest_dirpath_tempfile_path,
        )
        common.create_empty_files(self.src_dirpath_tempfile_path)

        # run dcmp, output entries with different types OR differ in existence
        self.run_distributed_successfully(
            "dcmp",
            self.src_dirpath,
            self.dest_dirpath,
            "-t",
            "-o", f"TYPE=DIFFER,EXIST=DIFFER:{self.output_filepath}",
        )

        # get output file contents
        output = common.get_output_file_list(self.output_filepath)

        # CHECK: src/tempfile is reported
        self.assertIn(self.src_dirpath_tempfile_path, output, msg=output)

        # CHECK: src/tempdir is reported
        self.assertIn(self.src_dirpath_tempdir_path, output, msg=output)

        # CHECK: dest/tempfile is reported
        self.assertIn(self.dest_dirpath_tempfile_path, output, msg=output)

    def test_extras_and_differences(self):
        """extras and diff comparison"""

        # Set up test paths with an extra source file to check counting.
        common.create_empty_directories(self.src_dirpath, self.dest_dirpath)
        common.create_empty_files(
            os.path.join(self.src_dirpath, "extrafile.txt")
        )
        common.create_random_files(os.path.join(self.src_dirpath, "tempfile"))

        # Copy the file to the destination
        shutil.copy(
            self.src_dirpath_tempfile_path, f"{self.dest_dirpath}{os.sep}"
        )

        # Run dcmp to check identical contents.
        dcmp_result = self.run_distributed_successfully(
            "dcmp", self.src_dirpath, self.dest_dirpath
        )

        # Parse the dcmp output to verify counts.
        output_lines = dcmp_result.stdout.strip().split("\n")

        # Two files should match and one extra should exist only in the source.
        found_same = False
        found_extras = False

        for line in output_lines:
            if "same content" in line.lower() and "(Src: 2 Dest: 2)" in line:
                found_same = True
            if (
                "exist only in one directory" in line.lower()
                and "(Src: 1 Dest: 0)" in line
            ):
                found_extras = True

        # Check that matching files were found.
        self.assertTrue(
            found_same,
            msg=(
                "Did not find expected 2 files with same content in output:\n"
                f"{dcmp_result.stdout}"
            ),
        )

        # Check that the extra source file was found.
        self.assertTrue(
            found_extras,
            msg=(
                "Did not find expected 1 extra file in src in output:\n"
                f"{dcmp_result.stdout}"
            ),
        )

        # Modify byte 0 of the source file to value 57 (ASCII "9").
        with open(os.path.join(self.src_dirpath, "tempfile"), "r+b") as file:
            file.seek(0)
            file.write(b"\x39")
            file.flush()

            file.seek(0)
            modified_byte = file.read(1)

        # Check that the byte was modified successfully.
        self.assertEqual(
            modified_byte,
            b"\x39",
            msg="The first byte was not modified correctly",
        )

        # Run dcmp again to check for differences.
        dcmp_result2 = self.run_distributed_successfully(
            "dcmp", self.src_dirpath, self.dest_dirpath
        )

        # Parse the dcmp output to verify that it detected the difference.
        output_lines2 = dcmp_result2.stdout.strip().split("\n")

        # Should find that 1 file has different content
        found_diff = False

        for line in output_lines2:
            if (
                "different contents" in line.lower()
                and "(Src: 1 Dest: 1)" in line
            ):
                found_diff = True

        # Check that files with different content were found.
        self.assertTrue(
            found_diff,
            msg=(
                "Did not find expected 1 file with different "
                "content in output:\n"
                f"{dcmp_result2.stdout}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
