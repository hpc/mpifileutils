#!/usr/bin/python3

import textwrap
from pathlib import Path
import typing as t

from .lib import (
    TestFileTreeCmp,
    dcmp_cmd,
    dsync_cmd,
    create_basic_layout,
    create_file,
)


class TestDcp(TestFileTreeCmp):

    def run_dcmp(
        self,
        dest: t.Optional[Path] = None,
    ):
        if not dest:
            dest = self.dst
        cmd = dcmp_cmd() + [self.src, dest]
        return self.run_cmd(cmd)

    def run_dsync(self):
        cmd = dsync_cmd() + [self.src, self.dst]
        return self.run_cmd(cmd)


class TestDcpBasic(TestDcp):

    def setUp(self):
        super().setUp()
        create_basic_layout(self.src)

    def test_dcmp(self):
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items     : 12
                """
            ),
        )

    def test_dcmp_after_dsync(self):
        self.run_dsync()
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Number of items that exist in both directories: 12 (Src: 12 Dest: 12)
                    Number of items that exist only in one directory: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same type: 12 (Src: 12 Dest: 12)
                    Number of items that exist in both directories and have different types: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same content: 12 (Src: 12 Dest: 12)
                    Number of items that exist in both directories and have different contents: 0 (Src: 0 Dest: 0)
                """
            ),
        )

    def test_dcmp_additional_file(self):
        self.run_dsync()
        create_file(self.dst / "newfile")
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Number of items that exist in both directories: 12 (Src: 12 Dest: 12)
                    Number of items that exist only in one directory: N/A (Src: 0 Dest: 1)
                    Number of items that exist in both directories and have the same type: 12 (Src: 12 Dest: 12)
                    Number of items that exist in both directories and have different types: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same content: 12 (Src: 12 Dest: 12)
                    Number of items that exist in both directories and have different contents: 0 (Src: 0 Dest: 0)
                """
            ),
        )

    def test_dcmp_missing_file(self):
        self.run_dsync()
        (self.dst / "file1").unlink()
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Number of items that exist in both directories: 11 (Src: 11 Dest: 11)
                    Number of items that exist only in one directory: N/A (Src: 1 Dest: 0)
                    Number of items that exist in both directories and have the same type: 11 (Src: 11 Dest: 11)
                    Number of items that exist in both directories and have different types: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same content: 11 (Src: 11 Dest: 11)
                    Number of items that exist in both directories and have different contents: 0 (Src: 0 Dest: 0)
                """
            ),
        )

    def test_dcmp_different_type(self):
        self.run_dsync()
        # change file1 in destination from regular file to directory
        (self.dst / "file1").unlink()
        (self.dst / "file1").mkdir()
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Number of items that exist in both directories: 12 (Src: 12 Dest: 12)
                    Number of items that exist only in one directory: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same type: 11 (Src: 11 Dest: 11)
                    Number of items that exist in both directories and have different types: 1 (Src: 1 Dest: 1)
                    Number of items that exist in both directories and have the same content: 11 (Src: 11 Dest: 11)
                    Number of items that exist in both directories and have different contents: 1 (Src: 1 Dest: 1)
                """
            ),
        )

    def test_dcmp_different_symlink_target(self):
        self.run_dsync()
        # change target of symlink2 from file2 to file1 in destination
        (self.dst / "symlink2").unlink()
        (self.dst / "symlink2").symlink_to("file1")
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Number of items that exist in both directories: 12 (Src: 12 Dest: 12)
                    Number of items that exist only in one directory: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same type: 12 (Src: 12 Dest: 12)
                    Number of items that exist in both directories and have different types: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same content: 11 (Src: 11 Dest: 11)
                    Number of items that exist in both directories and have different contents: 1 (Src: 1 Dest: 1)
                """
            ),
        )

    def test_dcmp_different_hardlink_target(self):
        self.run_dsync()
        # change target of hardlink3 from file3 to file1 in destination
        (self.dst / "hardlink3").unlink()
        (self.dst / "hardlink3").hardlink_to(self.dst / "file1")
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Number of items that exist in both directories: 12 (Src: 12 Dest: 12)
                    Number of items that exist only in one directory: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same type: 12 (Src: 12 Dest: 12)
                    Number of items that exist in both directories and have different types: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same content: 11 (Src: 11 Dest: 11)
                    Number of items that exist in both directories and have different contents: 1 (Src: 1 Dest: 1)
                """
            ),
        )

    def test_dcmp_missing_hardlink_ref(self):
        self.run_dsync()
        # Remove file4 (reference of hardlink4.1 and hardlink4.2) in source.
        #
        # In practice, a single file is missing in destination. However, due to
        # limitations in comparison algorithm, dcmp considers:
        # - file4 is missing in dest (ok)
        # - hardlink4.1 to be a reference file in source and a "secondary"
        #   hardlink in destination → type difference
        # - hardlink4.2 is a hardlink to file4 in source and a hardlink to
        #   hardlink4.1 in destination → content difference
        (self.src / "file4").unlink()
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Number of items that exist in both directories: 11 (Src: 11 Dest: 11)
                    Number of items that exist only in one directory: N/A (Src: 0 Dest: 1)
                    Number of items that exist in both directories and have the same type: 10 (Src: 10 Dest: 10)
                    Number of items that exist in both directories and have different types: 1 (Src: 1 Dest: 1)
                    Number of items that exist in both directories and have the same content: 9 (Src: 9 Dest: 9)
                    Number of items that exist in both directories and have different contents: 2 (Src: 2 Dest: 2)
                """
            ),
        )


    def test_dcmp_different_content(self):
        self.run_dsync()
        # change content of file1 in destination
        with open(self.dst / "file1", "w") as fh:
            fh.write("whatever")
        proc = self.run_dcmp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Number of items that exist in both directories: 12 (Src: 12 Dest: 12)
                    Number of items that exist only in one directory: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same type: 12 (Src: 12 Dest: 12)
                    Number of items that exist in both directories and have different types: 0 (Src: 0 Dest: 0)
                    Number of items that exist in both directories and have the same content: 11 (Src: 11 Dest: 11)
                    Number of items that exist in both directories and have different contents: 1 (Src: 1 Dest: 1)
                """
            ),
        )
