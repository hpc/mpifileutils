#!/usr/bin/python3

import os
import tempfile
import typing as t
from pathlib import Path
import textwrap

from .lib import (
    TestFileTreeCmp,
    dcp_cmd,
    dwalk_cmd,
    dfind_cmd,
    create_basic_layout,
)


class TestDcp(TestFileTreeCmp):

    def run_dcp(
        self,
        dereference: bool = False,
        preserve: bool = False,
        chunk: t.Optional[str] = None,
        buffer: t.Optional[str] = None,
        input: t.Optional[str] = None,
        dest: t.Optional[Path] = None,
    ):
        if not dest:
            dest = self.dst
        # Remove destination directory as it is created by dcp.
        dest.rmdir()
        cmd = dcp_cmd() + [self.src, dest]
        if dereference:
            cmd.insert(len(cmd) - 2, "--dereference")
        if preserve:
            cmd.insert(len(cmd) - 2, "--preserve")
        if chunk:
            cmd[-2:0] = ["--chunksize", chunk]
        if buffer:
            cmd[-2:0] = ["--bufsize", buffer]
        if input:
            cmd[-2:0] = ["--input", input]
        return self.run_cmd(cmd)

    def run_dwalk(
        self,
        output: t.Optional[Path] = None,
        lite: bool = False,
    ):
        cmd = dwalk_cmd() + [self.src]
        if output:
            cmd[-1:0] = ["--output", output]
        if lite:
            cmd.insert(len(cmd) - 1, "--lite")
        return self.run_cmd(cmd)

    def run_dfind(
        self,
        output: t.Optional[Path] = None,
    ):
        cmd = dfind_cmd() + [self.src]
        if output:
            cmd[-1:0] = ["--output", output]
        return self.run_cmd(cmd)


class TestDcpBasic(TestDcp):

    def setUp(self):
        super().setUp()
        create_basic_layout(self.src)

    def test_dcp(self):
        proc = self.run_dcp()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 9
                      Directories: 2
                      Files: 4
                      Links: 2
                      Hardlinks: 1
                """
            ),
        )
        # With basic dcp, files should have the same type and metadata except
        # mtime that is not copied.
        self.assertSrcDstEqual(ignore_mtime=True)

    def test_dcp_dereference(self):
        # Add content of file2, the target of symlink2
        with open(self.src / "file2", "w") as fh:
            fh.write("original")
        self.run_dcp(dereference=True)
        # Check source and destination have the same content, except for
        # symlinks and mtime.
        self.assertSrcDstEqual(
            ignore_paths=["symlink2", "dir1/symlink1"], ignore_mtime=True
        )
        # Check symlink2 is not a symlink in destination
        self.assertTrue((self.src / "symlink2").is_symlink())
        self.assertFalse((self.dst / "symlink2").is_symlink())
        # Check symlink2 has its own inode, distinct from file2
        self.assertNotEqual(
            (self.dst / "symlink2").stat().st_ino,
            (self.dst / "file2").stat().st_ino,
        )
        # Check both files have the same original content
        for filename in ["file2", "symlink2"]:
            with open(self.dst / filename) as fh:
                self.assertEqual(fh.read(), "original")

    def test_dcp_preserve(self):
        self.run_dcp(preserve=True)
        # With dcp --preserve, files must have the same metadata in source and
        # destination.
        self.assertSrcDstEqual()

    def test_dcp_preserve_chmod(self):
        # Change some file modes in source
        (self.src / "file1").chmod(0o400)
        (self.src / "hardlink3").chmod(0o400)
        (self.src / "dir1").chmod(0o700)
        self.run_dcp(preserve=True)
        # With dcp --preserve, files must have the same metadata in source and
        # destination.
        self.assertSrcDstEqual()

    def test_dcp_chunksize(self):
        # Add 16MB of data in a file to have multiple chunks
        with open(self.src / "file1", "wb") as fh:
            fh.write(os.urandom(16 * 10**6))
        self.run_dcp(chunk="1MB")
        self.assertSrcDstEqual(ignore_mtime=True)

    def test_dcp_bufsize(self):
        # Add 16MB of data in a file to fill multiple buffer
        with open(self.src / "file1", "wb") as fh:
            fh.write(os.urandom(16 * 10**6))
        self.run_dcp(buffer="1MB")
        self.assertSrcDstEqual(ignore_mtime=True)

    def test_dcp_dwalk_input(self):
        with tempfile.NamedTemporaryFile() as fh:
            self.run_dwalk(output=fh.name)
            proc = self.run_dcp(input=fh.name)
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 9
                      Directories: 2
                      Files: 4
                      Links: 2
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual(ignore_mtime=True)

    def test_dcp_dwalk_input_lite(self):
        with tempfile.NamedTemporaryFile() as fh:
            self.run_dwalk(output=fh.name, lite=True)
            proc = self.run_dcp(input=fh.name)
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 9
                      Directories: 2
                      Files: 4
                      Links: 2
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual(ignore_mtime=True)

    def test_dcp_dfind_input(self):
        with tempfile.NamedTemporaryFile() as fh:
            self.run_dfind(output=fh.name)
            proc = self.run_dcp(input=fh.name)
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 9
                      Directories: 2
                      Files: 4
                      Links: 2
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual(ignore_mtime=True)


class TestDcpDfilemaker(TestDcp):

    def setUp(self):
        super().setUp()
        self.run_dfilemaker()

    def test_dcp(self):
        self.run_dcp()
        self.assertSrcDstEqual(ignore_mtime=True)
