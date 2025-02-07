#!/usr/bin/python3

import tempfile
import os
import typing as t
from pathlib import Path
import textwrap

from .lib import (
    TestFileTreeCmp,
    dwalk_cmd,
    create_basic_layout,
)


class TestDwalk(TestFileTreeCmp):

    def run_dwalk(
        self,
        output: t.Optional[Path] = None,
        input: t.Optional[Path] = None,
        lite: bool = False,
        text: bool = False,
    ):
        cmd = dwalk_cmd()
        if output:
            cmd += ["--output", output]
        if input:
            cmd += ["--input", input]
        if lite:
            cmd.append("--lite")
        if text:
            cmd.append("--text")
        if not input:
            cmd.append(self.src)
        return self.run_cmd(cmd)


class TestDwalkBasic(TestDwalk):

    def setUp(self):
        super().setUp()
        oldmask = os.umask(0o022)
        create_basic_layout(self.src)
        os.umask(oldmask)

    def test_walk(self):
        proc = self.run_dwalk()
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

    def test_walk_output(self):
        # Create and delete a temporary file but keep its name.
        with tempfile.NamedTemporaryFile() as fh:
            output = Path(fh.name)
        proc = self.run_dwalk(output=output)
        # Check dwalk has created file
        self.assertTrue(output.exists())
        output.unlink()
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

    def test_walk_output_lite(self):
        # Create and delete a temporary file but keep its name.
        with tempfile.NamedTemporaryFile() as fh:
            output = Path(fh.name)
        proc = self.run_dwalk(output=output, lite=True)
        # dwalk with --lite do not call stat(), it is unable to detect hardlinks.
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 9
                      Directories: 2
                      Files: 5
                      Links: 2
                      Hardlinks: 0
                """
            ),
        )
        # Check dwalk has created file
        self.assertTrue(output.exists())
        with open(output) as fh:
            content = fh.read()
        output.unlink()
        for entry in [
            f"{self.src}|D",
            f"{self.src}/file3|F",
            f"{self.src}/hardlink3|F",
            f"{self.src}/file2|F",
            f"{self.src}/file1|F",
            f"{self.src}/symlink2|L",
            f"{self.src}/dir1|D",
            f"{self.src}/dir1/file4|F",
            f"{self.src}/dir1/symlink1|L",
        ]:
            self.assertIn(entry, content)

    def test_walk_output_text(self):
        with tempfile.NamedTemporaryFile(mode="w+") as fh:
            output = Path(fh.name)
        self.run_dwalk(output=output, text=True)
        # Check dwalk has created file
        self.assertTrue(output.exists())
        with open(output) as fh:
            content = fh.read()

        for entry in [
            rf"drwx------ .* {self.src}",
            rf"drwxr-xr-x .* {self.src}/dir1",
            rf"lrwxrwxrwx .* {self.src}/dir1/symlink1",
            rf"-rw-r--r-- .* {self.src}/dir1/file4",
            rf"lrwxrwxrwx .* {self.src}/symlink2",
            rf"-rw-r--r-- .* {self.src}/file1",
            rf"-rw-r--r-- .* {self.src}/file2",
            rf"-rw-r--r-- .* {self.src}/file3",
            rf"-rw-r--r-- .* {self.src}/hardlink3",
        ]:
            self.assertRegex(content, entry)

    def test_walk_input(self):
        with tempfile.NamedTemporaryFile(mode="w+") as fh:
            cache = Path(fh.name)
            self.run_dwalk(output=cache)
            proc = self.run_dwalk(input=cache)
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

    def test_walk_input_lite(self):
        with tempfile.NamedTemporaryFile(mode="w+") as fh:
            cache = Path(fh.name)
            self.run_dwalk(output=cache, lite=True)
            proc = self.run_dwalk(input=cache)
            # lite cache do not contain hardlinks, then dwalk misses it.
            self.assertInProcStdout(
                proc,
                textwrap.dedent(
                    """
                        Items: 9
                          Directories: 2
                          Files: 5
                          Links: 2
                          Hardlinks: 0
                    """
                ),
            )
