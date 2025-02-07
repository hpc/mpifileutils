#!/usr/bin/python3

import tarfile
from pathlib import Path
import os
import textwrap
import typing as t

import yaml
import xattr

from .lib import (
    TestFileTreeCmp,
    dtar_cmd,
    create_basic_layout,
    BASIC_FILES_LAYOUT,
)


class TestDtar(TestFileTreeCmp):

    def run_dtar(
        self,
        extract: bool = False,
        preserve_xattrs: bool = False,
        preserve_acls: bool = False,
        preserve_flags: bool = False,
        env: t.Optional[t.Dict[str, str]] = None,
    ):
        if extract:
            cmd = dtar_cmd() + ["-xf", self.archive]
            if preserve_xattrs:
                cmd.insert(len(cmd) - 2, "--preserve-xattrs")
            if preserve_acls:
                cmd.insert(len(cmd) - 2, "--preserve-acls")
            if preserve_flags:
                cmd.insert(len(cmd) - 2, "--preserve-flags")
            cwd = self.dst
        else:
            cmd = dtar_cmd() + [
                "-cf",
                self.archive,
                self.src.name,
            ]
            if preserve_xattrs:
                cmd.insert(len(cmd) - 3, "--preserve-xattrs")
            if preserve_acls:
                cmd.insert(len(cmd) - 3, "--preserve-acls")
            if preserve_flags:
                cmd.insert(len(cmd) - 3, "--preserve-flags")
            cwd = self.src.parent

        return self.run_cmd(cmd, cwd=cwd, env=env)

    def assertArchiveBasicTree(self):
        """Compare tarfile with YAML layout"""
        tar = tarfile.open(self.archive)
        tree = yaml.safe_load(BASIC_FILES_LAYOUT)
        self.assertSameDirArchive(tree, tar, Path(self.src.name))
        tar.close()

    def assertSameDirArchive(self, tree, tar, subdir: Path):
        for key, value in tree.items():
            try:
                member = tar.getmember(f"{subdir}/{key}")
            except KeyError:
                raise AssertionError(
                    f"Unable to find {subdir}/{key} in archive"
                )
            else:
                print(f"Comparing {subdir}/{key} in archive with source layout")
            match value.get("type", "file"):
                case "file":
                    self.assertTrue(member.isreg())
                case "symlink":
                    self.assertTrue(member.issym())
                case "hardlink":
                    self.assertTrue(member.islnk())
                case "dir":
                    self.assertTrue(member.isdir())
                    self.assertSameDirArchive(
                        tree[key]["layout"], tar, subdir / key
                    )


class TestDtarBasic(TestDtar):

    def setUp(self):
        super().setUp()
        create_basic_layout(self.src)

    def add_data_in_files(self):
        size = 16 * 10**6
        with open(self.src / "file1", "wb") as fh:
            fh.write(os.urandom(size))
        with open(self.src / "file2", "wb") as fh:
            fh.write(os.urandom(size))
        with open(self.src / "file3", "wb") as fh:
            fh.write(os.urandom(size))
        with open(self.src / "dir1" / "file4", "wb") as fh:
            fh.write(os.urandom(size))

    def test_dtar_create(self):
        self.add_data_in_files()
        proc = self.run_dtar()
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
        self.assertArchiveBasicTree()

    def test_dtar_create_algo_chunk(self):
        self.add_data_in_files()
        proc = self.run_dtar(env={"MFU_FLIST_ARCHIVE_CREATE": "CHUNK"})
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
        self.assertArchiveBasicTree()

    def test_dtar_create_algo_libcircle(self):
        self.add_data_in_files()
        proc = self.run_dtar(env={"MFU_FLIST_ARCHIVE_CREATE": "LIBCIRCLE"})
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
        self.assertArchiveBasicTree()

    def test_dtar_extract(self):
        self.run_dtar()
        proc = self.run_dtar(extract=True)
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Creating 2 directories
                    Creating 4 files
                    Creating 2 symlinks
                    Creating 1 hardlinks
                """
            ),
        )
        self.archive.unlink()
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)

    def test_dtar_extract_algo_libarchive(self):
        self.run_dtar()
        proc = self.run_dtar(
            extract=True, env={"MFU_FLIST_ARCHIVE_EXTRACT": "LIBARCHIVE"}
        )
        # With this algorithm, index file *.tar.dtaridx is counted.
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 10
                """
            ),
        )
        self.archive.unlink()
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)

    def test_dtar_extract_algo_libarchive_idx(self):
        self.run_dtar()
        proc = self.run_dtar(
            extract=True, env={"MFU_FLIST_ARCHIVE_EXTRACT": "LIBARCHIVE_IDX"}
        )
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 9
                """
            ),
        )
        self.archive.unlink()
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)

    def test_dtar_extract_algo_chunk(self):
        self.run_dtar()
        proc = self.run_dtar(
            extract=True, env={"MFU_FLIST_ARCHIVE_EXTRACT": "CHUNK"}
        )
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 9
                """
            ),
        )
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Creating 2 directories
                    Creating 4 files
                    Creating 2 symlinks
                    Creating 1 hardlinks
                """
            ),
        )
        self.archive.unlink()
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)

    def test_dtar_extract_algo_libcircle(self):
        self.run_dtar()
        proc = self.run_dtar(
            extract=True, env={"MFU_FLIST_ARCHIVE_EXTRACT": "LIBCIRCLE"}
        )
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 9
                """
            ),
        )
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Creating 2 directories
                    Creating 4 files
                    Creating 2 symlinks
                    Creating 1 hardlinks
                """
            ),
        )
        self.archive.unlink()
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)

    def test_dtar_preserve_xattrs(self):
        self.add_data_in_files()
        # add xattr
        xattr.setxattr(self.src / "file1", "user.xdg.comment", "test".encode())
        original_xattrs = xattr.listxattr(self.src / "file1")
        original_value = xattr.getxattr(self.src / "file1", "user.xdg.comment")
        self.run_dtar(preserve_xattrs=True)
        # check archive can be read by python standard library
        self.assertArchiveBasicTree()
        self.run_dtar(extract=True, preserve_xattrs=True)
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)
        new_xattrs = xattr.listxattr(self.dst / self.src.name / "file1")
        new_value = xattr.getxattr(
            self.dst / self.src.name / "file1", "user.xdg.comment"
        )
        self.assertCountEqual(original_xattrs, new_xattrs)
        self.assertEqual(original_value, new_value)

    def test_dtar_preserve_acls(self):
        self.add_data_in_files()
        # add posix ACL
        self.run_cmd(["setfacl", "-m", "user:root:r", self.src / "file1"])
        original_xattrs = xattr.listxattr(self.src / "file1")
        original_acl = xattr.getxattr(
            self.src / "file1", "system.posix_acl_access"
        )
        self.run_dtar(preserve_acls=True)
        # check archive can be read by python standard library
        self.assertArchiveBasicTree()
        self.run_dtar(extract=True, preserve_acls=True)
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)
        new_xattrs = xattr.listxattr(self.dst / self.src.name / "file1")
        new_acl = xattr.getxattr(
            self.dst / self.src.name / "file1", "system.posix_acl_access"
        )
        self.assertCountEqual(original_xattrs, new_xattrs)
        self.assertEqual(original_acl, new_acl)

    def test_dtar_preserve_flags(self):
        self.add_data_in_files()
        # add noatime flag
        self.run_cmd(["chattr", "+A", self.src / "file1"])
        self.run_dtar(preserve_flags=True)
        # check archive can be read by python standard library
        self.assertArchiveBasicTree()
        self.run_dtar(extract=True, preserve_flags=True)
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)
        output = self.run_cmd(["lsattr", self.dst / self.src.name / "file1"])
        self.assertIn("A", output.stdout.decode().split(" ")[0])


class TestDtarDfilemaker(TestDtar):

    def setUp(self):
        super().setUp()
        self.run_dfilemaker()

    def test_dtar(self):
        # Create and extract an archive with tree generated by dfilemaker and
        # compare.
        self.run_dtar()
        self.run_dtar(extract=True)
        self.assertSrcDstEqual(ignore_mtime=True, dest=self.dst / self.src.name)
