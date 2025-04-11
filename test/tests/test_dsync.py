#!/usr/bin/python3

import tempfile
import textwrap
from pathlib import Path
import os
import typing as t

from .lib import TestFileTreeCmp, dsync_cmd, create_basic_layout, create_file


class TestDsync(TestFileTreeCmp):

    def run_dsync(
        self,
        delete: bool = False,
        contents: bool = False,
        dereference: bool = False,
        dry_run: bool = False,
        link_dest: t.Optional[Path] = None,
        chunk: t.Optional[str] = None,
        buffer: t.Optional[str] = None,
        batch: t.Optional[int] = None,
        dest: t.Optional[Path] = None,
    ):
        if not dest:
            dest = self.dst
        cmd = dsync_cmd() + [self.src, dest]
        if delete:
            cmd.insert(len(cmd) - 2, "--delete")
        if contents:
            cmd.insert(len(cmd) - 2, "--contents")
        if dereference:
            cmd.insert(len(cmd) - 2, "--dereference")
        if dry_run:
            cmd.insert(len(cmd) - 2, "--dryrun")
        if link_dest:
            cmd[-2:0] = ["--link-dest", link_dest]
        if chunk:
            cmd[-2:0] = ["--chunksize", chunk]
        if buffer:
            cmd[-2:0] = ["--bufsize", buffer]
        if batch:
            cmd[-2:0] = ["--batch-files", str(batch)]
        return self.run_cmd(cmd)


class TestDsyncBasic(TestDsync):

    def setUp(self):
        super().setUp()
        create_basic_layout(self.src)

    def test_dsync_empty_dest(self):
        proc = self.run_dsync()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 8
                      Directories: 1
                      Files: 4
                      Links: 2
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual()

    def test_dsync_overwrite_dest(self):
        # Modify a file dest with different content and check it is overwritten.
        with open(self.src / "file2", "w+") as fh:
            fh.write("original")
        self.run_dsync()
        with open(self.dst / "file2", "w+") as fh:
            fh.write("modified")
        proc = self.run_dsync()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 1
                      Directories: 0
                      Files: 1
                      Links: 0
                      Hardlinks: 0
                """
            ),
        )
        self.assertSrcDstEqual()
        with open(self.dst / "file2", "r") as fh:
            self.assertEqual(fh.read(), "original")

    def test_dsync_remove_source(self):
        # Synchronize, remove one file in source, re-synchronize and check file
        # is still present in dest without dsync delete option.
        self.run_dsync()
        (self.src / "file2").unlink()
        proc = self.run_dsync()
        self.assertInProcStdout(
            proc,
            "Comparing file sizes and modification times of 3 items",
        )
        self.assertSrcDstEqual(ignore_paths=["file2"])
        self.assertFalse((self.src / "file2").exists())
        self.assertTrue((self.dst / "file2").exists())

    def test_dsync_remove_source_delete(self):
        # Synchronize, remove one file in source, re-synchronize and check file
        # is also removed in dest with dsync delete option.
        self.run_dsync()
        (self.src / "file1").unlink()
        proc = self.run_dsync(delete=True)
        self.assertInProcStdout(proc, "Removing 1 items")
        self.assertSrcDstEqual()

    def test_dsync_file_in_dst(self):
        # Create non-conflicting file in dst, synchronize and check the file
        # still exists after sync.
        additional_file = self.dst / "other-file"
        create_file(additional_file)
        proc = self.run_dsync()
        # Check dsync reported 2 items in destination
        self.assertInProcStdout(proc, "Walked 2 items in ")
        # Check dsync reported to copy everything from source
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 8
                      Directories: 1
                      Files: 4
                      Links: 2
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual(ignore_paths=[additional_file.name])
        self.assertTrue(additional_file.exists())

    def test_dsync_file_in_dst_delete(self):
        # Create non-conflicting file in dst, synchronize with delete option and
        # check the file is removed after sync.
        additional_file = self.dst / "other-file"
        create_file(additional_file)
        proc = self.run_dsync(delete=True)
        # Check dsync reported to remove an item
        self.assertInProcStdout(
            proc,
            "Removed 1 items in ",
        )
        self.assertSrcDstEqual()
        self.assertFalse(additional_file.exists())

    def test_dsync_symlink_dereference(self):
        # Add content of file2, the target of symlink2
        with open(self.src / "file2", "w") as fh:
            fh.write("original")
        self.run_dsync(dereference=True)
        # Check source and destination have the same content, except for
        # symlinks.
        self.assertSrcDstEqual(ignore_paths=["symlink2", "dir1/symlink1"])
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

    def test_dsync_symlink_target_change(self):
        # Synchronize, change symlink2 target in source, re-synchronize and
        # check.
        self.run_dsync()
        (self.src / "symlink2").unlink()
        (self.src / "symlink2").symlink_to("file1")
        proc = self.run_dsync()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 1
                      Directories: 0
                      Files: 0
                      Links: 1
                      Hardlinks: 0
                """
            ),
        )
        self.assertSrcDstEqual()

    def test_dsync_symlink_dereference_target_nlinks(self):
        # change symlink2 target in source for file3 which has nlink > 1 and
        # synchronize with dereference.
        (self.src / "symlink2").unlink()
        (self.src / "symlink2").symlink_to("file3")
        proc = self.run_dsync(dereference=True)

        # FIXME: when symlinks target file with multiple links (nlink > 1),
        # dsync creates in destination an additional link to this inode instead
        # of a regular copy for this symlink.

        # Check source and destination have the same content, except for
        # symlinks2, hardlink3 and file3 which have 3 nlinks in destination.
        self.assertSrcDstEqual(
            ignore_paths=["file3", "hardlink3", "symlink2", "dir1/symlink1"]
        )
        self.assertEqual((self.dst / "file3").stat().st_nlink, 3)
        self.assertEqual((self.dst / "hardlink3").stat().st_nlink, 3)
        self.assertEqual((self.dst / "symlink2").stat().st_nlink, 3)

        #  Check inode of symlink2 and file3 are the same.
        self.assertEqual(
            (self.dst / "symlink2").stat().st_ino,
            (self.dst / "file3").stat().st_ino,
        )

        # Check dsync reported creation of 2 hardlinks and 0 symlink.
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 8
                      Directories: 1
                      Files: 5
                      Links: 0
                      Hardlinks: 2
                """
            ),
        )

    def test_dsync_transform_hardlink(self):
        # Synchronize, transform hardlink in standalone inode, re-synchronize
        # and check.
        self.run_dsync()
        (self.src / "hardlink3").unlink()
        create_file(self.src / "hardlink3")
        proc = self.run_dsync()
        # Check dsync reported one modified file.
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 1
                      Directories: 0
                      Files: 1
                      Links: 0
                      Hardlinks: 0
                """
            ),
        )
        self.assertSrcDstEqual()

    def test_dsync_sync_same_size_mtime(self):
        # Fill source file2 with 64 random bytes and sync in empty dst.
        random1 = os.urandom(64)
        with open(self.src / "file2", "wb") as fh:
            fh.write(random1)
        previous_atime = (self.src / "file2").stat().st_atime_ns
        previous_mtime = (self.src / "file2").stat().st_mtime_ns
        self.run_dsync()

        # Update source file2 with other 64 random bytes, restore mtime and
        # resync.
        random2 = os.urandom(64)
        with open(self.src / "file2", "wb") as fh:
            fh.write(random2)
        os.utime(self.src / "file2", ns=(previous_atime, previous_mtime))
        proc = self.run_dsync()
        self.assertInProcStdout(
            proc, "Comparing file sizes and modification times of 4 items"
        )
        # Check src/dst metadata are equal but file2 still contains first 64
        # random bytes.
        self.assertSrcDstEqual()
        with open(self.dst / "file2", "rb") as fh:
            self.assertEqual(fh.read(), random1)

    def test_dsync_sync_same_diff_mtime(self):
        # Fill source file2 with 64 random bytes and sync in empty dst.
        random1 = os.urandom(64)
        with open(self.src / "file2", "wb") as fh:
            fh.write(random1)
        self.run_dsync()

        # Update source file2 with other 64 random bytes and resync.
        random2 = os.urandom(64)
        with open(self.src / "file2", "wb") as fh:
            fh.write(random2)
        proc = self.run_dsync()
        # Check dsync reported one modified file.
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 1
                      Directories: 0
                      Files: 1
                      Links: 0
                      Hardlinks: 0
                """
            ),
        )

        # Check src/dst metadata are equal but file2 is updated with new content
        # because of mtime difference.
        self.assertSrcDstEqual()
        with open(self.dst / "file2", "rb") as fh:
            self.assertEqual(fh.read(), random2)

    def test_dsync_sync_same_size_mtime_contents(self):
        # Fill source file2 with 64 random bytes and sync in empty dst.
        random1 = os.urandom(64)
        random2 = os.urandom(64)
        with open(self.src / "file2", "wb") as fh:
            fh.write(random1)
        previous_atime = (self.src / "file2").stat().st_atime_ns
        previous_mtime = (self.src / "file2").stat().st_mtime_ns
        self.run_dsync()

        # Update source file2 with other 64 random bytes, restore mtime and
        # resync with --contents.
        with open(self.src / "file2", "wb") as fh:
            fh.write(random2)
        os.utime(self.src / "file2", ns=(previous_atime, previous_mtime))
        proc = self.run_dsync(contents=True)
        self.assertInProcStdout(
            proc,
            "Comparing file contents of 4 items",
        )
        # FIXME: mtime on file2 do not match on src/dst even though it has been
        # updated with second dsync.
        self.assertSrcDstEqual(ignore_paths=["file2"])
        with open(self.dst / "file2", "rb") as fh:
            self.assertEqual(fh.read(), random2)

    def test_dsync_hardlink_dest_ref_changed(self):
        # Create a conflicting file in dest with different content and check it
        # is overwritten.
        with open(self.src / "file3", "w+") as fh:
            fh.write("original")
        self.run_dsync()
        with open(self.dst / "file3", "w+") as fh:
            fh.write("modified")
        proc = self.run_dsync()
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 2
                      Directories: 0
                      Files: 1
                      Links: 0
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual()
        for filename in ["file3", "hardlink3"]:
            with open(self.dst / filename, "r") as fh:
                self.assertEqual(fh.read(), "original")

    def test_dsync_hardlink_outside_tree(self):
        # Create temporary file outside src and dst tree, create hardlink in src
        # to this temporary file, sync and check. The destination should contain
        # a copy of the file (ie. with st_nlink 1).
        with tempfile.NamedTemporaryFile() as outside_file:
            (self.src / "hardlink3").unlink()
            (self.src / "hardlink3").hardlink_to(outside_file.name)
            proc = self.run_dsync()
            # Check dsync reported the hardlink1 as a regular file.
            self.assertInProcStdout(
                proc,
                textwrap.dedent(
                    """
                        Items: 8
                          Directories: 1
                          Files: 5
                          Links: 2
                          Hardlinks: 0
                    """
                ),
            )
            self.assertSrcDstEqual(ignore_paths=["hardlink3"])
            self.assertEqual((self.src / "hardlink3").stat().st_nlink, 2)
            self.assertEqual((self.dst / "hardlink3").stat().st_nlink, 1)
        # When the temporary file outside src and dst tree is removed, src and
        # dst must be equal.
        self.assertSrcDstEqual()

    def test_dsync_change_hardlink_dest(self):
        # Synchronize, change hardlink destination, re-synchronize
        # and check.
        self.run_dsync()
        (self.src / "hardlink3").unlink()
        (self.src / "hardlink3").hardlink_to(self.src / "file2")
        proc = self.run_dsync()
        # Check dsync reported hardlink to be updated.
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 1
                      Directories: 0
                      Files: 0
                      Links: 0
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual()

    def test_dsync_add_hardlink_same_inode(self):
        # Synchronize, add hardlink on inode which has already multiple links,
        # re-synchronize and check.
        self.run_dsync()
        (self.src / "hardlink2").hardlink_to(self.src / "file3")
        proc = self.run_dsync()
        # Check dsync reported hardlink to be updated.
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 1
                      Directories: 0
                      Files: 0
                      Links: 0
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual()

    def test_dsync_add_hardlink_another_inode(self):
        # Synchronize, add hardlink on inode with one link, re-synchronize and
        # check.
        self.run_dsync()
        (self.src / "hardlink2").hardlink_to(self.src / "file2")
        proc = self.run_dsync()
        # Check dsync reported hardlink to be updated.
        self.assertInProcStdout(
            proc,
            textwrap.dedent(
                """
                    Items: 1
                      Directories: 0
                      Files: 0
                      Links: 0
                      Hardlinks: 1
                """
            ),
        )
        self.assertSrcDstEqual()

    def test_dsync_dry_run_empty(self):
        # Check destination stays empty after dsync --dry-run.
        self.run_dsync(dry_run=True)
        self.assertCountEqual(list(self.dst.iterdir()), [])

    def test_dsync_dry_run_no_update(self):
        # Check destination is not updated after second dsync with --dry-run
        self.run_dsync()
        (self.src / "file2").unlink()
        create_file(self.src / "newfile")
        self.run_dsync(dry_run=True)
        self.assertTrue((self.dst / "file2").exists())
        self.assertFalse((self.dst / "newfile").exists())
        self.assertSrcDstEqual(ignore_paths=["file2", "newfile"])

    def test_dsync_link_dest(self):
        with tempfile.TemporaryDirectory() as _link_dest:
            link_dest = Path(_link_dest)
            # First sync in link destination
            self.run_dsync(dest=link_dest)
            # Add file in source
            create_file(self.src / "newfile")
            # Run dsync in destination with link destination
            self.run_dsync(link_dest=link_dest)
            # Check source and destination are the same, except for nlinks
            self.assertSrcDstEqual(ignore_nlink=True)
            # Check new file is present in destination but not in link
            # destination.
            self.assertTrue((self.dst / "newfile").exists())
            self.assertFalse((link_dest / "newfile").exists())
            # Check file which did not change between two synchronizations share
            # the same inode in destination and link destination.
            self.assertEqual(
                (self.dst / "file1").stat().st_ino,
                (link_dest / "file1").stat().st_ino,
            )

    def test_dsync_chunksize(self):
        # Add 16MB of data in a file to have multiple chunks
        with open(self.src / "file1", "wb") as fh:
            fh.write(os.urandom(16 * 10**6))
        self.run_dsync(chunk="1MB")
        self.assertSrcDstEqual()

    def test_dsync_bufsize(self):
        # Add 16MB of data in a file to fill multiple buffer
        with open(self.src / "file1", "wb") as fh:
            fh.write(os.urandom(16 * 10**6))
        self.run_dsync(buffer="1MB")
        self.assertSrcDstEqual()

    def test_dsync_batch(self):
        self.run_dsync(batch=2)
        self.assertSrcDstEqual()


class TestDsyncDfilemaker(TestDsync):

    def setUp(self):
        super().setUp()
        self.run_dfilemaker()

    def test_dsync_twice(self):
        self.run_dsync()
        self.assertSrcDstEqual()
        proc = self.run_dsync()
        self.assertInProcStdout(
            proc, "Comparing file sizes and modification times of 1000 items"
        )
        self.assertSrcDstEqual()
