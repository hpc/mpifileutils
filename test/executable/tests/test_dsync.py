#!/usr/bin/env python3

import os
import unittest

from tests import common, config, dcp_dsync_common_tests


class TestDSYNC(dcp_dsync_common_tests.CommonTests):
    """Tests for dsync"""

    def setUp(self):
        # set up unique directory information for this test
        super().setUp()

        # for common tests
        self.cp_cmd = "dsync"

    def test_data_copy(self):
        """Copy a directory tree without changing file data"""

        # set up directories/files to test on
        common.write_bytes(
            self.src_tree_tempfilepath, b"\x00dsync test data\xff" * 1024
        )

        # run dsync
        self.run_distributed_successfully(
            "dsync", self.src_dirpath, self.dest_dirpath
        )

        # CHECK: payload remains the same
        self.assertEqual(
            common.read_bytes(self.src_tree_tempfilepath),
            common.read_bytes(self.dest_tree_tempfilepath),
            msg="File data is different.",
        )

    def test_contents_option(self):
        """Use --contents to detect data changes hidden by matching metadata"""

        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath, self.dest_dirpath)
        common.write_bytes(self.src_tempfilepath, b"source data")
        common.write_bytes(self.dest_tempfilepath, b"target data")

        # ensure src and dest size and mtime are the same
        for path in [self.src_tempfilepath, self.dest_tempfilepath]:
            os.utime(path, ns=(1, 1))

        # CHECK: dysnc with no --contents option should not update dest date
        self.run_distributed_successfully(
            "dsync", self.src_dirpath, self.dest_dirpath
        )
        self.assertEqual(
            common.read_bytes(self.dest_tempfilepath),
            b"target data",
            msg="File data changed unexpectly.",
        )

        # CHECK: dsync with --contents option should update dest data
        self.run_distributed_successfully(
            "dsync", "--contents", self.src_dirpath, self.dest_dirpath
        )
        self.assertEqual(
            common.read_bytes(self.dest_tempfilepath),
            common.read_bytes(self.src_tempfilepath),
            msg="File data did not change",
        )

    def test_empty_source_preserves_destination(self):
        """Keep destination-only entries when --delete is not specified"""

        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath)
        common.write_bytes(self.dest_tree_tempfilepath, b"destination only")
        entries_before = common.get_tree_entries(self.dest_dirpath)

        # run dsync
        self.run_distributed_successfully(
            "dsync", self.src_dirpath, self.dest_dirpath
        )

        # CHECK: destination-only entries are unchanged
        self.assertEqual(
            common.get_tree_entries(self.dest_dirpath),
            entries_before,
            msg="Destination entries changed.",
        )
        self.assertEqual(
            common.read_bytes(self.dest_tree_tempfilepath),
            b"destination only",
            msg="Destination data changed.",
        )

    def test_missing_destination_is_created(self):
        """Create a missing destination and copy the source tree."""

        # set up directories/files to test on
        common.write_bytes(self.src_tree_tempfilepath, b"new destination")

        # run dsync
        self.run_distributed_successfully(
            "dsync", self.src_dirpath, self.dest_dirpath
        )

        # CHECK: dest dir gets created, and matches src dir
        self.assertEqual(
            common.get_tree_entries(self.dest_dirpath),
            common.get_tree_entries(self.src_dirpath),
            msg="Destination tree differs.",
        )
        self.assertEqual(
            common.read_bytes(self.dest_tree_tempfilepath),
            b"new destination",
            msg="Destination data differs.",
        )

    def test_delete_removes_destination_extras(self):
        """Remove destination-only files and directories with --delete"""

        # set up directories/files to test on
        src_file = os.path.join(self.src_dirpath, "keep", "file")
        dest_file = os.path.join(self.dest_dirpath, "keep", "file")
        extra_file = os.path.join(self.dest_dirpath, "extra", "file")
        common.write_bytes(src_file, b"source version")
        common.write_bytes(dest_file, b"old version")
        common.write_bytes(extra_file, b"remove me")

        # run dsync with delete option
        self.run_distributed_successfully(
            "dsync", "--delete", self.src_dirpath, self.dest_dirpath
        )

        # CHECK: dest file is updated and extra file is deleted
        self.assertEqual(
            common.get_tree_entries(self.dest_dirpath),
            common.get_tree_entries(self.src_dirpath),
            msg="Destination tree differs.",
        )
        self.assertEqual(
            common.read_bytes(dest_file),
            b"source version",
            msg="Destination data differs.",
        )
        self.assertFalse(
            os.path.exists(extra_file), msg="Extra file was not deleted."
        )

    def test_missing_source_fails(self):
        """Fail when the source path does not exist"""

        # set up directories/files to test on
        common.create_empty_directories(self.dest_dirpath)

        # CHECK: dsync fails when src does not exist
        self.run_distributed_with_failure(
            "dsync", self.src_dirpath, self.dest_dirpath
        )

    def test_missing_destination_parent_fails(self):
        """Fail when the destination parent does not exist"""

        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath)

        # CHECK: dsync fails when dest parent directory does not exist
        self.run_distributed_with_failure(
            "dsync",
            self.src_dirpath,
            os.path.join(self.dest_dirpath, "child"),
        )

    def test_metadata_sync(self):
        """Synchronize ownership, permissions, and mtimes."""

        # set up directories/files to test on
        common.write_bytes(self.src_tree_tempfilepath, b"new data")
        os.chmod(self.src_tree_tempfilepath, 0o710)
        os.utime(
            self.src_tree_tempfilepath,
            ns=(1_000_000_000, 1_000_000_000),
        )

        common.write_bytes(self.dest_tree_tempfilepath, b"old data")
        os.chmod(self.dest_tree_tempfilepath, 0o700)
        os.utime(
            self.dest_tree_tempfilepath,
            ns=(6_000_000_000, 6_000_000_000),
        )

        # run dsync
        self.run_distributed_successfully(
            "dsync", self.src_dirpath, self.dest_dirpath
        )

        # CHECK: source and destination metadata and content are identical.
        src_stat = os.stat(self.src_tree_tempfilepath)
        dest_stat = os.stat(self.dest_tree_tempfilepath)
        self.assertEqual(
            dest_stat.st_mode & 0o777,
            src_stat.st_mode & 0o777,
            msg="Permissions differ.",
        )
        self.assertEqual(
            dest_stat.st_uid, src_stat.st_uid, msg="User IDs differ."
        )
        self.assertEqual(
            dest_stat.st_gid, src_stat.st_gid, msg="Group IDs differ."
        )
        self.assertEqual(
            dest_stat.st_mtime_ns,
            src_stat.st_mtime_ns,
            msg="Modification times differ.",
        )
        self.assertEqual(
            common.read_bytes(self.src_tree_tempfilepath),
            common.read_bytes(self.dest_tree_tempfilepath),
            msg="File data differs.",
        )

    def test_repeated_sync(self):
        """Do not copy or create anything during a second identical sync"""

        # set up directories/files to test on
        common.create_empty_files(self.src_tempfilepath)

        # run initial rsync
        self.run_local_successfully(
            "rsync", "-aHAX", f"{self.src_dirpath}/", self.dest_dirpath
        )
        before = os.stat(self.dest_tempfilepath)

        # run identical dsync
        dsync_result = self.run_distributed_successfully(
            "dsync", self.src_dirpath, self.dest_dirpath
        )
        after = os.stat(self.dest_tempfilepath)

        # CHECK: directories are synced from rsync, but dsync did nothing
        self.assertEqual(after.st_ino, before.st_ino, msg="File was replaced.")
        self.assertEqual(
            common.get_tree_entries(self.src_dirpath),
            common.get_tree_entries(self.dest_dirpath),
            msg="Directory trees differ.",
        )
        self.assertNotIn(
            "Copying items to destination",
            dsync_result.stdout,
            msg="Items were copied.",
        )
        self.assertNotRegex(
            dsync_result.stdout,
            r"Creating [0-9]+ (files|directories)",
            msg="Entries were created.",
        )

    def test_source_walk_failure_disables_delete(self):
        """Disable delete if a source walk is incomplete"""
        
        # Skip test if root
        if os.geteuid() == 0:
            self.skipTest("Root can traverse directories with mode 000.")

        # set up directories/files to test on
        blocked_src = os.path.join(self.src_dirpath, "blocked")
        blocked_dest = os.path.join(self.dest_dirpath, "blocked")
        visible_src = os.path.join(self.src_dirpath, "visible")
        visible_dest = os.path.join(self.dest_dirpath, "visible")
        dest_extra = os.path.join(self.dest_dirpath, "destination_only")
        common.write_bytes(
            os.path.join(blocked_src, "source_only"), b"not walkable"
        )
        common.write_bytes(visible_src, b"walkable")
        common.write_bytes(dest_extra, b"must not be deleted")

        # remove access to file, so walk fails
        os.chmod(blocked_src, 0)

        # run dsync, with --delete option
        self.run_distributed_successfully(
            "dsync", "--delete", self.src_dirpath, self.dest_dirpath
        )

        # reset permissions
        os.chmod(blocked_src, 0o700)
        os.chmod(blocked_dest, 0o700)

        # CHECK: copy succeeded and delete is disabled
        self.assertTrue(
            os.path.exists(blocked_dest), msg="Blocked directory is missing."
        )
        self.assertEqual(
            common.read_bytes(visible_dest),
            b"walkable",
            msg="Visible file was not copied.",
        )
        self.assertEqual(
            common.read_bytes(dest_extra),
            b"must not be deleted",
            msg="Extra file was deleted.",
        )


if __name__ == "__main__":
    unittest.main()
