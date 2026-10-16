#!/usr/bin/env python3

import os

from tests import common, config


TEST_XATTRS = {
    "user.xattr1": b"xattr1:1234567890",
    "user.456": b"456:1234567890",
    "user.testing": b"testing:1234567890",
    "user.libattr_skip": b"libattr_skip:1234567890",
}


class CommonTests(common.TestCase):
    # Child setUp() requires self.cp_cmd to be defined

    def setUp(self):
        # set up unique directory information for this test
        base = os.path.join(config.test_dir, self.__class__.__name__)
        self.src_dirpath = os.path.join(base, f"src_{self._testMethodName}")
        self.dest_dirpath = os.path.join(base, f"dest_{self._testMethodName}")
        self.src_tempfilepath = os.path.join(self.src_dirpath, "tempfile")
        self.src_tempfilepath_symlink = os.path.join(
            self.src_dirpath, "tempfile_symlink"
        )
        self.dest_tempfilepath = os.path.join(self.dest_dirpath, "tempfile")
        self.dest_tempfilepath_symlink = os.path.join(
            self.dest_dirpath, "tempfile_symlink"
        )
        self.src_tree_tempfilepath = os.path.join(
            self.src_dirpath, "dir1", "dir2", "tempfile"
        )
        self.dest_tree_tempfilepath = os.path.join(
            self.dest_dirpath, "dir1", "dir2", "tempfile"
        )

    def sparse_copy_test_setup(self):
        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath, self.dest_dirpath)

        # Skip test on Lustre filesystem
        if any(
            common.filesystem_type(path) == "lustre"
            for path in (self.src_dirpath, self.dest_dirpath)
        ):
            self.skipTest("Source or destination is on Lustre.")

    def symlink_test_setup(self):
        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath, self.dest_dirpath)
        common.create_random_files(self.src_tempfilepath)

        # create a symlink to that file
        os.symlink(self.src_tempfilepath, self.src_tempfilepath_symlink)

    def xattr_test_setup(self):
        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath, self.dest_dirpath)
        common.create_empty_files(self.src_tempfilepath)
        if any(
            not user_xattrs_allowed(path)
            for path in (self.src_dirpath, self.dest_dirpath)
        ):
            self.skipTest("Source or destination does not permit user xattrs.")

        # set xattrs
        for name, value in TEST_XATTRS.items():
            os.setxattr(self.src_tempfilepath, name, value)

    def test_copy_0(self):
        """Copy file with no holes"""
        
        # set up directories/files to test on and check for skip
        self.sparse_copy_test_setup()

        # create file with no holes
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=4M",
            "count=1",
        )
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=4M",
            "seek=1",
            "count=1",
        )

        # run self.cp_cmd with sparse option
        self.run_distributed_successfully(
            self.cp_cmd,
            "--sparse",
            self.src_tempfilepath,
            self.dest_tempfilepath,
        )

        # CHECK: total written bytes is correct
        self.assertEqual(
            get_total_written_bytes(self.dest_tempfilepath),
            8388608,
            msg="Incorrect total written bytes in copied file.",
        )

    def test_copy_1(self):
        """Copy file with a front 4K hole"""

        # set up directories/files to test on and check for skip
        self.sparse_copy_test_setup()

        # create file with a front 4K hole
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=4K",
            "seek=1",
            "count=1",
        )
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=4k",
            "seek=2",
            "count=1",
        )

        # run self.cp_cmd with sparse option
        self.run_distributed_successfully(
            self.cp_cmd,
            "--sparse",
            self.src_tempfilepath,
            self.dest_tempfilepath,
        )

        # CHECK: total written bytes is correct
        self.assertEqual(
            get_total_written_bytes(self.dest_tempfilepath),
            8192,
            msg="Incorrect total written bytes in copied file.",
        )

    def test_copy_2(self):
        """Copy file with a middle 1G hole"""

        # set up directories/files to test on and check for skip
        self.sparse_copy_test_setup()

        # create file with a middle 1G hole
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=1M",
            "count=1",
        )
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=1M",
            "seek=1025",
            "count=1",
        )

        # run self.cp_cmd with sparse option
        self.run_distributed_successfully(
            self.cp_cmd,
            "--sparse",
            self.src_tempfilepath,
            self.dest_tempfilepath,
        )

        # CHECK: total written bytes is correct
        self.assertEqual(
            get_total_written_bytes(self.dest_tempfilepath),
            2097152,
            msg="Incorrect total written bytes in copied file.",
        )

    def test_copy_3(self):
        """Copy file with an end 1G hole"""

        # set up directories/files to test on and check for skip
        self.sparse_copy_test_setup()

        # create file with an end 1G hole
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=1M",
            "count=1",
        )
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=1M",
            "seek=1025",
            "count=0",
        )

        # run self.cp_cmd with sparse option
        self.run_distributed_successfully(
            self.cp_cmd,
            "--sparse",
            self.src_tempfilepath,
            self.dest_tempfilepath,
        )

        # CHECK: total written bytes is correct
        self.assertEqual(
            get_total_written_bytes(self.dest_tempfilepath),
            1048576,
            msg="Incorrect total written bytes in copied file.",
        )

    def test_copy_4(self):
        """Copy file with a front 4M, middle 1G, end 1G hole"""

        # set up directories/files to test on and check for skip
        self.sparse_copy_test_setup()

        # create file with a front 4M, middle 1G, end 1G hole
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=4M",
            "seek=1",
            "count=1",
        )
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=4M",
            "seek=258",
            "count=1",
        )
        self.run_local_successfully(
            "dd",
            "if=/dev/urandom",
            f"of={self.src_tempfilepath}",
            "bs=4M",
            "seek=515",
            "count=0",
        )

        # run self.cp_cmd with sparse option
        self.run_distributed_successfully(
            self.cp_cmd,
            "--sparse",
            self.src_tempfilepath,
            self.dest_tempfilepath,
        )

        # CHECK: total written bytes is correct
        self.assertEqual(
            get_total_written_bytes(self.dest_tempfilepath),
            8388608,
            msg="Incorrect total written bytes in copied file.",
        )

    def test_symlink_copy(self):
        """Symlink copy"""

        self.symlink_test_setup()

        # run self.cp_cmd
        self.run_distributed_successfully(
            self.cp_cmd,
            self.src_tempfilepath_symlink,
            self.dest_tempfilepath_symlink,
        )

        # CHECK: symlink was preserved in dest
        self.assertTrue(
            os.path.islink(self.dest_tempfilepath_symlink),
            msg="Copied file is not a symlink.",
        )

    def test_symlink_dereference_copy(self):
        """Symlink copy with dereference"""

        # set up directories/files to test on
        self.symlink_test_setup()

        # run self.cp_cmd with dereference option
        self.run_distributed_successfully(
            self.cp_cmd,
            "--dereference",
            self.src_tempfilepath_symlink,
            self.dest_tempfilepath_symlink,
        )

        # CHECK: symlink was dereferenced in dest
        self.assertFalse(
            os.path.islink(self.dest_tempfilepath_symlink),
            msg="Copied file is a symlink.",
        )

    def test_symlink_no_dereference_copy(self):
        """Symlink copy with no dereference, does not follow symlink"""

        # set up directories/files to test on
        self.symlink_test_setup()

        # delete the file
        os.remove(self.src_tempfilepath)

        # run self.cp_cmd with no dereference option
        self.run_distributed_successfully(
            self.cp_cmd,
            "--no-dereference",
            self.src_tempfilepath_symlink,
            self.dest_tempfilepath_symlink,
        )

        # CHECK: symlink was preserved in dest
        self.assertTrue(
            os.path.islink(self.dest_tempfilepath_symlink),
            msg="Copied file is not a symlink.",
        )

        # run self.cp_cmd, should fail
        self.run_distributed_with_failure(
            self.cp_cmd,
            self.src_tempfilepath_symlink,
            self.dest_tempfilepath_symlink,
        )

    def test_no_xattr_copy(self):
        """Copy no xattrs"""

        # set up directories/files to test on and check for skip
        self.xattr_test_setup()

        # run self.cp_cmd
        self.run_distributed_successfully(
            self.cp_cmd,
            "--xattrs", "none",
            self.src_tempfilepath,
            self.dest_tempfilepath,
        )

        # CHECK: no xattrs were copied
        copied_xattrs = set(os.listxattr(self.dest_tempfilepath))
        self.assertTrue(
            TEST_XATTRS.keys().isdisjoint(copied_xattrs),
            msg="xattrs were copied unexpectedly.",
        )

    def test_all_xattr_copy(self):
        """Copy all xattrs"""

        # set up directories/files to test on and check for skip
        self.xattr_test_setup()

        # run self.cp_cmd
        self.run_distributed_successfully(
            self.cp_cmd,
            "--xattrs", "all",
            self.src_tempfilepath,
            self.dest_tempfilepath,
        )

        # CHECK: all xattrs were copied
        for name, value in TEST_XATTRS.items():
            self.assertEqual(
                os.getxattr(self.dest_tempfilepath, name),
                value,
                msg=f"Value for {name} was not preserved.",
            )

    def test_libattr_xattr_copy(self):
        """Copy xattrs not excluded by /etc/xattr.conf"""

        # set up directories/files to test on and check for skip
        self.xattr_test_setup()

        # Skip test if /etc/xattr.conf cannot be writted to
        config_path = os.path.join(f"{os.sep}etc", "xattr.conf")
        if not os.access(config_path, os.W_OK):
            self.skipTest(f"Writing to {config_path} requires root.")

        # exclude attribute in /etc/xattr.conf
        added_rule = "user.libattr_skip\tskip\n"
        with open(config_path, "a") as config_file:
            config_file.write(added_rule)

        # run self.cp_cmd, and reset /etc/xattr.conf afterwards
        try:
            self.run_distributed_successfully(
                self.cp_cmd,
                "--xattrs", "libattr",
                self.src_tempfilepath,
                self.dest_tempfilepath,
            )
        finally:
            with open(config_path) as config_file:
                contents = config_file.read()
            contents = contents.replace(f"{added_rule}\n", "")
            with open(config_path, "w") as config_file:
                config_file.write(contents)

        # CHECK: all xattrs were copied except for the one excluded
        for name, value in TEST_XATTRS.items():
            if name != "user.libattr_skip":
                self.assertEqual(
                    os.getxattr(self.dest_tempfilepath, name),
                    value,
                    msg=f"Value for {name} was not preserved.",
                )
        self.assertNotIn(
            "user.libattr_skip",
            os.listxattr(self.dest_tempfilepath),
            msg="An xattr marked as skip by libattr was copied.",
        )


def user_xattrs_allowed(path):
    probe_name = "user.mpifileutils_test_probe"

    try:
        os.setxattr(path, probe_name, b"probe")
        os.removexattr(path, probe_name)
    except OSError as error:
        return False

    return True


def get_total_written_bytes(filepath):
    file_size = os.path.getsize(filepath)
    written_bytes = 0
    offset = 0

    with open(filepath, "rb") as file:
        fd = file.fileno()

        while offset < file_size:
            try:
                data_offset = os.lseek(fd, offset, os.SEEK_DATA)
            except OSError:
                break

            try:
                hole_offset = os.lseek(fd, data_offset, os.SEEK_HOLE)
            except OSError:
                hole_offset = file_size

            written_bytes += hole_offset - data_offset
            offset = hole_offset

    return written_bytes
