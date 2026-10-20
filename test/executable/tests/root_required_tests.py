#!/usr/bin/env python3

import os
import pwd
import grp
import time
import random

from tests import common, config


TEST_XATTRS = {
    "user.xattr1": b"xattr1:1234567890",
    "user.456": b"456:1234567890",
    "user.testing": b"testing:1234567890",
    "user.libattr_skip": b"libattr_skip:1234567890",
}


class RootTests(common.TestCase):
    """Tests that require root, and should be ran seperately from other tests"""

    def setUp(self):
        # set up unique directory information for this test
        base = os.path.join(config.test_dir, self.__class__.__name__)
        self.src_dirpath = os.path.join(base, f"src_{self._testMethodName}")
        self.dest_dirpath = os.path.join(base, f"dest_{self._testMethodName}")
        self.src_tempfilepath = os.path.join(self.src_dirpath, "tempfile")
        self.dest_tempfilepath = os.path.join(self.dest_dirpath, "tempfile")

    def libattr_xattr_test_template(self, cp_cmd):
        # Skip test if /etc/xattr.conf cannot be writted to
        config_path = os.path.join(f"{os.sep}etc", "xattr.conf")
        if not os.access(config_path, os.W_OK):
            self.skipTest(f"Writing to {config_path} requires root.")

        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath, self.dest_dirpath)
        common.create_empty_files(self.src_tempfilepath)
        if any(
            not common.user_xattrs_allowed(path)
            for path in (self.src_dirpath, self.dest_dirpath)
        ):
            self.skipTest("Source or destination does not permit user xattrs.")

        # set xattrs
        for name, value in TEST_XATTRS.items():
            os.setxattr(self.src_tempfilepath, name, value)

        # exclude attribute in /etc/xattr.conf
        added_rule = "user.libattr_skip\tskip\n"
        with open(config_path, "a") as config_file:
            config_file.write(added_rule)

        # run cp_cmd, and reset /etc/xattr.conf afterwards
        try:
            self.run_distributed_successfully(
                cp_cmd,
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

    def test_libattr_xattr_dcp(self):
        """Copy xattrs not excluded by /etc/xattr.conf"""

        self.libattr_xattr_test_template("dcp")

    def test_libattr_xattr_dsync(self):
        """Copy xattrs not excluded by /etc/xattr.conf"""

        self.libattr_xattr_test_template("dsync")

    def test_preserve_dcp(self):
        """Copy file while preserving permissions, groups, and timestamps"""

        # set up directories/files to test on
        common.create_empty_directories(self.src_dirpath, self.dest_dirpath)
        common.create_empty_files(self.src_tempfilepath)

        # set permissions to a random valid octal mode
        octal = str(random.randrange(0, 8)) + str(random.randrange(0, 8)) + str(random.randrange(0, 8))
        os.chmod(self.src_tempfilepath, int(octal, 8))

        # set ownership to nobody/nobody
        uid = pwd.getpwnam('nobody').pw_uid
        gid = grp.getgrnam('nobody').gr_gid
        os.chown(self.src_tempfilepath, uid, gid)

        # wait 1 second for timestamps to change if preserve is not working
        time.sleep(1)

        # run dcp, with preserve option
        self.run_distributed_successfully(
            "dcp", "--preserve", self.src_tempfilepath, self.dest_dirpath
        )

        # CHECK: permissions, group, and timestamps are the same
        src_stat = os.stat(self.src_tempfilepath)
        dest_stat = os.stat(self.dest_tempfilepath)
        self.assertEqual(
            f"{dest_stat.st_mode & 0o777:o}",
            octal,
            msg="Permissions not preserved in copied file.",
        )
        self.assertEqual(
            dest_stat.st_uid,
            src_stat.st_uid,
            msg="User ID not preserved in copied file.",
        )
        self.assertEqual(
            dest_stat.st_gid,
            src_stat.st_gid,
            msg="Group ID not preserved in copied file.",
        )
        self.assertEqual(
            dest_stat.st_atime_ns,
            src_stat.st_atime_ns,
            msg="Access timestamp not preserved in copied file.",
        )
        self.assertEqual(
            dest_stat.st_mtime_ns,
            src_stat.st_mtime_ns,
            msg="Modification timestamp not preserved in copied file.",
        )


if __name__ == "__main__":
    unittest.main()
