#!/usr/bin/python3

import unittest
import dataclasses
from pathlib import Path
import tempfile
import os
import stat
import subprocess
import typing as t
import shlex

import yaml


def mpirun_cmd():
    mpirun = os.environ.get("MFU_MPIRUN_CMD", "mpirun")
    args = os.environ.get("MFU_MPIRUN_ARGS")
    if args is None:
        args = []
    else:
        args = args.split(" ")
    return [mpirun] + args


def mfu_cmd(cmd):
    return mpirun_cmd() + [os.path.join(os.environ["MFU_BIN"], cmd)]


def dfilemaker_cmd():
    return mfu_cmd("dfilemaker")


def dwalk_cmd():
    return mfu_cmd("dwalk")


def dfind_cmd():
    return mfu_cmd("dfind")


def dsync_cmd():
    return mfu_cmd("dsync")


def dcp_cmd():
    return mfu_cmd("dcp")


def dcmp_cmd():
    return mfu_cmd("dcmp")


def dtar_cmd():
    return mfu_cmd("dtar")


def create_file(path: Path):
    fh = open(path, "w+")
    fh.close()


# Global variables holding the real dataclasses instanciated in tests for
# comparisons.
RegularFile = None
Symlink = None
Directory = None


@dataclasses.dataclass
class FilesystemObject:
    parent: Path = dataclasses.field(compare=False)
    name: str
    mode: int
    uid: int
    gid: int
    mtime: int

    @property
    def path(self) -> Path:
        return self.parent / self.name


@dataclasses.dataclass
class _RegularFile(FilesystemObject):
    inode: int = dataclasses.field(compare=False)
    nlink: int
    size: int

    def __str__(self):
        return (
            f"File[{self.path} inode:{self.inode} nlink:{self.nlink}, "
            f"size:{self.size}, mode: {stat.filemode(self.mode)}, "
            f"uid:{self.uid}, gid:{self.gid}, mtime:{self.mtime}]"
        )


@dataclasses.dataclass
class _Symlink(FilesystemObject):
    target: str

    def __str__(self):
        return (
            f"Symlink[{self.path}→{self.target} "
            f"mode: {stat.filemode(self.mode)}, "
            f"uid:{self.uid}, gid:{self.gid}, mtime:{self.mtime}]"
        )


@dataclasses.dataclass
class _Directory(FilesystemObject):
    content: dict = dataclasses.field(default_factory=dict)

    def get(self, path: str):
        value = self.content
        components = path.split(os.path.sep)
        for component in components[:-1]:
            value = value.get(component)
        return value.get(components[-1])

    def __str__(self):
        return (
            f"Directory[{self.path} "
            f"mode: {stat.filemode(self.mode)}, "
            f"uid:{self.uid}, gid:{self.gid}, mtime:{self.mtime}]"
        )

    def dump(self, indent=0):
        if not self.content:
            print(f"{' '*indent}∅")
        for name, item in self.content.items():
            print(f"{' '*indent}{name:20}: {item}")
            if isinstance(item, Directory):
                item.dump(indent + 2)

    @classmethod
    def from_path(cls, path: Path):
        fs_o_stat = path.stat()
        dir_o = cls(
            path.parent,
            path.name,
            fs_o_stat.st_mode,
            fs_o_stat.st_uid,
            fs_o_stat.st_gid,
            fs_o_stat.st_mtime,
        )
        for item in Path(path).iterdir():
            fs_o_stat = item.lstat()
            if item.is_symlink():
                fs_o = Symlink(
                    dir_o.path,
                    item.name,
                    fs_o_stat.st_mode,
                    fs_o_stat.st_uid,
                    fs_o_stat.st_gid,
                    fs_o_stat.st_mtime,
                    str(item.readlink()),
                )
            elif item.is_dir():
                fs_o = Directory.from_path(item)
            else:
                fs_o = RegularFile(
                    dir_o.path,
                    item.name,
                    fs_o_stat.st_mode,
                    fs_o_stat.st_uid,
                    fs_o_stat.st_gid,
                    fs_o_stat.st_mtime,
                    fs_o_stat.st_ino,
                    fs_o_stat.st_nlink,
                    fs_o_stat.st_size,
                )
            dir_o.content[item.name] = fs_o
        return dir_o


class FileTree(_Directory):

    def dump(self):
        print(f"\nFile tree {self.path}:")
        super().dump()


def _create_dir_content(path: Path, layout: dict):
    for s_item, attrs in layout.items():
        fs_o_type = attrs.get("type", "file")
        if fs_o_type == "hardlink":
            continue
        item = path / s_item
        match fs_o_type:
            case "file":
                create_file(item)
            case "symlink":
                item.symlink_to(attrs["target"])
            case "dir":
                item.mkdir()
                _create_dir_content(item, attrs["layout"])


def _create_hardlinks(path: Path, layout: dict):
    for s_item, attrs in layout.items():
        fs_o_type = attrs.get("type", "file")
        if fs_o_type not in ["hardlink", "dir"]:
            continue
        item = path / s_item
        match fs_o_type:
            case "hardlink":
                target = path / attrs["target"]
                item.hardlink_to(target)
            case "dir":
                _create_hardlinks(item, attrs["layout"])


def create_filetree_from_yaml(path: str, layout: str):
    layout = yaml.safe_load(layout)
    # 1st pass without hardlinks
    _create_dir_content(path, layout)
    # create hardlinks recursively on 2nd pass
    _create_hardlinks(path, layout)


BASIC_FILES_LAYOUT = """
file1: {}
file2: {}
file3: {}
symlink2:
    type: symlink
    target: file2
hardlink3:
    type: hardlink
    target: file3
dir1:
    type: dir
    layout:
        file4: {}
        symlink1:
            type: symlink
            target: ../file1
"""


def create_basic_layout(dst):
    create_filetree_from_yaml(
        path=dst,
        layout=BASIC_FILES_LAYOUT,
    )


class TestFileTreeCmp(unittest.TestCase):

    def setUp(self):
        self._tmp_src = tempfile.TemporaryDirectory()
        self.src = Path(self._tmp_src.name)
        self._tmp_dst = tempfile.TemporaryDirectory()
        self.dst = Path(self._tmp_dst.name)
        self.archive = self.dst / "archive.tar"  # used for dtar tests

    def tearDown(self):
        self._tmp_src.cleanup()
        self._tmp_dst.cleanup()

    def assertSameFileTree(
        self,
        dir1: Directory,
        dir2: Directory,
        root_dir1: Path,
        root_dir2: Path,
        ignore_paths: None | list[str] = None,
    ):
        if ignore_paths is None:
            ignore_paths = []
        try:
            # discard ignored items in this directory
            if ignore_paths:
                dir1.content = {
                    key: value
                    for key, value in dir1.content.items()
                    if str(value.path.relative_to(root_dir1))
                    not in ignore_paths
                }
                dir2.content = {
                    key: value
                    for key, value in dir2.content.items()
                    if str(value.path.relative_to(root_dir2))
                    not in ignore_paths
                }
            self.assertCountEqual(
                dir1.content.keys(),
                dir2.content.keys(),
                f"Directories {dir1} and {dir2} do not have the same content",
            )
            for key in dir1.content.keys():
                fso_1 = dir1.content[key]
                fso_2 = dir2.content[key]
                self.assertEqual(
                    type(fso_1),
                    type(fso_2),
                    f"Paths {fso_1} and {fso_2} do not have the same type",
                )
                if isinstance(fso_1, Directory):
                    self.assertSameFileTree(
                        fso_1, fso_2, root_dir1, root_dir2, ignore_paths
                    )
                else:
                    self.assertEqual(
                        fso_1,
                        fso_2,
                        f"Paths {fso_1} and {fso_2} are not equal",
                    )
        except AssertionError as err:
            dir1.dump()
            dir2.dump()
            raise AssertionError(err)

    def assertSrcDstEqual(
        self,
        ignore_paths=None,
        ignore_nlink=False,
        ignore_mtime=False,
        dest: t.Optional[Path] = None,
    ):
        if dest is None:
            dest = self.dst
        global RegularFile
        global Symlink
        global Directory
        if ignore_nlink or ignore_mtime:
            # Create new dataclasses in which nlink or mtime are ignored in
            # __eq__  operator.
            RegularFile = dataclasses.make_dataclass(
                "RegularFile",
                [
                    ("nlink", int, dataclasses.field(compare=not ignore_nlink)),
                    ("mtime", int, dataclasses.field(compare=not ignore_mtime)),
                ],
                bases=(_RegularFile,),
            )
            Symlink = dataclasses.make_dataclass(
                "Symlink",
                [
                    ("mtime", int, dataclasses.field(compare=not ignore_mtime)),
                ],
                bases=(_Symlink,),
            )
            Directory = dataclasses.make_dataclass(
                "Directory",
                [
                    ("mtime", int, dataclasses.field(compare=not ignore_mtime)),
                ],
                bases=(_Directory,),
            )

        else:
            RegularFile = _RegularFile
            Symlink = _Symlink
            Directory = _Directory

        ft_src = FileTree.from_path(self.src)
        ft_dst = FileTree.from_path(dest)
        self.assertSameFileTree(ft_src, ft_dst, self.src, dest, ignore_paths)

    def assertInProcStdout(self, proc, msg):
        # Remove timestamp prefix from dsync output
        def untimestamp_line(line):
            if line.startswith("["):
                return line.split(" ", maxsplit=1)[1]
            return line

        unprefixed_output = "\n".join(
            [
                untimestamp_line(line)
                for line in proc.stdout.decode().strip().split("\n")
            ]
        )
        msg = msg.strip()
        if msg not in unprefixed_output:
            raise AssertionError(
                "Unable to find message in output.\n"
                f" - Message:\n{msg}\n"
                f" - Output:\n{unprefixed_output}"
            )

    def run_cmd(
        self,
        cmd: t.List[str | Path | int],
        cwd: t.Optional[Path] = None,
        env: t.Optional[t.Dict[str, str]] = None,
    ) -> subprocess.CompletedProcess:
        _env = os.environ.copy()
        if env:
            _env.update(env)

        def cmd_result(proc) -> str:
            return (
                f" - exit code: {proc.returncode}\n"
                f" - stdout:\n{proc.stdout.decode()}\n"
                f" - stderr:\n{proc.stderr.decode()}\n"
            )

        cmd_s = shlex.join([str(arg) for arg in cmd])
        print(f"\n→ Running command: {cmd_s}")
        try:
            proc = subprocess.run(
                cmd, check=True, capture_output=True, cwd=cwd, env=_env
            )
            print(cmd_result(proc))
        except subprocess.CalledProcessError as err:
            raise AssertionError(
                "Command error:\n" f" - command: {cmd_s}\n" + cmd_result(err)
            ) from err
        else:
            return proc

    def run_dfilemaker(self, dest: t.Optional[Path] = None):
        if not dest:
            dest = self.src
        cmd = dfilemaker_cmd() + [dest]
        return self.run_cmd(cmd)
