#!/usr/bin/env python3

import subprocess
import unittest
import random
import os
from tests import config

class TestCase(unittest.TestCase):
    def run_local_successfully(self, *command):
        result = run_local(*command)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        return result

    def run_distributed_successfully(self, *command):
        result = run_distributed(*command)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        return result

    def run_local_with_failure(self, *command):
        result = run_local(*command)
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        return result

    def run_distributed_with_failure(self, *command):
        result = run_distributed(*command)
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        return result

def run_local(*command):
    command = list(map(str, command))
    # print(f"Running: {' '.join(command)}")

    result = subprocess.run(
        command, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        universal_newlines=True
    )
    
    # print("--- COMMAND STDOUT START ---")
    # print(result.stdout)
    # print("---- COMMAND STDOUT END ----")

    return result

def run_distributed(*command):
    return run_local(*config.run_cmd, *command)

def create_empty_directories(*directories):
    for dirpath in directories:
        os.makedirs(dirpath, exist_ok=True)
    
def create_empty_files(*files):
    for filepath in files:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        open(filepath, 'w').close()

def create_random_files(*files):
    for filepath in files:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        blocksize_mb = f"{random.randint(1, 4)}M"
        run_local("dd", "if=/dev/urandom", f"of={filepath}", f"bs={blocksize_mb}", "count=1")

def mount_basename(path):
    path = os.path.abspath(path)

    while not os.path.ismount(path):
        parent = os.path.dirname(path)
        if parent == path:
            return None
        path = parent

    return os.path.basename(os.path.normpath(path))

def get_file_contents(filepath):
    with open(filepath, "r") as file:
        output = file.read()
    return output

def get_tree_permissions(root_dir):
    permissions_map = dict()
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for dirname in dirnames:
            fullpath = os.path.join(dirpath, dirname)
            mode = os.stat(fullpath).st_mode & 0o777
            permissions_map[fullpath] = f"{mode:o}"

        for filename in filenames:
            fullpath = os.path.join(dirpath, filename)
            mode = os.stat(fullpath).st_mode & 0o777
            permissions_map[fullpath] = f"{mode:o}"

    return permissions_map

def write_bytes(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "wb") as file:
        file.write(data)

def read_bytes(filepath):
    with open(filepath, "rb") as file:
        return file.read()

def get_tree_entries(root):
    entries = set()
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            entries.add(os.path.relpath(os.path.join(dirpath, name), root))
    return entries

def filesystem_type(path):
    result = run_local("df", "-T", path)
    lines = result.stdout.strip().splitlines()

    if len(lines) < 2:
        raise RuntimeError(f"Unexpected df output: {result.stdout!r}")

    return lines[1].split()[1]
