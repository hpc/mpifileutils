dcmp
====

SYNOPSIS
--------

**dcmp [OPTION] SRC DEST**

DESCRIPTION
-----------

Parallel MPI application to compare two files or to recursively compare
files with same relative paths within two different directories.

dcmp provides functionality similar to a recursive :manpage:`cmp(1)`. It reports
how many files in two different directories are the same or different.

dcmp can be configured to compare a number of different file properties.


OPTIONS
-------

.. option:: -b, --base

   Do a base comparison.

.. option:: -o, --output EXPR:FILE

   Writes list of files matching expression EXPR to specified FILE.
   The expression consists of a set of fields and states described below.
   More than one -o option is allowed in a single invocation,
   in which case, each option should provide a different output file name.

.. option:: -v, --verbose

   Run in verbose mode. Prints a list of statistics/timing data for the
   command. Files walked, started, completed, seconds, files, bytes
   read, byte rate, and file rate.

.. option:: -h, --help

   Print the command usage, and the list of options available.

EXPRESSIONS
-----------

An expression is made up of one or more conditions, where each condition specifies a field and a state.
A single condition consists of a field name, an '=' sign, and a state name.

Valid fields are listed below, along with the property of the entry that is checked.

+---------+----------------------------------------------------------------------+
| Field   | Property of entry                                                    |
+=========+======================================================================+
| EXIST   | whether entry exists                                                 |
+---------+----------------------------------------------------------------------+
| TYPE    | type of entry, e.g., regular file, directory, symlink                |
+---------+----------------------------------------------------------------------+
| SIZE    | size of entry in bytes, if a regular file                            |
+---------+----------------------------------------------------------------------+
| UID     | user id of entry                                                     |
+---------+----------------------------------------------------------------------+
| GID     | group id of entry                                                    |
+---------+----------------------------------------------------------------------+
| ATIME   | time of last access                                                  |
+---------+----------------------------------------------------------------------+
| MTIME   | time of last modification                                            |
+---------+----------------------------------------------------------------------+
| CTIME   | time of last status change                                           |
+---------+----------------------------------------------------------------------+
| PERM    | permission bits of entry                                             |
+---------+----------------------------------------------------------------------+
| ACL     | ACLs associated with entry, if any                                   |
+---------+----------------------------------------------------------------------+
| CONTENT | file contents of entry, byte-for-byte comparision, if a regular file |
+---------+----------------------------------------------------------------------+

Valid conditions for the EXIST field are:

+----------------+------------------------------------------------------------+
| Condition      | Meaning                                                    |
+================+============================================================+
| EXIST=SRC_ONLY | entry exists only in source path                           |
+----------------+------------------------------------------------------------+
| EXIST=DST_ONLY | entry exists only in destination path                      |
+----------------+------------------------------------------------------------+
| EXIST=DIFFER   | entry exists in either source or destination, but not both |
+----------------+------------------------------------------------------------+
| EXIST=COMMON   | entry exists in both source and destination                |
+----------------+------------------------------------------------------------+

All other fields may only specify the DIFFER and COMMON states.

Conditions can be joined together with AND (@) and OR (,) operators without spaces to build complex expressions.
For example, the following expression reports entries that exist in both source and destination paths, but are of different types:

    EXIST=COMMON@TYPE=DIFFER

When used with the -o option, one must also specify a file name at the end of the expression, separated with a ':'.
The list of any files that match the expression are written to the named file.
For example, to list any entries matching the above expression to a file named outfile1,
one should use the following option:

    -o EXIST=COMMON@TYPE=DIFFER:outfile1

EXAMPLES
--------

1. Compare two files in different directories:

``mpirun -np 128 dcmp /src1/file1 /src2/file2``

2. Compare two directories with verbose output. The verbose output
   prints timing and number of bytes read:

``mpirun -np 128 dcmp -v /src1 /src2``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
