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

.. option:: -o, --output EXPR:FILE

   Writes list of files matching expression EXPR to specified FILE.
   The expression consists of a set of fields and states described below.
   More than one -o option is allowed in a single invocation,
   in which case, each option should provide a different output file name.

.. option:: -t, --text

   Change --output to write files in text format rather than binary.

.. option:: -b, --base

   Enable base checks and normal stdout results when --output is used.

.. option:: --progress N

   Print progress message to stdout approximately every N seconds.
   The number of seconds must be a non-negative integer.
   A value of 0 disables progress messages.

.. option:: -v, --verbose

   Run in verbose mode. Prints a list of statistics/timing data for the
   command. Files walked, started, completed, seconds, files, bytes
   read, byte rate, and file rate.

.. option:: -q, --quiet

   Run tool silently. No output is printed.

.. option:: -l, --lite

  lite mode does a comparison of file modification time and size. If
  modification time and size are the same, then the contents are assumed
  to be the same. Similarly, if the modification time or size is different,
  then the contents are assumed to be different. The lite mode does no comparison
  of data/content in the file.

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
For example, the following expression reports entries that exist in both source and destination paths, but are of different types::

    EXIST=COMMON@TYPE=DIFFER

The AND operator binds with higher precedence than the OR operator.
For example, the following expression matches on entries which either (exist in both source and destination and whose types differ) or (only exist in the source)::

    EXIST=COMMON@TYPE=DIFFER,EXIST=SRC_ONLY

Some conditions imply others.
For example, for CONTENT to be considered the same,
the entry must exist in both source and destination, the types must match, the sizes must match, and finally the contents must match::

    SIZE=COMMON    => EXISTS=COMMON@TYPE=COMMON@SIZE=COMMON
    CONTENT=COMMON => EXISTS=COMMON@TYPE=COMMON@SIZE=COMMON@CONTENT=COMMON

A successful check on any other field also implies that EXIST=COMMON.

When used with the -o option, one must also specify a file name at the end of the expression, separated with a ':'.
The list of any entries that match the expression are written to the named file.
For example, to list any entries matching the above expression to a file named outfile1,
one should use the following option::

    -o EXIST=COMMON@TYPE=DIFFER:outfile1

If the --base option is given or when no output option is specified,
the following expressions are checked and numeric results are reported to stdout::

    EXIST=COMMON
    EXIST=DIFFER
    EXIST=COMMON@TYPE=COMMON
    EXIST=COMMON@TYPE=DIFFER
    EXIST=COMMON@CONTENT=COMMON
    EXIST=COMMON@CONTENT=DIFFER

EXAMPLES
--------

1. Compare two files in different directories:

``mpirun -np 128 dcmp /src1/file1 /src2/file2``

2. Compare two directories with verbose output. The verbose output prints timing and number of bytes read:

``mpirun -np 128 dcmp -v /src1 /src2``

3. Write list of entries to outfile1 that are only in src1 or whose names exist in both src1 and src2 but whose types differ:

``mpirun -np 128 dcmp -o EXIST=COMMON@TYPE=DIFFER,EXIST=SRC_ONLY:outfile1 /src1 /src2``

4. Same as above but also write list of entries to outfile2 that exist in either src1 or src2 but not both:

``mpirun -np 128 dcmp -o EXIST=COMMON@TYPE=DIFFER,EXIST=SRC_ONLY:outfile1 -o EXIST=DIFFER:outfile2 /src1 /src2``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
