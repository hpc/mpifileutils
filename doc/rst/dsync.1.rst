dsync
=====

SYNOPSIS
--------

**dsync [OPTION] SRC DEST**

DESCRIPTION
-----------

Parallel MPI application to synchronize two files or two directory trees.

dsync makes DEST match SRC, adding missing entries from DEST, and updating
existing entries in DEST as necessary so that SRC and DEST have identical
content, ownership, timestamps, and permissions.

OPTIONS
-------

.. option:: --dryrun

   Show differences without changing anything.

.. option:: -b, --batch-files N

   Batch files into groups of up to size N during copy operation.

.. option:: -c, --contents

   Compare files byte-by-byte rather than checking size and mtime
   to determine whether file contents are different.

.. option:: -D, --delete

   Delete extraneous files from destination.

.. option:: -l, --link-dest=LINK_DIR

   Create hardlink to files in LINK_DIR when unchanged, rather than
   creating file and copying data. Means, users can use this option
   to achieve the goal of an incremental backup, and save storage space.
   For example:
   dsync /src/ /init_backup/
   dsync -l /init_backup/ /src/ /incremental_backup_19_4_20/
   dsync -l /init_backup/ /src/ /incremental_backup_19_4_29/

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

.. option:: -h, --help

   Print the command usage, and the list of options available.

EXAMPLES
--------

1. Synchronize dir2 to match dir1:

``mpirun -np 128 dsync /path/to/dir1 /path/to/dir2``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
