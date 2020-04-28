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

.. option:: --link-dest DIR

   Create hardlink in DEST to files in DIR when file is unchanged
   rather than create a new file. One can use this option to conserve
   storage space during an incremental backup.

   For example in the following, any file that would be copied from
   /src to /src.bak.inc that is the same as the file already existing
   in /src.bak will instead be hardlinked to the file in /src.bak:

   # initial backup of /src
   dsync /src /src.bak

   # incremental backup of /src
   dsync --link-dest /src.bak /src /src.bak.inc

.. option:: -S, --sparse

   Create sparse files when possible.

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
