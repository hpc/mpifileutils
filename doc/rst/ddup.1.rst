ddup
=======

SYNOPSIS
--------

**ddup [OPTION] PATH**

DESCRIPTION
-----------

Parallel MPI application to report files under a directory tree having identical content.

ddup reports path names to files having identical content (duplicate files).
A top-level directory is specified, and the path name to any file that is a duplicate
of another anywhere under that same directory tree is reported.
The path to each file is reported, along with a final hash representing its content.
Multiple sets of duplicate files can be matched using this final reported hash.

OPTIONS
-------

.. option:: --open-noatime

   Open files with O_NOATIME flag, if possible.

.. option:: -d, --debug LEVEL

   Set verbosity level.  LEVEL can be one of: fatal, err, warn, info, dbg.

.. option:: -v, --verbose

   Run in verbose mode.

.. option:: -q, --quiet

   Run tool silently. No output is printed.

.. option:: -h, --help

   Print the command usage, and the list of options available.

EXAMPLES
--------

1. To report any duplicate files under a directory tree:

``mpirun -np 128 ddup /path/to/haystack``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
