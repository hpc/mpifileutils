daos-deserialize
===

SYNOPSIS
--------

**daos-deserialize [OPTION] [<file> <file> ...] || [</path/to/directory>]**

DESCRIPTION
-----------

Parallel MPI application to deserialize HDF5 files into a DAOS container.

daos-deserialize is a deserialization tool that will restore DAOS data written
to an HDF5 file with daos-serialize. The tool will restore the data in the
the HDF5 file into a DAOS container.

OPTIONS
-------
.. option:: --pool UUID 

   Specify the pool where the restored DAOS container will be created 

.. option:: -v, --verbose

   Run in verbose mode.

.. option:: -q, --quiet

   Run tool silently. No output is printed.

.. option:: -h, --help

   Print a brief message listing the :manpage:`daos-deserialize(1)` options and usage.

EXAMPLES
--------

1. To deserialize a DAOS container by specifying individual files:

``mpirun -np 128 daos-deserialize <file1> <file2>``

2. To deserialize a DAOS container by specifying a directory with HDF5 files:

``mpirun -np 128 daos-deserialize /path/to/dir``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
