daos-serialize
===

SYNOPSIS
--------

**daos-serialize [OPTION] /<pool>/<cont>**

DESCRIPTION
-----------

Parallel MPI application to serialize a DAOS container to an HDF5 file.

daos-serialize is a serialization tool that will allow storing any type
of DAOS container on a POSIX filesystem. It allows DAOS container data to
be stored outside of the DAOS system. The DAOS container is converted
to a set of files in HDF5 format. The serialization format of the container is meant to
be deserialized by daos-deserialize.

OPTIONS
-------
.. option:: -o, --output-path PATH

   Write the HDF5 files generated during serialization to the specified
   output path.

.. option:: -v, --verbose

   Run in verbose mode.

.. option:: -q, --quiet

   Run tool silently. No output is printed.

.. option:: -h, --help

   Print a brief message listing the :manpage:`daos-serialize(1)` options and usage.

EXAMPLES
--------

1. To serialize a DAOS container:

``mpirun -np 128 daos-serialize /<pool>/<container>``

2. To serialize a DAOS container to a specific directory:

``mpirun -np 128 daos-serialize -o /path/to/output/dir /<pool>/<container>``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
