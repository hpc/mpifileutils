dbz2
====

SYNOPSIS
--------

**dbz2 [OPTIONS] [-z|-d] FILE**

DESCRIPTION
-----------

Parallel MPI application to compress or decompress a file.

When compressing, a new file will be created with a .dbz2 extension.
When decompressing, the .dbz2 extension will be dropped from the file name.

OPTIONS
-------

.. option:: -d, --decompress

   Decompress the file

.. option:: -z, --compress

   Compress the file

.. option:: -k, --keep

   Keep the input file.

.. option:: -f, --overwrite

   Overwrite the output file, if it exists.

.. option:: -b, --block SIZE

   Set the compression block size, from 1 to 9.
   Where 1=100kB ... and 9=900kB. Default is 9.

.. option:: -v, --verbose

   Verbose output (optional).

.. option:: -h, --help

   Print usage.

EXAMPLES
--------

1. To compress a file:

``mpirun -np 128 dbz2 --compress /path/to/file``

2. To compress a file and overwrite any existing output file:

``mpirun -np 128 dbz2 --force --compress /path/to/file``

3. To decompress a file:

``mpirun -np 128 dbz2 --decompress /path/to/file.dbz2``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
