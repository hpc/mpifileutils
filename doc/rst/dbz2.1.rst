dbz2
====

SYNOPSIS
--------

**dbz2 [OPTIONS] [-z|-d] FILE**

DESCRIPTION
-----------

Parallel MPI application to compress or decompress a file.

OPTIONS
-------

.. option:: -d, --decompress

   Decompress the file

.. option:: -z, --compress

   Compress the file

.. option:: -k, --keep

   Keep the input file (optional).

.. option:: -f, --overwrite

   Overwrite the output file, if it exists (optional).

.. option:: -b, --block SIZE

   Set the compression block size, from 1 to 9.
   Where 1=100kB ... and 9=900kB. Default is 9 (optional).

.. option:: -m, --memory SIZE

   Limit the memory that can be used by a processs, in bytes (optional).

.. option:: -v, --verbose

   Verbose output (optional).

.. option:: --debug

   Show debug output (optional).
