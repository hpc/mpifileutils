dtar
====

SYNOPSIS
--------

**dtar [OPTION] -c -f ARCHIVE SOURCE...**

**dtar [OPTION] -x -f ARCHIVE**

DESCRIPTION
-----------

Parallel MPI application to create and extract tar archive files.

dtar writes archives using the pax file format.
In addition to the archive file, dtar creates an index that records
the byte offset of each entry in the archive.
dtar reads the index file to improve performance when extracting the archive.

dtar can extract archives in various tar formats, including archives created by other tools.
Archives can be compressed with gzip, bz2, or compress.
Compressed archives are significantly slower to extract than uncompressed archives.
Uncompressed archives can be extracted fastest when a corresponding dtar index file exists.
If an index file does not exist, dtar can create an index file
during extraction to benefit subsequent extractions of the same archive.

OPTIONS
-------
.. option:: -c, --create

   Create a tar archive.

.. option:: -x, --extract

   Extract a tar archive.

.. option:: -f, --file NAME

   Name of archive file.

.. option:: -C, --chdir DIR

   Change directory to DIR before executing.

.. option:: --preserve-owner

   Apply recorded owner and group to extracted files.
   Default uses effective uid/gid of the running process.

.. option:: --preserve-times

   Apply recorded atime and mtime to extracted files.
   Default uses current system times.

.. option:: --preserve-perm

   Apply recorded permissions to extracted files.
   Default applies umask of the running process.

.. option:: --fsync

   Call fsync before closing files after writing.

.. option:: --blocksize SIZE

   Set the I/O buffer to be SIZE bytes.  Units like "MB" and "GB" may
   immediately follow the number without spaces (eg. 8MB). The default
   blocksize is 1MB.

.. option:: --chunksize SIZE

   Multiple processes copy a large file in parallel by dividing it into chunks.
   Set chunk to be at minimum SIZE bytes.  Units like "MB" and
   "GB" can immediately follow the number without spaces (eg. 64MB).
   The default chunksize is 1MB.

.. option:: --memory SIZE

   Set the memory limit to be SIZE bytes when reading archive files.
   For some archives, dtar can distribute the file across processes
   to store the full archive in memory for faster processing.
   Units like "MB" and "GB" may immediately follow the number
   without spaces (eg. 8MB). The default is 1GB.

.. option:: --progress N

   Print progress message to stdout approximately every N seconds.
   The number of seconds must be a non-negative integer.
   A value of 0 disables progress messages.

.. option:: -v, --verbose

   Run in verbose mode.

.. option:: -q, --quiet

   Run tool silently. No output is printed.

.. option:: -h, --help

   Print a brief message listing the :manpage:`dtar(1)` options and usage.

EXAMPLES
--------

1. To create an archive of dir named dir.tar:

``mpirun -np 128 dtar -c -f dir.tar dir/``

2. To extract an archive named dir.tar:

``mpirun -np 128 dtar -x -f dir.tar``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
