dtar
====

SYNOPSIS
--------

**dtar [OPTION] -c -f ARCHIVE SOURCE...**

**dtar [OPTION] -x -f ARCHIVE**

DESCRIPTION
-----------

Parallel MPI application to create and extract tar files.

dtar writes archives in pax file format.
In addition to the archive file, dtar creates an index to record
the number of items and the starting byte offset of each entry within the archive.
This index enables faster parallel extraction.
By default, dtar appends its index as the last entry of the archive.
Optionally, the index may be written as a separate file (with a .dtaridx extension)
or as an extended attribute (named user.dtar.idx) of the archive file.

dtar can extract archives in various tar formats, including archive files that were created by other tools like tar.
dtar can also extract archives that have been compressed with gzip, bz2, or compress.
Compressed archives are significantly slower to extract than uncompressed archives,
because decompression inhibits available parallelism.

Archives are extracted fastest when a dtar index exists.
If an index does not exist, dtar can create and record an index
during extraction to benefit subsequent extractions of the same archive file.

When extracting an archive, dtar skips the entry corresponding to its index.
If other tools, like tar, are used to extract the archive, the index
entry is extracted as a regular file that is placed in the current working directory
with a file extension of ".dtaridx" and having the same basename as the original archive file.
For an archive that was named "file.tar" when it was created, the dtar index file is named "file.tar.dtaridx".

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

.. option:: --preserve-perms

   Apply recorded permissions to extracted files.
   Default subtracts umask from file permissions.

.. option:: --preserve-xattrs

   Record extended attributes (xattrs) when creating archive.
   Apply recorded xattrs to extracted files.
   Default does not record or extract xattrs.

..
   .. option:: --preserve-acls

   Record access control lists (acls) when creating archive.
   Apply recorded acls to extracted files.
   Default does not record or extract acls.

..
   .. option:: --preserve-flags

   Record ioctl iflags (flags) when creating archive.
   Apply recorded flags to extracted files.
   Default does not record or extract flags.

.. option:: --fsync

   Call fsync before closing files after writing.

.. option:: --bufsize SIZE

   Set the I/O buffer to be SIZE bytes.  Units like "MB" and "GB" may
   immediately follow the number without spaces (eg. 8MB). The default
   bufsize is 64MB.

.. option:: --chunksize SIZE

   Multiple processes copy a large file in parallel by dividing it into chunks.
   Set chunk to be at minimum SIZE bytes.  Units like "MB" and
   "GB" can immediately follow the number without spaces (eg. 64MB).
   The default chunksize is 64MB.

.. option:: --memsize SIZE

   Set the memory limit to be SIZE bytes when reading archive files.
   For some archives, dtar can distribute the file across processes
   to store segments of the archive in memory for faster processing.
   Units like "MB" and "GB" may immediately follow the number
   without spaces (eg. 8MB). The default is 256MB.

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

LIMITATIONS
-----------

dtar only supports directories, regular files, and symlinks.

dtar works best on Lustre and GPFS.
There are no known restrictions for creating or extracting archives on these file systems.
These file systems also deliver the highest bandwidth and file create rates.

dtar can be used on NFS, but there is one key restriction.
Namely, one should not create an archive file in NFS.
To create an archive of NFS files, the archive file itself should be written to a directory in Lustre or GPFS.
The dtar tool writes to an archive file from multiple processes in parallel,
and the algorithms used to write the archive are not valid for NFS.

dtar can be used to extract an archive file into NFS.
The archive file that is being extracted may be on any file system.

The target items to be archived must be under the current working directory where dtar is running, so commands like these work.

``dtar -cf foo.tar foo/``

``dtar -cf foo.tar dir/foo/``

But commands like the following are not supported:

``dtar -cf foo.tar ../foo/``

``dtar -cf foo.tar /some/other/absolute/path/to/foo/``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
