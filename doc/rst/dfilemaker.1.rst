dfilemaker
==========

SYNOPSIS
--------

**dfilemaker <nitems> <nlevels> <maxflen>**

DESCRIPTION
-----------

dfilemaker creates a random directory tree with files having random data
that is useful for testing.

Files and directories are created in the current working directory where the tool is executed.
dfilemaker takes three positional parameters:

.. option:: nitems

   Total number of items to create.

.. option:: nlevels

   Maximum depth to create in directory level (relative to current path).

.. option:: maxflen

   Maximum number of bytes to write to a file.


The following options are planned in future releases, but they are not yet implemented.

OPTIONS
-------

.. option:: -d, --depth=*min*-*max*

   Specify the depth of the file system tree to generate. The depth
   will be selected at random within the bounds of min and max. The
   default depth is set to 10 min, 20 max.

.. option:: -f, --fill=*type*

   Specify the fill pattern of the file. Current options available are:
   ``random``, ``true``, ``false``, and ``alternate``. ``random`` will
   fill the file using :manpage:`urandom(4)`. ``true`` will fill the file
   with a 0xFF pattern. ``false`` will fill the file with a 0x00 pattern.
   ``alternate`` will fill the file with a 0xAA pattern. The default
   fill is ``random``.

.. option:: -r, --ratio=*min*-*max*

   Specify the ratio of files to directories as a percentage. The ratio
   will be chosen at random within the bounds of min and max. The
   default ratio is 5% min to 20% max.

.. option:: -i, --seed=*integer*

   Specify the seed to use for random number generation. This can be
   used to create reproducible test runs. The default is to generate a
   random seed.

.. option:: -s, --size=*min*-*max*

   Specify the file sizes to generate. The file size will be chosen at
   random random within the bounds of min and max. The default file
   size is set from 1MB to 5MB.

.. option:: -w, --width=*min*-*max*

   Specify the width of the file system tree to generate. The width
   will be selected at random within the bounds of min and max. The
   width of the tree is determined by counting directories. The default
   width is set to 10 min, 20 max.

.. option:: -h, --help

   Print a brief message listing the *dfilemaker(1)* options and usage.

.. option:: -v, --version

   Print version information and exit.

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
