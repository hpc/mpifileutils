dfilemaker
==========

SYNOPSIS
--------

**dfilemaker <destination_path>**

DESCRIPTION
-----------

dfilemaker creates a number of random directory trees with subdirectories and
files of various sizes, that is useful for testing.

Directory trees are created under destination_path, which must be a directory.

.. option:: destination_path

   Directory under which the new directory tree will be created.

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

.. option:: -i, --seed=*integer*

   Specify the seed to use for random number generation. This can be
   used to create reproducible test runs. The default is to generate a
   random seed.

.. option:: -s, --size=*min*-*max*

   Specify the file sizes to generate. The file size will be chosen at
   random random within the bounds of min and max. The default file
   size is set from 1MB to 5MB.

.. option:: -h, --help

   Print a brief message listing the *dfilemaker(1)* options and usage.

.. option:: -v, --version

   Print version information and exit.

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
