========================
libmfu
========================

Functionality that is common to multiple tools is moved to the common library,
libmfu. This goal of this library is to make it easy to develop new tools and
to provide consistent behavior across tools in the suite. The library can also
be useful to end applications, e.g., to efficiently create or remove a large
directory tree in a portable way across different parallel file systems.

---------------------------------------
libmfu: the mpiFileUtils common library
---------------------------------------

The mpiFileUtils common library defines data structures and methods on those
data structures that makes it easier to develop new tools or for use within HPC
applications to provide portable, performant implementations across file
systems common in HPC centers.

.. code-block:: C

    #include "mfu.h"

This file includes all other necessary headers.

---------------------------------------
mfu_flist
---------------------------------------

The key data structure in libmfu is a distributed file list called mfu_flist.
This structure represents a list of files, each with stat-like metadata, that
is distributed among a set of MPI ranks.

The library contains functions for creating and operating on these lists. For
example, one may create a list by recursively walking an existing directory or
by inserting new entries one at a time. Given a list as input, functions exist
to create corresponding entries (inodes) on the file system or to delete the
list of files. One may filter, sort, and remap entries. One can copy a list of
entries from one location to another or compare corresponding entries across
two different lists. A file list can be serialized and written to or read from
a file.

Each MPI rank "owns" a portion of the list, and there are routines to step
through the entries owned by that process. This portion is referred to as the
"local" list. Functions exist to get and set properties of the items in the
local list, for example to get the path name, type, and size of a file.
Functions dealing with the local list can be called by the MPI process
independently of other MPI processes.

Other functions operate on the global list in a collective fashion, such as
deleting all items in a file list. All processes in the MPI job must invoke
these functions simultaneously.

For full details, see `mfu_flist.h <https://github.com/hpc/mpifileutils/blob/master/src/common/mfu_flist.h>`_
and refer to its usage in existing tools.

---------------------------------------
mfu_path
---------------------------------------

mpiFileUtils represents file paths with the `mfu_path <https://github.com/hpc/mpifileutils/blob/master/src/common/mfu_path.h>`_
structure. Functions are available to manipulate paths to prepend and append
entries, to slice paths into pieces, and to compute relative paths.

---------------------------------------
mfu_param_path
---------------------------------------

Path names provided by the user on the command line (parameters) are handled
through the `mfu_param_path <https://github.com/hpc/mpifileutils/blob/master/src/common/mfu_param_path.h>_`
structure. Such paths may have to be checked for existence and to determine
their type (file or directory). Additionally, the user may specify many such
paths through invocations involving shell wildcards, so functions are available
to check long lists of paths in parallel.

---------------------------------------
mfu_io_and_mfu_util
---------------------------------------

The `mfu_io.h <https://github.com/hpc/mpifileutils/blob/master/src/common/mfu_io.h>`_
functions provide wrappers for many POSIX-IO functions. This is helpful for
checking error codes in a consistent manner and automating retries on failed
I/O calls. One should use the wrappers in mfu_io if available, and if not, one
should consider adding the missing wrapper.

The `mfu_util.h <https://github.com/hpc/mpifileutils/blob/master/src/common/mfu_util.h>`_
functions provide wrappers for error reporting and memory allocation.

