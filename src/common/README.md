# libmfu: the mpiFileUtils common library
The mpiFileUtils common library defines data structures and methods on those data structures
that makes it easier to develop new tools or for use within HPC applications to provide portable, performant
implementations across file systems common in HPC centers.

To use this library, include mfu.h.

    #include "mfu.h"

This file includes all other necessary headers.

## mfu\_flist
The key data structure in libmfu is a distributed file list called mfu\_flist.
This structure represents a list of files, each with stat-like metadata, that is distributed among a set of MPI ranks.

The library contains functions for creating and operating on these lists.
For example, one may create a list by recursively walking an existing directory
or by inserting new entries one at a time.
Given a list as input, functions exist to create corresponding entries (inodes) on the file system
or to delete the list of files.
One may filter, sort, and remap entries.
One can copy a list of entries from one location to another
or compare corresponding entries across two different lists.
A file list can be serialized and written to or read from a file.

Each MPI rank "owns" a portion of the list, and there are routines to step through the entries owned by that process.
This portion is referred to as the "local" list.
Functions exist to get and set properties of the items in the local list,
for example to get the path name, type, and size of a file.
Functions dealing with the local list can be called by the MPI process independently of other MPI processes.

Other functions operate on the global list in a collective fashion,
such as deleting all items in a file list.
All processes in the MPI job must invoke these functions simultaenously.

For full details, see [mfu_flist.h](mfu_flist.h) and refer to its usage in existing tools.

## mfu\_path
mpiFileUtils represents file paths with the [mfu_path](mfu_path.h) structure.
Functions are available to manipulate paths to prepend and append entries,
to slice paths into pieces, and to compute relative paths.

## mfu\_param\_path
Path names provided by the user on the command line (parameters) are handled through the [mfu_param_path](mfu_param_path.h) structure.
Such paths may have to be checked for existence and to determine their type (file or directory).
Additionally, the user may specify many such paths through invocations involving shell wildcards,
so functions are available to check long lists of paths in parallel.

## mfu\_io and mfu\_util
The [mfu\_io.h](mfu_io.h) functions provide wrappers for many POSIX-IO functions.
This is helpful for checking error codes in a consistent manner and automating retries on failed I/O calls.
One should use the wrappers in mfu\_io if available, and if not, one should consider adding the missing wrapper.

The [mfu_util.h](mfu_util.h) functions provide wrappers for error reporting and memory allocation.
