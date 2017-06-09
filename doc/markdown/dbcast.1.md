% DBCAST(1)

# NAME

dbcast - distributed broadcast

# SYNOPSIS

**dbcast [OPTION] SRC DEST**

# DESCRIPTION

Parallel MPI application to recursively broadcast a single file from a global file system to node-local storage, like ramdisk or an SSD.

The file is logically sliced into chunks and collectively copied from a global file system to node-local storage.  The source file SRC must be readable by all MPI processes.  The destination file DEST should be the full path of the file in node-local storage.  If needed, parent directories for the destination file will be created as part of the broadcast.

In the current implementation, dbcast requires at least two MPI processes per compute node, and all compute nodes must run an equal number of MPI processes.

# OPTIONS

-s, \--size SIZE
:	The chunk size in bytes used to segment files during the broadcast. Units like "MB" and "GB" should be immediately follow the number without spaces (ex. 2MB). The default size is 1MB.  It is recommended to use the stripe size of a file if this is known.

-h, \--help
: 	Print the command usage, and the list of options available.

# EXAMPLES

1. To broadcast a file to /ssd on each node:

mpirun -np 128 dbcast /global/path/to/filenane /ssd/filename

2. Same thing, but slicing at 10MB chunks:

mpirun -np 128 dbcast -s 10MB /global/path/to/filenane /ssd/filename

3. To read the current striping parameters of a file on Lustre:

lfs getstripe /global/path/to/filename

# SEE ALSO

`dbcast` (1).
`dchmod` (1).
`dcmp` (1).
`dcp` (1).
`drm` (1).
`dstripe` (1).
`dwalk` (1).

The mpiFileUtils source code and all documentation may be downloaded from
<https://github.com/hpc/mpifileutils>
