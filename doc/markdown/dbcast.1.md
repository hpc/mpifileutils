% DBCAST(1)

# NAME

dbcast - distributed broadcast

# SYNOPSIS

**dbcast [OPTION] SRC DEST**

# DESCRIPTION

Parallel MPI application to recursively broadcast a single file from a global file system to node-local storage.

The file is logically sliced into chunks and collectively copied from a global file system to node-local storage.  The path to the source file is given as SRC.  The source file must be readable by all MPI processes.  The destination path is given in DEST.  The destination file should be a path to a node-local storage location, like ramdisk or an SSD.

In its current implementation, the tool requires at least two MPI processes per compute node, and all compute nodes should have the same number of MPI processes.

# OPTIONS

-s, \--size SIZE
:	The chunk size to segment files during the broadcast. It is possible to use units like "MB" and "GB" after the number, which should be immediately follow the number without spaces (ex. 2MB). The default size is 1MB.  It is recommended to use the stripe size of a file if this is known.

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
