% DBCAST(1)

# NAME

dbcast - distributed broadcast

# SYNOPSIS

**dbcast CHUNK_SIZE SRC_FILE DEST_FILE**

# DESCRIPTION

Parallel MPI application to recursively broadcast a file from a global file system to node-local storage.

dbcast helps one copy a source file to node-local storage on each compute node.

The file is logically sliced at chunk boundaries as specified by CHUNK_SIZE and collectively copied from a global file system to node-local storage.  The CHUNK_SIZE parameter should be specified in bytes, and one may use units like "MB" or "GB".  It is recommended to use the stripe size of a file if this is known.  The path to the source file should be given in SRC_FILE.  The source file should be in a path readable by all MPI processes.  The destination path is given in DEST_FILE.  The destination file should be a path to a node-local storage location, like ramdisk or an SSD.

In its current implementation, the tool requires at least two MPI processes per compute node, and all compute nodes should have the same number of MPI processes.

# OPTIONS

# EXAMPLES

1. To broadcast a file to /ssd on each node slicing at 1MB chunks:

mpirun -np 128 dbcast 1MB /global/path/to/filenane /ssd/filename

2. To read the current striping parameters of a file on Lustre:

lfs getstripe /path/to/file

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
