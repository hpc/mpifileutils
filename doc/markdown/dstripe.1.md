% DSTRIPE(1)

# NAME

dstripe - restripe files on underlying storage

# SYNOPSIS

**dstripe STRIPE_COUNT STRIPE_SIZE SRC_FILE DEST_FILE**

# DESCRIPTION

Parallel MPI application to restripe a given file.

This tool is in active development.  It will eventually report striping information and it will also support recursive operation on directories.  It currently only works on Lustre.

dstripe enables one to restripe a file across the underlying storage devices.  dstripe will make a full copy of the source file with the new striping parameters and write it to the destination file.

One must specify the integer number of stripes as the first parameter in STRIPE_COUNT.  One can specify -1 to use all available stripes.  One must specify the stripe size in bytes in STRIPE_SIZE.  It is possible to use units like "MB" or "GB" after the number.  The units should come immediately after the number without spaces.  Then one must give the source file in SRC_FILE and the name for the new file in DEST_FILE.

# OPTIONS

# EXAMPLES

1. To stripe a file on all storage devices using a 1MB stripe size:

mpirun -np 128 dstripe -1 1MB /path/to/file /path/to/file2

2. To stripe a file across 20 storage devices with a 1GB stripe size:

mpirun -np 128 dstripe 20 2GBB /path/to/file /path/to/file2

3. To read the current striping parameters of a file on Lustre:

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
