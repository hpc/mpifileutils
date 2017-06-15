% DCMP(1)

# NAME
dcmp - distributed compare

# SYNOPSIS
**dcmp [OPTION] SRC DEST ** 

# DESCRIPTION

Parallel MPI application to compare two files or to recursively compare files with same relative paths within two different directories.

dcmp provides functionality similar to a recursive *cmp(1)*.  It reports how many files in two different directories are the same (or different).

# OPTIONS
-b, \--base 
: 	Do a base comparison.

-v, \--verbose 
: 	Run in verbose mode.  Prints a list of statistics/timing data for the command. Files walked, started, completed, seconds, files, bytes read, byte rate, and file rate. 

-h, \--help 
: 	Print the command usage, and the list of options available. 

# EXAMPLES

1. Compare two files in different directories:

mpirun -np 128 dcmp /src1/file1 /src2/file2

2. Compare two directories with verbose output. The verbose output prints timing and number of bytes read:

mpirun -np 128 dcmp -v /src1 /src2

# SEE ALSO

`dbcast` (1).
`dchmod` (1).
`dcmp` (1).
`dcp` (1).
`drm` (1).
`dstripe` (1).
`dwalk` (1).

The mpiFileUtils source code and all documentation may be downloaded from <https://github.com/hpc/mpifileutils>
