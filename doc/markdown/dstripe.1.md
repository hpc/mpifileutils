% DSTRIPE(1)

# NAME

dstripe - restripe files on underlying storage

# SYNOPSIS

**dstripe [OPTION] FILE**

# DESCRIPTION

Parallel MPI application to restripe a given file.

This tool is in active development.  It will eventually report striping information and it will also support recursive operation on directories.  It currently only works on Lustre.

dstripe enables one to restripe a file across the underlying storage devices.  One must specify the source file (FILE) to perform a restripe with. By default, stripe size is 1MB and stripe count is -1 allowing dstripe to use all available stripes.

# OPTIONS

-o, \--output DEST_FILE
:	Write the restriped file to DEST_FILE rather than overwriting FILE. If DEST_FILE is equivalent to FILE, the restriped file overwrites FILE. Otherwise, FILE will not be removed after restriping.

-c, \--count STRIPE_COUNT
:	The number of stripes to use when restriping FILE. If STRIPE_COUNT is -1, then all available stripes are used. The default stripe count is -1.

-s, \--size STRIPE_SIZE
:	The stripe size to use during restriping. It is possible to use units like "MB" and "GB" after the number, which should be immediately follow the number without spaces (ex. 2MB). The default stripe size is 1MB.

-r, \--report
:	Display the stripe count and stripe size of FILE. No restriping is performed when using this option.

-v, \--verbose
: 	Run in verbose mode.

-h, \--help
: 	Print the command usage, and the list of options available.

# EXAMPLES

1. To stripe a file on all storage devices using a 1MB stripe size:

mpirun -np 128 dstripe -s 1MB /path/to/file

2. To stripe a file across 20 storage devices with a 1GB stripe size:

mpirun -np 128 dstripe -c 20 -s 1GB -o /path/to/file2 /path/to/file

3. To display the current stripe count and stripe size of a file:

dstripe -r /path/to/file

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
