% DSTRIPE(1)

# NAME

dstripe - restripe files on underlying storage

# SYNOPSIS

**dstripe [OPTION] PATH...**

# DESCRIPTION

Parallel MPI application to restripe a given file.

This tool is in active development. It currently only works on Lustre.

dstripe enables one to restripe a file across the underlying storage devices. One must specify a list of paths to recursively walk. By default, stripe size is 1MB and stripe count is -1 allowing dstripe to use all available stripes. 

# OPTIONS

-c, \--count STRIPE_COUNT
:	The number of stripes to use during file restriping. If STRIPE_COUNT is -1, then all available stripes are used. If STRIPE_COUNT is 0, the lustre file system default is used. The default stripe count is -1.

-s, \--size STRIPE_SIZE
:	The stripe size to use during file restriping. It is possible to use units like "MB" and "GB" after the number, which should be immediately follow the number without spaces (ex. 2MB). The default stripe size is 1MB.

-r, \--report
:	Display the stripe count and stripe size of all files found in PATH. No restriping is performed when using this option.

-v, \--verbose
: 	Run in verbose mode.

-h, \--help
: 	Print the command usage, and the list of options available.

# EXAMPLES

1. To stripe a file on all storage devices using a 1MB stripe size:

mpirun -np 128 dstripe -s 1MB /path/to/file

2. To stripe a file across 20 storage devices with a 1GB stripe size:

mpirun -np 128 dstripe -c 20 -s 1GB /path/to/file

3. To restripe all files in /path/to/files/ across 10 storage devices with 2MB stripe size:

mpirun -np 128 dstripe -c 10 -s 2MB /path/to/files/

4. To display the current stripe count and stripe size of all files in /path/to/files/:

mpirun -np 128 dstripe -r /path/to/files/

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
