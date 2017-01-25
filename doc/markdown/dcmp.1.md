% DCMP(1)

# NAME
dcmp - distributed *cmp(1)* program

# SYNOPSIS
**dcmp [OPTION] SOURCE DEST ** 

# DESCRIPTION
Parallel MPI application to recurseively walk, and then compare files with the same name that live in different directories. This tools is similar to *cmp(1)*. Also, it will tell you how many files in two different directories are the same (or different), out of the total set of files and directories.  

dcmp prrovides functionality similar to *cmp(1)*.  

# OPTIONS
-b, \--base 
: 	Do a base comparison.

-o, \--output <FIELD0=STATE0@FIELD1=STATE1,FIELD2=STATE2:FILE> 
: 	write output fields and states to a file. 

-h, \--help 
: 	Print the command usage, and the list of options available. 

-v, \--verbose 
: 	Prints a list of statistics/timing data for the command. Files walked, started, completed, seconds, files, bytes read, byte rate, and file rate. 

# EXAMPLES

Example will use 4 nodes:
1. salloc -N4 -ppdebug

Compare two files in different directories:
2. srun -n4 dcmp /src1/file1 /src2/file2

Compare two directories with verbose output. The verbose output will give you the timing & bytes read info as well:
3. srun -n4 dcmp -v /src1 /src2

### Known bugs
None found (so far).

# SEE ALSO
`dchmod` (1). 
`dcp` (1). 
`drm` (1). 
`dwalk` (1).

The mpiFileUtils source code and all documentation may be downloaded from <http://fileutils.io>
