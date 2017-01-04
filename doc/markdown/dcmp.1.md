% DCMP(1)

# NAME
dcmp - distributed cmp program

# SYNOPSIS
**dcmp [OPTION] SOURCE DEST ** 

# DESCRIPTION
Parallel MPI application to recurseively walk, and then compare files with the same name that live in different directories. 

dcmp prrovides functionality similar to cmp.  

# OPTIONS
-b, \--base 
: 	Do a base comparison.

-o, \--output 
: 	write output fields and states to a file. 

-n, \--name 
:	 Match or exclude the regular expression based only on file name, and not the full path. Should be used in combination with the match and/or exclude options if you do not want to match/exclude the full path name, but just the file name. 

-h, \--help 
: 	Print the command usage, and the list of options available. 

-v, \--verbose 
: 	Prints a list of statistics/timing data for the command. Files walked, started, completed, seconds, files, bytes read, byte rate, and file rate. 

### Known bugs

None found (so far).

# SEE ALSO
`dchmod` (1). 
`dcp` (1). 
`dfilemaker` (1). 
`dfind` (1). 
`dgrep` (1). 
`dparallel` (1). 
`drm` (1). 
`dtar` (1). 
`dwalk` (1).

The FileUtils source code and all documentation may be downloaded from <http://fileutils.io> (Should I put github link here instead??)
