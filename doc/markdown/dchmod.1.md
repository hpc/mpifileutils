% DCHMOD(1)

# NAME
dchmod - distributed chmod program

# SYNOPSIS
**dchmod [OPTION] PATH...**

# DESCRIPTION
Parallel MPI application to recurseively walk, and then change permissions starting from the top level directory. 

dchmod prrovides functionality similar to *chmod(1)*. Like *chmod(1)*, the tool supports the use of octal or symbolic mode to change the permissions. But unlinke *chmod(1)*, there is no need to use a recursive option, as the permissions will be updated from the top level directory all the way to the bottom of the tree by default. 
 

# OPTIONS
-g, \--group 
:   Change group to specified group name. 

-m, \--mode 
:   The mode that you want the file or directory to be. 

-e, \--exclude 
:   Exclude a set of files from command given a regular expression. 

-m, \--match 
:   Match a set of files from command given a regular expression.

-n, \--name 
:   Match or exclude the regular expression based only on file name, and not the full path. Should be used in combination with the match and/or exclude options if you don not want to match/exclude the full path name, but just the file name. 

-h, \--help 
: 	Print the command usage, and the list of options available. 

-v, \--verbose 
: 	Prints a list of statistics/timing data for the command. How many files walked, how many levels there are in the tree, and how many files the command operated on. This option also prints the files/sec for each of those.

### Known bugs

N/A

# SEE ALSO
`dcmp` (1). 
`dcp` (1). 
`drm` (1). 
`dwalk` (1).

The mpiFileUtils source code and all documentation may be downloaded from <http://fileutils.io> 
