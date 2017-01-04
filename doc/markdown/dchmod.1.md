% DCHMOD(1)

NAME
dchmod - distributed chmod program

SYNOPSIS
dchmod [OPTION] PATH... 

DESCRIPTION
Parallel MPI application to recurseively walk, and then change permissions starting from the top level directory. 

dchmod prrovides functionality similar to chmod. Like chmod, the tool supports the use of octal or symbolic mode to change the permissions. 

OPTIONS
-g, --group : Change group to specified group name. 

-m, --mode : The mode that you want the file or directory to be. 

-e, --exclude : Exclude a set of files from command given a regular expression. 

-m, --match : Match a set of files from command given a regular expression.

-n, --name : Match or exclude the regular expression based only on file name, and not the full path. Should be used in combination with the match and/or exclude options if you don not want to match/exclude the full path name, but just the file name. 

-h, --help : Print the command usage, and the list of options available. 

-v, --verbose : Prints a list of statistics/timing data for the command. How many files walked, how many levels there are in the tree, and how many files the command operated on. This option also prints the files/sec for each of those.

Known bugs

Not sure if I should mention the behavior of walking and changing the permissions (turning on the read & execute bits) of parent directories in the case that they are not already on? Not sure that really classifies as a bug or not. But maybe it is something that should be explained somewhere?

SEE ALSO
dcmp (1). dcp (1). dfilemaker (1). dfind (1). dgrep (1). dparallel (1). drm (1). dtar (1). dwalk (1).

The FileUtils source code and all documentation may be downloaded from http://fileutils.io (Should I put github link here instead??)

Also, I looked at the Mode section of the chmod man page, and was not sure if I should add this section to here or not, because most of it would be repeating what the chmod man page already says? Should I reference it somehow?
