% DRM(1)

# NAME

drm - distributed file remove program

# SYNOPSIS

**drm [OPTION] PATH...**

# DESCRIPTION

drm is a tool for removing files recurseivly in parallel. 
drm is similar to *rm(1)*, but unlike *rm(1)* there is no
need to pass a recursive option as files will be removed
from the top level directory to the bottom of the file tree
by default.  

# OPTIONS

-i, \--input=<FILE>
:	Read a list from a file.

-l, \--lite 
:	Walk file system without stat.

--exclude=<REGEX>
:   Exclude a list of files from command.

--match=<REGEX>
:	Match a list of files from command.

-n, \--name
:	Exclude and/or match a list of files from command based only on file name.

-d, \--dryrun
:	Print out a list of files that **would** be deleted. This is especially useful
        if you are matching or excluding with a regular expression option to make sure
        what will be deleted is what you want and/or expect.

-h, \--help
:   Print a brief message listing the *drm(1)* options and usage.

-v, \--version
:   Print version information and exit.

# EXAMPLES

Example will use 4 nodes:
1. salloc -N4 -ppdebug

Regular remove:
2. srun -n4 drm /src

Remove with a --dryrun option. This run will NOT delete anything. It will just print the files to be deleted. 
3. srun -n4 drm --dryrun /src

Use drm --dryrun option with a regex filter to only delete files that match a file or directory name (instead of matching the full path).
4. srun -n4 drm --dryrun --name --match afilename /dir/to/search

### Known bugs
Not sure.

# SEE ALSO

`dchmod` (1).
`dcmp` (1).
`dcp` (1).
`drm` (1).
`dwalk` (1).

The mpiFileUtils source code and all documentation may be downloaded from
<http://fileutils.io>
