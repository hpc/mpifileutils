% DRM(1)

# NAME

drm - distributed file remove program

# SYNOPSIS

**drm [OPTION] PATH...**

# DESCRIPTION

drm is a tool for removing files recurseivly in parallel. 

# OPTIONS

-i, \--input <file>
:	Read a list from a file.

-l, \--lite 
:	Walk file system without stat.

--exclude regex
:   Exclude a list of files from command.

--match regex
:	Match a list of files from command.

-n, \--name
:	Exclude and/or match a list of files from command based only on file name.

-h, \--help
:   Print a brief message listing the *drm(1)* options and usage.

-v, \--version
:   Print version information and exit.

### Known bugs
Not sure.

# SEE ALSO

`dchmod` (1).
`dcmp` (1).
`dcp` (1).
`dfilemaker` (1).
`dfind` (1).
`dgrep` (1).
`dparallel` (1).
`drm` (1).
`dtar` (1).
`dwalk` (1).

The FileUtils source code and all documentation may be downloaded from
<http://fileutils.io>
