% DWALK(1)

# NAME

dwalk - distributed file listing program

# SYNOPSIS

dwalk can walk a file tree in parallel with various different options. You can sort the output of the distributed file walk based on different fields (i.e name, use, group, size, etc), or get a file list distribution that will put the amount of files you have of a certain size into bins specified by the user. For instance, if you wanted to know how many files in a large directory have 0 bytes then you would use the --distribution option. 

# DESCRIPTION

# OPTIONS

-i, \--input <file>
:   Read a list of inputs from a file.

-o, \--output <file>
:   Write the processed list to a file.

-l, \--lite
:   Walk file system without stat.

-s, \--sort <fields>
:   Sort output by comma-delimited fields. Available fields: name, user, group, uid, gid, atime, mtime, ctime, size.

-d, \--distribution <field>:<separators>. 
:   Print the file distribution by field. So, for the size field you could do size:0,80,100 which would then tell you
    how many files from the directory and/or subdirectories you pass in are between 0-0 bytes, 1-80 bytes, 81-99 bytes, 
    and 100 - MAX file size bytes. 

-p, \--print
:   Print the files that will be processed to the screen.

-h, \--help
:   Print usage.

-v, \--verbose
:   Print verbose output.

### Known bugs

# SEE ALSO

`dcmp` (1).
`dcp` (1).
`drm` (1).
`dwalk` (1).

The mpiFileUtils source code and all documentation may be downloaded from
<http://fileutils.io>
