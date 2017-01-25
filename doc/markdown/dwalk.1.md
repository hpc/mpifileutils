% DWALK(1)

# NAME

dwalk - distributed file listing program

# SYNOPSIS

dwalk can walk a file tree in parallel with various different options. You can sort the output of the distributed file walk based on different fields (i.e name, use, group, size, etc), or get a file list distribution that will put the amount of files you have of a certain size into bins specified by the user. For instance, if you wanted to know how many files in a large directory have 0 bytes then you would use the --distribution option. 

# DESCRIPTION

# OPTIONS

-i, \--input <FILE>
:   Read a list of inputs from a file.

-o, \--output <FILE>
:   Write the processed list to a file.

-l, \--lite
:   Walk file system without stat.

-s, \--sort <FIELD>
:   Sort output by comma-delimited fields. Available fields: name, user, group, uid, gid, atime, mtime, ctime, size.

-d, \--distribution <FIELD>:<SEPARATORS>. 
:   Print the file distribution by field. So, for the size field you could do size:0,80,100 which would then tell you
    how many files from the directory and/or subdirectories you pass in are between 0-0 bytes, 1-80 bytes, 81-99 bytes, 
    and 100 - MAX file size bytes. 

-p, \--print
:   Print the files that will be processed to the screen.

-h, \--help
:   Print usage.

-v, \--verbose
:   Print verbose output.

# EXAMPLES

Example will use 4 nodes:
1. salloc -N4 -ppdebug

Walk a file path:
2. srun -n4 dwalk /src

Walk a file path & print to terminal with verbose output. Verbose output will also give you the # of directories, files, and links from the top level:
3. srun -n4 dwalk -p -v /src

Print to screen with verbose output the file distribution based on the size field from the top level directory.
4. srun -n4 dwalk -p -v -d size:0,20,1G src/ 

So, #4 will give you something like the following:

syrah144{user1}173: install/bin/dwalk -p -v -d size:0,20,1G testing/
Seperators: 0, 20, 1073741824
2017-01-24T15:53:19: Walking /g/g0/sikich1/mpifileutils/testing
Walked 2 files in 0.003210 seconds (623.086088 files/sec)

drwx------ sikich1 sikich1   4.000 KB Jan 24 2017 15:52 /g/g0/sikich1/mpifileutils/testing
-rw------- sikich1 sikich1   0.000  B Jan 24 2017 15:52 /g/g0/sikich1/mpifileutils/testing/sparse.txt

Items: 2
  Directories: 1
  Files: 1
  Links: 0
Data: 4.000 KB (4.000 KB per file)
Range	Number
[0~0]	                1
[1~20]	                0
[21~1073741824]	        1
[1073741825~MAX]	0

### Known bugs

# SEE ALSO

`dcmp` (1).
`dcp` (1).
`drm` (1).
`dwalk` (1).

The mpiFileUtils source code and all documentation may be downloaded from
<http://fileutils.io>
