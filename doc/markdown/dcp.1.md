% DCP(1)

# NAME

dcp - distributed file copy program

# SYNOPSIS

**dcp [OPTION] SOURCE DEST**

# DESCRIPTION

dcp is a file copy tool in the spirit of *cp(1)* that evenly distributes work
across a large cluster without any centralized state. It is designed for
copying files which are located on a distributed parallel file system. The
method used in the file copy process is a self-stabilization algorithm which
enables per-node autonomous processing and a token passing scheme to detect
termination. Unlike *cp(1)* there is no need to use a recursive option, as dcp
will copy from the top level directory to the bottom of the file tree by default.

# OPTIONS

-d, \--debug <LEVEL>
:   Specify the level of debug information to output. Level may be one of:
    *fatal*, *err*, *warn*, *info*, or *dbg*. Increasingly verbose debug
    levels include the output of less verbose debug levels.
    Levels: dbg, info, warn, err, fatal

-f, \--force
:   Remove existing destination files if creation or truncation fails. If the
    destination filesystem is specified to be unreliable
    (`-U`, `--unreliable-filesystem`), this option may lower performance since
    each failure will cause the entire file to be invalidated and copied again.

-g, \--grouplock
:   Use Lustre grouplock when reading/writing a file.

-i, \--input <FILE>
:   Read an input list from a file.

-p, \--preserve 
:   Preserve the original files owner, group, permissions (including the
    setuid and setgid bits), time of last  modification and time of last
    access. In case duplication of owner or group fails, the setuid and setgid
    bits are cleared.

-s, \--synchronous
:   Use synchronous read/write calls (0_DIRECT)

-S, \--sparse
:   Create sparse files when possible. 

-v, \--version
:   Print version information and exit.

-h, \--help
:   Print a brief message listing the *dcp(1)* options and usage.

# EXAMPLES

Example will use 4 nodes:

1. salloc -N4 -ppdebug

Do a regular copy:

2. srun -n4 dcp src/ dest/

Copy & preserve permissions, ownership, timestamps, and extended attributes:

3. srun -n4 dcp -d src/ dest/

### Known bugs
When the force option is specified and truncation fails, the copy and
truncation will be stuck in an infinite loop until the truncation operation
returns with success.

The maximum supported filename length for any file transfered is approximately
4068 characters. This may be less than the number of characters that your
operating system supports.

Using the -S option for sparse files does not work yet at LLNL. If you try
to use it then dcp will default to a normal copy.

# SEE ALSO

`dchmod` (1).
`dcmp` (1).
`drm` (1).
`dwalk` (1).

The mpiFileUtils source code and all documentation may be downloaded from
<http://fileutils.io>
