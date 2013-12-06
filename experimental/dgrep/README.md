dgrep (distributed grep)
========================
dgrep is a utility to search a large filesystem using multiple nodes of a
cluster. It is well suited to applications that involve a large parallel
filesystem. Checkpointing and automatic redistribution of load is supported.

Usage
-----
```
mpirun dgrep -p <path> -a <pattern>
```

Dependencies
------------
* libcircle <http://github.com/hpc/libcircle>
