===============
Utilities
===============

The tools in mpiFileUtils are MPI applications. They must be launched
as MPI applications, e.g., within a compute allocation on a cluster using
mpirun. The tools do not currently checkpoint, so one must be careful that an
invocation of the tool has sufficient time to complete before it is killed.

- dbcast - Broadcast a file to compute nodes.
- dbz2 - Compress a file with bz2.
- dchmod - Change owner, group, and permissions on files.
- dcmp - Compare files.
- dcp - Copy files.
- ddup - Find duplicate files.
- dfind - Filter files.
- dreln - Update symlinks.
- drm - Remove files.
- dstripe - Restripe files.
- dsync - Synchronize files.
- dtar - Create and extract tape archive files.
- dwalk - List, sort, and profile files.

==============================
Experimental Utilities
==============================

Experimental utilities are under active development. They are not considered to
be production worthy, but they are available in the distribution for those
who are interested in developing them further or to provide additional examples.

- dgrep - Run grep on files in parallel.
- dparallel - Perform commands in parallel.
- dsh - List and remove files with interactive commands.
- dfilemaker - Generate random files.
