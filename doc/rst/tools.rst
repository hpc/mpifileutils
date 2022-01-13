===============
Utilities
===============

The tools in mpiFileUtils are MPI applications. They must be launched
as MPI applications, e.g., within a compute allocation on a cluster using
mpirun. The tools do not currently checkpoint, so one must be careful that an
invocation of the tool has sufficient time to complete before it is killed.

- :doc:`dbcast <dbcast.1>` - Broadcast a file to compute nodes.
- :doc:`dbz2 <dbz2.1>` - Compress a file with bz2.
- :doc:`dchmod <dchmod.1>` - Change owner, group, and permissions on files.
- :doc:`dcmp <dcmp.1>` - Compare files.
- :doc:`dcp <dcp.1>` - Copy files.
- :doc:`ddup <ddup.1>` - Find duplicate files.
- :doc:`dfind <dfind.1>` - Filter files.
- :doc:`dreln <dreln.1>` - Update symlinks.
- :doc:`drm <drm.1>` - Remove files.
- :doc:`dstripe <dstripe.1>` - Restripe files.
- :doc:`dsync <dsync.1>` - Synchronize files.
- :doc:`dtar <dtar.1>` - Create and extract tape archive files.
- :doc:`dwalk <dwalk.1>` - List, sort, and profile files.

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
