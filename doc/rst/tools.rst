===============
Utilities
===============

The tools in mpiFileUtils are MPI applications. They must be launched
as MPI applications, e.g., within a compute allocation on a cluster using
mpirun. The tools do not currently checkpoint, so one must be careful that an
invocation of the tool has sufficient time to complete before it is killed.

- :doc:`dbcast <dbcast.1>` - Broadcast a file to each compute node.
- :doc:`dbz2 <dbz2.1>` - Compress and decompress a file with bz2.
- :doc:`dchmod <dchmod.1>` - Change owner, group, and permissions on files.
- :doc:`dcmp <dcmp.1>` - Compare contents between directories or files.
- :doc:`dcp <dcp.1>` - Copy files.
- :doc:`ddup <ddup.1>` - Find duplicate files.
- :doc:`dfind <dfind.1>` - Filter files.
- :doc:`dreln <dreln.1>` - Update symlinks to point to a new path.
- :doc:`drm <drm.1>` - Remove files.
- :doc:`dstripe <dstripe.1>` - Restripe files (Lustre).
- :doc:`dsync <dsync.1>` - Synchronize source and destination directories or files.
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

==============================
Usage tips
==============================
Since the tools are MPI applications, it helps to keep a few things in mind:

- One typically needs to run the tools within a job allocation.  The sweet spot for most tools is about 2-4 nodes.  One can use more nodes for large datasets, so long as tools scale sufficiently well.
- One must launch the job using the MPI job launcher like mpirun or mpiexec.  One should use most CPU cores, though leave a few cores idle on each node for the file system client processes.
- Most tools do not checkpoint their progress.  Be sure to request sufficient time in your allocation to allow the job to complete.  One may need to start over from the beginning if a tool is interrupted.
- One cannot pipe output of one tool to the input of another.  However, the --input and --output file options are good approximations.
- One cannot easily check the return codes of tools.  Instead, inspect stdout and stderr output for errors.

==============================
Examples and frequently used commands
==============================
If your MPI library supports it, most tools can run as MPI singletons (w/o mpirun, which runs a single-task MPI job).
For brevity, the examples in this section are shown as MPI singleton runs.
In a real run, one would precede the command shown with an appropriate MPI launch command and options, e.g.,::

  mpirun -np 128 dwalk /path/to/walk

In addition to the man page, each tool provides a help screen for a brief reminder of available options.::

  dwalk --help

The normal output from dwalk shows a summary of item and byte counts.
This is useful to determine the number of files and bytes under a path of interest::

  dwalk /path/to/walk

When walking large directory trees, you can write the list to an output file.
Then you can read that list back without having to walk the file system again.::

  dwalk --output list.mfu /path/to/walk
  dwalk --input list.mfu

The default file format is a binary file intended for use in other tools, not humans, but one can ask for a text-based output::

 dwalk --text --output list.txt /path/to/walk

The text-based output is lossy, and it cannot be read back in to a tool.
If you want both, save to binary format first, then read the binary file to convert it to text.::

  dwalk --output list.mfu /path/to/walk
  dwalk --input list.mfu --text --output list.txt

dwalk also provides a sort option to order items in the list in various ways,
e.g., to order the list by username, then by access time::

  dwalk --input list.mfu --sort user,atime --output user_atime.mfu

To order items from largest to smallest number of bytes::

  dwalk --input list.mfu --sort '-size' --output big_to_small.mfu

dfind can be used to filter items with a string of find-like expressions,
e.g., files owned by user1 that are bigger than 100GB::

  dfind --input list.mfu --user user1 --size +100GB --output user1_over_100GB.mfu

dchmod is like chmod and chgrp in one, so one can change uid/gid/mode with a single command::

  dchmod --group grp1 --mode g+rw /path/to/walk

drm is like "rm -rf" but in parallel::

  drm /path/to/remove

dbcast provides an efficient way to broadcast a file to all compute nodes,
e.g., upload a tar file of a dataset to an SSD local to each compute node::

  dbcast /path/to/file.dat /ssd/file.dat

dsync is the recommended way to make a copy a large set of files::

  dsync /path/src /path/dest

For large directory trees, the --batch-files option offers a type of checkpoint.
It moves files in batches, and if interrupted, a restart picks up from the last completed batch.::

  dsync --batch-files 100000 /path/src /path/dest

The tools can be composed in various ways using the --input and --output options.
For example, the following sequence of commands executes a purge operation,
which deletes any file that has not been accessed in the past 180 days.::

  # walk directory to stat all files, record list in file
  dwalk --output list.mfu /path/to/walk

  # filter list to identify all regular files that were last accessed over 180 days ago
  dfind --input list.mfu --type f --atime +180 --output purgelist.mfu

  # delete all files in the purge list
  drm --input purgelist.mfu

