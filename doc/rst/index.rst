.. mpiFileUtils documentation master file, created by
   sphinx-quickstart on Fri Apr 13 11:47:51 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

===================================
**Documentation for mpiFileUtils**
===================================

Overview
*************

mpiFileUtils provides both a library called libmfu and a suite of MPI-based
tools to manage large datasets, which may vary from large directory trees to
large files. High-performance computing users often generate large datasets with
parallel applications that run with many processes (millions in some cases).
However those users are then stuck with single-process tools like cp and rm to
manage their datasets. This suite provides MPI-based tools to handle typical
jobs like copy, remove, and compare for such datasets, providing speedups of up
to 50x. It also provides a library that simplifies the creation of new tools
or can be used in applications

Utilities
***************

The tools in mpiFileUtils are actually MPI applications. They must be launched
as MPI applications, e.g., within a compute allocation on a cluster using
mpirun. The tools do not currently checkpoint, so one must be careful that an
invocation of the tool has sufficient time to complete before it is killed.
Example usage of each tool is provided below.

- dbcast - Broadcast files to compute nodes.
- dchmod - Change owner, group, and permissions on files.
- dcmp - Compare files.
- dcp - Copy files.
- ddup - Find duplicate files.
- dfilemaker - Generate random files.
- drm - Remove files.
- dstripe - Restripe files.
- dsync - Synchronize files
- dwalk - List files.

Experimental Utilities
***************************

Experimental utilities are under active development. They are not considered to
be production worthy, but they are available in the distribution for those
interested in developing them further or to provide additional examples. To
enable experimental utilities, run configure with the enable experimental
option.

.. code-block:: Bash

    $ ./configure --enable-experimental

- dbz2 - Compress a file with bz2.
- dfind - Search for files in parallel.
- dgrep - Run grep on files in parallel.
- dparallel - Perform commands in parallel. experimental/dparallel.1
- dsh - List and remove files with interactive commands.
- dtar - Create file tape archives.

User Guide
***************************

.. toctree::
   :maxdepth: 3

   build.rst
   proj-design.rst
   libmfu.rst

Man Pages
***************************

.. toctree::
   :maxdepth: 2

   dbcast.1
   dbz2.1
   dchmod.1
   dcmp.1
   dcp.1
   ddup.1
   dfilemaker.1
   drm.1
   dstripe.1
   dsync.1
   dwalk.1
   dfind.1
   experimental/dgrep.1
   experimental/dparallel.1
   experimental/dtar.1

Indices and tables
*********************

* :ref:`genindex`
* :ref:`search`
