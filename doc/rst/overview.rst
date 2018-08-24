================
Overview
================

mpiFileUtils provides both a library called libmfu and a suite of MPI-based
tools to manage large datasets, which may vary from large directory trees to
large files. High-performance computing users often generate large datasets with
parallel applications that run with many processes (millions in some cases).
However those users are then stuck with single-process tools like cp and rm to
manage their datasets. This suite provides MPI-based tools to handle typical
jobs like copy, remove, and compare for such datasets, providing speedups of up
to 50x. It also provides a library that simplifies the creation of new tools
or can be used in applications

---------------------------
Utilities
---------------------------

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
    - dsync - Synchronize files.
    - dwalk - List files.
