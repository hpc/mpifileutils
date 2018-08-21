# mpiFileUtils
mpiFileUtils provides both a library called [libmfu](src/common/README.md) and a suite of MPI-based tools to manage large datasets, which may vary from large directory trees to large files. High-performance computing users often generate large datasets with parallel applications that run with many processes (millions in some cases). However those users are then stuck with single-process tools like cp and rm to manage their datasets. This suite provides MPI-based tools to handle typical jobs like copy, remove, and compare for such datasets, providing speedups of up to 20-30x.  It also provides a library that simplifies the creation of new tools or can be used in applications.

Documentation is available on [ReadTheDocs](http://mpifileutils.readthedocs.io) and in this repo.

## Utilities
The tools in mpiFileUtils are actually MPI applications.
They must be launched as MPI applications, e.g., within a compute allocation on a cluster using mpirun.
The tools do not currently checkpoint,
so one must be careful that an invocation of the tool has sufficient time to complete before it is killed.
Example usage of each tool is provided below.

 - [dbcast](doc/rst/dbcast.1.rst) - Broadcast files to compute nodes.
 - [dchmod](doc/rst/dchmod.1.rst) - Change owner, group, and permissions on files.
 - [dcmp](doc/rst/dcmp.1.rst) - Compare files.
 - [dcp](doc/rst/dcp.1.rst) - Copy files.
 - [ddup](doc/rst/ddup.1.rst) - Find duplicate files.
 - [dfilemaker](doc/rst/dfilemaker.1.rst) - Generate random files.
 - [drm](doc/rst/drm.1.rst) - Remove files.
 - [dstripe](doc/rst/dstripe.1.rst) - Restripe files.
 - [dsync](doc/rst/dsync.1.rst) - Synchronize files.
 - [dwalk](doc/rst/dwalk.1.rst) - List files.

## Experimental Utilities
Experimental utilities are under active development.
They are not considered to be production worthy, but they are available in the distribution
for those interested in developing them further or to provide additional examples.
To enable experimental utilities, run configure with the enable experimental option.

    ./configure --enable-experimental

 - dbz2 - Compress a file with bz2.
 - dfind - Search for files in parallel.
 - dgrep - Run grep on files in parallel.
 - dparallel - Perform commands in parallel.
 - dsh - List and remove files with interactive commands.
 - dtar - Create file tape archives.

## libmfu
Functionality that is common to multiple tools is moved to the common library, libmfu.
This goal of this library is to make it easy to develop new tools and to provide consistent behavior across tools in the suite.
The library can also be useful to end applications, e.g.,
to efficiently create or remove a large directory tree in a portable way across different parallel file systems.
For documentation on how to use this library, see [libmfu](src/common/README.md).

## Build
mpiFileUtils depends on several libraries.
mpiFileUtils is available in [Spack](https://spack.io/), which simplifies the install to just:

    spack install mpifileutils

or to enable all features:

    spack install mpifileutils +lustre +experimental

To build from a release tarball, there are two scripts: buildme\_dependencies and buildme.  The buildme\_dependencies script downloads and installs all the necessary libraries.  The buildme script then builds mpiFileUtils assuming the libraries have been installed.  Both scripts require that mpicc is in your path, and that it is for an MPI library that supports at least v2.2 of the MPI standard.  Please review each buildme script, and edit if necessary.  Then run them in sequence:

    ./buildme_dependencies
    ./buildme

To build from a clone, it may also be necessary to first run the buildme\_autotools script to obtain the required set of autotools, then use buildme\_dev instead of the buildme script:

    ./buildme_autotools
    ./buildme_dependencies
    ./buildme_dev

## Project Design Principles
The following principles drive design decisions in the project.

### Scale
The library and tools should be designed such that running with more processes increases performance,
provided there are sufficient data and parallelism available in the underlying file systems.
The design of the tool should not impose performance scalability bottlenecks.

### Performance
While it is tempting to mimic the interface, behavior, and file formats of familiar tools like cp, rm, and tar,
when forced with a choice between compatibility and performance, mpiFileUtils chooses performance.
For example, if an archive file format requires serialization that inhibits parallel performance,
mpiFileUtils will opt to define a new file format that enables parallelism rather than being constrained to existing formats.
Similarly, options in the tool command line interface may have different semantics from familiar tools
in cases where performance is improved.
Thus, one should be careful to learn the options of each tool.

### Portability
The tools are intended to support common file systems used in HPC centers, like Lustre, GPFS, and NFS.
Additionally, methods in the library should be portable and efficient across multiple file systems.
Tool and library users can rely on mpiFileUtils to provide portable and performant implementations.

### Composability
While the tools do not support chaining with Unix pipes,
they do support interoperability through input and output files.
One tool may process a dataset and generate an output file that another tool can read as input,
e.g., to walk a directory tree with one tool, filter the list of file names with another, and perhaps delete a subset of matching files with a third.
Additionally, when logic is deemed to be useful across multiple tools or is anticipated to be useful in future tools or applications,
it should be provided in the common library.

## Contributors
We welcome contributions to the project.  For details on how to help, see our [Contributor Guide](.github/CONTRIBUTORS.md)

### Copyrights

Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
  Produced at the Lawrence Livermore National Laboratory
  Written by Adam Moody <moody20@llnl.gov>.
  CODE-673838

Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
  (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)

Copyright (2013-2015) UT-Battelle, LLC under Contract No.
DE-AC05-00OR22725 with the Department of Energy.

Copyright (c) 2015, DataDirect Networks, Inc.

All rights reserved.

## Build Status
The current status of the mpiFileUtils master branch is [![Build Status](https://travis-ci.org/hpc/mpifileutils.png?branch=master)](https://travis-ci.org/hpc/mpifileutils).
