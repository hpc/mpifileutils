# mpiFileUtils
mpiFileUtils is a suite of MPI-based tools to manage large datasets, which may vary from large directory trees to large files. High-performance computing users often generate large datasets with parallel applications that run with many processes (millions in some cases). However those users are then stuck with single-process tools like cp and rm to manage their datasets. This suite provides MPI-based tools to handle typical jobs like copy, remove, and compare for such datasets, providing speedups of up to 20-30x.

 - [dchmod](doc/markdown/dchmod.1.md) - Change permissions and group access on files.
 - [dcmp](doc/markdown/dcmp.1.md) - Compare files.
 - [dcp](doc/markdown/dcp.1.md) - Copy files.
 - [dfilemaker](doc/markdown/dfilemaker.1.md) - Generate random files.
 - [drm](doc/markdown/drm.1.md) - Remove files.
 - [dwalk](doc/markdown/dwalk.1.md) - List files.

## Experimental Utilities
To enable experimental utilities, run configure with the enable experimental option.

  ./configure --enable-experimental

 - [dfind](doc/markdown/dfind.1.md) - Find files by path name (experimental).
 - [dgrep](doc/markdown/dgrep.1.md) - Search contents of files (experimental).
 - [dparallel](doc/markdown/dparallel.1.md) - Perform commands in parallel (experimental).
 - [dtar](doc/markdown/dtar.1.md) - Create file tape archives (experimental).

## Build Status
The current status of the fileutils master branch is [![Build Status](https://travis-ci.org/hpc/fileutils.png?branch=master)](https://travis-ci.org/hpc/fileutils).
