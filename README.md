# mpiFileUtils
mpiFileUtils provides both a library called [libmfu](src/common/README.md) and a suite of MPI-based tools to manage large datasets, which may vary from large directory trees to large files. High-performance computing users often generate large datasets with parallel applications that run with many processes (millions in some cases). However those users are then stuck with single-process tools like cp and rm to manage their datasets. This suite provides MPI-based tools to handle typical jobs like copy, remove, and compare for such datasets, providing speedups of up to 20-30x.  It also provides a library that simplifies the creation of new tools or can be used in applications.

Documentation is available on [ReadTheDocs](http://mpifileutils.readthedocs.io).

## DAOS Support

mpiFileUtils supports a DAOS backend for dcp, dsync, and dcmp. Custom serialization and deserialization for DAOS containers to and from a POSIX filesystem is provided with daos-serialize and daos-deserialize. Details and usage examples are provided in [DAOS Support](DAOS-Support.md).
 
## Contributors
We welcome contributions to the project.  For details on how to help, see our [Contributor Guide](CONTRIBUTING.md)

### Copyrights

Copyright (c) 2013-2015, Lawrence Livermore National Security, LLC.
  Produced at the Lawrence Livermore National Laboratory
  CODE-673838

Copyright (c) 2006-2007,2011-2015, Los Alamos National Security, LLC.
  (LA-CC-06-077, LA-CC-10-066, LA-CC-14-046)

Copyright (2013-2015) UT-Battelle, LLC under Contract No.
DE-AC05-00OR22725 with the Department of Energy.

Copyright (c) 2015, DataDirect Networks, Inc.

All rights reserved.

## Build Status
The current status of the mpiFileUtils master branch is [![Build Status](https://travis-ci.org/hpc/mpifileutils.png?branch=master)](https://travis-ci.org/hpc/mpifileutils).
