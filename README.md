# mpiFileUtils
mpiFileUtils provides both a library called [libmfu](src/common/README.md) and a suite of MPI-based tools to manage large datasets, which may vary from large directory trees to large files. High-performance computing users often generate large datasets with parallel applications that run with many processes (millions in some cases). However those users are then stuck with single-process tools like cp and rm to manage their datasets. This suite provides MPI-based tools to handle typical jobs like copy, remove, and compare for such datasets, providing speedups of up to 20-30x.  It also provides a library that simplifies the creation of new tools or can be used in applications.

Documentation is available on [ReadTheDocs](http://mpifileutils.readthedocs.io).

## Docker Build

This repository includes a multi-stage [Dockerfile](Dockerfile) that builds
mpiFileUtils and its core dependencies (libcircle, lwgrp, dtcmp) and then
copies the built artifacts into a smaller runtime image.

### Prebuilt images

[.github/workflows/docker.yml](.github/workflows/docker.yml) builds the runtime
image and publishes it to the GitHub Container Registry under whichever
repository the workflow runs in, so a fork publishes to its own namespace:

```bash
docker pull ghcr.io/jeking3/mpifileutils:latest
docker run --rm ghcr.io/jeking3/mpifileutils:latest dcp --help
```

`main` publishes `:main` and `:latest`, a release tag such as `v1.2.3`
publishes `:1.2.3` and `:1.2`, and every build is additionally tagged
`:sha-<short-commit>`.  Branches under `feature/` publish under their own
branch name (`:feature-docker` and so on), so work in progress never moves
`:latest`.  Pull requests build and smoke test the image without publishing it.
Publishing uses the workflow's own `GITHUB_TOKEN`, so no registry secrets need
to be configured.

### Setting build options

All CMake settings (build type, feature flags, compiler flags) are controlled
through a **CMake initial-cache file** located at
[cmake/build-options.cmake](cmake/build-options.cmake).  That file is
loaded with `cmake -C` before `CMakeLists.txt` is read, so it can set any
CMake cache variable — `CMAKE_BUILD_TYPE`, `CMAKE_C_FLAGS`,
`CMAKE_EXE_LINKER_FLAGS`, feature toggles, and so on.

Build with the shipped defaults (`Release`, all optional features off):

```bash
docker build -t mpifileutils:local .
```

To customize the build — for example from a GitLab pipeline in another
repository — copy your options file over the default before building:

```bash
# In your pipeline (other repo):
git clone <this-repo-url> mpifileutils
cp my-build-options.cmake mpifileutils/cmake/build-options.cmake
docker build -t mpifileutils:custom mpifileutils/
```

Run an interactive shell with the tools on your `PATH`:

```bash
docker run --rm -it mpifileutils:local
```

Run one of the tools directly:

```bash
docker run --rm mpifileutils:local dcp --help
```

If you need to run `mpirun` inside the container as root, Open MPI typically
requires additional environment flags:

```bash
docker run --rm -it \
  -e OMPI_ALLOW_RUN_AS_ROOT=1 \
  -e OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 \
  mpifileutils:local mpirun -np 2 dfind --help
```

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
