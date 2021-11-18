# mpiFileUtils release tarball
The builddist script creates an mpiFileUtils release tarball.

```bash
    ./builddist main
```

This tarball is added as a binary attachment to the corresponding mpiFileUtils release page.
This contains source for mpiFileUtils, LWGRP, DTCMP, and libcircle.
It also contains a set of top-level CMake files that compiles all source files into a single libmfu library.

# Steps to add a new release
To add a new release:
1. Define new CMake files if needed (TODO: support multiple versions of top-level CMake files)
2. Edit builddist to define the appropriate tags for all packages
3. Run builddist for appropriate tag
4. Test build and run with resulting tarball
5. Attach tarball to github release page (attach binary)
6. Update mpiFileUtils readthedocs to describe builds from this tarball
