==============================
Build
==============================

mpiFileUtils and its dependencies can be installed with CMake or Spack.
Several build variations are described in this section:

- CMake
- Spack
- development build with CMake
- development build with Spack

-------------------------
CMake
-------------------------

mpiFileUtils requires CMake 3.1 or higher.
Before running cmake, ensure that the MPI wrapper scripts like mpicc are loaded in your environment.

The simplest mpiFileUtils install for many users is to build from a release package.
This packages the source for a specific version of mpiFileUtils along with the corresponding source for several of its dependencies in a single tarball.
mpiFileUtils release packages are available as attachments from their respective GitHub Releases page:

https://github.com/hpc/mpifileutils/releases

mpiFileUtils optionally depends on libarchive, version 3.5.1.
If new enough, the system install of libarchive may be sufficient,
though even newer versions may be incompatible with the required version.
To be certain of compatibility, it is recommended that one install libarchive-3.5.1 with commands like the following

.. code-block:: Bash

   #!/bin/bash
   mkdir install
   installdir=`pwd`/install

   wget https://github.com/libarchive/libarchive/releases/download/v3.5.1/libarchive-3.5.1.tar.gz
   tar -zxf libarchive-3.5.1.tar.gz
   cd libarchive-3.5.1
     ./configure --prefix=$installdir
     make install
   cd ..

To build on PowerPC, one may need to add :code:`--build=powerpc64le-redhat-linux-gnu` to the configure command.

Assuming libarchive has been installed to an `install` directory as shown above,
one can then build mpiFileUtils from a release like v0.12 with commands like the following:

.. code-block:: Bash

   wget https://github.com/hpc/mpifileutils/releases/download/v0.12/mpifileutils-v0.12.tgz
   tar -zxf mpifileutils-v0.12.tgz
   cd mpifileutils-v0.12
     mkdir build
     cd build
       cmake .. \
         -DWITH_LibArchive_PREFIX=../../install \
         -DCMAKE_INSTALL_PREFIX=../../install
       make -j install
     cd ..
   cd ..

Additional CMake options:

* :code:`-DENABLE_LIBARCHIVE=[ON/OFF]` : use libarchive and build tools requiring libarchive like dtar, defaults to :code:`ON`
* :code:`-DENABLE_XATTRS=[ON/OFF]` : use extended attributes and libattr, defaults to :code:`ON`
* :code:`-DENABLE_LUSTRE=[ON/OFF]` : specialization for Lustre, defaults to :code:`OFF`
* :code:`-DENABLE_GPFS=[ON/OFF]` : specialization for GPFS, defaults to :code:`OFF`
* :code:`-DENABLE_HPSS=[ON/OFF]` : specialization for HPSS, defaults to :code:`OFF`
* :code:`-DENABLE_EXPERIMENTAL=[ON/OFF]` : build experimental tools, defaults to :code:`OFF`

-------------------------------------------
DAOS support
-------------------------------------------

To build with DAOS support, first install mpiFileUtils dependencies as mentioned above,
and also make sure DAOS is installed. If DAOS is installed under a standard
system path then specifying the DAOS path with :code:`-DWITH_DAOS_PREFIX` is unnecessary.

.. code-block:: Bash

   cmake ../mpifileutils \
     -DCMAKE_INSTALL_PREFIX=../install \
     -DWITH_DAOS_PREFIX=</path/to/daos/> \
     -DENABLE_DAOS=ON
   make -j install

Some DAOS-enabled tools require HDF5.
To use the `daos-serialize` and `daos-deserialize` tools, HDF5 1.2+ is required.
To copy HDF5 containers with `dcp`, HDF5 1.8+ is required, along with the daos-vol.

To build with HDF5 support, add the following flags during CMake.
If HDF5 is installed under a standard system path then specifying the HDF5 path with :code:`-DWITH_HDF5_PREFIX` is unnecessary.

.. code-block:: Bash

   -DENABLE_HDF5=ON \
   -DWITH_HDF5_PREFIX=</path/to/hdf5>

---------------------------
Spack
---------------------------

To use `Spack <https://github.com/spack/spack>`_, it is recommended that one first create a `packages.yaml` file to list system-provided packages, like MPI.
Without doing this, Spack will fetch and install an MPI library that may not work on your system.
Make sure that you've set up Spack in your shell (see `these instructions <https://spack.readthedocs.io/en/latest/getting_started.html>`_).

Once Spack has been configured, mpiFileUtils can be installed as:

.. code-block:: Bash

    spack install mpifileutils

or to enable all features:

.. code-block:: Bash

    spack install mpifileutils +lustre +gpfs +hpss +experimental

----------------------------
Development build with CMake
----------------------------

To make changes to mpiFileUtils, one may wish to build from a clone of the repository.
This requires that one installs the mpiFileUtils dependencies separately,
which can be done with the following commands:

.. code-block:: Bash

   #!/bin/bash
   mkdir install
   installdir=`pwd`/install

   mkdir deps
   cd deps
     wget https://github.com/hpc/libcircle/releases/download/v0.3/libcircle-0.3.0.tar.gz
     wget https://github.com/llnl/lwgrp/releases/download/v1.0.4/lwgrp-1.0.4.tar.gz
     wget https://github.com/llnl/dtcmp/releases/download/v1.1.4/dtcmp-1.1.4.tar.gz
     wget https://github.com/libarchive/libarchive/releases/download/v3.5.1/libarchive-3.5.1.tar.gz

     tar -zxf libcircle-0.3.0.tar.gz
     cd libcircle-0.3.0
       ./configure --prefix=$installdir
       make install
     cd ..

     tar -zxf lwgrp-1.0.4.tar.gz
     cd lwgrp-1.0.4
       ./configure --prefix=$installdir
       make install
     cd ..

     tar -zxf dtcmp-1.1.4.tar.gz
     cd dtcmp-1.1.4
       ./configure --prefix=$installdir --with-lwgrp=$installdir
       make install
     cd ..

     tar -zxf libarchive-3.5.1.tar.gz
     cd libarchive-3.5.1
       ./configure --prefix=$installdir
       make install
     cd ..
   cd ..

One can then clone, build, and install mpiFileUtils:

.. code-block:: Bash

   git clone https://github.com/hpc/mpifileutils
   mkdir build
   cd build
   cmake ../mpifileutils \
     -DWITH_DTCMP_PREFIX=../install \
     -DWITH_LibCircle_PREFIX=../install \
     -DWITH_LibArchive_PREFIX=../install \
     -DCMAKE_INSTALL_PREFIX=../install
   make -j install

The same CMake options as described in earlier sections are available.

----------------------------
Development build with Spack
----------------------------

One can also build from a clone of the mpiFileUtils repository
after using Spack to install its dependencies via the `spack.yaml` file that is distributed with mpiFileUtils.
From the root directory of mpiFileUtils, run the command `spack find` to determine which packages Spack will install.
Next, run `spack concretize` to have Spack perform dependency analysis.
Finally, run `spack install` to build the dependencies.

There are two ways to tell CMake about the dependencies.
First, you can use `spack load [depname]` to put the installed dependency into your environment paths.
Then, at configure time, CMake will automatically detect the location of these dependencies.
Thus, the commands to build become:

.. code-block:: Bash

   git clone https://github.com/hpc/mpifileutils
   mkdir build install
   cd mpifileutils
   spack install
   spack load dtcmp
   spack load libcircle
   spack load libarchive
   cd ../build
   cmake ../mpifileutils

The other way to use Spack is to create a "view" to the installed dependencies.
Details on this are coming soon.
