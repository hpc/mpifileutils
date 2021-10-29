# DAOS Support


[DAOS](https://github.com/daos-stack/daos) is supported as a backend
storage system in the mpiFileUtils commands dcp, dsync, and dcmp.
The build instructions for enabling DAOS support in mpiFileUtils
can be found here:
[Enable DAOS](https://mpifileutils.readthedocs.io/en/latest/build.html#build-everything-directly).

With dcp and dsync, the DAOS backend can be used to move data across
two DAOS containers of the same type, as well as between a DAOS
POSIX container and a (non-DAOS) POSIX filesystem like Lustre or GPFS:

1. DAOS POSIX container -> POSIX FS
2. POSIX FS -> DAOS POSIX container
3. DAOS POSIX container  -> DAOS POSIX container
4. DAOS non-POSIX container  -> DAOS non-POSIX container

When the source or the destination is a (non-DAOS) POSIX filesystem,
the DAOS File System (DFS) API will be used.
When both the source and the destination are DAOS POSIX containers,
both the DFS API and the DAOS Object API are supported to perform
the copy/sync operations (by default, the DFS API will be used).
When both the source and the destination are DAOS non-POSIX containers
(of the same type),
the DAOS Object API will be used to perform the copy/sync operations.

When DAOS and HDF5 support is enabled, mpiFileUtils also provides the
daos-serialize and daos-deserialize commands.
These commands can be used to save the contents of a DAOS container
and its associated metadata into a POSIX filesystem,
and to restore that DAOS container with its associated metadata
back into a DAOS storage system at a later time.
This functionality is also known as "container parking",
and it is supported for any type of DAOS container.
The typical use case is the "archiving" of a DAOS container to a
lower cost, higher capacity storage system
to free up space in the DAOS storage system for other projects.
This step is performed with the daos-serialze command.
When the DAOS container needs to be accessed again, it can be re-established
in the DAOS storage system by using the daos-deserialize command.
The daos-serialize and daos-deserialize commands are MPI-parallel
applications.
They use HDF5 to write and read the DAOS container data and metadata
as "opaque" HDF5 files, one HDF5 file per MPI rank.
In order to use these commands, HDF5 needs to be installed and
mpiFileUtils needs to be built with both DAOS support and HDF5 support.
See the above build instructions for details.



## Data Movement Examples with dcp, using the DFS API

#### Example One

Perform a copy from a regular /tmp source path to a DAOS POSIX container.
A DAOS Unified Namespace (DUNS) path is used as the destination,
which enables the tool to look up the pool UUID and container UUID from
the DUNS path. This feature can only be used if the
container has been created with the ` --path=` option
to store the container information as extended attributes in a DUNS path.

Note: The DUNS path should be a directory in a globally accessible filesystem.
All nodes in the MPI job that execute the dcp command will need to
extract the DAOS pool and container information.
If the path given to the `daos cont create` command is in a local
 filesystem on the node that runs this daos command
(like a node-local /tmp), that information will only be visible locally
on that node and the DUNS lookup on other nodes will fail.

```shell
$ mpirun -np 3 dcp -v /tmp/$USER/s /lustrefs/$USER/conts/p1cont1
[2020-04-23T17:04:15]   Items: 6
[2020-04-23T17:04:15]   Directories: 3
[2020-04-23T17:04:15]   Files: 3
[2020-04-23T17:04:15]   Links: 0
```

#### Example Two

Perform a copy from a POSIX filesystem to a DAOS POSIX container,
where the destination pool and container UUIDs are passed in directly.
This option can be used if the type of the DAOS container is POSIX,
but the container was not created with a DUNS path.
The destination is a relative path within the DAOS container, which
in this example is the root of the container.

```shell
$ mpirun -np 3 dcp -v /tmp/$USER/s daos://$pool/$cont
[2020-04-23T17:17:51] Items: 6
[2020-04-23T17:17:51]   Directories: 3
[2020-04-23T17:17:51]   Files: 3
[2020-04-23T17:17:51]   Links: 0
```

#### Example Three

Perform a copy from one DAOS POSIX container to another DAOS POSIX container.
The two containers can reside in the same pool, or in different pools.
In this example, a DAOS Unified Namespace path is used as the source and
as the destination.
The complete source container will be copied to the destination.

```shell
$ mpirun -np 3 dcp -v /lustrefs/$USER/conts/p1cont1 /lustrefs/$USER/conts/p1cont2
[2020-04-23T17:04:15] Items: 6
[2020-04-23T17:04:15]   Directories: 3
[2020-04-23T17:04:15]   Files: 3
[2020-04-23T17:04:15]   Links: 0
```

#### Example Four

Perform a copy from one DAOS POSIX container to another DAOS POSIX container.
The two containers can reside in the same pool, or in different pools.
This example passes in the pool and container UUIDs directly. The source path
is a relative path within the DAOS container, which in this case is a subdirectory within
the source DAOS container. Only this subdirectory tree will be copied to the destination.

```shell
$ mpirun -np 3 dcp -v daos://$pool1/$cont1/s/biggerfile daos://$pool2/$cont2
[2020-04-28T00:47:59] Items: 1
[2020-04-28T00:47:59]   Directories: 0
[2020-04-28T00:47:59]   Files: 1
[2020-04-28T00:47:59]   Links: 0
```

#### Example Five

This example copies data from a DAOS POSIX container to /tmp,
where a DAOS Unified Namespace path is used as the source.

```shell
$ mpirun -np 3 dcp -v /lustrefs/$USER/conts/p1cont1 /tmp/$USER/d
[2020-04-23T17:17:51] Items: 6
[2020-04-23T17:17:51]   Directories: 3
[2020-04-23T17:17:51]   Files: 3
[2020-04-23T17:17:51]   Links: 0
```


## Data Movement Examples with dcp, using the DAOS API

#### Example One

Perform a copy from one DAOS POSIX container to another DAOS POSIX container.
The two containers can reside in the same pool, or in different pools.
in this example, the user requests to copy the DAOS POSIX container
using the DAOS object level API instead of the DFS API.

```shell
$ mpirun -np 3 dcp -v --daos-api=DAOS daos://$pool1/$p1cont1 daos://$pool1/$p1cont2
[2021-01-20T16:16:25] Successfully copied to DAOS Destination Container.
```
#### Example Two

Perform a copy from one DAOS container to another DAOS container,
where both DAOS containers are *not* of type POSIX.
Both containers must be of the same type.
This scenario does not need the `--daos-api=DAOS` option:
It will be automatically detected that the DAOS containers are of a non-POSIX
container type, and thus the DAOS object API has to be used.

```shell
$ mpirun -np 3 dcp -v daos://$pool1/$p1cont1 daos://$pool1/$p1cont2
[2021-01-20T16:16:25] Successfully copied to DAOS Destination Container.
```


## Data Movement Examples with dsync

The usage for dsync is similar to the usage for dcp.
The source and destination can be DAOS, POSIX, or DUNS paths.

#### Example One

Perform the sync from one DAOS container to another, where both of the DAOS
containers are of type POSIX. In this example, both the source and
the destination use a path relative to the root of each container.

```shell
$ mpirun -np 3 dsync -v daos://$pool1/$cont1/source daos://$pool2/$cont2/dest
[2020-04-28T00:47:59] Items: 1
[2020-04-28T00:47:59]   Directories: 0
[2020-04-28T00:47:59]   Files: 1
[2020-04-28T00:47:59]   Links: 0
```

#### Example Two

Perform the sync from one DAOS container to another, where both of the DAOS
containers are *not* of type POSIX. Both containers must be of the same type.

```shell
$ mpirun -np 3 dsync -v daos://$pool1/$cont1 daos://$pool1/$cont2
[2021-03-04T19:14:17] Objects    : 6
[2021-03-04T19:14:17]   D-Keys   : 10
[2021-03-04T19:14:17]   A-Keys   : 14
[2021-03-04T19:14:17] Bytes read    : 4.000 MiB (4194660 bytes)
[2021-03-04T19:14:17] Bytes written : 4.000 MiB (4194660 bytes)
```

#### Example Three

Perform the same sync as in Example Two, but here the destination container
already contains the same data as the source container.
In this case the data in the destination is compared to the source,
and nothing needs to be written.

```shell
$ mpirun -np 3 dsync -v daos://$pool1/$cont1 daos://$pool1/$cont2
[2021-03-04T19:15:03] Objects    : 6
[2021-03-04T19:15:03]   D-Keys   : 10
[2021-03-04T19:15:03]   A-Keys   : 14
[2021-03-04T19:15:03] Bytes read    : 8.000 MiB (8389320 bytes)
[2021-03-04T19:15:03] Bytes written : 0.000 B (0 bytes)
```


## Data Comparison Examples with dcmp

Similar to dcp and dsync, dcmp supports DAOS, POSIX, and UNS paths.
dcmp currently only supports DAOS containers of type POSIX.

#### Example One

Perform the comparison between two DAOS containers, where both of the DAOS
containers are of type POSIX. In this example, both containers use a
path relative to the root of each container.

```shell
$ mpirun -np 3 dcmp -v daos://$pool1/$cont1/source daos://$pool2/$cont2/dest
Number of items that exist in both directories: 1 (Src: 1 Dest: 1)
Number of items that exist only in one directory: 0 (Src: 0 Dest: 0)
Number of items that exist in both directories and have the same type: 1 (Src: 1 Dest: 1)
Number of items that exist in both directories and have different types: 0 (Src: 0 Dest: 0)
Number of items that exist in both directories and have the same content: 1 (Src: 1 Dest: 1)
Number of items that exist in both directories and have different contents: 0 (Src: 0 Dest: 0)
```

## DAOS Container Serialization and Deserialization Examples

daos-serialize and daos-deserialize can be used on any type of DAOS container.
They are DAOS only tools that require HDF5.

daos-serialize will serialize a DAOS container to one or more HDF5 files.
Depending on the amount of data, multiple files may be written
for each rank specified in the job.

daos-deserialize will deserialize or restore the HDF5 files that have been
previously created by daos-serialize. A pool UUID to deserialize the data to
must be specified, and a container with the original data and metadata
will be created in that DAOS pool.

#### Example One

Perform the serialization of a DAOS container into a set of HDF5 files.
In this example, the HDF5 files are stored in the current working directory
within a global parallel filesystem.

```shell
$ mpirun -np 3 daos-serialize -v daos://$pool1/$cont1
Serializing Container to 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
Serializing Container to 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5
Serializing Container to 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5
```

#### Example Two

Perform the serialization of a DAOS container into a set of HDF5 files.
In this example, a directory in which to store the HDF5 files is specified.

```shell
$ mpirun -np 3 daos-serialize -v -o serialized-cont daos://$pool1/$cont1
Serializing Container to serialized-cont/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
Serializing Container to serialized-cont/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5
Serializing Container to serialized-cont/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5
```

#### Example Three

Perform the deserialization of a DAOS container that was previously serialized
into a number of HDF5 files. In this example, the HDF5 files are individually
specified on the command line.

```shell
$ mpirun -np 3 daos-deserialize -v --pool $pool1 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5 2-7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5

Successfully created container cbc52064-303e-497d-afaf-fa554c18e08f
    Deserializing filename: 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
    Deserializing filename: 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5
    Deserializing filename: 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5
```

#### Example Four

Perform the deserialization of a DAOS container that was previously serialized
into a number of HDF5 files. In this example, the directory in which 
he HDF5 files are residing is specified on the command line.

```shell
$ mpirun -np 3 daos-deserialize -v serialized-cont

Successfully created container 8d6a6083-4009-4afe-8364-7caa5ebaa72b
    Deserializing filename: serialized-cont/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
    Deserializing filename: serialized-cont/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5
    Deserializing filename: serialized-cont/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5
```
