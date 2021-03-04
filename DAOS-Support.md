# DAOS Support

[DAOS](https://github.com/daos-stack/daos) is supported as a backend storage system in
dcp, dsync, and dcmp. Custom serialization and deserialization for DAOS
containers to and from a POSIX filesystem is provided with daos-serialize and
daos-deserialize. A DAOS container and its associated
metadata can be serialized to an HDF5 file for storage on a POSIX filesystem using daos-serialize. The container data
can be later restored to DAOS via the use of daos-deserialize.

The build instructions for enabling DAOS support can be found here:
[Enable DAOS](https://mpifileutils.readthedocs.io/en/latest/build.html#build-everything-directly).
The following are ways that DAOS can be used to move data both across DAOS as well as POSIX
filesystems in dcp and dsync:

1. DAOS  -> POSIX
2. POSIX -> DAOS
3. DAOS  -> DAOS

For the DAOS->DAOS case, both the DFS API and DAOS Object API are supported.

For daos-serialize and daos-deserialize, any type of DAOS container is supported.

## DAOS POSIX Data Movement Examples with dcp

#### Example One

Show a copy from a regular /tmp source path to a DAOS container. 
A DAOS Unified Namespace path is used as the destination, which allows you to lookup
the pool and container UUID from the path. This feature can only be used if the 
container is created with a path.

```shell
$ mpirun -np 3 dcp -v /tmp/$USER/s /tmp/$USER/conts/p1cont1
[2020-04-23T17:04:15]   Items: 6
[2020-04-23T17:04:15]   Directories: 3
[2020-04-23T17:04:15]   Files: 3
[2020-04-23T17:04:15]   Links: 0
```

#### Example Two

Show a copy where the pool and container UUID are passed in directly. This option
can be used if the type of container is POSIX, but the container was not created
with a path. The destination is the relative path within the DAOS container, which 
in this example is the root of the container. 

```shell
$ mpirun -np 3 dcp -v /tmp/$USER/s daos://$pool/$cont
[2020-04-23T17:17:51] Items: 6
[2020-04-23T17:17:51]   Directories: 3
[2020-04-23T17:17:51]   Files: 3
[2020-04-23T17:17:51]   Links: 0
```
#### Example Three

Show a copy from one DAOS container to another container that exists in the same
pool. A DAOS Unified Namespace path is used as the source and the destination.

```shell
$ mpirun -np 3 dcp -v /tmp/$USER/conts/p1cont1 /tmp/$USER/conts/p1cont2
[2020-04-23T17:04:15] Items: 6
[2020-04-23T17:04:15]   Directories: 3
[2020-04-23T17:04:15]   Files: 3
[2020-04-23T17:04:15]   Links: 0
```

#### Example Four

This example passes in the pool and container UUID directly. The source path
is the relative path within the DAOS container, which in this case is a subset of 
the DAOS container. 

```shell
$ mpirun -np 3 dcp -v daos://$pool1/$cont1/s/biggerfile daos://$pool2/$cont2
[2020-04-28T00:47:59] Items: 1
[2020-04-28T00:47:59]   Directories: 0
[2020-04-28T00:47:59]   Files: 1
[2020-04-28T00:47:59]   Links: 0
```

#### Example Five

This example copies data from a DAOS container to /tmp, where a DAOS
Unified Namespace path is used as the source. 

```shell
$ mpirun -np 3 dcp -v /tmp/$USER/conts/p1cont1 /tmp/$USER/d
[2020-04-23T17:17:51] Items: 6
[2020-04-23T17:17:51]   Directories: 3
[2020-04-23T17:17:51]   Files: 3
[2020-04-23T17:17:51]   Links: 0
```
## DAOS non-POSIX Data Movement Examples with dcp

#### Example One 

Show the copy from one DAOS container to another, where both of the DAOS
containers are of the POSIX type, but a user would like to copy DAOS
containers at the object level.

```shell
$ mpirun -np 3 dcp -v --daos-api=DAOS daos://$pool1/$p1cont1 daos://$pool1/$p1cont2
[2021-01-20T16:16:25] Successfully copied to DAOS Destination Container.
```
#### Example Two

Show the copy from one DAOS container to another, where both of the DAOS
containers are not of the POSIX type. This run does not require passing
in the --daos-api=DAOS flag as it will detect the containers as non-POSIX.

```shell
$ mpirun -np 3 dcp -v daos://$pool1/$p1cont1 daos://$pool1/$p1cont2
[2021-01-20T16:16:25] Successfully copied to DAOS Destination Container.
```

## DAOS POSIX Data Movement Examples with dsync

The usage for dsync is similar to the usage for dcp. The source and destination
can be DAOS, POSIX, or UNS paths.

#### Example One

Show the sync from one DAOS container to another, where both of the DAOS
containers are of the POSIX type, and both the source and destination use
a path relative to the root of each container.

```shell
$ mpirun -np 3 dsync -v daos://$pool1/$cont1/source daos://$pool2/$cont2/dest
[2020-04-28T00:47:59] Items: 1
[2020-04-28T00:47:59]   Directories: 0
[2020-04-28T00:47:59]   Files: 1
[2020-04-28T00:47:59]   Links: 0
```

#### Example Two

Show the sync from one DAOS container to another, where both of the DAOS
containers are not of the POSIX type.

```shell
$ mpirun -np 3 dsync -v daos://$pool1/$cont1 daos://$pool1/$cont2
[2021-03-04T19:14:17] Objects    : 6
[2021-03-04T19:14:17]   D-Keys   : 10
[2021-03-04T19:14:17]   A-Keys   : 14
[2021-03-04T19:14:17] Bytes read    : 4.000 MiB (4194660 bytes)
[2021-03-04T19:14:17] Bytes written : 4.000 MiB (4194660 bytes)
```

#### Example Three

Show the same sync from the previous example, but where the destination
already contains the source data. In this case, the data in the destination
is compared to the source, and nothing is written.

```shell
$ mpirun -np 3 dsync -v daos://$pool1/$cont1 daos://$pool1/$cont2
[2021-03-04T19:15:03] Objects    : 6
[2021-03-04T19:15:03]   D-Keys   : 10
[2021-03-04T19:15:03]   A-Keys   : 14
[2021-03-04T19:15:03] Bytes read    : 8.000 MiB (8389320 bytes)
[2021-03-04T19:15:03] Bytes written : 0.000 B (0 bytes)
```

## DAOS POSIX Data Comparison Examples with dcmp

Similar to dcp and dsync, dcmp supports DAOS, POSIX, and UNS paths,
but dcmp currently only supports POSIX-type DAOS containers.

#### Example One

Show the comparison between two DAOS containers, where both of the DAOS
containers are of the POSIX type, and both containers use a path relative
to the root of each container.

```shell
$ mpirun -np 3 dcmp -v daos://$pool1/$cont1/source daos://$pool2/$cont2/dest
Number of items that exist in both directories: 1 (Src: 1 Dest: 1)
Number of items that exist only in one directory: 0 (Src: 0 Dest: 0)
Number of items that exist in both directories and have the same type: 1 (Src: 1 Dest: 1)
Number of items that exist in both directories and have different types: 0 (Src: 0 Dest: 0)
Number of items that exist in both directories and have the same content: 1 (Src: 1 Dest: 1)
Number of items that exist in both directories and have different contents: 0 (Src: 0 Dest: 0)
```

## DAOS Serialization and Deserialization Examples

daos-serialize and daos-deserialize can be used on any type of DAOS container.
They are DAOS only tools that require HDF5.

daos-serialize will serialize a DAOS container to an HDF5 file. Depending on
the amount of data, multiple files may be written for each rank specified in the job.

daos-deserialize will deserialize or restore the HDF5 files written into a new
DAOS container, and a pool UUID to deserialize the data to must be specified.

#### Example One

Show the serialization of a DAOS container

```shell
$ mpirun -np 3 daos-serialize -v daos://$pool1/$cont1
Serializing Container to 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
Serializing Container to 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5
Serializing Container to 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5
```

#### Example Two

Show the serialization of a DAOS container by specifying output directory

```shell
$ mpirun -np 3 daos-serialize -v -o serialize daos://$pool1/$cont1
Serializing Container to serialize/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
Serializing Container to serialize/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5
Serializing Container to serialize/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5
```
#### Example Three 

Show the deserialization of an HDF5 file

```shell
$ mpirun -np 3 daos-deserialize -v --pool $pool1 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5 2-7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5

Successfully created container cbc52064-303e-497d-afaf-fa554c18e08f
    Deserializing filename: 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
    Deserializing filename: 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5
    Deserializing filename: 7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5
```

#### Example Four

Show the deserialization of HDF5 files from a specified directory

```shell
$ mpirun -np 3 daos-deserialize -v serialize

Successfully created container 8d6a6083-4009-4afe-8364-7caa5ebaa72b
    Deserializing filename: serialize/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank0.h5
    Deserializing filename: serialize/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank1.h5
    Deserializing filename: serialize/7bf8037d-823f-4fa5-ac2a-c2cae8f81f57_rank2.h5
```
