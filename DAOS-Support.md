# DAOS Support

[DAOS](https://github.com/daos-stack/daos) is supported as a backend storage system in dcp. The build instructions for
enabling DAOS support can be found here:
[Enable DAOS](https://mpifileutils.readthedocs.io/en/latest/build.html#build-everything-directly).
The following are ways that DAOS can be used to move data both across DAOS as well as POSIX
filesystems:

1. DAOS  -> POSIX
2. POSIX -> DAOS
3. DAOS  -> DAOS

## DAOS Data Movement Use Cases

In each use case, it is assumed the the pools used already exist. Also,
only one DAOS source is supported.

1. **DAOS Destination**
    * POSIX container may or may not exist already

2. **DAOS Source**
    * POSIX source container exists

3. **DAOS Source and Destination**
    * Copy across two different pools
    * Copy across containers in the same pool

## DAOS Data Movement Examples

#### Example One

Show a copy from a regular /tmp source path to a DAOS container. 
A DAOS Unified Namespace path is used as the destination, which allows you to lookup
the pool and container UUID from the path. This feature can only be used if the 
container is created with a path.

```shell
$ mpirun -np 3 --daos-dst-svcl 0 -v /tmp/$USER/s /tmp/$USER/conts/p1cont1
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
$ mpirun -np 3 dcp --daos-dst-svcl 0 -v --daos-dst-pool $pool --daos-dst-cont $p1cont1 /tmp/$USER/s /
[2020-04-23T17:17:51] Items: 6
[2020-04-23T17:17:51]   Directories: 3
[2020-04-23T17:17:51]   Files: 3
[2020-04-23T17:17:51]   Links: 0
```
#### Example Three

Show a copy from one DAOS container to another container that exists in the same
pool. A DAOS Unified Namespace path is used as the source and the destination.

```shell
$ mpirun -np 3 dcp --daos-src-svcl 0 --daos-dst-svcl 0 -v /tmp/$USER/conts/p1cont1 /tmp/$USER/conts/p1cont2
[2020-04-23T17:04:15] Items: 6
[2020-04-23T17:04:15]   Directories: 3
[2020-04-23T17:04:15]   Files: 3
[2020-04-23T17:04:15]   Links: 0
```

#### Example Four

This example passes in the pool and container UUID directly. The destination path
is the relative path within the DAOS container, which in this case is a subset of 
the DAOS container. 

```shell
$ mpirun -np 3 dcp --daos-src-svcl 0 --daos-dst-svcl 0 -v --daos-src-pool $pool --daos-src-cont $p1cont1 \
--daos-dst-pool $pool2 --daos-dst-cont $p2cont2 /s/biggerfile /
[2020-04-28T00:47:59] Items: 1
[2020-04-28T00:47:59]   Directories: 0
[2020-04-28T00:47:59]   Files: 1
[2020-04-28T00:47:59]   Links: 0
```

#### Example Five

This example copies data from a DAOS container to /tmp, where a DAOS
Unified Namespace path is used as the source. 

```shell
$ mpirun -np 3 dcp --daos-src-svcl 0 -v /tmp/$USER/conts/p1cont1 /tmp/$USER/d
[2020-04-23T17:17:51] Items: 6
[2020-04-23T17:17:51]   Directories: 3
[2020-04-23T17:17:51]   Files: 3
[2020-04-23T17:17:51]   Links: 0
```
