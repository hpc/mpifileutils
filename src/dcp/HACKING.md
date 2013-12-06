Contributor Guidelines for dcp
------------------------------
This document describes the basic design of dcp and how changes to dcp may
be shared with the community.

Continuous Integration
======================
If you have trouble building dcp, please check the continuous integration
status at <https://travis-ci.org/hpc/dcp>. The build and all tests must be
passing before comprehensive testing for a release is initiated.

Submitting code
===============
When submitting code to dcp, please fork the project and create a new branch
named to reflect your changes before submitting a new pull request.

The master repository for dcp is located at <http://github.com/hpc/dcp>.

To produce a clean changeset, we ask that you perform an interactive rebase
before submitting a pull request to make commits easier to understand.

Clean, documented, and understandable code is required. At a minimum, we ask
that you follow the options listed in astyle.options in the top directory of
the repository. New code should not generate any warnings from newer versions
of clang or gcc. If possible, effort should be made to fix warnings in
existing code.

Command Line Option Handling Design
===================================
A subtle problem in creating a POSIX-like frontend is handling the many
combinations of directories and files that the user may use for input. To keep
things simple, I’m going to ignore everything but simple files and directories.
Block and character devices, local domain sockets, named pipes, and symbolic
links should be handled as well, but will not be covered here.

To give an idea of what we’re dealing with, here’s the usage message which
defines our POSIX-like interface:

````
    usage: dcp [CdfhpRrv] [--] source_file target_file
           dcp [CdfhpRrv] [--] source_file ... target_directory
````

For all input, we need to know what the base name of the directory is that
we’ll be writing files into. We’ll also need the path of the destination and a
list of source paths. There are also a few “impossible” situations, like trying
to copy a directory into a file. We need to prune out these situations and
present a nice error message to the user.

To figure out what needs to be pruned out, first we need to know what we have.
This is a bit tricky because sometimes the destination does not initially exist
based on what the user is trying to do. However, we’re not in the business of
creating new directories, so it’s safe to say that the destination should be a
single file or directory if the source is a single file or directory. However,
sometimes an error condition will pop up when it doesn’t make sense for
multiple source paths to be copied into a file.

Here’s some pseudocode to demonstrate this concept. First, we check to see if
something exists on disk at the destination path. If it does, we remember that
state for later.

````
if(is_file(last_argument) {
    last_argument_is_file = true
    last_argument_is_dir = false
} elif( is_dir(last_argument) {
    last_argument_is_dir = true
    last_argument_is_file = false
````

If the destination path doesn’t exist, we check to see what the source paths
are. If recursion is turned on, we’ll have a file as the destination if the
source is a single file, otherwise, we need the destination to be a directory.

````
} else {
    if(allow_recursion) {
        last_argument_is_file = true
         for(src in srcs) {
             if (is_dir(src)) {
                 last_argument_is_file = false
             }
         }
        if( number_of_source_files != 1 && last_argument_is_file ) {
            error(impossible_condition)
        } else {
             last_argument_is_dir = !last_argument_is_file
        }
    } else {
        last_argument_is_file = true
    }
````

Now that we know what the last argument should be, we can reason about what
the end result should be. This makes it trivial to prune out the impossible
situations. Writing down all potential input combinations yields the following
impossible conditions: copying one or more directories into a file, copying
many files into a file, copying one or more directories and files into a file.
Encountering any of this input will lead to an error condition.

Take note that all of the impossible conditions we’ve listed have the property
of the destination being a file. We can take advantage of this by catching
many of the error conditions in the logic that determines if we’re in the mode
of copying a single file into another file.

````
if( last_argument_is_file ) {
    if( number_of_source_files == 1 && is_file(src_path) ) {
        set_base(basename_of_last_argument)
        push_work_queue(src_path);
    } else {
        error(impossible_condition)
    }
````

Now that we’ve handled everything but copying source files into directories,
we can easily handle the rest of the potential inputs.

````
} elif( last_argument_is_directory ) {
    set_base(last_argument)

    for(src in srcs) {
        push_work_queue(src.path)
    }

} else {
    error(unsupported_filetype);
}
````

So there we have it, an algorithm to handle user input of directories and files
for dcp.

Workload Distribution Core Design
=================================
Note: Please see <http://doi.acm.org/10.1145/2388996.2389114> for a formal
description of how dcp distributes work.

Conceptually, the workload distribution in dcp can be described with two
queues. One queue is a global queue that spans all nodes in the distributed
system. The other queue is a queue which is local to each process (MPI rank).

From this point on, we define terminology to reference these queues. The global
queue will simply be called the "queue" and the local queue will be called the
"internal queue". The phrase "internal queue" is used throughout the code base
in reference to the local queue.

The global queue is simply an abstraction on top of the collection of internal
queues. It only exists as a collection of internal queues.

The library known as "libcircle" is used to manage the movement of work items
between internal queues across all nodes. It will handle balancing the number
of work items in the internal queues across all processes (MPI ranks).

Libcircle is also used to determine when the internal queues on all nodes are
empty so the program knows when to exit.

The API to libcircle requires two function callbacks to be defined. Once these
two functions are defined, libcircle is told to begin computation.

The first function callback that libcircle requires is the "create" function.
This function creates a seed item that will be placed on the internal queue.

For dcp, this seed item is the root of the directory tree which will be
recursively copied. The seed item is placed on the queue through a "handle"
which is passed in by libcircle to the callback defined by dcp. The handle
defines two helper functions to modify the queue (enqueue and dequeue).

The second function, which has the same signature as the create callback (the
handle is passed to it as well), is called the "process" callback. In dcp, the
process callback works by using the handle to dequeue a work item. Then, the
work item is decoded to determine what type of action (also known as a "stage")
is performed.

For example, if the command line argument specified was a directory, and the
argument was a directory, the work item which is dequeued will contain a work
item which holds the path to this directory (enqueued by the create function).
The job of the process callback in this case is to queue the child paths of the
specified directory and return.

Libcircle will call the process callback in a loop on every node until the
global queue (the collection of internal queues) is completely empty.

There are four basic actions that may be performed when the process callback is
called by libcircle. The action that is performed is based on the "stage" value
that is encoded in the work operation.

The stages that a copy operation will follow is the following:

````
    treewalk -> copy -> compare -> cleanup
````

A copy operation will start by enqueueing the command line arguments into a 
work operation (struct DCOPY_operation_t) using the create callback. Then,
libcircle will handle calling the process callback once the queue is non-empty.

The process callback will then perform the "treewalk" stage which will create
additional work operations. It will enqueue child objects in the case where the
work operation is a directory. In the case of a file in the work operation, it
will chunk up the file and create work operations (with a copy stage directive)
for each chunk of the file.

The copy operation works with the same basic concepts. It will dequeue
operations which have a copy stage flag specified in the work operation struct.
The copy operation will seek the filesystem and perform the actual copy. Once
the chunk is copied, a new work operation is created which is then enqueued so
the compare operation can work on the chunk.

The compare operation is very similiar to the copy operation, however it simply
compares the chunk written to disk with the contents of the original file.

During the treewalk stage, the first chunk of each file encountered also has a
work operation for the cleanup stage placed on the queue. Only the first chunk
of the file has a cleanup stage work operation encoded because the cleanup
stage is only for operations which should be performed once on each file (such
as metadata operations and truncation).

Once the files have passed the cleanup and compare stages without being
reenqueued, the global queue will empty out and libcircle will recognize this
and terminate.

A Note on Fault Tolerance
=========================
Due to the completely asynchronous nature of ring termination, liveness in the
event of node failure is impossible (FLP impossibility, see Fisher et al.). If
you intend to introduce fault tolerance to dcp, it can only be accomplished by
replacing the ring termination with a partially asynchronous termination
process before replication of stage messages is attempted. Since a partially
asynchronous algorithm will incur a high message cost, I (jonb) recommend
using a pipelined Paxos, such as Ring Paxos, to increase throughput at the
cost of latency.
