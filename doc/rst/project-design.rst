=========================
Project Design Principles
=========================

The following principles drive design decisions in the project.

---------------------------
Scale
---------------------------

The library and tools should be designed such that running with more processes
increases performance, provided there are sufficient data and parallelism
available in the underlying file systems. The design of the tool should not
impose performance scalability bottlenecks.

---------------------------
Performance
---------------------------

While it is tempting to mimic the interface, behavior, and file formats of
familiar tools like cp, rm, and tar, when forced with a choice between
compatibility and performance, mpiFileUtils chooses performance. For example,
if an archive file format requires serialization that inhibits parallel
performance, mpiFileUtils will opt to define a new file format that enables
parallelism rather than being constrained to existing formats. Similarly,
options in the tool command line interface may have different semantics from
familiar tools in cases where performance is improved. Thus, one should be
careful to learn the options of each tool.

---------------------------
Portability
---------------------------

The tools are intended to support common file systems used in HPC centers, like
Lustre, GPFS, and NFS. Additionally, methods in the library should be portable
and efficient across multiple file systems. Tool and library users can rely on
mpiFileUtils to provide portable and performant implementations.

---------------------------
Composability
---------------------------

While the tools do not support chaining with Unix pipes, they do support
interoperability through input and output files. One tool may process a dataset
and generate an output file that another tool can read as input, e.g., to walk
a directory tree with one tool, filter the list of file names with another, and
perhaps delete a subset of matching files with a third. Additionally, when
logic is deemed to be useful across multiple tools or is anticipated to be
useful in future tools or applications, it should be provided in the common
library.
