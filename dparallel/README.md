# dparallel
##### run commands in parallel on a distributed system.

### SYNOPSIS
```
dparallel ...
```

### DESCRIPTION
dparallel is a tool in the spirit of *parallel(1)* that evenly distributes commands large cluster without any centralized state.

### PREREQUISITES
An MPI environment is required (such as [Open MPI](http://www.open-mpi.org/)'s *mpirun(1)*) as well as the self-stabilization library known as [LibCircle](https://github.com/hpc/libcircle).

### OPTIONS
**-h**, **--help**

Print a brief message listing the *dparallel(1)* options and usage.

**-v**, **--version**

Print version information and exit.

### Contributions
Please view the *HACKING.md* file for more information on how to contribute to dparallel.
