# dparallel
##### run commands in parallel on a distributed system.

### SYNOPSIS
This is a prototype. Just pipe commands into stdin.
```
dparallel ...
```

### DESCRIPTION
dparallel is a tool in the spirit of *parallel(1)* that evenly distributes
commands large cluster without any centralized state.

### PREREQUISITES
An MPI environment is required (such as [Open MPI](http://www.open-mpi.org/)'s
*mpirun(1)*) as well as the self-stabilization library known as [LibCircle](https://github.com/hpc/libcircle).

### OPTIONS
**-h**, **--help**

Print a brief message listing the *dparallel(1)* options and usage.

**-v**, **--version**

Print version information and exit.

## EXAMPLE
A distributed find-and-replace. This finds all files in 'dir', searches for
'ugly', and replaces it with 'beautiful'. Note how the commands are separated
by newlines.
```
find ./dir -printf 'sed -i \'s/ugly/beautiful/g\' %f\n';' | dparallel 
```

### Contributions
Please view the *HACKING.md* file for more information on how to contribute to
dparallel.
