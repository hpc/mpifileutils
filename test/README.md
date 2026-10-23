# Testing mpiFileUtils

The test suite is registered with CTest and contains:

- executable tests, which run the mpiFileUtils commands through an MPI
  launcher.
- unit tests

The following steps assume that mpiFileUtils has already been built into the
directory `build`.

## Configure tests

Set `TEST_TMPDIR` to a location on the filesystem which the tests can write to.

CMake discovers the MPI configurations automatically. But, they should be set
explicity:

```bash
cd build
cmake /path/to/mpifileutils \
    -DTEST_TMPDIR=/path/to/test-tmp \
    -DMPIEXEC_EXECUTABLE=/path/to/mpiexec \
    -DMPIEXEC_NUMPROC_FLAG=-n \
    -DMPIEXEC_MAX_NUMPROCS=4
```

### Slurm example

Run the tests inside an allocation and configure `srun` as the launcher:

```bash
cd build
cmake /path/to/mpifileutils \
    -DTEST_TMPDIR=/path/on/shared/filesystem/test-tmp \
    -DMPIEXEC_EXECUTABLE="$(which srun)" \
    -DMPIEXEC_NUMPROC_FLAG=-n \
    -DMPIEXEC_MAX_NUMPROCS="$((SLURM_NTASKS * SLURM_NNODES))"
```

## Run tests

### From `build`,
Run the complete suite:

```bash
ctest --output-on-failure
```

Run all executable tests:

```bash
ctest --label-regex executable --output-on-failure
```

List all available tests:

```bash
ctest --show-only
```

Run the tests for one executable, for example `dcp`:

```bash
ctest --tests-regex '^executable\.dcp$' --output-on-failure
```

### Root-required tests

NOTE: CMake should not be configured as root.

Tests requiring root privileges should be run individually from `ctest` with:
```bash
cd /path/to/mpifileutils/test/executable

export PATH="/path/to/install/bin:$PATH"

TEST_TMPDIR=/path/on/shared/filesystem/test-tmp \
MPIEXEC_EXECUTABLE=/path/to/mpiexec \
MPIEXEC_NUMPROC_FLAG=-n \
MPIEXEC_MAX_NUMPROCS=4 \
python3 -m unittest -v "tests/root_required_tests.py"
```

To run the full test suite, run the tests as a non-root user.
