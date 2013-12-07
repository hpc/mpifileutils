DTEST is a testing application that is used for benchmarking, and validating the parallel unix utilities. there is currently support for dtar and dcp, but more will be added.


### If running on titan make sure to execute '$> module load python' in order to load a new enough version of python for argparse to work. ###


USAGE:

./dtest <options> <path-to-binary> [test directory]

-s <arg>	--size		total tree size to test				DEFAULT: 1G
-np <arg>	--proc		number of process to use with dtar		DEFAULT: 3
-c <arg>	--count		number of files to test				DEFAULT: 10
-bs <arg>	--block		block size for writing files.			DEFAULT: 256M
-i <arg> 	--iterations	iterations of dtar to run to average results	DEFAULT: 3
-v		--verbose	verbose mode

--genfiles	this flag causes dtest to only generate a file tree without doing testing.
		use the same parameters that you would use to run a test, except you do not
		have to specify a binary to test. (e.g.)  ./dtest --genfiles -s 10G -c 100 test-dir

--validate	run a validation test to check integrity of the output files vs. th input files.

--debug		shows the output of the binary subprocess, and doesn't clean up files afterwords.'

--noclean	prevents the program from cleaning up test files on completion

--sparse	write sparse files to test instead of zero files. (much faster)


