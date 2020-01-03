#!/bin/bash
####################
# Author: Robert E. Novak
# email: novak5@llnl.gov
#
# Find and return the parent directory that contains ".git"
#
# That is the parent directory that is the "root" of all evil so that we can
# execute binaries located in ../install/bin
####################
while [ ! -d .git ]
do
	cd ..
done
pwd
