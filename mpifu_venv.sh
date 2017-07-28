#!/bin/bash

# This script assumes you have at least python-2.7.10 installed
# Pass in the top level directory of where you have the project downloaded
MPIFU_DIR=$1
echo $MPIFU_DIR

CONDA_BIN=${MPIFU_DIR}/miniconda2/bin
ACTIVATE_VENV=${MPIFU_DIR}/miniconda2/envs/mpifu_venv
CONDA_INSTALL=${MPIFU_DIR}/miniconda2

# get the installer for miniconda
wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
chmod +x Miniconda2-latest-Linux-x86_64.sh

# run the install 
./Miniconda2-latest-Linux-x86_64.sh -b -p ${CONDA_INSTALL}

# create and activate the mpifu virtual env
${CONDA_BIN}/conda create -n mpifu_venv
source activate $ACTIVATE_VENV

# now install pypandoc (which has pandoc as well) 
# you can use either pypandoc or pandoc to create 
# the man pages from the markdown format
${CONDA_BIN}/conda config --add channels conda-forge
${CONDA_BIN}/conda install pypandoc

# install nose2 via pip
${CONDA_BIN}/pip install --trusted-host pypi.python.org nose2
