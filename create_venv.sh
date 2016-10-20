#!/bin/bash

#This script assumes you have at least python-2.7.10 installed
#Pass in the top level directory of where you have the project downloaded 

FILEUTILS_DIR=$1

#in case a user doesn't already have this
pip install virtualenv

cd $FILEUTILS_DIR
virtualenv venv
venv/bin/activate
pip install nose2 
