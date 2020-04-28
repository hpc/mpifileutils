# Documentation

Edits should be made to the `.rst` files.
The documentation can be built with `make html` or `make man`.
The generated files will be found in the `build` directory.

## Installing Sphinx

Sphinx is used to generate man pages from the `.rst` files.
If Sphinx is not installed on the system, the following may be used to install Sphinx into a virtualenv.

``` shell
virtualenv --system-site-packages env
source env/bin/activate

pip install recommonmark
pip install guzzle_sphinx_theme
```

If using this method, then source the virtualenv before running the Makefile steps below:

``` shell
source env/bin/activate
```

## Man Pages

Since there is no guarantee of Sphinx on each system, the man pages for each release are committed directly to the repo.
This can be done with:

Be sure to update the version number in the `doc/rst/conf.py` file if needed,
then run Sphinx to generate the man pages from the `.rst` files.

``` shell
cd doc
make man
make install
```

