# Documentation

Edits should be made to the `.rst` files.
The documentation can be built with `make html` or `make man`.
The generated files will be found in the `build` directory.

## Man Pages

Since there is no guarantee of Sphinx on each system, the man pages for each release are committed directly to the repo.
This can be done with:

``` shell
cd doc
make man
make install
```

Be sure to update the version number in the `doc/rst/conf.py` file as well.
