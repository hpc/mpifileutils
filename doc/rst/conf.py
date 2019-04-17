# -*- coding: utf-8 -*-
#
# mpiFileUtils documentation build configuration file, created by
# sphinx-quickstart on Fri Apr 13 11:47:51 2018.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.coverage']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'mpiFileUtils'
copyright = u'2018, LLNL/LANL/UT-Battelle/DDN'
author = u'HPC'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = u'0.9.1'
# The full version, including alpha/beta/rc tags.
release = u'0.9.1'

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = []

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

from recommonmark.parser import CommonMarkParser

source_parsers = {
    '.md': CommonMarkParser,
}

source_suffix = ['.rst', '.md']

source_encoding = 'utf-8-sig'

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
import guzzle_sphinx_theme

html_theme_path = guzzle_sphinx_theme.html_theme_path()
html_theme = 'guzzle_sphinx_theme'

# Register the theme as an extension to generate a sitemap.xml
extensions.append("guzzle_sphinx_theme")

# Guzzle theme options (see theme.conf for more information)
html_theme_options = {
    # Set the name of the project to appear in the sidebar
    "project_nav_name": "mpiFileUtils",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
#html_static_path = ['_static']


# -- Options for HTMLHelp output ------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = 'mpiFileUtilsdoc'


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    # 'papersize': 'letterpaper',

    # The font size ('10pt', '11pt' or '12pt').
    #
    # 'pointsize': '10pt',

    # Additional stuff for the LaTeX preamble.
    #
    # 'preamble': '',

    # Latex figure (float) alignment
    #
    # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, 'mpiFileUtils.tex', u'mpiFileUtils Documentation',
     u'HPC', 'manual'),
]


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'mpifileutils', u'mpiFileUtils Documentation',[author], 1),
    ('dbcast.1', 'dbcast', u'distributed broadcast',[author], 1),
    ('dbz2.1', 'dbz2', u'distributed bz2 compression',[author], 1),
    ('dchmod.1', 'dchmod', u'distributed tool to set permissions and group',[author], 1),
    ('dcmp.1', 'dcmp', u'distributed compare',[author], 1),
    ('dcp.1', 'dcp', u'distributed copy',[author], 1),
    ('ddup.1', 'ddup', u'report files with identical content',[author], 1),
    ('dfind.1', 'dfind', u'distributed file filtering',[author], 1),
    ('dreln.1', 'dreln', u'distributed relink',[author], 1),
    ('drm.1', 'drm', u'distributed remove',[author], 1),
    ('dstripe.1', 'dstripe', u'restripe files on underlying storage',[author], 1),
    ('dsync.1', 'dsync', u'synchronize directory trees',[author], 1),
    ('dwalk.1', 'dwalk', u'distributed walk and list',[author], 1)
]

smartquotes = False

# -- Options for Texinfo output -------------------------------------------

# Grouping the document tree into Texinfo files. List of tuples
# (source start file, target name, title, author,
#  dir menu entry, description, category)
texinfo_documents = [
    (master_doc, 'mpiFileUtils', u'mpiFileUtils Documentation',
     author, 'mpiFileUtils', 'One line description of project.',
     'Miscellaneous'),
]
