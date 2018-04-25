hpycc Readme
============

.. image:: img/logo.jpg
   :scale: 100 %

Documentation
-------------
The below readme and package documentation is available at http://hpycc.readthedocs.io

The package's github is available at: https://github.com/OdinProAgrica/hpycc

This package is released under GNU GPLv3 Licence: https://www.gnu.org/licenses/gpl-3.0.en.html

Want to use this in R? Then the reticulate package is your friend! Just save as a CSV and read back in. That
or you can use an R notebook with a Python chunk.


Installation
------------
Install with:

pip install git+git://github.com/OdinProAgrica/hpycc

Current Status
--------------
Tested and working on HPCC v6.4.2 and python 3.5.2 under windows 10. Will make linux-able in the future (if it isn't already) 

Dependencies
------------
The package itself mainly uses core Python, Pandas is needed for outputting dataframes.

There is a dependency for client tools to run ECL scripts (you need ecl.exe and eclcc.exe).
Make sure you install the right client tools for your HPCC version and add the dir to your system path,
e.g. C:\Program Files (x86)\HPCCSystems\X.X.X\clienttools\bin.

Tests require an HPCC instance running on your local machine, docker images do exist for this.

Main Functions
--------------
Below summarises the key functions and non-optional parameters. For specific arguments see the relevant
function's documentation. Note that while retrieving a file is a multi-thread process, running a script
and getting the results is not. Therefore if your file is quite big you may be better off saving the
results of a script using run_script_internal() with a thor file output then downloading the file with
get_file_internal().

get_output(script, server...) & save_output(script, server, path...)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Run a given ECL script and either return the first result as a pandas dataframe or save it to file.

get_outputs(script, server...) & save_outputs(script, server...)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Run a given ECL script and return all results as a dict of pandas dataframes or save them to files.

get_file_internal(logical_file, server...) & save_file(logical_file, path, server...)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Get a logical file and either return as a pandas dataframe or save it to file.

run_script_internal(script, server...)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Run a given ECL script. 10 rows will be returned but they will be dumped, no output is given. 

Frequently Used Arguments
^^^^^^^^^^^^^^^^^^^^^^^^^
See function documentation for full list but those of note include:

* Specifying a local ECL repository for scripts (repo=...)
* Allowing files to be overridden when saving (refresh=...)
* Altering the default HPCC port (port=...)
* Adding usernames (username=...) and passwords (password=...)
* Suppressing all but essential messages (silent=...)

Examples 
--------
TODO but check the tests directory for some example ECL scripts and calls. 

Issues, Bugs, Comments? 
-----------------------
Please use the package's github: https://github.com/OdinProAgrica/hpycc

Any contributions are also welcome.
