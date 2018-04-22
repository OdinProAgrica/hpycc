# hpycc

## Installation
Install with
pip install git+git://github.com/OdinProAgrica/hpycc

## Current Status
Tested and working on HPCC v6.4.2 and python 3.5.2 under windows 10. Will make linux-able in the future (if it isn't already) 

## Dependencies
The package itself mainly uses core Python, Pandas is needed for outputting dataframes.  
There is a dependency for client tools to run ECL scripts (you need ecl.exe and eclcc.exe), make sure you install the right client tools for your HPCC version. This must be in your system path.   
Tests require an HPCC instance running on your local machine, docker images do exist for this.

## Main Functions
Below summarises the key functions and non-optional parameters. For specific arguments see the relevent function's documentation.  
Note that while retrieving a file is a multi-thread process, running a script and getting the results is not. Therefore if your file is quite big you may be better off saving the results of a script using run_script() with a thor file output then downloading the file with get_file(). 

### get_output(script, server...) & save_output(script, server, path...)
Run a given ECL script and either return the first result as a pandas dataframe or save it to file.

### get_outputs(script, server...) & save_outputs(script, server...)
Run a given ECL script and return all results as a dict of pandas dataframes or save them to files.

### get_file(logical_file, server...) & save_file(logical_file, path, server...)
Get a logical file and either return as a pandas dataframe or save it to file.

### run_script(script, server...)
Run a given ECL script. 10 rows will be returned but they will be dumped, no output is given. 

### key_parameters
See function documentation for full list but those of note include:
* Specifying a local ECL repository for scripts (repo=...)
* Allowing files to be overriden when saving (refresh=...)
* Altering the default HPCC port (port=...) 
* Adding usernames (username=...) and passwords (password=...)
* Suppressing all but essential messages (silent=...)

## Examples 
TODO but check the tests directory for some example ECL scripts and calls. 

## Issues, Bugs, Comments? 
Use the github tools to report them, any contributions welcome.
