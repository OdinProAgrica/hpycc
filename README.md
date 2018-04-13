# hpycc

To install, open the root directory and run 
pip install -e . 
This should also update it as the source changes. Cool, no? 

Tested and working on HPCC v6.4.2 and python 3.5.2. Tested and working on Windows 10, will make linux-able in the future (if it isn't already) 

see testScript.py for example calls. 

Note that while retrieving a file is a multithread process, running a script and getting the results is not. Therefore if your file is quite big you may be better off saving the results of a script call then downloading the file. 

There is a dependency for client tools to run ECL scripts. This must be in your system path. 

