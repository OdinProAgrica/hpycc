import hpycc.getfiles.fileinterface as interface
from hpycc.getscripts.getscripts import run_command
import json

hpcc_addr = 'http://localhost'
port = '8010'
file_name = '~a::test::file'
column_names = ['a', 'b']

# Create HPCC test files
run_command(("ecl run --server {} --port {} --username {} --password {} -legacy "
               "thor {} {}").format(hpcc_addr, '8010', 'test_call', '" "', './tests/make_test_file.ecl', ''))


