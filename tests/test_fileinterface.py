import hpycc.getfiles.fileinterface as interface
from hpycc.getscripts.getscripts import run_command

hpcc_addr = 'http://localhost'
port = '8010'
file_name = '~a::test::file'
last = 1
split = 2

results = ''
column_names = ''
csv_file = ''

command = ("ecl run --server {} --port {} --username {} --password {} -legacy "
               "thor {} {}").format(hpcc_addr, '8010', 'test_call', '" "', './tests/make_test_file.ecl', '')

result = run_command(command)


def test_make_url_request():
    interface.make_url_request(hpcc_addr, port, file_name, last, split)
    assert False


def _run_url_request(request):
    interface._run_url_request(request)
    assert False


def parse_json_output():
    interface.parse_json_output(results, column_names, csv_file)
    assert False
