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


def test_make_url_request_1():
    current_row = 1
    chunk = 2
    result = interface.make_url_request(hpcc_addr, port, file_name, current_row, chunk)
    result = result['Result']['Row']
    expected_result = [{'a': '3', 'b': '4', '__fileposition__': '16'},
                       {'a': '5', 'b': '6', '__fileposition__': '32'}]

    assert result == expected_result


def test_make_url_request_2():
    current_row = 1
    chunk = 6
    result = interface.make_url_request(hpcc_addr, port, file_name, current_row, chunk)
    result = result['Result']['Row']
    assert result == [{'a': '3', 'b': '4', '__fileposition__': '16'},
                      {'a': '5', 'b': '6', '__fileposition__': '32'},
                      {'a': '7', 'b': '8', '__fileposition__': '48'},
                      {'a': '9', 'b': '10', '__fileposition__': '64'},
                      {'a': '11', 'b': '12', '__fileposition__': '80'}]


def test_make_url_request_3():
    current_row = 2
    chunk = 2
    result = interface.make_url_request(hpcc_addr, port, file_name, current_row, chunk)
    result = result['Result']['Row']
    expected_result = [{'a': '5', 'b': '6', '__fileposition__': '32'},
                      {'a': '7', 'b': '8', '__fileposition__': '48'}]

    assert result == expected_result


def test_parse_json_output_1():
    csv_file = False
    results = interface.make_url_request(hpcc_addr, port, '~a::test::file', 1, 6)['Result']['Row']
    result = interface.parse_json_output(results, column_names, csv_file)
    assert result == {'a': ['3', '5', '7', '9', '11'], 'b': ['4', '6', '8', '10', '12']}


def test_parse_json_output_2():
    csv_file = True
    results = interface.make_url_request(hpcc_addr, port, '~a::test::filecsv', 2, 6)['Result']['Row']
    result = interface.parse_json_output(results, column_names, csv_file)
    assert result == {'a': ['3', '5', '7', '9', '11'], 'b': ['4', '6', '8', '10', '12']}
