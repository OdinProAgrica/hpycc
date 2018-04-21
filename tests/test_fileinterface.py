import hpycc.getfiles.fileinterface as interface
from hpycc.getscripts.scriptinterface import run_command

server = 'http://localhost'
port = '8010'
logical_file = '~a::test::file'
column_names = ['a', 'b']
username = "hpycc_get_output"
password = '" "'
silent = False

# Create HPCC test files
run_command(("ecl run --server {} --port {} --username {} --password {} -legacy "
               "thor {} {}").format(server, '8010', 'test_call', '" "', './tests/ECLtest_makeTestData.ecl', ''))


def test_make_url_request_1():
    current_row = 1
    chunk = 2
    result = interface.make_url_request(server, port, username, password, logical_file, current_row, chunk, silent)
    result = result['Result']['Row']
    expected_result = [{'a': '3', 'b': '4', '__fileposition__': '16'},
                       {'a': '5', 'b': '6', '__fileposition__': '32'}]

    assert result == expected_result


def test_make_url_request_2():
    current_row = 1
    chunk = 6
    result = interface.make_url_request(server, port, username, password, logical_file, current_row, chunk, silent)
    result = result['Result']['Row']
    assert result == [{'a': '3', 'b': '4', '__fileposition__': '16'},
                      {'a': '5', 'b': '6', '__fileposition__': '32'},
                      {'a': '7', 'b': '8', '__fileposition__': '48'},
                      {'a': '9', 'b': '10', '__fileposition__': '64'},
                      {'a': '11', 'b': '12', '__fileposition__': '80'}]


def test_make_url_request_3():
    current_row = 2
    chunk = 2
    result = interface.make_url_request(server, port, username, password, logical_file, current_row, chunk, silent)
    result = result['Result']['Row']
    expected_result = [{'a': '5', 'b': '6', '__fileposition__': '32'},
                      {'a': '7', 'b': '8', '__fileposition__': '48'}]

    assert result == expected_result


def test_parse_json_output_1():
    csv_file = False
    results = interface.make_url_request(server, port, username, password,
                                         '~a::test::file', 1, 6, silent)['Result']['Row']

    result = interface.parse_json_output(results, column_names, csv_file, silent)
    assert result == {'a': ['3', '5', '7', '9', '11'], 'b': ['4', '6', '8', '10', '12']}


def test_parse_json_output_2():
    csv_file = True
    results = interface.make_url_request(server, port, username, password,
                                         '~a::test::filecsv', 2, 6, silent)['Result']['Row']

    result = interface.parse_json_output(results, column_names, csv_file, silent)
    assert result == {'a': ['3', '5', '7', '9', '11'], 'b': ['4', '6', '8', '10', '12']}
