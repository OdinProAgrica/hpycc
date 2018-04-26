import hpycc.utils.datarequests
import hpycc.utils.parsers
import hpycc.scriptrunning.runscript as runscript
from hpycc.utils.datarequests import run_command
import pandas as pd
from pandas.util.testing import assert_frame_equal

silent = False
logical_file = '~a::test::file'
column_names = ['a', 'b']
hpcc_server = {
        'server': 'localhost',
        'port': '8010',
        'repo': None,
        'username': "hpycc_get_output",
        'password':  '" "',
        'legacy': False
    }

# Create HPCC test files
runscript.run_script_internal('./tests/ECLtest_makeTestData.ecl', hpcc_server, False)


def test_make_url_request_1():
    current_row = 1
    chunk = 2
    result = hpycc.utils.datarequests.make_url_request(hpcc_server, logical_file, current_row, chunk)
    result = result['Result']['Row']
    expected_result = [{'a': '3', 'b': '4', '__fileposition__': '16'},
                       {'a': '5', 'b': '6', '__fileposition__': '32'}]

    assert result == expected_result


def test_make_url_request_2():
    current_row = 1
    chunk = 6
    result = hpycc.utils.datarequests.make_url_request(hpcc_server, logical_file, current_row, chunk)
    result = result['Result']['Row']
    assert result == [{'a': '3', 'b': '4', '__fileposition__': '16'},
                      {'a': '5', 'b': '6', '__fileposition__': '32'},
                      {'a': '7', 'b': '8', '__fileposition__': '48'},
                      {'a': '9', 'b': '10', '__fileposition__': '64'},
                      {'a': '11', 'b': '12', '__fileposition__': '80'}]


def test_make_url_request_3():
    current_row = 2
    chunk = 2
    result = hpycc.utils.datarequests.make_url_request(hpcc_server, logical_file, current_row, chunk)
    result = result['Result']['Row']
    expected_result = [{'a': '5', 'b': '6', '__fileposition__': '32'},
                      {'a': '7', 'b': '8', '__fileposition__': '48'}]

    assert result == expected_result


def test_parse_json_output_1():
    csv_file = False
    results = hpycc.utils.datarequests.make_url_request(hpcc_server, '~a::test::file', 1, 6)['Result']['Row']

    result = hpycc.utils.parsers.parse_json_output(results, column_names, csv_file)
    expected_result = pd.DataFrame({'a': [3, 5, 7, 9, 11], 'b': [4, 6, 8, 10, 12]})
    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_parse_json_output_2():
    csv_file = True
    results = hpycc.utils.datarequests.make_url_request(hpcc_server, '~a::test::filecsv', 2, 6)['Result']['Row']

    result = hpycc.utils.parsers.parse_json_output(results, column_names, csv_file)
    expected_result = pd.DataFrame({'a': [3, 5, 7, 9, 11], 'b': [4, 6, 8, 10, 12]})
    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)

