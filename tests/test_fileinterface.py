import pandas as pd
import hpycc.utils.parsers
from hpycc.utils.HPCCconnector import HPCCconnector
from pandas.util.testing import assert_frame_equal
import tests.make_data as md

silent = False
logical_file = '~a::test::file'
column_names = ['a', 'b']
hpcc_connection = HPCCconnector('localhost', '8010', None, "hpycc_get_output", '" "', False)

md.upload_small_data(hpcc_connection)


def test_make_url_request_1():
    current_row = 1
    chunk = 2
    result = hpcc_connection.make_url_request(logical_file, current_row, chunk)
    result = result['Result']['Row']
    expected_result = [{'a': '3', 'b': '4', '__fileposition__': '16'},
                       {'a': '5', 'b': '6', '__fileposition__': '32'}]
    assert result == expected_result


def test_make_url_request_2():
    current_row = 1
    chunk = 6
    result = hpcc_connection.make_url_request(logical_file, current_row, chunk)
    result = result['Result']['Row']
    assert result == [{'a': '3', 'b': '4', '__fileposition__': '16'},
                      {'a': '5', 'b': '6', '__fileposition__': '32'},
                      {'a': '7', 'b': '8', '__fileposition__': '48'},
                      {'a': '9', 'b': '10', '__fileposition__': '64'},
                      {'a': '11', 'b': '12', '__fileposition__': '80'}]


def test_make_url_request_3():
    current_row = 2
    chunk = 2
    result = hpcc_connection.make_url_request(logical_file, current_row, chunk)
    result = result['Result']['Row']
    expected_result = [{'a': '5', 'b': '6', '__fileposition__': '32'},
                       {'a': '7', 'b': '8', '__fileposition__': '48'}]
    assert result == expected_result


def test_parse_json_output_1():
    csv_file = False
    results = hpcc_connection.make_url_request('~a::test::file', 1, 6)['Result']['Row']

    result = hpycc.utils.parsers.parse_json_output(results, column_names, csv_file)
    expected_result = pd.DataFrame({'a': [3, 5, 7, 9, 11], 'b': [4, 6, 8, 10, 12]})
    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_parse_json_output_2():
    csv_file = True
    results = hpcc_connection.make_url_request('~a::test::filecsv', 2, 6)['Result']['Row']

    result = hpycc.utils.parsers.parse_json_output(results, column_names, csv_file)
    expected_result = pd.DataFrame({'a': [3, 5, 7, 9, 11], 'b': [4, 6, 8, 10, 12]})
    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)

