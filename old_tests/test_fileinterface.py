import hpycc.delete
import hpycc.get
import hpycc.utils.parsers
from hpycc.utils.HPCCconnector import HPCCconnector
import hpycc.run as run
from pandas.util.testing import assert_frame_equal
import old_tests.make_data as md

silent = False
hpcc_connection = HPCCconnector('localhost', '8010', None, "hpycc_get_output", '" "', False)
test_logical_file_small = '~TEMP::HPYCC::TEST::DATASMALL'
test_logical_file_smallcsv = '~TEMP::HPYCC::TEST::DATASMALLCSV'
test_df_sml = md.upload_data(test_logical_file_small, 10, hpcc_connection)
md.make_csv_format_logicalfile(test_logical_file_small, test_logical_file_smallcsv, hpcc_connection, False)


def test_parse_json_output_1():
    csv_file = False
    results = hpcc_connection.make_url_request(test_logical_file_small, 1, 6)['Result']['Row']
    result = hpycc.utils.parsers.parse_json_output(results, test_df_sml.columns, csv_file)
    expected_result = test_df_sml.loc[1:6, :]
    expected_result.reset_index(inplace=True, drop=True)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_parse_json_output_2():
    csv_file = True
    results = hpcc_connection.make_url_request(test_logical_file_smallcsv, 2, 6)['Result']['Row']

    result = hpycc.utils.parsers.parse_json_output(results, test_df_sml.columns, csv_file)
    expected_result = test_df_sml.loc[1:6, :]
    expected_result.reset_index(inplace=True, drop=True)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_tidy_up():
    hpycc.delete.delete_logical_file(test_logical_file_small, 'localhost')
    hpycc.delete.delete_logical_file(test_logical_file_smallcsv, 'localhost')


# def test_make_url_request_1():
#     current_row = 1
#     chunk = 2
#     result = hpcc_connection.make_url_request(logical_file, current_row, chunk)
#     result = result['Result']['Row']
#     expected_result = [{'a': '3', 'b': '4', '__fileposition__': '16'},
#                        {'a': '5', 'b': '6', '__fileposition__': '32'}]
#     assert result == expected_result
#
#
# def test_make_url_request_2():
#     current_row = 1
#     chunk = 6
#     result = hpcc_connection.make_url_request(logical_file, current_row, chunk)
#     result = result['Result']['Row']
#     assert result == [{'a': '3', 'b': '4', '__fileposition__': '16'},
#                       {'a': '5', 'b': '6', '__fileposition__': '32'},
#                       {'a': '7', 'b': '8', '__fileposition__': '48'},
#                       {'a': '9', 'b': '10', '__fileposition__': '64'},
#                       {'a': '11', 'b': '12', '__fileposition__': '80'}]
#
#
# def test_make_url_request_3():
#     current_row = 2
#     chunk = 2
#     result = hpcc_connection.make_url_request(logical_file, current_row, chunk)
#     result = result['Result']['Row']
#     expected_result = [{'a': '5', 'b': '6', '__fileposition__': '32'},
#                        {'a': '7', 'b': '8', '__fileposition__': '48'}]
#     assert result == expected_result