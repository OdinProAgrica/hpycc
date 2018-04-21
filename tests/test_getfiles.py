import hpycc.filerunning.getfiles as get
from hpycc.utils.datarequests import run_command
import pandas as pd
from pandas.util.testing import assert_frame_equal

server = 'http://localhost'
port = '8010'
column_names = ['a', 'b']
expected_result = pd.DataFrame({'a': ['1', '3', '5', '7', '9', '11'], 'b': ['2', '4', '6', '8', '10', '12']})
expected_result_chunk = pd.DataFrame({'a': ['3', '5', '7'], 'b': ['4', '6', '8']})
username = "hpycc_get_output"
password = '" "'
silent = False

# Create HPCC test files
run_command(("ecl run --server {} --port {} --username {} --password {} -legacy "
               "thor {} {}").format(server, '8010', 'test_call', '" "', './tests/ECLtest_makeTestData.ecl', ''))


def test_get_file_1():
    file_name = '~a::test::file'
    csv_file = False
    result = get.get_file(file_name, server, port, username, password, csv_file, silent)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    result = get.get_file(file_name, server, port, username, password, csv_file, silent)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_structure_1():
    file_name = '~a::test::file'
    csv_file = False
    result = get._get_file_structure(file_name, server, port, username, password, csv_file, silent)
    assert result == (['a', 'b'], [6], 0)


def test_get_file_structure_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    result = get._get_file_structure(file_name, server, port, username, password, csv_file, silent)
    assert result == (['a', 'b'], [6], 1)


def test_get_file_chunk_1():
    file_name = '~a::test::file'
    csv_file = False
    current_row = 1
    chunk = 3

    result = get._get_file_chunk(file_name, csv_file, server, port, username, password, current_row, chunk, column_names, silent)
    assert_frame_equal(result, expected_result_chunk, check_dtype=False, check_like=False)



def test_get_file_chunk_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    current_row = 2
    chunk = 3

    result = get._get_file_chunk(file_name, csv_file, server, port, username, password, current_row, chunk, column_names, silent)
    assert_frame_equal(result, expected_result_chunk, check_dtype=False, check_like=False)


