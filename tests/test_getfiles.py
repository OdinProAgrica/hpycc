import hpycc.filerunning.getfiles as getfiles
from pandas.util.testing import assert_frame_equal
import pandas as pd
import os
import hpycc.utils.filechunker
from hpycc.utils.HPCCconnector import HPCCconnector
import tests.make_data as md

script_loc = ''
# script_loc = './tests/'
column_names = ['a', 'b']
silent = False
download_threads = 15
hpcc_connection = HPCCconnector('localhost', '8010', None, "hpycc_get_output", '" "', False)

expected_result = pd.DataFrame({'a': [1, 3, 5, 7, 9, 11], 'b': [2, 4, 6, 8, 10, 12]})
expected_result_chunk = pd.DataFrame({'a': [3, 5, 7], 'b': [4, 6, 8]})

expected_big_data = md.upload_large_data(hpcc_connection)
md.upload_small_data(hpcc_connection)


def test_get_file_1():
    file_name = '~a::test::file'
    csv_file = False
    result = getfiles.get_file_internal(file_name, hpcc_connection, csv_file, download_threads)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    result = getfiles.get_file_internal(file_name, hpcc_connection, csv_file, download_threads)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_3():
    file_name = '~a::test::filecsv'
    csv_file = True
    result = getfiles.get_file_internal(file_name, hpcc_connection, csv_file, download_threads)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_4():
    file_name = '~a::test::bigfile'
    csv_file = False
    result = getfiles.get_file_internal(file_name, hpcc_connection, csv_file, download_threads)

    result.reset_index(inplace=True, drop=True)
    expected_big_data.reset_index(inplace=True, drop=True)

    assert_frame_equal(result, expected_big_data, check_dtype=False, check_like=False)


def test_get_file_structure_1():
    file_name = '~a::test::file'
    csv_file = False
    result = getfiles._get_file_structure(file_name, hpcc_connection, csv_file)
    assert result == (['a', 'b'], 6)


def test_get_file_structure_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    result = getfiles._get_file_structure(file_name, hpcc_connection, csv_file)
    assert result == (['a', 'b'], 6)


def test_get_file_chunk_1():
    file_name = '~a::test::file'
    csv_file = False
    current_row = 1
    chunk = 3

    result = getfiles._get_file_chunk(file_name, csv_file, hpcc_connection, current_row, chunk, column_names)
    assert_frame_equal(result, expected_result_chunk, check_dtype=False, check_like=False)


def test_get_file_chunk_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    current_row = 2
    chunk = 3

    result = getfiles._get_file_chunk(file_name, csv_file, hpcc_connection, current_row, chunk, column_names)
    assert_frame_equal(result, expected_result_chunk, check_dtype=False, check_like=False)


def test_make_chunks_1():
    file_size = 10
    csv_file = False

    start_rows, chunks = hpycc.utils.filechunker.make_chunks(file_size, csv_file)
    assert start_rows == [0]
    assert chunks == [10]


def test_make_chunks_2():
    file_size = 10
    csv_file = True

    start_rows, chunks = hpycc.utils.filechunker.make_chunks(file_size, csv_file)
    assert start_rows == [1]
    assert chunks == [10]


def test_make_chunks_3():
    file_size = 10001
    csv_file = False

    start_rows, chunks = hpycc.utils.filechunker.make_chunks(file_size, csv_file)
    assert start_rows == [0, 10000]
    assert chunks == [10000, 1]


def test_make_chunks_4():
    file_size = 10002
    csv_file = True

    start_rows, chunks = hpycc.utils.filechunker.make_chunks(file_size, csv_file)
    assert start_rows == [1, 10001]
    assert chunks == [10000, 1]




