import hpycc.filerunning.getfiles as getfiles
from pandas.util.testing import assert_frame_equal

import hpycc.delete
import hpycc.run as run
import hpycc.utils.filechunker
from hpycc.utils.HPCCconnector import HPCCconnector
import old_tests.make_data as md


silent = False
download_threads = 15
test_logical_file = '~TEMP::HPYCC::TEST::DATALARGE'
test_logical_file_csv = '~TEMP::HPYCC::TEST::DATALARGECSV'
size = 15000

hpcc_connection = HPCCconnector('localhost', '8010', None, "hpycc_get_output", '" "', False)

result_df = md.upload_data(test_logical_file, size, hpcc_connection)
md.make_csv_format_logicalfile(test_logical_file, test_logical_file_csv, hpcc_connection, False)


def test_get_file_1():
    csv_file = False
    result = getfiles.get_file_internal(test_logical_file, hpcc_connection, csv_file, download_threads)

    assert_frame_equal(result, result_df, check_dtype=False, check_like=False)


def test_get_file_2():
    csv_file = True
    result = getfiles.get_file_internal(test_logical_file_csv, hpcc_connection, csv_file, download_threads)

    # todo: Make data takes the last row off the resul when it makes a csv?
    assert_frame_equal(result, result_df[0:(len(result_df)-1)], check_dtype=False, check_like=False)


def test_get_file_3():
    csv_file = True
    result = getfiles.get_file_internal(test_logical_file_csv, hpcc_connection, csv_file, download_threads)

    assert_frame_equal(result, result_df[0:(len(result_df)-1)], check_dtype=False, check_like=False)


def test_get_file_4():
    csv_file = False
    result = getfiles.get_file_internal(test_logical_file, hpcc_connection, csv_file, download_threads)

    assert_frame_equal(result, result_df, check_dtype=False, check_like=False)


def test_get_file_structure_1():
    csv_file = False
    col, sze = getfiles._get_file_structure(test_logical_file, hpcc_connection, csv_file)

    assert col == result_df.columns.tolist()
    assert sze == size


def test_get_file_structure_2():
    csv_file = True
    col, sze = getfiles._get_file_structure(test_logical_file_csv, hpcc_connection, csv_file)

    assert col == result_df.columns.tolist()
    assert sze == size


def test_get_file_chunk_1():
    csv_file = False
    current_row = 1
    chunk = 3

    expected_result = result_df[1:4]
    expected_result.reset_index(inplace=True, drop=True)
    result = getfiles._get_file_chunk(test_logical_file, csv_file, hpcc_connection, current_row, chunk, result_df.columns.tolist())
    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_chunk_2():
    csv_file = True
    current_row = 2
    chunk = 3

    expected_result = result_df[1:4]
    expected_result.reset_index(inplace=True, drop=True)
    result = getfiles._get_file_chunk(test_logical_file_csv, csv_file, hpcc_connection, current_row, chunk, result_df.columns.tolist())
    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


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


def test_tidy_up():
    hpycc.delete.delete_logical_file(test_logical_file, 'localhost')
    hpycc.delete.delete_logical_file(test_logical_file_csv, 'localhost')
