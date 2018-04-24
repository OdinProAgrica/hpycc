import hpycc.filerunning.getfiles as get
import hpycc.scriptrunning.runscript as run
from hpycc.utils.datarequests import run_command
import pandas as pd
from pandas.util.testing import assert_frame_equal


# script_loc = ''
script_loc = './tests/'
server = 'localhost'
port = '8010'
column_names = ['a', 'b']
expected_result = pd.DataFrame({'a': [1, 3, 5, 7, 9, 11], 'b': [2, 4, 6, 8, 10, 12]})
expected_result_chunk = pd.DataFrame({'a': [3, 5, 7], 'b': [4, 6, 8]})
username = "hpycc_get_output"
password = '" "'
silent = False
download_threads = 15


def make_small_data():
    script = script_loc + 'ECLtest_makeTestData.ecl'
    repo = None
    legacy = False
    do_syntaxcheck = True
    run.run_script_internal(script, server, port, repo, username, password, legacy, do_syntaxcheck)


def make_large_data(start_row=0, size=25000):
    wrapper = """a := DATASET([%s], {INTEGER a; INTEGER b});
              OUTPUT(a, ,'~a::test::bigfile', EXPIRE(1), OVERWRITE);"""

    some_data = list(range(start_row, size))
    backwards_data = list(reversed(some_data))

    row_string = '{%s,%s},'
    out_rows = ''
    for i, j in zip(some_data, backwards_data):
        out_rows += row_string % (i, j)
    out_rows += '{0,0}'

    ECLcommand = wrapper % out_rows
    with open(script_loc + 'ECLtest_makeBIGTestData.ecl', 'w') as f:
        f.writelines(ECLcommand)

    some_data.append(0)
    backwards_data.append(0)
    expected_big_data = pd.DataFrame({'a': some_data, 'b': backwards_data})

    script = script_loc + 'ECLtest_makeBIGTestData.ecl'
    repo = None
    legacy = False
    do_syntaxcheck = True
    run.run_script_internal(script, server, port, repo, username, password, legacy, do_syntaxcheck)

    return expected_big_data


def test_get_file_1():
    file_name = '~a::test::file'
    csv_file = False
    make_small_data()
    result = get.get_file_internal(file_name, server, port, username, password, csv_file, download_threads)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    # make_small_data()
    result = get.get_file_internal(file_name, server, port, username, password, csv_file, download_threads)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_3():
    file_name = '~a::test::filecsv'
    csv_file = True
    # make_small_data()
    result = get.get_file_internal(file_name, server, port, username, password, csv_file, download_threads)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_get_file_4():
    file_name = '~a::test::bigfile'
    csv_file = False
    expected_big_data = make_large_data()
    result = get.get_file_internal(file_name, server, port, username, password, csv_file, download_threads)

    result.reset_index(inplace=True, drop=True)
    expected_big_data.reset_index(inplace=True, drop=True)

    assert_frame_equal(result, expected_big_data, check_dtype=False, check_like=False)


def test_get_file_structure_1():
    file_name = '~a::test::file'
    csv_file = False
    # make_small_data()
    result = get._get_file_structure(file_name, server, port, username, password, csv_file)
    assert result == (['a', 'b'], 6)


def test_get_file_structure_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    # make_small_data()
    result = get._get_file_structure(file_name, server, port, username, password, csv_file)
    assert result == (['a', 'b'], 6)


def test_get_file_chunk_1():
    file_name = '~a::test::file'
    csv_file = False
    # make_small_data()
    current_row = 1
    chunk = 3

    result = get._get_file_chunk(file_name, csv_file, server, port, username, password, current_row, chunk, column_names)
    assert_frame_equal(result, expected_result_chunk, check_dtype=False, check_like=False)


def test_get_file_chunk_2():
    file_name = '~a::test::filecsv'
    csv_file = True
    # make_small_data()
    current_row = 2
    chunk = 3

    result = get._get_file_chunk(file_name, csv_file, server, port, username, password, current_row, chunk, column_names)
    assert_frame_equal(result, expected_result_chunk, check_dtype=False, check_like=False)


def test_make_chunks_1():
    file_size = 10
    csv_file = False

    start_rows, chunks = get._make_chunks(file_size, csv_file)
    assert start_rows == [0]
    assert chunks == [10]


def test_make_chunks_2():
    file_size = 10
    csv_file = True

    start_rows, chunks = get._make_chunks(file_size, csv_file)
    assert start_rows == [1]
    assert chunks == [10]


def test_make_chunks_3():
    file_size = 10001
    csv_file = False

    start_rows, chunks = get._make_chunks(file_size, csv_file)
    assert start_rows == [0, 10000]
    assert chunks == [10000, 1]


def test_make_chunks_4():
    file_size = 10002
    csv_file = True

    start_rows, chunks = get._make_chunks(file_size, csv_file)
    assert start_rows == [1, 10001]
    assert chunks == [10000, 1]




