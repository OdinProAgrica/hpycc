import hpycc.get as get
from hpycc.utils.datarequests import run_command
import pandas as pd
from pandas.util.testing import assert_frame_equal
import os

# test_script_location = ''
test_script_location = 'tests/'
server = 'localhost'
port = "8010"
repo = None
username = "hpycc_get_output"
password = '" "'
silent = False
legacy = True
do_syntaxcheck = True
log_to_file = True
debg = True


expected_result_1 = pd.DataFrame({'a': [1, 3, 5, 7, 9, 11], 'b': [2, 4, 6, 8, 10, 12]})
expected_result_2 = pd.DataFrame({'a': [11, 13, 15, 17, 19, 111], 'b': [12, 14, 16, 18, 110, 112]})
expected_result = pd.DataFrame({'a': ['1', '3', '5', '7', '9', '11'], 'b': ['2', '4', '6', '8', '10', '12']})
expected_result_chunk = pd.DataFrame({'a': ['3', '5', '7'], 'b': ['4', '6', '8']})

# Create HPCC test files
run_command(("ecl run --server {} --port {} --username {} --password {} -legacy "
               "thor {} {}").format(server, '8010', 'test_call', '" "', './tests/ECLtest_makeTestData.ecl', ''))


def test_get_output():
    script = test_script_location + 'ECLtest_runScript_1output.ecl'

    result = get.get_output(script, server, port="8010", repo=None,
               username="hpycc_get_output", password='" "', silent=False,
               legacy=False, do_syntaxcheck=True, log_to_file=log_to_file, debg=debg)

    assert_frame_equal(result, expected_result_1, check_dtype=False, check_like=False)


def test_get_outputs():
    script = test_script_location + 'ECLtest_runScript_2outputs.ecl'

    result = get.get_outputs(script, server, port="8010", repo=None,
                username="hpycc_get_output", password='" "', silent=False,
                legacy=False, do_syntaxcheck=True, log_to_file=log_to_file, debg=debg)

    assert_frame_equal(result['Result 1'], expected_result_1, check_dtype=False, check_like=False)
    assert_frame_equal(result['Result 2'], expected_result_2, check_dtype=False, check_like=False)


def test_get_file():
    logical_file = '~a::test::file'
    result = get.get_file(logical_file, server, port='8010', username=username,
                          password=password, csv_file=False, silent=silent, debg=debg)

    assert_frame_equal(result, expected_result, check_dtype=False, check_like=False)


def test_save_output():
    script = test_script_location + 'ECLtest_runScript_1output.ecl'
    path = 'testOuputSave.csv'

    get.save_output(script, server, path, port="8010", repo=None,
                username="hpycc_get_output", password='" "', silent=False,
                compression=None, legacy=False, log_to_file=log_to_file, debg=debg)

    result = pd.read_csv(path)
    os.remove(path)

    assert_frame_equal(result, expected_result_1, check_dtype=False, check_like=False)


def test_save_outputs():
    script = test_script_location + 'ECLtest_runScript_2outputs.ecl'

    get.save_outputs(
        script, server, directory=".", port="8010", repo=None,
        username="hpycc_get_output", password='" "', silent=False,
        compression=None, filenames=None, prefix="", legacy=False,
        do_syntaxcheck=True, log_to_file=log_to_file, debg=debg)

    result_1 = pd.read_csv('Result 1.csv')
    result_2 = pd.read_csv('Result 2.csv')

    os.remove('Result 1.csv')
    os.remove('Result 2.csv')

    assert_frame_equal(result_1, expected_result_1, check_dtype=False, check_like=False)
    assert_frame_equal(result_2, expected_result_2, check_dtype=False, check_like=False)


def test_save_file():
    logical_file = '~a::test::file'
    path = 'testOuputSave.csv'

    get.save_file(logical_file, path, server, port, username=username,
                  password=password, csv_file=False, compression=None,
                  silent=silent, log_to_file=log_to_file, debg=debg)

    result = pd.read_csv(path)
    os.remove(path)

    assert_frame_equal(result, expected_result_1, check_dtype=False, check_like=False)
