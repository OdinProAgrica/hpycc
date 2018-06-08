import os
import pandas as pd
from pandas.util.testing import assert_frame_equal

import hpycc.delete
import hpycc.get as get
import hpycc.run as run
import hpycc.save
import old_tests.make_data as md
from hpycc.utils.HPCCconnector import HPCCconnector

if __name__ == "__main__":
    test_script_location = ''
else:
    test_script_location = './old_tests/'

do_syntaxcheck = True
log_to_file = True
debg = True
server = 'localhost'
port = "8010"
repo = None
username = "hpycc_get_output"
password = '" "'
silent = False
legacy = True
logical_file = '~TEMP::HPYCC::TEST::FILE'
hpcc_connection = HPCCconnector(server, port, repo, username, password, legacy)


# TODO this should use the data generator.
expected_result_1 = pd.DataFrame({'a': [1, 3, 5, 7, 9, 11], 'b': [2, 4, 6, 8, 10, 12]})
expected_result_2 = pd.DataFrame({'a': [11, 13, 15, 17, 19, 111], 'b': [12, 14, 16, 18, 110, 112]})
expected_result = pd.DataFrame({'a': [1, 3, 5, 7, 9, 11], 'b': [2, 4, 6, 8, 10, 12]})
expected_result_chunk = pd.DataFrame({'a': [3, 5, 7], 'b': [4, 6, 8]})

result_df = md.upload_data(logical_file, 15000, hpcc_connection)


def test_get_output():
    script = test_script_location + 'ECLtest_runScript_1output.ecl'

    result = get.get_output(script, server, port="8010", repo=None,
                            username="hpycc_get_output", password='" "',
                            legacy=False, syntax_check=True,
                            silent=False, debg=True)

    assert_frame_equal(result, expected_result_1, check_dtype=False, check_like=False)


def test_get_outputs():
    script = test_script_location + 'ECLtest_runScript_2outputs.ecl'

    result = get.get_outputs(script, server, port="8010", repo=None,
                             username="hpycc_get_output", password='" "', silent=False,
                             legacy=False, syntax_check=True, log_to_file=log_to_file, debg=debg)

    assert_frame_equal(result['Result 1'], expected_result_1, check_dtype=False, check_like=False)
    assert_frame_equal(result['Result 2'], expected_result_2, check_dtype=False, check_like=False)


def test_get_file():
    logical_file_getfile = '~TEMP::HPYCC::TEST::GETFILE'
    result_df_getfile = md.upload_data(logical_file_getfile, 15000, hpcc_connection)

    result = get.get_logical_file(logical_file_getfile, server, port='8010', username=username,
                                  password=password, csv=False, silent=silent, debg=debg)

    hpycc.delete.delete_logical_file(logical_file_getfile, 'localhost')

    assert_frame_equal(result, result_df_getfile, check_dtype=False, check_like=False)


def test_save_output():
    script = test_script_location + 'ECLtest_runScript_1output.ecl'
    path = 'testOuputSave.csv'

    hpycc.save.save_output(script, server, path, port="8010", repo=None,
                           username="hpycc_get_output", password='" "', silent=False,
                           compression=None, legacy=False, log_to_file=log_to_file, debg=debg)

    result = pd.read_csv(path)
    os.remove(path)

    assert_frame_equal(result, expected_result_1, check_dtype=False, check_like=False)


def test_save_outputs():
    script = test_script_location + 'ECLtest_runScript_2outputs.ecl'

    hpycc.save.save_outputs(
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
    logical_file_savefile = '~TEMP::HPYCC::TEST::SAVEFILE'
    result_df_savefile = md.upload_data(logical_file_savefile, 15000, hpcc_connection)
    path = 'testOuputSave.csv'

    hpycc.save.save_logical_file(logical_file_savefile, path, server, port, username=username,
                                 password=password, csv_file=False, compression=None,
                                 silent=silent, log_to_file=log_to_file, debg=debg)

    result = pd.read_csv(path)
    os.remove(path)
    hpycc.delete.delete_logical_file(logical_file_savefile, 'localhost')
    assert_frame_equal(result, result_df_savefile, check_dtype=False, check_like=False)


def test_tidy_up():
    hpycc.delete.delete_logical_file(logical_file, 'localhost')
