import hpycc.scriptrunning.getscripts as getscripts
import pandas as pd
from pandas.util.testing import assert_frame_equal
from hpycc.utils.HPCCconnector import HPCCconnector
import tests.make_data as md

# script_loc = ''
script_loc = './tests/'
silent = False
do_syntaxcheck = True

hpcc_connection = HPCCconnector('localhost', '8010', None, "hpycc_get_output", '" "', False)

expected_result_1 = pd.DataFrame({'a': [1, 3, 5, 7, 9, 11], 'b': [2, 4, 6, 8, 10, 12]})
expected_result_2 = pd.DataFrame({'a': [11, 13, 15, 17, 19, 111], 'b': [12, 14, 16, 18, 110, 112]})

md.upload_small_data(hpcc_connection)


def test_get_script_1():
    script = script_loc + 'ECLtest_runScript_1output.ecl'

    result = getscripts.get_script_internal(script, hpcc_connection, do_syntaxcheck)
    assert_frame_equal(result[0][1], expected_result_1, check_dtype=False, check_like=False)


def test_get_script_2():
    script = script_loc + 'ECLtest_runScript_2outputs.ecl'
    result = getscripts.get_script_internal(script, hpcc_connection, do_syntaxcheck)

    assert_frame_equal(result[0][1], expected_result_1, check_dtype=False, check_like=False)
    assert_frame_equal(result[1][1], expected_result_2, check_dtype=False, check_like=False)
