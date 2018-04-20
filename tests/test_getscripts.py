import hpycc.getscripts.getscripts as get
import pandas as pd
from pandas.util.testing import assert_frame_equal

server = 'localhost'
port = "8010"
repo = None
username = "hpycc_get_output"
password = '" "'
silent = False
legacy = True

expected_result_1 = pd.DataFrame({'a': [1, 3, 5, 7, 9, 11], 'b': [2, 4, 6, 8, 10, 12]})
expected_result_2 = pd.DataFrame({'a': [11, 13, 15, 17, 19, 111], 'b': [12, 14, 16, 18, 110, 112]})


def test_get_script():
    script = 'tests/ECLtest_runScript_1output.ecl'

    result = get.get_script(script, server, port, repo, username, password, silent, legacy)
    assert_frame_equal(result[0][1], expected_result_1, check_dtype=False, check_like=False)


def test_get_script():
    script = 'tests/ECLtest_runScript_2outputs.ecl'

    result = get.get_script(script, server, port, repo, username, password, silent, legacy)

    assert_frame_equal(result[0][1], expected_result_1, check_dtype=False, check_like=False)
    assert_frame_equal(result[1][1], expected_result_2, check_dtype=False, check_like=False)
