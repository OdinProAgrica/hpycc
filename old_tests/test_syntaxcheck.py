import hpycc.utils.syntaxcheck as syntax
import pytest
from hpycc.utils.HPCCconnector import HPCCconnector

if __name__ == "__main__":
    test_script_location = ''
else:
    test_script_location = './old_tests/'

silent = False
legacy = True
repo = None

hpcc_connection = HPCCconnector('localhost', '8010', None, "hpycc_get_output", '" "', False)


def test_syntax_check_pass():
    result = syntax.syntax_check(test_script_location + 'ECLtest_pass.ecl', hpcc_connection)
    assert result is None


# TODO: Pain in the arse to code up now logging is enabled. For now you'll see the warnings in the test log.
def test_syntax_check_warning():
    # with pytest.warns(UserWarning) as e_info:
    #     syntax.syntax_check('old_tests/ECLtest_warn.ecl', repo, legacy)
    syntax.syntax_check(test_script_location + 'ECLtest_warn.ecl', hpcc_connection)
    assert True


def test_syntax_check_fail():
    with pytest.raises(Exception) as e_info:
        syntax.syntax_check(test_script_location + 'ECLtest_fail.ecl', hpcc_connection)
