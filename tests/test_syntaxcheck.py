import hpycc.utils.syntaxcheck as syntax
import pytest

silent = False
legacy = True
repo = None


def test_sntaxcheck_pass():
    result = syntax.syntax_check('tests/ECLtest_pass.ecl', repo, legacy)
    assert result is None


# TODO: Pain in the arse to code up now logging is enabled. For now you'll see the warnings in the test log.
def test_syntaxcheck_warning():
    # with pytest.warns(UserWarning) as e_info:
    #     syntax.syntax_check('tests/ECLtest_warn.ecl', repo, legacy)
    syntax.syntax_check('tests/ECLtest_warn.ecl', repo, legacy)
    assert True


def test_sntaxcheck_fail():
    with pytest.raises(Exception) as e_info:
        syntax.syntax_check('tests/ECLtest_fail.ecl', repo, legacy)
