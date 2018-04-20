import hpycc.utils.syntaxcheck as syntax
import pytest


def test_sntaxcheck_pass():
    result = syntax.syntax_check('ECLtest_pass.ecl', repo=None, silent=False)
    assert result is None


def test_sntaxcheck_warning():
    with pytest.warns(UserWarning) as e_info:
        syntax.syntax_check('ECLtest_warn.ecl', repo=None, silent=False)


def test_sntaxcheck_fail():
    with pytest.raises(Exception) as e_info:
        syntax.syntax_check('ECLtest_fail.ecl', repo=None, silent=False)
