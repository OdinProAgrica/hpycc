import hpycc.utils.syntaxcheck as syntax
import pytest

silent = False
legacy = True
repo = None

def test_sntaxcheck_pass():
    result = syntax.syntax_check('tests/ECLtest_pass.ecl', repo, silent, legacy)
    assert result is None


def test_sntaxcheck_warning():
    with pytest.warns(UserWarning) as e_info:
        syntax.syntax_check('tests/ECLtest_warn.ecl', repo, silent, legacy)


def test_sntaxcheck_fail():
    with pytest.raises(Exception) as e_info:
        syntax.syntax_check('tests/ECLtest_fail.ecl', repo, silent, legacy)
