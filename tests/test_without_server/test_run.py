import unittest
from unittest.mock import patch

from hpycc import run_script, Connection


class TestRunScript(unittest.TestCase):
    @patch.object(Connection, "run_ecl_script")
    def test_run_script_uses_default_parameters(self, mock):
        conn = Connection("user", test_conn=False)
        run_script(conn, "abc.ecl")
        mock.assert_called_with("abc.ecl", True, True)

    @patch.object(Connection, "run_ecl_script")
    def test_run_script_uses_custom_parameters(self, mock):
        conn = Connection("user", test_conn=False)
        run_script(conn, "abc.ecl", False, False)
        mock.assert_called_with("abc.ecl", False, False)
