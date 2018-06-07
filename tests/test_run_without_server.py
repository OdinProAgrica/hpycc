import unittest
from unittest.mock import patch

from hpycc import run_script, Connection


class TestRunScript(unittest.TestCase):
    @patch.object(Connection, "run_ecl_script")
    def test_run_script_uses_parameters(self, mock):
        conn = Connection("user", test_conn=False)
        run_script(conn, "abc.ecl", False)
        mock.assert_called_with("abc.ecl", False)
