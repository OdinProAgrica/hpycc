import os
from tempfile import TemporaryDirectory
import unittest

import hpycc
from tests.test_helpers import hpcc_functions


# noinspection PyPep8Naming
def setUpModule():
    a = hpcc_functions.start_hpcc_container()
    hpcc_functions.start_hpcc(a)


# noinspection PyPep8Naming
def tearDownModule():
    hpcc_functions.stop_hpcc_container()


class TestRunWithServer(unittest.TestCase):
    def test_run_script_saves_logical_file(self):
        conn = hpycc.Connection("user", test_conn=False)
        good_script = "\n".join([
            "a:= DATASET([{'1', 'a'}], "
            "{STRING a; STRING b});",
            "output(a, ,'testrunscriptsaveslogicalfile');"
        ])

        with TemporaryDirectory() as d:
            p = os.path.join(d, "test.ecl")
            with open(p, "w+") as file:
                file.write(good_script)
            self.assertTrue(conn.run_ecl_script(p, syntax_check=True))
        res = conn.get_logical_file_chunk(
            "thor::testrunscriptsaveslogicalfile", 0, 1, 3, 1)
        expected_result = [{"a": "1", "b": "a", "__fileposition__": "0"}]
        self.assertEqual(expected_result, res)
