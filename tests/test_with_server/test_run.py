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
            res = conn.run_ecl_script(p, syntax_check=True,
                                      delete_workunit=False)
            self.assertTrue(res)
        res = conn.get_logical_file_chunk(
            "thor::testrunscriptsaveslogicalfile", 0, 1, 3, 1)
        expected_result = [{"a": "1", "b": "a", "__fileposition__": "0"}]
        self.assertEqual(expected_result, res)

    def test_run_script_deletes_workunit(self):
        conn = hpycc.Connection("user", test_conn=False)
        good_script = ("#WORKUNIT('name','test_run_script_deletes_workunit');"
                       "OUTPUT(2);")
        with TemporaryDirectory() as d:
            p = os.path.join(d, "test.ecl")
            with open(p, "w+") as file:
                file.write(good_script)
            hpycc.run_script(conn, p)
        res = conn._run_command(
            "ecl getwuid -n test_run_script_deletes_workunit")
        self.assertEqual("", res.stdout)

    def test_run_script_does_not_delete_workunit(self):
        conn = hpycc.Connection("user", test_conn=False)
        good_script = ("#WORKUNIT('name','test_run_script_deletes_workunit');"
                       "OUTPUT(2);")
        with TemporaryDirectory() as d:
            p = os.path.join(d, "test.ecl")
            with open(p, "w+") as file:
                file.write(good_script)
            hpycc.run_script(conn, p, delete_workunit=False)
        res = conn._run_command(
            "ecl getwuid -n test_run_script_deletes_workunit")
        self.assertRegex(res.stdout, "W[0-9]{8}-[0-9]{6}")

    def test_run_script_returns_true(self):
        conn = hpycc.Connection("user", test_conn=False)
        good_script = "OUTPUT(2);"
        with TemporaryDirectory() as d:
            p = os.path.join(d, "test.ecl")
            with open(p, "w+") as file:
                file.write(good_script)
            res = hpycc.run_script(conn, p, delete_workunit=False)
        self.assertTrue(res)
