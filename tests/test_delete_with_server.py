import os
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
import warnings
import hpycc
import numpy as np
import pandas as pd

import hpycc
from tests.test_helpers import hpcc_functions


# noinspection PyPep8Naming
def setUpModule():
    a = hpcc_functions.start_hpcc_container()
    hpcc_functions.start_hpcc(a)


# noinspection PyPep8Naming
def tearDownModule():
    hpcc_functions.stop_hpcc_container()

def _get_output_from_ecl_string(conn, string, syntax=True,
                                delete_workunit=True):
    with TemporaryDirectory() as d:
        p = os.path.join(d, "test.ecl")
        with open(p, "w+") as file:
            file.write(string)
        res = hpycc.get_output(conn, p, syntax,  delete_workunit)
        return res


def _get_outputs_from_ecl_string(conn, string, syntax=True,
                                 delete_workunit=True):
    with TemporaryDirectory() as d:
        p = os.path.join(d, "test.ecl")
        with open(p, "w+") as file:
            file.write(string)
        res = hpycc.get_outputs(conn, p, syntax,  delete_workunit)
        return res


class TestDeleteWorkunitwithServer(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user")


    def test_delete_workunit_actually_deletes_workunit(self):
        string = "OUTPUT(2);"
        result = self.conn.run_ecl_string(string, syntax_check=True)
        result = result.stdout.replace("\r\n", "")
        wuid = hpycc.get.get_wuid_xml(result)
        res = hpycc.delete_workunit(self.conn, wuid)
        expected = {'WUDeleteResponse': {}}
        self.assertEqual(expected, res)


    def test_delete_workunit_fails_on_nonexistent_workunit(self):

        wuid = 'IReallyHopeThisIsNotARealWorkunitID'
        res = hpycc.delete_workunit(self.conn, wuid)
        expected = {'WUDeleteResponse': {'ActionResults': {'WUActionResult':
                 [{'Action': 'Delete', 'Result': 'Failed: Invalid Workunit ID:'
                             ' IReallyHopeThisIsNotARealWorkunitID',
                   'Wuid': 'IReallyHopeThisIsNotARealWorkunitID'}]}}}
        self.assertEqual(expected, res)
