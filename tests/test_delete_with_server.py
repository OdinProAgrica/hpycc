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

#TODO Create a check delete. 1. Creature WUID. 2. Delete with correct response. 3. Trt delete again with correct response.
#TODO try to somehow seew what would would happen if you input a string? maybe set the

    def test_delete_workunit_actually_deletes_workunit(self):
        script = "OUTPUT(2);"
        res = _get_output_from_ecl_string(self.conn, script)
        res = res.stdout.replace("\r\n", "")
        wuid = hpycc.get.get_wuid_xml(res)

        res = hpycc.delete_workunit(self.conn, wuid)

        expected = """{'WUDeleteResponse': {}}"""
        pd.testing.assert_frame_equal(expected, res)


    def test_delete_workunit_fails_on_nonexistent_workunit(self):

        wuid = 'IReallyHopeThisIsntARealWorkunitID'
        res = hpycc.delete_workunit(self.conn, wuid)
        res2 = list(res['WUDeleteResponse'].values()).str
        expected = """[{'WUActionResult': [{'Action': 'Delete',    'Result': '',    'Wuid': 'IReallyHopeThisIsntARealWorkunitID'}]}]"""
        pd.testing.assert_frame_equal(expected, res2)