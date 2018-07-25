import unittest

import hpycc
import hpycc.utils.parsers
from tests.test_helpers import hpcc_functions


# noinspection PyPep8Naming
def setUpModule():
    a = hpcc_functions.start_hpcc_container()
    hpcc_functions.start_hpcc(a)


# noinspection PyPep8Naming
def tearDownModule():
    hpcc_functions.stop_hpcc_container()


class TestDeleteWorkunitWithServer(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user")

    def test_delete_workunit_actually_deletes_workunit(self):
        string = "OUTPUT(2);"
        result = self.conn.run_ecl_string(string, syntax_check=True,
                                          delete_workunit=False)
        result = result.stdout.replace("\r\n", "")
        wuid = hpycc.utils.parsers.parse_wuid_from_xml(result)
        res = hpycc.delete_workunit(self.conn, wuid)
        self.assertTrue(res)
        a = self.conn._run_command("ecl getname --wuid {}".format(wuid))
        self.assertEqual("", a.stdout)
        self.assertEqual("", a.stderr)

    def test_delete_workunit_fails_on_nonexistent_workunit(self):
        wuid = 'IReallyHopeThisIsNotARealWorkunitID'
        expected = (
            "{'WUDeleteResponse': {'ActionResults': {'WUActionResult': "
            "[{'Wuid': 'IReallyHopeThisIsNotARealWorkunitID', 'Action': "
            "'Delete', 'Result': 'Failed: Invalid Workunit ID: "
            "IReallyHopeThisIsNotARealWorkunitID'}]}}}"
        )
        with self.assertRaises(ValueError, msg=expected):
            hpycc.delete_workunit(self.conn, wuid)
