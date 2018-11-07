import unittest

import hpycc
import hpycc.utils.parsers
from hpycc.delete import delete_logical_file
from hpycc.utils import docker
import re


# noinspection PyPep8Naming
def setUpModule():
    docker.HPCCContainer(tag="6.4.26-1")


# noinspection PyPep8Naming
def tearDownModule():
    docker.HPCCContainer(pull=False, start=False).stop_container()


def check_file_exists(conn, file_name):
    res = conn.run_ecl_string("IMPORT std; STD.File.FileExists('%s');" % file_name, False, False, None)
    x = re.findall('<Row><Result_1>([a-z]+)</Result_1></Row>', str(res))

    return x[0] == 'true'


class TestDeleteWorkunitWithServer(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user")

    def test_delete_workunit_actually_deletes_workunit(self):
        string = "OUTPUT(2);"
        result = self.conn.run_ecl_string(string, syntax_check=True,
                                          delete_workunit=False, stored={})
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


class TestDeleteLogicalFile(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user")

    def test_delete_logical_file_deletes(self):
        string = "a := DATASET([{1}], {INTEGER int;}); OUTPUT(a,,'~test_delete_logical_file_deletes1');"
        self.conn.run_ecl_string(string, syntax_check=True,
                                     delete_workunit=True, stored={})

        res1 = check_file_exists(self.conn, '~test_delete_logical_file_deletes1')
        delete_logical_file(self.conn, '~test_delete_logical_file_deletes1')
        res2 = check_file_exists(self.conn, '~test_delete_logical_file_deletes1')

        self.assertTrue(res1)
        self.assertFalse(res2)


    def test_delete_logical_file_doesnt_delete_other_files(self):
        string = "a := DATASET([{1}], {INTEGER int;}); OUTPUT(a,,'~test_delete_logical_file_doesnt_delete1');"
        self.conn.run_ecl_string(string, syntax_check=True,
                                     delete_workunit=True, stored={})
        string = "a := DATASET([{1}], {INTEGER int;}); OUTPUT(a,,'~test_delete_logical_file_doesnt_delete2');"
        self.conn.run_ecl_string(string, syntax_check=True,
                                     delete_workunit=True, stored={})

        res1 = check_file_exists(self.conn, '~test_delete_logical_file_doesnt_delete1')
        res2 = check_file_exists(self.conn, '~test_delete_logical_file_doesnt_delete2')
        delete_logical_file(self.conn, '~test_delete_logical_file_doesnt_delete1')
        res3 = check_file_exists(self.conn, '~test_delete_logical_file_doesnt_delete1')
        res4 = check_file_exists(self.conn, '~test_delete_logical_file_doesnt_delete2')

        assert all([res1, res2, ~res3, res4])
