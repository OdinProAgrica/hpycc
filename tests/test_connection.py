"""
This module contains old_tests for hpycc.connection.

"""
from collections import namedtuple
import os
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from requests.exceptions import ConnectionError, HTTPError

import hpycc.connection
from tests.test_helpers import hpcc_functions


class TestConnectionDefaultAttributes(unittest.TestCase):
    @patch.object(hpycc.connection.Connection, "test_connection")
    def setUp(self, _):
        self.conn = hpycc.connection.Connection("username")

    def test_server(self):
        self.assertEqual(self.conn.server, "localhost")

    def test_port(self):
        self.assertEqual(self.conn.port, 8010)

    def test_repo(self):
        self.assertEqual(self.conn.repo, None)

    def test_password(self):
        self.assertEqual(self.conn.password, None)

    def test_legacy(self):
        self.assertEqual(self.conn.legacy, False)


class TestConnectionCustomAttributes(unittest.TestCase):
    @patch.object(hpycc.connection.Connection, "test_connection")
    def setUp(self, _):
        self.conn = hpycc.connection.Connection(
            "some_user", "some_server", 9000, "some_repo", "password", True)

    def test_username(self):
        self.assertEqual(self.conn.username, "some_user")

    def test_server(self):
        self.assertEqual(self.conn.server, "some_server")

    def test_port(self):
        self.assertEqual(self.conn.port, 9000)

    def test_repo(self):
        self.assertEqual(self.conn.repo, "some_repo")

    def test_password(self):
        self.assertEqual(self.conn.password, "password")

    def test_legacy(self):
        self.assertEqual(self.conn.legacy, True)


class TestConnectionInitTestConnection(unittest.TestCase):
    @patch.object(hpycc.connection.Connection, "test_connection")
    def test_calls_test_connection_on_init(self, mock):
        hpycc.connection.Connection("usrname")
        mock.assert_called()


class TestConnectionTestConnectionWithNoAuth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = hpcc_functions.start_hpcc_container()
        hpcc_functions.start_hpcc(cls.container)

    @classmethod
    def tearDownClass(cls):
        cls.container.stop()

    def test_test_connection_passes_successfully_with_no_auth(self):
        conn = hpycc.connection.Connection(username=None)
        result = conn.test_connection()
        self.assertTrue(result)

    def test_test_connection_passes_successfully_with_username(self):
        conn = hpycc.connection.Connection(username="testuser")
        result = conn.test_connection()
        self.assertTrue(result)

    def test_test_connection_works_with_numeric_server(self):
        conn = hpycc.connection.Connection(username="test", server="127.0.0.1")
        result = conn.test_connection()
        self.assertTrue(result)

    def test_test_connection_fails_with_incorrect_server(self):
        with self.assertRaises(ConnectionError):
            conn = hpycc.connection.Connection(username="test", server="l")
            conn.test_connection()

    def test_test_connection_fails_with_incorrect_port(self):
        with self.assertRaises(ConnectionError):
            conn = hpycc.connection.Connection(username="test", port=9999)
            conn.test_connection()

    def test_test_connection_passes_with_password(self):
        conn = hpycc.connection.Connection(username="test", password="pw")
        result = conn.test_connection()
        self.assertTrue(result)


class TestConnectionTestConnectionWithAuth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = hpcc_functions.start_hpcc_container()
        hpcc_functions.password_hpcc(cls.container)
        hpcc_functions.start_hpcc(cls.container)

        cls.error_string = ("401 Client Error: Unauthorized for url: "
                            "http://localhost:8010/")

    @classmethod
    def tearDownClass(cls):
        cls.container.stop()
        pass

    def test_test_connection_passes_with_correct_auth(self):
        conn = hpycc.connection.Connection(username="test1", password="1234")
        result = conn.test_connection()
        self.assertTrue(result)

    def test_test_connection_fails_auth_with_incorrect_username(self):
        with self.assertRaisesRegex(HTTPError, self.error_string):
            conn = hpycc.connection.Connection(username="abc", password="1234")
            conn.test_connection()

    def test_test_connection_fails_auth_with_missing_username(self):
        with self.assertRaisesRegex(HTTPError, self.error_string):
            conn = hpycc.connection.Connection(username=None, password="1234")
            conn.test_connection()

    def test_test_connection_fails_auth_with_incorrect_password(self):
        with self.assertRaisesRegex(HTTPError, self.error_string):
            conn = hpycc.connection.Connection(username="test1", password="12")
            conn.test_connection()

    def test_test_connection_fails_auth_with_missing_password(self):
        with self.assertRaisesRegex(HTTPError, self.error_string):
            conn = hpycc.connection.Connection(username="abc", password=None)
            conn.test_connection()


class TestConnectionRunCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = hpycc.connection.Connection("user", test_conn=False)
        cls.Result = namedtuple("Result", ["stdout", "stderr"])

    @patch.object(subprocess, "run")
    def test_run_command_calls_subprocess_run(self, mock):
        result_tuple = self.Result(b"a", b"b")
        mock.return_value = result_tuple
        self.conn._run_command("ls")
        mock.assert_called()

    def test_run_command_raises_error_if_bad_return_code(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.conn._run_command("fghdf")

    def test_run_command_passes_if_good_return_code(self):
        cmd_result = self.conn._run_command("cd .")
        expected = self.Result("", "")
        self.assertEqual(cmd_result, expected)

    def test_run_command_passes_if_stderr_present(self):
        cmd_result = self.conn._run_command('>&2 echo error')
        expected = self.Result("", "error\r\n")
        self.assertEqual(cmd_result, expected)

    def test_run_command_returns_stdout(self):
        cmd_result = self.conn._run_command('echo hello')
        expected = "hello\r\n"
        self.assertEqual(cmd_result.stdout, expected)

    def test_run_command_returns_named_tuple(self):
        result = self.conn._run_command('>&2 echo error')
        self.assertEqual(result.__class__.__name__, "Result")
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "error\r\n")

    def test_run_command_returns_decoded_strings(self):
        result = self.conn._run_command('>&2 echo error')
        self.assertIsInstance(result.stderr, str)
        self.assertIsInstance(result.stdout, str)


class TestConnectionCheckSyntax(unittest.TestCase):
    @patch.object(hpycc.Connection, "_run_command")
    def test_check_syntax_builds_correct_path_with_no_legacy_or_repo(
            self, mock):
        conn = hpycc.Connection("user", legacy=False, repo=None, test_conn=False)
        conn.check_syntax("non.ecl")
        mock.assert_called_with("eclcc -syntax non.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_check_syntax_builds_correct_path_with_legacy(self, mock):
        conn = hpycc.Connection("user", legacy=True, repo=None, test_conn=False)
        conn.check_syntax("non.ecl")
        mock.assert_called_with("eclcc -syntax -legacy non.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_check_syntax_builds_correct_path_with_legacy_and_repo(self, mock):
        conn = hpycc.Connection("user", legacy=True, repo="./dir", test_conn=False)
        conn.check_syntax("non.ecl")
        mock.assert_called_with("eclcc -syntax -legacy -I ./dir non.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_check_syntax_builds_correct_path_with_repo(self, mock):
        conn = hpycc.Connection("user", legacy=False, repo="./dir", test_conn=False)
        conn.check_syntax("non.ecl")
        mock.assert_called_with("eclcc -syntax -I ./dir non.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_check_syntax_calls_run_command(self, mock):
        conn = hpycc.Connection("user", test_conn=False)
        conn.check_syntax("non.ecl")
        mock.assert_called()

    def test_check_syntax_passes_with_good_script(self):
        conn = hpycc.Connection("user", test_conn=False)
        with TemporaryDirectory() as d:
            p = os.path.join(d, "tmp.ecl")
            with open(p, "w+") as file:
                file.write("output(2);")
            self.assertIsNone(conn.check_syntax(p))

    def test_check_syntax_passes_with_warnings(self):
        conn = hpycc.Connection("user", test_conn=False)
        warning_string = "\n".join([
            "a := DATASET([{1,2}], {INTEGER a; INTEGER b});",
            "a;",
            "b := DATASET([{1,2}], {INTEGER a; INTEGER b});",
            "b;",
            "groupREC := RECORD",
            "STRING a := a.a;",
            "STRING b := a.b;",
            "INTEGER n := count(group);",
            "END;",
            "TABLE(a, groupREC, a)"
        ])
        with TemporaryDirectory() as d:
            p = os.path.join(d, "tmp.ecl")
            with open(p, "w+") as file:
                file.write(warning_string)
            self.assertIsNone(conn.check_syntax(p))

    def test_check_syntax_fails_with_errors(self):
        conn = hpycc.Connection("user", test_conn=False)
        with TemporaryDirectory() as d:
            p = os.path.join(d, "tmp.ecl")
            with open(p, "w+") as file:
                file.write("sdfdsf")
            with self.assertRaises(subprocess.CalledProcessError):
                conn.check_syntax(p)


class TestConnectionRunEclScript(unittest.TestCase):
    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_server_port_and_username(self, mock):
        conn = hpycc.Connection("user", test_conn=False, server="abc",
                                port=123)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server abc --port 123 --username user --password 'None' "
            "thor test.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_username_if_none(self, mock):
        conn = hpycc.Connection(None, test_conn=False, server="abc",
                                port=123)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server abc --port 123 --username None --password 'None' "
            "thor test.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_password(self, mock):
        conn = hpycc.Connection("user", password="abc", test_conn=False)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server localhost --port 8010 --username user "
            "--password 'abc' thor test.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_works_with_special_password_chars(
            self, mock):
        conn = hpycc.Connection("user", password="a\nb", test_conn=False)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server localhost --port 8010 --username user "
            "--password 'a\nb' thor test.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_password_if_none(self, mock):
        conn = hpycc.Connection("user", password=None, test_conn=False)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server localhost --port 8010 --username user "
            "--password 'None' thor test.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_password_if_all_spaces(self, mock):
        conn = hpycc.Connection("user", password="  ", test_conn=False)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server localhost --port 8010 --username user "
            "--password '  ' thor test.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_legacy(self, mock):
        conn = hpycc.Connection("user", legacy=True, test_conn=False)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server localhost --port 8010 --username user "
            "--password 'None' -legacy thor test.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_legacy_if_none(self, mock):
        conn = hpycc.Connection("user", legacy=None, test_conn=False)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server localhost --port 8010 --username user "
            "--password 'None' thor test.ecl")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_repo(self, mock):
        conn = hpycc.Connection("user", test_conn=False, repo="C:")
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server localhost --port 8010 --username user "
            "--password 'None' thor test.ecl -I C:")

    @patch.object(hpycc.Connection, "_run_command")
    def test_run_ecl_script_command_uses_repo_if_none(self, mock):
        conn = hpycc.Connection("user", test_conn=False, repo=None)
        conn.run_ecl_script("test.ecl", syntax_check=False)
        mock.assert_called_with(
            "ecl run --server localhost --port 8010 --username user "
            "--password 'None' thor test.ecl")

    def test_run_script_checks_syntax_if_true(self):
        pass
    def test_run_script_does_not_check_syntax_if_false(self):
        pass
    def test_run_script_fails_syntax_check_with_bad_script(self):
        pass
    def test_run_script_passes_syntax_check_with_good_script(self):
        pass
    def test_run_script_calls_run_command(self):
        pass
    def test_run_script_runs_script(self):
        pass
    def test_run_script_returns_correct_tuple(self):
        pass
    def test_run_script_fails_if_server_unavailable(self):
        pass
    def test_run_script_fails_if_file_not_found(self):
        pass



class TestConnectionRunURLRequest:
    pass


class TestConnectionGetLogicalFileChunk:
    pass


class TestConnectionRunEclString:
    pass
