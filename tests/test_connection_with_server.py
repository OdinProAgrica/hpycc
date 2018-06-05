"""
This module contains old_tests for hpycc.connection.

"""
import os
import subprocess
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
import requests
from requests.exceptions import ConnectionError

import hpycc.connection
from tests.test_helpers import hpcc_functions


def setUpModule():
    a = hpcc_functions.start_hpcc_container()
    hpcc_functions.start_hpcc(a)


def tearDownModule():
    hpcc_functions.stop_hpcc_container()


class TestConnectionTestConnectionWithNoAuth(unittest.TestCase):
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


class TestConnectionRunECLScriptWithServer(unittest.TestCase):
    def test_run_script_runs_script(self):
        conn = hpycc.Connection("user", test_conn=False)
        good_script = "output(2);"
        with TemporaryDirectory() as d:
            p = os.path.join(d, "test.ecl")
            with open(p, "w+") as file:
                file.write(good_script)
            self.assertTrue(conn.run_ecl_script(p, syntax_check=False))

    def test_run_script_returns_correct_tuple(self):
        conn = hpycc.Connection("user", test_conn=False)
        good_script = "output(2);"
        expected_out ="\r\n".join([
            "\r\n<Result>", "<Dataset name='Result 1'>",
            " <Row><Result_1>2</Result_1></Row>",
            "</Dataset>", "</Result>\r\n"
        ])
        with TemporaryDirectory() as d:
            p = os.path.join(d, "test.ecl")
            with open(p, "w+") as file:
                file.write(good_script)
            result = conn.run_ecl_script(p, syntax_check=False)
        self.assertEqual(result.__class__.__name__, "Result")
        self.assertEqual(result.stdout, expected_out)
        self.assertEqual(result.stderr, "")

    def test_run_script_fails_if_file_not_found(self):
        conn = hpycc.Connection("user", test_conn=False)
        with self.assertRaises(subprocess.SubprocessError):
            conn.run_ecl_script("test.ecl", syntax_check=False)


class TestRunURLRequestWithServer(unittest.TestCase):
    def test_run_url_request_returns_response(self):
        conn = hpycc.Connection("user", test_conn=False)
        result = conn.run_url_request("http://localhost:8010")
        self.assertIsInstance(result, requests.Response)


class TestConnectionGetLogicalFileChunkWithServer(unittest.TestCase):
    def test_get_logical_file_chunk_returns_correct_json(self):
        expected_result = [
            {'__fileposition__': '10', 'a': '2', 'b': 'b'},
            {'__fileposition__': '20', 'a': '3', 'b': 'c'}
        ]
        conn = hpycc.Connection("user")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        with TemporaryDirectory() as d:
            p = os.path.join(d, "data.csv")
            df.to_csv(p, index=False)
            hpycc.spray_file(conn, p, "data", chunk_size=3)

        result = conn.get_logical_file_chunk("thor::data", 1, 3, 3, 0)
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], dict)
        self.assertEqual(result, expected_result)


class TestConnectionRunEclString(unittest.TestCase):
    # does it call run_ecl_script?
    # does it send the string through properly?
    # does it call a syntax check?
    # does it listen to syntax check?
    # does it raise the right error if it fails syntax check?

    pass
