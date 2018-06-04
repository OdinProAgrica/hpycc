"""
This module contains old_tests for hpycc.connection.

"""
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
        self.assertIsTrue(result)

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


class TestConnectionRunCommand:
    pass


class TestConnectionCheckSyntax:
    pass


class TestConnectionRunEclScript:
    pass


class TestConnectionRunURLRequest:
    pass


class TestConnectionGetLogicalFileChunk:
    pass


class TestConnectionRunEclString:
    pass
