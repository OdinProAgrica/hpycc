"""
This module contains tests for hpycc.connection.

"""
import unittest
from unittest.mock import patch

import hpycc.connection


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

