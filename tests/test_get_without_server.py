import unittest

import hpycc


class TestGetWUIDwithoutServer(unittest.TestCase):

    def test_get_wuid_error_with_bracketed_wuid(self):
        string = '\r\n Hi There ' \
                 '\r\n the wuid is W12345678-123456(2) and should return it'
        expected = 'W12345678-123456(2)'
        res1 = hpycc.get.get_wuid_error(string)
        res = res1.group(0)
        self.assertEqual(expected, res)

    def test_get_wuid_error_returns_with_extra_dash_wuid(self):
        string = '\r\n Hi There' \
                 '\r\n wuid is W12345678-123456-5 and should return it'
        expected = 'W12345678-123456-5'
        res1 = hpycc.get.get_wuid_error(string)
        res = res1.group(0)
        self.assertEqual(expected, res)

    def test_get_wuid_error_returns_intended(self):
        string = '\r\n Hi There' \
                 '\r\n this is the wuid W12345678-123456 and should return it'
        expected = 'W12345678-123456'
        res1 = hpycc.get.get_wuid_error(string)
        res = res1.group(0)
        self.assertEqual(expected, res)

    def test_get_wuid_error_cannot_find_wuid(self):
        string = '\r\n Hi There' \
                 '\r\n this is the wuid W1234567-1234 and should return it'
        expected = None
        res = hpycc.get.get_wuid_error(string)
        self.assertEqual(expected, res)

    def test_get_wuid_xml_returns_intended(self):
        string = "wuid:  W12345678-123456   state:"
        expected = 'W12345678-123456'
        res = hpycc.get.get_wuid_xml(string)
        self.assertEqual(expected, res)

    def test_get_wuid_xml_with_bracketed_wuid(self):
        string = "wuid:  W12345678-123456(2)   state:"
        expected = 'W12345678-123456(2)'
        res = hpycc.get.get_wuid_xml(string)
        self.assertEqual(expected, res)

    def test_get_wuid_xml_returns_with_extra_dash_wuid(self):
        string = "wuid:  W12345678-123456-5   state:"
        expected = 'W12345678-123456-5'
        res = hpycc.get.get_wuid_xml(string)
        self.assertEqual(expected, res)

    def test_get_wuid_xml_cannot_find_wuid(self):
        with self.assertRaises(AttributeError):
            string = "wuid:  W1234567-123456   state:"
            hpycc.get.get_wuid_xml(string)