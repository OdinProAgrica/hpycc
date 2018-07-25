import unittest

from hpycc.utils.parsers import parse_wuid_from_failed_response, parse_wuid_from_xml


class TestParseWUIDFromFailedResponseWithoutServer(unittest.TestCase):
    def test_parse_wuid_from_failed_response_with_bracketed_wuid(self):
        string = 'W20180702-083256(2) failed\r\n'
        expected = 'W20180702-083256(2)'
        res = parse_wuid_from_failed_response(string)
        self.assertEqual(expected, res)

    def test_parse_wuid_error_returns_with_extra_dash_wuid(self):
        string = 'W20180702-083256-5 failed\r\n'
        expected = 'W20180702-083256-5'
        res = parse_wuid_from_failed_response(string)
        self.assertEqual(expected, res)

    def test_parse_wuid_error_returns_intended(self):
        string = 'W20180702-083256 failed\r\n'
        expected = 'W20180702-083256'
        res = parse_wuid_from_failed_response(string)
        self.assertEqual(expected, res)

    def test_parse_wuid_error_cannot_find_wuid(self):
        string = 'no such thing'
        res = parse_wuid_from_failed_response(string)
        self.assertIsNone(res)


class TestParseWUIDFromXMLWithoutServer(unittest.TestCase):
    def setUp(self):
        self.xml = (
            "Using eclcc path C:\\Program Files (x86)\\HPCCSystems\\"
            "6.4.4\\clienttools\\bin\\eclcc\r\n\r\nDeploying ECL "
            "Archive C:\\Users\\cooperj\\AppData\\Local\\Temp\\tmphadm5gjo\\"
            "ecl_string.ecl\r\n\r\nDeployed\r\n   wuid: {}\r\n   "
            "state: compiled\r\n\r\nRunning deployed workunit W20180702-085912"
            "\r\n<Result>\r\n<Dataset name='Result 1'>\r\n <Row><Result_1>2"
            "</Result_1></Row>\r\n</Dataset>\r\n</Result>\r\n")

    def test_parse_wuid_from_xml_returns_intended(self):
        expected = 'W20180702-085912'
        string = self.xml.format(expected)
        res = parse_wuid_from_xml(string)
        self.assertEqual(expected, res)

    def test_parse_wuid_from_xml_with_bracketed_wuid(self):
        expected = 'W12345678-123456(2)'
        string = self.xml.format(expected)
        res = parse_wuid_from_xml(string)
        self.assertEqual(expected, res)

    def test_parse_wuid_from_xml_returns_with_extra_dash_wuid(self):
        expected = 'W12345678-123456-5'
        string = self.xml.format(expected)
        res = parse_wuid_from_xml(string)
        self.assertEqual(expected, res)

    def test_parse_wuid_from_xml_cannot_find_wuid(self):
        string = self.xml.format("W1234567")
        res = parse_wuid_from_xml(string)
        self.assertIsNone(res)