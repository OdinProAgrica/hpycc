import unittest
from xml.etree import ElementTree

from hpycc.utils.parsers import (
    parse_wuid_from_failed_response,
    parse_wuid_from_xml,
    parse_schema_from_xml,
    get_python_type_from_ecl_type
)


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


class TestParseSchemaFromXML(unittest.TestCase):
    def setUp(self):
        self.bare_schema = "\n".join([
                ('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" '
                 'xmlns:hpcc="urn:hpccsystems:xsd:appinfo" elementFormDefault='
                 '"qualified" attributeFormDefault="unqualified">'),
                ('<xs:element name="Dataset"><xs:complexType><xs:sequence '
                 'minOccurs="0" maxOccurs="unbounded">'),
                '<xs:element name="Row"><xs:complexType><xs:sequence>',
                '{}',
                ('<xs:element name="__fileposition__" type='
                 '"xs:nonNegativeInteger"/>'),
                '</xs:sequence></xs:complexType></xs:element>',
                '</xs:sequence></xs:complexType></xs:element>',
                '</xs:schema>'
            ])
        self.column_schema = '<xs:element name="int" type="{}"/>'
        self.set_schema = "\n".join([
            '<xs:element name="set_int">',
            ('<xs:complexType><xs:sequence><xs:element name="All" '
             'minOccurs="0"/>'),
            ('<xs:element name="Item" minOccurs="0" maxOccurs="unbounded" '
             'type="{}"/>'),
            '</xs:sequence></xs:complexType></xs:element>'
        ])

    def test_parse_schema_from_xml_parses_types_correctly(self):
        types = [
            ("xs:integer", int),
            ("xs:nonNegativeInteger", int),
            ("decimal10", float),
            ("decimal5", float),
            ("decimal5_3", float),
            ("decimal8_2", float),
            ("udecimal10", float),
            ("udecimal5", float),
            ("udecimal5_3", float),
            ("udecimal8_2", float),
            ("xs:double", float),
            ("xs:string", str),
            ("xs:boolean", bool),
            ("xs:hexBinary", str),
            ("data16", str),
            ("data32", str),
            ("string3", str),
            ("string8", str)
        ]
        for t in types:
            col = self.column_schema.format(t[0])
            schema = self.bare_schema.format(col)
            res = [i for i in parse_schema_from_xml(schema)][0]
            self.assertEqual("int", res.name)
            self.assertFalse(res.is_set)
            self.assertEqual(res.type, t[1])

    def test_parse_schema_from_xml_returns_file_position(self):
        col = self.column_schema.format("data16")
        schema = self.bare_schema.format(col)
        res = [i for i in parse_schema_from_xml(schema)][1]
        self.assertEqual(res.name, "__fileposition__")
        self.assertFalse(res.is_set)
        self.assertEqual(res.type, int)

    def test_parse_schema_from_xml_returns_all_columns(self):
        cols = [self.column_schema.format("string")] * 100
        joined = "\n".join(cols)
        schema = self.bare_schema.format(joined)
        res = [i for i in parse_schema_from_xml(schema)][:-1]
        self.assertEqual(100, len(res))

    def test_parse_schema_from_xml_parses_set_types_correctly(self):
        types = [
            ("xs:integer", int),
            ("xs:nonNegativeInteger", int),
            ("decimal10", float),
            ("decimal5", float),
            ("decimal5_3", float),
            ("decimal8_2", float),
            ("udecimal10", float),
            ("udecimal5", float),
            ("udecimal5_3", float),
            ("udecimal8_2", float),
            ("xs:double", float),
            ("xs:string", str),
            ("xs:boolean", bool),
            ("xs:hexBinary", str),
            ("data16", str),
            ("data32", str),
            ("string3", str),
            ("string8", str)
        ]
        for t in types:
            col = self.set_schema.format(t[0])
            schema = self.bare_schema.format(col)
            res = [i for i in parse_schema_from_xml(schema)][0]
            self.assertEqual("set_int", res.name)
            self.assertTrue(res.is_set)
            self.assertEqual(res.type, t[1])

    def test_parse_schema_from_xml_parses_sets_and_cols(self):
        col = self.column_schema.format("xs:string")
        s = self.set_schema.format("xs:double")
        sc = "\n".join([col, s])
        schema = self.bare_schema.format(sc)
        res = sorted(parse_schema_from_xml(schema), key=lambda x: x.name)
        expected = [
            ("__fileposition__", False, int),
            ("int", False, str),
            ("set_int", True, float)
        ]
        for e, r in zip(expected, res):
            self.assertEqual(e[0], r[0])
            self.assertEqual(e[1], r[1])
            self.assertEqual(e[2], r[2])


class TestGetPythonTypeFromECLType(unittest.TestCase):
    def test_get_python_type_from_ecl_type_parses_column_types(self):
        types = [
            ("xs:integer", int),
            ("xs:nonNegativeInteger", int),
            ("decimal10", float),
            ("decimal5", float),
            ("decimal5_3", float),
            ("decimal8_2", float),
            ("udecimal10", float),
            ("udecimal5", float),
            ("udecimal5_3", float),
            ("udecimal8_2", float),
            ("xs:double", float),
            ("xs:string", str),
            ("xs:boolean", bool),
            ("xs:hexBinary", str),
            ("data16", str),
            ("data32", str),
            ("string3", str),
            ("string8", str),
            ("madeup", str)
        ]
        for t in types:
            st = '<xs:element name="int" type="{}"/>'.format(t[0])
            start = ('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" '
                     'xmlns:hpcc="urn:hpccsystems:xsd:appinfo" '
                     'elementFormDefault="qualified" '
                     'attributeFormDefault="unqualified">')
            end = "</xs:schema>"
            sch = "\n".join([start, st, end])
            child = ElementTree.fromstring(sch)[0]
            r = get_python_type_from_ecl_type(child)
            self.assertEqual(t[1], r)

    def test_get_python_type_from_ecl_type_parses_set_types(self):
        types = [
            ("xs:integer", int),
            ("xs:nonNegativeInteger", int),
            ("decimal10", float),
            ("decimal5", float),
            ("decimal5_3", float),
            ("decimal8_2", float),
            ("udecimal10", float),
            ("udecimal5", float),
            ("udecimal5_3", float),
            ("udecimal8_2", float),
            ("xs:double", float),
            ("xs:string", str),
            ("xs:boolean", bool),
            ("xs:hexBinary", str),
            ("data16", str),
            ("data32", str),
            ("string3", str),
            ("string8", str),
            ("madeup", str)
        ]
        for t in types:
            st = "\n".join([
                '<xs:element name="set_int">',
                ('<xs:complexType><xs:sequence><xs:element name="All" '
                 'minOccurs="0"/>'),
                ('<xs:element name="Item" minOccurs="0" maxOccurs="unbounded" '
                 'type="{}"/>'),
                '</xs:sequence></xs:complexType></xs:element>'
            ]).format(t[0])
            start = ('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" '
                     'xmlns:hpcc="urn:hpccsystems:xsd:appinfo" '
                     'elementFormDefault="qualified" '
                     'attributeFormDefault="unqualified">')
            end = "</xs:schema>"
            sch = "\n".join([start, st, end])
            child = ElementTree.fromstring(sch)[0]
            r = get_python_type_from_ecl_type(child)
            self.assertEqual(t[1], r)
