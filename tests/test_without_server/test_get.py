import unittest

from hpycc.get import get_logical_file


class TestGetLogicalFile(unittest.TestCase):
    def setUp(self):
        self.msg = ("This function has been deprecated, use get_thor_"
                    "file instead.")

    def test_get_logical_file_raises_import_error(self):
        with self.assertRaisesRegex(ImportError, self.msg):
            get_logical_file()

    def test_get_logical_file_takes_kwargs(self):
        with self.assertRaisesRegex(ImportError, self.msg):
            get_logical_file(a="123", b=12)
