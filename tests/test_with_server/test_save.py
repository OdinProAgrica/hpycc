from pathlib import Path
import unittest
from tempfile import TemporaryDirectory

import pandas as pd

import hpycc
from tests.test_helpers import hpcc_functions


# noinspection PyPep8Naming
def setUpModule():
    a = hpcc_functions.start_hpcc_container()
    hpcc_functions.start_hpcc(a)


# noinspection PyPep8Naming
def tearDownModule():
    hpcc_functions.stop_hpcc_container()


class TestSaveOutputs(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user")
        self.string = "1;2;3;"

    def test_save_outputs_accepts_filenames_as_none(self):
        expected = {
            "Result_1.csv": pd.DataFrame({"Result_1": 1}, index=[0]),
            "Result_2.csv": pd.DataFrame({"Result_2": 2}, index=[0]),
            "Result_3.csv": pd.DataFrame({"Result_3": 3}, index=[0]),
        }
        with TemporaryDirectory() as d:
            p = Path(d) / "test.ecl"
            p.write_text(self.string)
            hpycc.save_outputs(self.conn, str(p), filenames=None,
                               directory=str(d), index=False)
            for e in expected:
                actual_df_path = Path(d) / e
                actual_df = pd.read_csv(actual_df_path)
                pd.testing.assert_frame_equal(expected[e], actual_df)

    def test_save_outputs_raises_index_error_if_filenames_too_short(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "test.ecl"
            p.write_text(self.string)
            with self.assertRaises(IndexError):
                hpycc.save_outputs(
                    self.conn, str(p), filenames=["a.csv"], directory=str(d),
                    index=False)

    def test_save_outputs_raises_index_error_if_filenames_too_long(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "test.ecl"
            p.write_text(self.string)
            with self.assertRaises(IndexError):
                hpycc.save_outputs(
                    self.conn, str(p), filenames=["a", "b", "c", "d"],
                    directory=str(d), index=False)

    def test_save_outputs_uses_parsed_filenames_if_filenames_is_none(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "test.ecl"
            p.write_text(self.string)
            hpycc.save_outputs(self.conn, str(p), filenames=None,
                               directory=str(d), index=False)
            contents = sorted([i.stem for i in Path(d).iterdir()])
        expected = sorted(["test", "Result_1", "Result_2", "Result_3"])
        self.assertEqual(expected, contents)

    def test_save_outputs_uses_filenames(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "test.ecl"
            p.write_text(self.string)
            hpycc.save_outputs(
                self.conn, str(p), filenames=["a.csv", "b.csv", "c.csv"],
                directory=str(d), index=False)
            contents = sorted([i.stem for i in Path(d).iterdir()])
        expected = sorted(["test", "a", "b", "c"])
        self.assertEqual(expected, contents)
