from pathlib import Path
import os
import unittest
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

import hpycc
from hpycc.save import save_thor_file
from tests.test_helpers import hpcc_functions


# noinspection PyPep8Naming
def setUpModule():
    a = hpcc_functions.start_hpcc_container()
    hpcc_functions.start_hpcc(a)


# noinspection PyPep8Naming
def tearDownModule():
    hpcc_functions.stop_hpcc_container()


def _spray_df(conn, df, name):
    with TemporaryDirectory() as d:
        p = os.path.join(d, "test.csv")
        df.to_csv(p, index=False)
        hpycc.spray_file(conn, p, name)


def _save_output_from_ecl_string(
        conn, string, syntax=True, delete_workunit=True,
        stored=None, path_or_buf='test.csv', **kwargs):

    with TemporaryDirectory() as d:
        path_or_buf = os.path.join(d, path_or_buf) if path_or_buf else None
        p = os.path.join(d, "test.ecl")
        with open(p, "w+") as file:
            file.write(string)
        res = hpycc.save_output(conn, p, syntax_check=syntax, delete_workunit=delete_workunit,
                                stored=stored, path_or_buf=path_or_buf, **kwargs)
        res = pd.read_csv(path_or_buf) if path_or_buf else res

        return res


# TODO: test with index=True
def _get_a_save(connection, thor_file, path_or_buf='test.csv',
                max_workers=15, chunk_size=10000, max_attempts=3,
                max_sleep=10, dtype=None, **kwargs):
    with TemporaryDirectory() as d:
        path_or_buf = os.path.join(d, path_or_buf) if path_or_buf else None

        res = save_thor_file(connection, thor_file, path_or_buf,
                             max_workers, chunk_size, max_attempts,
                             max_sleep, dtype, **kwargs)
        df = pd.read_csv(path_or_buf) if path_or_buf else res

    return df


class TestGetOutputWithServer(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user")

    def test_save_output_returns_dataset(self):
        script = "OUTPUT(DATASET([{1, 'a'}], {INTEGER a; STRING b;}));"
        res = _save_output_from_ecl_string(self.conn, script, index=False)
        expected = pd.DataFrame({"a": 1, "b": "a"}, index=[0])
        pd.testing.assert_frame_equal(expected, res)

    def test_save_output_only_returns_first_output(self):
        script = "OUTPUT('a'); OUTPUT(1);"
        res = _save_output_from_ecl_string(self.conn, script, index=False)
        expected = pd.DataFrame({"Result_1": 'a'}, index=[0])

        print(expected)
        print(res)
        pd.testing.assert_frame_equal(expected, res)

    def test_save_output_returns_no_name(self):
        script = "OUTPUT(DATASET([{1, 'a'}], {INTEGER a; STRING b;}));"
        res = _save_output_from_ecl_string(self.conn, script, index=False, path_or_buf=None)
        expected = pd.DataFrame({"a": 1, "b": "a"}, index=[0]).to_csv(index=False)
        self.assertEqual(expected, res)


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


class TestSaveThorFile(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user", test_conn=False)

    def test_save_thor_file_returns_empty_dataset(self):
        self.conn.run_ecl_string(
            "a := DATASET([], {INTEGER int;}); "
            "OUTPUT(a,,'~test_save_thor_file_returns_empty_dataset');",
            True,
            True,
            None
        )
        res = _get_a_save(connection=self.conn, thor_file="test_save_thor_file_returns_empty_dataset", index=False)
        expected = pd.DataFrame(columns=["int", "__fileposition__"])
        pd.testing.assert_frame_equal(expected, res)

    def test_save_thor_file_returns_single_row_dataset(self):
        self.conn.run_ecl_string(
            "a := DATASET([{1}], {INTEGER int;}); "
            "OUTPUT(a,,'~test_save_thor_file_returns_single_row_dataset');",
            True,
            True,
            None
        )
        res = _get_a_save(connection=self.conn, thor_file="test_save_thor_file_returns_single_row_dataset", index=False)
        expected = pd.DataFrame({"int": [1], "__fileposition__": 0},
                                dtype=np.int64)
        pd.testing.assert_frame_equal(expected, res)

    def test_save_thor_file_returns_100_row_dataset(self):
        lots_of_1s = "[" + ",".join(["{1}"]*100) + "]"
        self.conn.run_ecl_string(
            "a := DATASET({}, {{INTEGER int;}}); "
            "OUTPUT(a,,'~test_save_thor_file_returns_100_row_dataset');".format(
                lots_of_1s),
            True,
            True,
            None
        )
        res = _get_a_save(connection=self.conn, thor_file="test_save_thor_file_returns_100_row_dataset", index=False)
        expected = pd.DataFrame({
            "int": [1]*100,
            "__fileposition__": [i*8 for i in range(100)]
        }, dtype=np.int64)
        pd.testing.assert_frame_equal(expected, res)

    def test_save_thor_file_returns_no_path(self):
        lots_of_1s = "[" + ",".join(["{1}"]*10) + "]"
        self.conn.run_ecl_string(
            "a := DATASET({}, {{INTEGER int;}}); "
            "OUTPUT(a,,'~test_save_thor_file_returns_no_path');".format(
                lots_of_1s),
            True,
            True,
            None
        )
        res = _get_a_save(connection=self.conn, thor_file="test_save_thor_file_returns_no_path",
                          path_or_buf=None, index=False)
        expected = pd.DataFrame({
            "int": [1]*10,
            "__fileposition__": [i*8 for i in range(10)]
        }, dtype=np.int64).to_csv(index=False)

        print(expected)
        print(res)
        self.assertEqual(expected, res)

    def test_save_thor_file_returns_a_set(self):
        file_name = "test_save_thor_file_returns_a_set"
        s = ("a := DATASET([{{[1, 2, 3]}}], {{SET OF INTEGER set;}}); "
             "OUTPUT(a,,'~{}');").format(file_name)
        self.conn.run_ecl_string(s, True, True, None)
        res = _get_a_save(self.conn, file_name, index=False)
        self.assertEqual(res.set.values[0], '[1, 2, 3]')
