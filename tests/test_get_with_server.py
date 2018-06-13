import os
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
import warnings

import numpy as np
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


def _spray_df(conn, df, name):
    with TemporaryDirectory() as d:
        p = os.path.join(d, "test.csv")
        df.to_csv(p, index=False)
        hpycc.spray_file(conn, p, name)


def _get_output_from_ecl_string(conn, string, syntax=True):
    with TemporaryDirectory() as d:
        p = os.path.join(d, "test.ecl")
        with open(p, "w+") as file:
            file.write(string)
        res = hpycc.get_output(conn, p, syntax)
        return res


def _get_outputs_from_ecl_string(conn, string, syntax=True):
    with TemporaryDirectory() as d:
        p = os.path.join(d, "test.ecl")
        with open(p, "w+") as file:
            file.write(string)
        res = hpycc.get_outputs(conn, p, syntax)
        return res


class TestGetOutputWithServer(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user")

    def test_get_output_returns_single_value_int(self):
        script = "OUTPUT(2);"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"Result_1": 2}, index=[0])
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_returns_single_value_str(self):
        script = "OUTPUT('a');"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"Result_1": 'a'}, index=[0])
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_returns_dataset(self):
        script = "OUTPUT(DATASET([{1, 'a'}], {INTEGER a; STRING b;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": 1, "b": "a"},
                                index=[0])
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_only_returns_first_output(self):
        script = "OUTPUT('a'); OUTPUT(1);"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"Result_1": 'a'}, index=[0])
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_bools_all_true(self):
        script = "OUTPUT(DATASET([{true}, {true}], {BOOLEAN a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [True, True]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_bools_all_false(self):
        script = "OUTPUT(DATASET([{false}, {false}], {BOOLEAN a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [False, False]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_bools_true_and_false(self):
        script = "OUTPUT(DATASET([{true}, {false}], {BOOLEAN a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [True, False]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_bools_true_and_false_strings(self):
        script = "OUTPUT(DATASET([{'true'}, {'false'}], {STRING a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [True, False]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_bools_true_and_false_strings_with_blank(self):
        script = "OUTPUT(DATASET([{'true'}, {'false'}, {''}], {STRING a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [True, False, np.nan]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_blank_string_as_nan(self):
        script = "OUTPUT(DATASET([{''}], {STRING a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [np.nan]}, index=[0])
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_ints(self):
        script = "OUTPUT(DATASET([{1}, {2}], {INTEGER a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [1, 2]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_floats(self):
        script = "OUTPUT(DATASET([{1.0}, {2.1}], {REAL a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [1.0, 2.1]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_mixed_floats_and_ints(self):
        script = "OUTPUT(DATASET([{1}, {2.1}], {REAL a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [1, 2.1]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_numbers_as_strings(self):
        script = "OUTPUT(DATASET([{'1'}, {'2.1'}], {STRING a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [1, 2.1]})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_empty_dataset(self):
        script = "a := DATASET([{'a'}, {'a'}], {STRING a;});a(a != 'a');"
        with warnings.catch_warnings(record=True) as w:
            res = _get_output_from_ecl_string(self.conn, script)
            self.assertEqual(len(w), 1)
            expected_warn = ("The output does not appear to contain a "
                             "dataset. Returning an empty DataFrame.")
            self.assertEqual(str(w[-1].message), expected_warn)
        expected = pd.DataFrame()
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_parses_mixed_columns_as_strings(self):
        script = "OUTPUT(DATASET([{'1'}, {'a'}], {STRING a;}));"
        res = _get_output_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": ['1', 'a']})
        pd.testing.assert_frame_equal(expected, res)

    def test_get_output_raises_with_bad_script(self):
        script = "asa"
        with self.assertRaises(subprocess.SubprocessError):
            _get_output_from_ecl_string(self.conn, script)

    @patch.object(hpycc.Connection, "check_syntax")
    def test_get_output_runs_syntax_check_if_true(self, mock):
        script = "OUTPUT(2);"
        _get_output_from_ecl_string(self.conn, script)
        mock.assert_called()

    @patch.object(hpycc.Connection, "check_syntax")
    def test_get_output_doesnt_run_syntax_check_if_false(self, mock):
        script = "OUTPUT(2);"
        _get_output_from_ecl_string(self.conn, script, False)
        self.assertFalse(mock.called)


class TestGetOutputsWithServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = hpycc.Connection("user")
        script = ("OUTPUT(2);OUTPUT('a');OUTPUT(DATASET([{1, 'a'}], "
                  "{INTEGER a; STRING b;}), NAMED('ds'));")
        cls.res = _get_outputs_from_ecl_string(cls.conn, script)
        a = [
            {"col": "alltrue", "content": "[{true}, {true}, {true}]",
             "coltype": "BOOLEAN"},
            {"col": "allfalse", "content": "[{false}, {false}, {false}]",
             "coltype": "BOOLEAN"},
            {"col": "trueandfalse", "content": "[{true}, {false}, {true}]",
             "coltype": "BOOLEAN"},
            {"col": "truefalsestrings",
             "content": "[{'true'}, {'false'}, {'false'}]",
             "coltype": "STRING"},
            {"col": "truefalseblank", "content": "[{'true'}, {'false'}, {''}]",
             "coltype": "STRING"}
        ]
        f = [("OUTPUT(DATASET({0[content]}, {{{0[coltype]} {0[col]};}}), "
             "NAMED('{0[col]}'));").format(i) for i in a]
        cls.t_f_res = _get_outputs_from_ecl_string(cls.conn, "\n".join(f))

    def test_get_outputs_returns_single_value_int(self):
        expected = pd.DataFrame({"Result_1": 2}, index=[0])
        pd.testing.assert_frame_equal(expected, self.res["Result_1"])

    def test_get_outputs_returns_single_value_str(self):
        expected = pd.DataFrame({"Result_2": 'a'}, index=[0])
        pd.testing.assert_frame_equal(expected, self.res["Result_2"])

    def test_get_outputs_returns_dataset(self):
        expected = pd.DataFrame({"a": 1, "b": "a"},
                                index=[0])
        pd.testing.assert_frame_equal(expected, self.res["ds"])

    def test_get_outputs_returns_all_outputs(self):
        self.assertEqual(list(self.res.keys()), ["Result_1", "Result_2", "ds"])

    def test_get_outputs_parses_bools_all_true(self):
        expected = pd.DataFrame({"alltrue": [True, True, True]})
        pd.testing.assert_frame_equal(expected, self.t_f_res["alltrue"])

    def test_get_outputs_parses_bools_all_false(self):
        expected = pd.DataFrame({"allfalse": [False, False, False]})
        pd.testing.assert_frame_equal(expected, self.t_f_res["allfalse"])

    def test_get_outputs_parses_bools_true_and_false(self):
        expected = pd.DataFrame({"trueandfalse": [True, False, True]})
        pd.testing.assert_frame_equal(expected, self.t_f_res["trueandfalse"])

    def test_get_outputs_parses_bools_true_and_false_strings(self):
        expected = pd.DataFrame({"truefalsestrings": [True, False, False]})
        pd.testing.assert_frame_equal(
            expected, self.t_f_res["truefalsestrings"])

    def test_get_outputs_parses_bools_true_and_false_strings_with_blank(self):
        expected = pd.DataFrame({"truefalseblank": [True, False, np.nan]})
        pd.testing.assert_frame_equal(expected, self.t_f_res["truefalseblank"])

    def test_get_outputs_parses_blank_string_as_nan(self):
        script = "OUTPUT(DATASET([{''}], {STRING a;}));"
        res = _get_outputs_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [np.nan]}, index=[0])
        pd.testing.assert_frame_equal(expected, res["Result_1"])

    def test_get_outputs_parses_ints(self):
        script = "OUTPUT(DATASET([{1}, {2}], {INTEGER a;}));"
        res = _get_outputs_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [1, 2]})
        pd.testing.assert_frame_equal(expected, res["Result_1"])

    def test_get_outputs_parses_floats(self):
        script = "OUTPUT(DATASET([{1.0}, {2.1}], {REAL a;}));"
        res = _get_outputs_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [1.0, 2.1]})
        pd.testing.assert_frame_equal(expected, res["Result_1"])

    def test_get_outputs_parses_mixed_floats_and_ints(self):
        script = "OUTPUT(DATASET([{1}, {2.1}], {REAL a;}));"
        res = _get_outputs_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [1, 2.1]})
        pd.testing.assert_frame_equal(expected, res["Result_1"])

    def test_get_outputs_parses_numbers_as_strings(self):
        script = "OUTPUT(DATASET([{'1'}, {'2.1'}], {STRING a;}));"
        res = _get_outputs_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": [1, 2.1]})
        pd.testing.assert_frame_equal(expected, res["Result_1"])

    def test_get_output_parses_empty_dataset(self):
        script = ("a := DATASET([{'a'}, {'a'}], {STRING a;});a(a != 'a');"
                  "OUTPUT(2);")

        with warnings.catch_warnings(record=True) as w:
            res = _get_outputs_from_ecl_string(self.conn, script)
            self.assertEqual(len(w), 1)
            expected_warn = (
                "One or more of the outputs do not appear to contain a "
                "dataset. They have been replaced with an empty DataFrame")
            self.assertEqual(str(w[-1].message), expected_warn)
        self.assertEqual(list(res.keys()), ["Result_1", "Result_2"])
        pd.testing.assert_frame_equal(res["Result_1"], pd.DataFrame())

    def test_get_output_parses_mixed_columns_as_strings(self):
        script = "OUTPUT(DATASET([{'1'}, {'a'}], {STRING a;}));"
        res = _get_outputs_from_ecl_string(self.conn, script)
        expected = pd.DataFrame({"a": ['1', 'a']})
        pd.testing.assert_frame_equal(expected, res["Result_1"])

    def test_get_output_raises_with_bad_script(self):
        script = "asa"
        with self.assertRaises(subprocess.SubprocessError):
            _get_outputs_from_ecl_string(self.conn, script)

    @patch.object(hpycc.Connection, "check_syntax")
    def test_get_output_runs_syntax_check_if_true(self, mock):
        script = "OUTPUT(2);"
        _get_outputs_from_ecl_string(self.conn, script)
        mock.assert_called()

    @patch.object(hpycc.Connection, "check_syntax")
    def test_get_output_doesnt_run_syntax_check_if_false(self, mock):
        script = "OUTPUT(2);"
        _get_outputs_from_ecl_string(self.conn, script, False)
        self.assertFalse(mock.called)
