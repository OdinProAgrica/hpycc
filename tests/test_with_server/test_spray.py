import os
from tempfile import TemporaryDirectory
import unittest

import pandas as pd
import numpy as np
import hpycc
from tests.test_helpers import hpcc_functions

from hpycc.spray import concatenate_logical_files, spray_file
from hpycc.get import get_thor_file


# noinspection PyPep8Naming
def setUpModule():
    a = hpcc_functions.start_hpcc_container()
    hpcc_functions.start_hpcc(a)


# noinspection PyPep8Naming
def tearDownModule():
    hpcc_functions.stop_hpcc_container()


CONCAT_SCRIPT_BASE = "\n".join([
    "a := DATASET([{'%s', '%s'}], "
    "{STRING a; STRING b});",
    "output(a, ,'%s');"
])


def send_file_chunks(conn, a_script):
    # print(a_script)

    with TemporaryDirectory() as d:
        p = os.path.join(d, "test.ecl")
        with open(p, "w+") as file:
            file.write(a_script)
        _ = conn.run_ecl_script(p, syntax_check=True,
                                delete_workunit=True, stored={})


class Testconcatenatelogicalfiles(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user")

    def test_concatenate_logical_files_concatenates_files(self):
        thor_file = '~thor::testsprayconcatenatelogicalfiles'
        overwrite = True
        expire = 1
        delete_workunit = True
        conn = hpycc.Connection("user", test_conn=False)

        output_names = ['~a1', '~b1', '~c1', '~x1']
        col_1_values = ['1', '3', '5', '6']
        col_2_values = ['aa', 'ab', 'ac', 'ad']

        [send_file_chunks(conn, CONCAT_SCRIPT_BASE % (col1, col2, nam))
         for col1, col2, nam in zip(col_1_values, col_2_values, output_names)]

        concatenate_logical_files(conn, output_names, thor_file, 'STRING a; STRING b;',
                                   overwrite, expire, delete_workunit)

        res = get_thor_file(connection=conn, thor_file=thor_file)[['a', 'b']].sort_values('a')
        expected_result = pd.DataFrame({"a": col_1_values, "b": col_2_values}).sort_values('a')

        np.array_equal(res.values, expected_result.values)

    def test_concatenate_logical_files_concatenates_one_file(self):
        thor_file = '~thor::testsprayconcatenateonefile'
        overwrite = True
        expire = 1
        delete_workunit = True
        conn = hpycc.Connection("user", test_conn=False)

        output_names = ['~a2']
        col_1_values = ['1']
        col_2_values = ['aa']

        [send_file_chunks(conn, CONCAT_SCRIPT_BASE % (col1, col2, nam))
         for col1, col2, nam
         in zip(col_1_values, col_2_values, output_names)]

        concatenate_logical_files(conn, output_names,
                                  thor_file,
                                  'STRING a; STRING b;',
                                   overwrite, expire, delete_workunit)

        res = get_thor_file(connection=conn, thor_file=thor_file)[['a', 'b']].sort_values('a')
        expected_result = pd.DataFrame({"a": col_1_values, "b": col_2_values}).sort_values('a')

        np.array_equal(res.values, expected_result.values)


class Testsprayfiles(unittest.TestCase):
    def setUp(self):
        self.conn = hpycc.Connection("user", test_conn=False)

    def test_spray_file_df(self):
        thor_file = '~thor::testsprayfiledf'
        overwrite = True
        expire = 1
        delete_workunit = True
        chunk_size = 10000
        max_workers = 3
        conn = hpycc.Connection("user", test_conn=False)

        col_1_values = ['1', '3', '5', '6']
        col_2_values = ['aa', 'ab', 'ac', 'ad']
        df = pd.DataFrame({"a": col_1_values, "b": col_2_values}).sort_values('a')

        spray_file(conn, df, thor_file, overwrite, expire, chunk_size, max_workers, delete_workunit)

        res = get_thor_file(connection=conn, thor_file=thor_file)[['a', 'b']].sort_values('a')

        np.array_equal(res.values, df.values)

    def test_spray_file_string(self):
        thor_file = '~thor::testsprayfilestring'
        overwrite = True
        expire = 1
        delete_workunit = True
        chunk_size = 10000
        max_workers = 3
        conn = hpycc.Connection("user", test_conn=False)

        col_1_values = ['1', '3', '5', '6']
        col_2_values = ['aa', 'ab', 'ac', 'ad']
        df = pd.DataFrame({"a": col_1_values, "b": col_2_values}).sort_values('a')

        with TemporaryDirectory() as d:
            p = os.path.join(d, "test.csv")
            df.to_csv(p, index=False)
            spray_file(conn, p, thor_file, overwrite, expire, chunk_size, max_workers, delete_workunit)

        res = get_thor_file(connection=conn, thor_file=thor_file)[['a', 'b']].sort_values('a')

        np.array_equal(res.values, df.values)

    def test_spray_file_string_no_tilde(self):
        thor_file = 'testsprayfilenotilde'
        overwrite = True
        expire = 1
        delete_workunit = True
        chunk_size = 10000
        max_workers = 3
        conn = hpycc.Connection("user", test_conn=False)

        col_1_values = ['1', '3', '5', '6']
        col_2_values = ['aa', 'ab', 'ac', 'ad']
        df = pd.DataFrame({"a": col_1_values, "b": col_2_values}).sort_values('a')

        spray_file(conn, df, thor_file, overwrite, expire, chunk_size, max_workers, delete_workunit)

        res = get_thor_file(connection=conn, thor_file='thor::' + thor_file)[['a', 'b']].sort_values('a')

        np.array_equal(res.values, df.values)

    def test_spray_file_string_small_chunks(self):
        thor_file = '~testsprayfilenotilde'
        overwrite = True
        expire = 1
        delete_workunit = True
        chunk_size = 1
        max_workers = 3
        conn = hpycc.Connection("user", test_conn=False)

        col_1_values = ['1', '3', '5', '6']
        col_2_values = ['aa', 'ab', 'ac', 'ad']
        df = pd.DataFrame({"a": col_1_values, "b": col_2_values}).sort_values('a')

        spray_file(conn, df, thor_file, overwrite, expire, chunk_size, max_workers, delete_workunit)

        res = get_thor_file(connection=conn, thor_file=thor_file)[['a', 'b']].sort_values('a')

        np.array_equal(res.values, df.values)
