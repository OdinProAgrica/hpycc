from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import unittest
import warnings
from tempfile import TemporaryDirectory
from unittest.mock import patch

import numpy as np
import pandas as pd

import hpycc
from hpycc.save import save_thor_file
from tests.test_helpers import hpcc_functions


def _save_outputs_from_ecl_string(
        conn, string, syntax=True, delete_workunit=True,
        stored=None, directory=".", **kwargs):
    with TemporaryDirectory() as d:
        p = os.path.join(d, "test.ecl")
        with open(p, "w+") as file:
            file.write(string)
        print(string)
        hpycc.save_outputs(conn, p, syntax_check=syntax, delete_workunit=delete_workunit,
                           stored=stored, directory=directory, **kwargs)
        res = pd.read_csv(directory)  # TODO: read in all results
        return res


conn = hpycc.Connection(server='10.53.57.31', username='Rob_Mansfield_Systest', password='MustEngine60')
script = ("OUTPUT(2);OUTPUT('a');OUTPUT(DATASET([{1, 'a'}], "
          "{INTEGER a; STRING b;}), NAMED('ds'));")
# res = _save_outputs_from_ecl_string(conn, script, index=False)

res = conn.run_ecl_string("IMPORT std; STD.File.FileExists('~ROB1');", syntax_check=True,
                          delete_workunit=True, stored={})



