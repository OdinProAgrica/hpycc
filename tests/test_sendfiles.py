import pytest
import pandas as pd
import hpycc.filerunning.sendfiles as sendfiles
import hpycc.run as run
from hpycc.utils.HPCCconnector import HPCCconnector
import tests.make_data as md
import hpycc.filerunning.getfiles as getfiles
from pandas.util.testing import assert_frame_equal


# script_loc = ''
script_loc = './tests/'

upload_loc_sml = '~a::temp::filesml'
upload_loc_lrg = '~a::temp::filelrg'

silent = False
legacy = True
repo = None

hpcc_connection = HPCCconnector('localhost', '8010', None, "hpycc_get_output", '" "', False)

overwrite = True
delete = False

large_test_csv = 'largetest.csv'
large_expected_result = md.make_csv(large_test_csv, size=15000)

small_test_csv = 'smalltest.csv'
small_expected_result = md.make_csv(small_test_csv, size=10)


def test_get_type():
    test_types = pd.DataFrame({'a': [1], 'b': ['z'], 'c': [1.234], 'd': ['true'], 'e': [True]})
    given_types = test_types.dtypes.tolist()
    expected_ecl_types = ['STRING', 'STRING', 'STRING', 'STRING', 'STRING']

    for ecl_typ, typ in zip(expected_ecl_types, given_types):
        typ_out = sendfiles._get_type(typ)
        assert typ_out == ecl_typ


def test_send_file_internal_sml():
    sendfiles.send_file_internal(small_test_csv, upload_loc_sml, overwrite, delete,
                                 hpcc_connection, temp_script='sendFileTemp.ecl',
                                 chunk_size=10000)

    result = getfiles.get_file_internal(upload_loc_sml, hpcc_connection, False, 3)
    run.delete(upload_loc_sml, 'localhost')

    assert_frame_equal(result, small_expected_result, check_dtype=False, check_like=False)


def test_send_file_internal_lrg():
    sendfiles.send_file_internal(large_test_csv, upload_loc_lrg, overwrite, delete,
                                 hpcc_connection, temp_script='sendFileTemp.ecl',
                                 chunk_size=10000)

    lrg_result = getfiles.get_file_internal(upload_loc_lrg, hpcc_connection, False, 3)
    run.delete(upload_loc_lrg, 'localhost')

    assert_frame_equal(lrg_result, large_expected_result, check_dtype=False, check_like=False)
