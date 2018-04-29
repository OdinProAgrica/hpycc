import pytest
import os
import hpycc.scriptrunning.runscript as runscript
import pandas as pd

syntax_check = False

script_loc = './tests/'
# script_loc = ''


def upload_small_data(hpcc_connection):
    # TODO: DOn't use a script for this now we have send file and generators.
    # TODO: return the df so we can use if for dynamic testing.
    runscript.run_script_internal(script_loc + 'ECLtest_makeTestData.ecl', hpcc_connection, syntax_check)


def upload_large_data(hpcc_connection, start_row=0, size=25000):
    wrapper = "a := DATASET([%s], {INTEGER a; INTEGER b}); OUTPUT(a, ,'~a::test::bigfile', EXPIRE(1), OVERWRITE);"

    some_data = list(range(start_row, size))
    backwards_data = list(reversed(some_data))

    row_string = '{%s,%s},'
    out_rows = ''
    for i, j in zip(some_data, backwards_data):
        out_rows += row_string % (i, j)
    out_rows += '{0,0}'

    script = 'ECLtest_makeBIGTestData.ecl'
    ecl_command = wrapper % out_rows
    with open(script, 'w') as f:
        f.writelines(ecl_command)

    some_data.append(0)
    backwards_data.append(0)
    expected_big_data = pd.DataFrame({'a': some_data, 'b': backwards_data})

    runscript.run_script_internal(script, hpcc_connection, syntax_check)
    os.remove(script)

    return expected_big_data


def make_df(size=15000):
    int_col = list(range(0, size))
    df = pd.DataFrame({
        'intcol':  int_col,
        'floatcol': [round(x/3, 2) for x in list(range(0, size))],
        'charcol': ['Spam and ham and eggs and jam'] * size,
        'logicalcol': ([False]*5) + [True]*(size-5)
    })

    return df


def make_csv(save_loc, size=15000):
    df = make_df(size)
    df.to_csv(save_loc, index=False)
    return df
