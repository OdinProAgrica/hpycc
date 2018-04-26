import pandas as pd
import hpycc.scriptrunning.runscript as run
import hpycc.filerunning.getfiles as getfiles
import os
import logging

# source_name = 'bitest.csv'
# script_loc = 'testscript.ecl'


def _get_type(typ):
    """
    Takes a dtyp and matches it to the relevent HPCC datatype

    :param typ, dtype:
        pandas dtype, obtained by getting a columns type.
    :return: str
        the relevent ECL datatype, assumes the largest, least
        space efficient to prevent truncation
    """

    typ = str(typ)
    if 'float' in typ:
        return 'DECIMAL32_12'
    elif 'int' in typ:
        return 'INTEGER'
    elif 'bool' in typ:
        return 'BOOLEAN'
    else:
        return 'STRING' # TODO: do we need to convert dates more cleanly?


def send_file_internal(source_name, target_name, server, port,
                       username, password, csv_file, overwrite,
                       delete, server, port, repo, username, password, legacy
                       , temp_script='sendFileTemp.ecl',
                       chunk_size=10000):

    logger = logging.getLogger('send_file_internal')
    logger.debug("sending file %s to %s" % (source_name, target_name))

    df = pd.read_csv(source_name, encoding='latin')
    df, record_set = make_recordset(df)

    if len(df) > chunk_size:
        _send_file_in_chunks(df, target_name, chunk_size, record_set, overwrite, delete, temp_script, server, port, repo, username, password, legacy)
    else:
        all_rows = make_rows(df, 0, len(df))
        send_data(all_rows, record_set, target_name, temp_script)

    return None


def _send_file_in_chunks(df, target_name, chunk_size, record_set, overwrite, delete, temp_script, server, port, repo, username, password, legacy):
    logger = logging.getLogger('_send_file_in_chunks')

    logger.debug('Establishing rownumbers for chunks')
    break_positions, _ = getfiles._make_chunks(len(df), csv_file=False, chunk_size=chunk_size)
    end_rows = break_positions[1:-1] + [len(df)]

    start_rows = [0] + [pos + 1 for pos in break_positions[1:-1]]
    logger.debug('Running upload in chunks. Starts: %s, ends: %s' % (start_rows, end_rows))

    logger.debug('Uploading %s chunks' % len(start_rows))
    target_names = []
    for start, end in zip(start_rows, end_rows):
        target_name_tmp = "TEMPHPYCC::%sfrom%sto%s" % (target_name, start, end)
        logger.debug('Sending row %s to %s to file: %s' % (start, end, target_name_tmp))

        target_names.append(target_name_tmp)
        all_rows = make_rows(df, start, end)
        send_data(all_rows, record_set, target_name_tmp, overwrite)

    concat_files(target_names, target_name, record_set, overwrite, delete, temp_script, server, port, repo, username, password, legacy)

    return None


def concat_files(target_names, target_name, record_set, overwrite, delete, temp_script, server, port, repo, username, password, legacy):

    overwrite_flag = ', OVERWRITE' if overwrite else ''
    script_in = "a := %s;\nOUTPUT(a, ,'%s' %s);"

    read_script = "DATASET('%s', {%s}, THOR)"
    read_files = [read_script % (nam, record_set) for nam in target_names]
    read_files = '+\n'.join(read_files)

    script = script_in % (read_files, target_name, overwrite_flag)

    if delete:
        delete_script = "STD.File.DeleteLogicalFile('%s')"
        delete_files = [delete_script % nam for nam in target_names]
        delete_files = ';'.join(delete_files)
        script += '\n\nIMPORT std;\n' + delete_files + ';'

    logger.debug(script)
    with open(temp_script, 'w') as f:
        f.writelines(script)

    run.run_script_internal(script, server, port, repo, username, password, legacy, do_syntaxcheck=False)
    os.remove(temp_script)

    return None


def make_rows(df, start, end):
    rows = '{' + df.loc[start:end, :].apply(lambda x: ','.join(x.astype('str').values.tolist()), axis=1) + '}'
    all_rows = ','.join(rows.tolist())

    return all_rows


def make_recordset(df):

    col_names = df.columns.tolist()
    col_types = df.dtypes.tolist()
    record_set = []

    print(col_types)
    print(col_names)

    for typ, nam in zip(col_types, col_names):
        ECL_typ = _get_type(typ)
        record_set.append(ECL_typ + ' ' + nam)

        if ECL_typ == 'STRING':
            df[nam] = "'" + df[nam].astype('str').str.replace("'", "\\'") + "'"
    record_set = ';'.join(record_set)

    return df, record_set


def send_data(all_rows, record_set, target_name, overwrite, temp_script):
    overwrite_flag = ', OVERWRITE' if overwrite else ''
    script_in = """a := DATASET([%s], {%s});\nOUTPUT(a, ,'%s' , EXPIRE(1)%s);"""

    script = script_in % (all_rows, record_set, target_name, overwrite_flag)
    print(script)
    with open(temp_script, 'w') as f:
        f.writelines(script)

    get.run_script(temp_script, server, port="8010", repo=None,
                   username="hpycc_get_output", password='" "',
                   legacy=False, do_syntaxcheck=True,
                   silent=True, debg=False, log_to_file=False)
    os.remove(temp_script)

    return None

send_file_internal('bitest.csv', 'a:temp:file')


