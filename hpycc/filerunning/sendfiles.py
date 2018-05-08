import pandas as pd
import re
import hpycc.run
import os
import logging

import hpycc.utils.filechunker


def _get_type(typ):
    """
    Takes a dtyp and matches it to the relevent HPCC datatype

    :param typ, dtype:
        pandas dtype, obtained by getting a columns type.
    :return: str
        the relevent ECL datatype, assumes the largest, least
        space efficient to prevent truncation
    """

    # typ = str(typ)
    # if 'float' in typ:
    #     return 'DECIMAL32_12'
    # elif 'int' in typ:
    #     return 'INTEGER'
    # elif 'bool' in typ:
    #     return 'BOOLEAN'
    # else:
    #     return 'STRING' # TODO: do we need to convert dates more cleanly?
    # TODO: at present we just return string as we have an issue with nans in ECL
    return 'STRING'


def send_file_internal(source_file, target_name,
                       overwrite, delete, connection,
                       temp_script='sendFileTemp.ecl',
                       chunk_size=10000):

    """
    Internal call fo sending a file. Creates chunks and orchestrates upload.

    :param source_file: str, pd.DataFrame
        The source CSV or pd.DataFrame to spray
    :param target_name: str,
        Logical file to spray too
    :param overwrite: bool,
        Should we allow overriding of files that have the same name as target_name
    :param delete: bool,
        Should temporary files and scripts be deleted at completion
    :param connection: HPCCconnector,
        Connection details for an HPCC instance.
    :param temp_script:
        Name for temporary ECL script created to upload data
    :param chunk_size:
        Size at which chunks should be created (and size of said chunks)

    :return: None
    """

    logger = logging.getLogger('send_file_internal')
    logger.debug("sending file %s to %s" % (source_file, target_name))

    if isinstance(source_file, pd.DataFrame):
        logger.debug('Source file has been given as a pd.DataFrame, using asis')
        df = source_file
    elif isinstance(source_file, str):
        logger.debug('Source file has been given as a string, using as path')
        df = pd.read_csv(source_file, encoding='latin')
    else:
        raise TypeError('Input file is neither a path nor a pd.DataFrame but %s' % type(source_file))

    df, record_set = make_recordset(df)

    if len(df) > chunk_size:
        _send_file_in_chunks(df, target_name, chunk_size, record_set, overwrite, delete, temp_script, connection)
    else:
        all_rows = make_rows(df, 0, len(df))
        send_data(all_rows, record_set, target_name, overwrite, temp_script, connection, delete)

    return None


def _send_file_in_chunks(df, target_name, chunk_size, record_set, overwrite, delete, temp_script, connection):
    """
    File chunker for sending files to an HPCC instance

    :param df: pd.DataFrame,
        The source df to chunk
    :param target_name: str,
        Logical file to spray too
    :param chunk_size: int,
        Size of chunk
    :param record_set: str,
        Recordset for sprayed dataset
    :param overwrite: bool,
        Should we allow overriding of files that have the same name as target_name
    :param delete: bool,
        Should temporary files and scripts be deleted at completion
    :param temp_script:
        Name for temporary ECL script created to upload data
    :param connection: HPCCconnector,
        Connection details for an HPCC instance.

    :return: None
    """
    logger = logging.getLogger('_send_file_in_chunks')

    logger.debug('Establishing rownumbers for chunks')
    break_positions, _ = hpycc.utils.filechunker.make_chunks(len(df), csv_file=False, chunk_size=chunk_size)
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
        send_data(all_rows, record_set, target_name, overwrite, temp_script, connection, delete)

    _concat_files(target_names, target_name, record_set, overwrite, delete, temp_script, connection)

    return None


def _concat_files(target_names, target_name, record_set, overwrite, delete, temp_script, connection):
    """
    When files have been uploaded in chunks, this will concatenate them.

    :param target_names: list,
        list of temporary files to be concatenated
    :param target_name: str,
        Logical file to spray too
    :param record_set: str,
        Recordset for sprayed dataset
    :param overwrite: bool,
        Should we allow overriding of files that have the same name as target_name
    :param delete: bool,
        Should temporary files and scripts be deleted at completion
    :param temp_script:
        Name for temporary ECL script created to upload data
    :param connection: HPCCconnector,
        Connection details for an HPCC instance.

    :return: None
    """
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

    # logger.debug(script)
    with open(temp_script, 'w') as f:
        f.writelines(script)

    connection.run_command(script, 'ecl')
    os.remove(temp_script)

    return None


def make_rows(df, start, end):
    """
    Make rows to be written to HPCC. This is then inserted into the upload script.

    :param df: pd.DataFrame,
        Data frame whose rows you want to parse
    :param start: int
        start row
    :param end: int
        end row

    :return: str,
        Rows to be inserted into the upload script
    """
    rows = '{' + df.loc[start:end, :].apply(lambda x: ','.join(x.astype('str').values.tolist()), axis=1) + '}'
    all_rows = ','.join(rows.tolist())

    return all_rows


def make_recordset(df):
    """
    Makes an ECL type recordset for uploading a file.

    :param df: pd.DataFrame
        DataFrame to create a recordset for

    :return: str,
        ECl recordset
    """
    col_names = df.columns.tolist()
    col_types = df.dtypes.tolist()
    record_set = []

    print(col_types)
    print(col_names)

    unnamed_iterator = 0
    for typ, nam in zip(col_types, col_names):
        ecl_script = _get_type(typ)

        # Make sure name is allowed by ECL syntax
        safe_name = re.sub('[^A-Za-z0-9]', '', nam)
        if safe_name == '':
            safe_name = 'unnamed%s' % unnamed_iterator
            unnamed_iterator += 1
        if re.match('^[0-9]', safe_name):
            safe_name = 'num' + safe_name
        new_entry = ecl_script + ' ' + safe_name

        # Make sure name is unique
        column_append = ''
        while new_entry + str(column_append) in record_set:
            column_append = 1 if column_append == '' else (column_append + 1)
        record_set.append(new_entry + str(column_append))

        # If a string, make sure quotes are escaped and nans are blank. Else nulls == 0 (thanks ecl)
        if ecl_script == 'STRING':
            df[nam] = df[nam].astype('str')
            df.loc[df[nam].str.lower() == 'nan', nam] = ''
            df.loc[df[nam].str.lower() == 'na', nam] = ''
            df.loc[df[nam].str.lower() == 'null', nam] = ''
            df[nam] = "'" + df[nam].str.replace("'", "\\'") + "'"
        else:
            df[nam] = df[nam].fillna(0)
    record_set = ';'.join(record_set)

    return df, record_set


def send_data(all_rows, record_set, target_name, overwrite, temp_script, connection, delete):
    """
    Creates an upload script and sends the data to HPCC.

    :param all_rows: str,
        All rows to be inserted
    :param record_set: str,
        Recordset for sprayed dataset
    :param overwrite: bool,
        Should we allow overriding of files that have the same name as target_name
    :param temp_script:
        Name for temporary ECL script created to upload data
    :param hpcc_connection: HPCCconnector,
        Connection details for an HPCC instance.
    :param delete: bool,
        Should temporary files and scripts be deleted at completion

    :return: None
    """
    overwrite_flag = ', OVERWRITE' if overwrite else ''
    script_in = "a := DATASET([%s], {%s});\nOUTPUT(a, ,'%s' , EXPIRE(1)%s);"

    script = script_in % (all_rows, record_set, target_name, overwrite_flag)
    print(script)
    with open(temp_script, 'w') as f:
        f.writelines(script)

    connection.run_ecl_script(script, True)

    if delete:
        os.remove(temp_script)

    return None
