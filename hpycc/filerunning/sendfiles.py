import pandas as pd
import hpycc.get as get
import os
server = 'localhost'

# def get_file_internal(source_name, destination_name, server, port, username, password, csv_file, download_threads):

source_name = 'bitest.csv'
script_loc = 'testscript.ecl'


def get_type(typ):

    typ = str(typ)
    if 'float' in typ:
        return 'DECIMAL32_12'
    elif 'int' in typ:
        return 'INTEGER'
    elif 'bool' in typ:
        return 'BOOLEAN'
    else:
        return 'STRING' # TODO: do we need to convert dates more cleanly?


def send_file_internal(source_name, target_name):#, destination_name, server, port, username, password, csv_file, download_threads):
    chunk_size = 10000

    df = pd.read_csv(source_name, encoding='latin')
    df, record_set = make_recordset(df)

    df_len = len(df)
    if df_len > chunk_size:
        break_positions, _ = _make_chunks(df_len, False, chunk_size=chunk_size) #TODO use the original function
        end_rows = break_positions[1:-1]
        end_rows.append(df_len)

        start_rows = [0] + [pos+1 for pos in break_positions[1:-1]]

        # print(break_positions)
        # print(start_rows)
        # input(end_rows)

        target_names = []
        for start, end in zip(start_rows, end_rows):
            target_name_tmp = "TEMPHPYCC::%sfrom%sto%s" % (target_name, start, end)
            target_names.append(target_name_tmp)

            all_rows = make_rows(df, start, end)
            send_data(all_rows, record_set, target_name_tmp)

        concat_files(target_names, target_name, record_set, delete=True)

    else:
        all_rows = make_rows(df, 0, len(df))
        send_data(all_rows, record_set, target_name)


def concat_files(target_names, target_name, record_set, delete=True):
    script_in = """ a := %s;\nOUTPUT(a, ,'%s',EXPIRE(1), OVERWRITE);"""
    read_script = "DATASET('%s', {%s}, THOR)"
    read_files = [read_script % (nam, record_set) for nam in target_names]
    read_files = '+\n'.join(read_files)

    script = script_in % (read_files, target_name)

    if delete:
        delete_script = "STD.File.DeleteLogicalFile('%s')"
        delete_files = [delete_script % (nam) for nam in target_names]
        delete_files = ';'.join(delete_files)
        script += '\n\nIMPORT std;\n' + delete_files + ';'

    print(script)
    with open(script_loc, 'w') as f:
        f.writelines(script)

    get.run_script(script_loc, server, port="8010", repo=None,
                   username="hpycc_get_output", password='" "',
                   legacy=False, do_syntaxcheck=True,
                   silent=True, debg=False, log_to_file=False)
    os.remove(script_loc)


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
        ECL_typ = get_type(typ)
        record_set.append(ECL_typ + ' ' + nam)

        if ECL_typ == 'STRING':
            df[nam] = "'" + df[nam].astype('str').str.replace("'", "\\'") + "'"
    record_set = ';'.join(record_set)

    return df, record_set


def send_data(all_rows, record_set, target_name):

    script_in = """a := DATASET([%s], {%s});\nOUTPUT(a, ,'%s' , EXPIRE(1), OVERWRITE);"""

    script = script_in % (all_rows, record_set, target_name)
    print(script)
    with open(script_loc, 'w') as f:
        f.writelines(script)

    get.run_script(script_loc, server, port="8010", repo=None,
                   username="hpycc_get_output", password='" "',
                   legacy=False, do_syntaxcheck=True,
                   silent=True, debg=False, log_to_file=False)
    os.remove(script_loc)


def _make_chunks(file_size, csv_file, chunk_size=10000):
    """
    Makes start row and chunk size lists for threading logical file
    downloads.

    :param file_size: int
        Total size of file for chunking
    :param csv_file: bool
        Is it a CSV file? Alters starting row to avoid headers
    :param chunk_size: int, optional
        Max chunk size, defaults to 10,000

    :return: list
       List of starting rows
    :return: int
       List of chunk sizes

    """

    start_rows = [1] if csv_file else [0]
    if file_size > chunk_size:
        chunks = [chunk_size]

        while start_rows[-1] + chunks[-1] < file_size:
            next_chunk_start = start_rows[-1] + chunk_size
            start_rows.append(next_chunk_start)

            ending_row = next_chunk_start + chunk_size
            next_chunk_length = chunk_size if ending_row < file_size else file_size - start_rows[-1]
            chunks.append(next_chunk_length)

    else:
        chunks = [file_size]

    return start_rows, chunks


send_file_internal(source_name, 'a:temp:file')
