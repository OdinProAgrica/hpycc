"""
The module contains functions to send files to HPCC.
"""
import pandas as pd

from hpycc.utils.filechunker import make_chunks

# TODO make sure this shouts at the user if they use bad column names


def format_df_for_ecl(df):
    for col in df.columns:
        dtype = df.dtypes[col]
        ecl_type = _get_type(dtype)
        if ecl_type == "STRING":
            df[col] = df[col].fillna("").str.replace("'\\'")
            df[col] = "'" + df[col] + "'"
        else:
            df[col] = df[col].fillna(0)

    return df.reset_index()


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


def send_file(source_file, logical_file, connection, overwrite=False,
              chunk_size=10000):
    """
    Send a file to HPCC.

    :param source_file: str, pd.DataFrame,
         Either a string path to a csv for upload or a pandas dataframe. Using the
         latter allows you to control how the df is read in (as we haven't implemented
         kwargs yet).
    :param logical_file: str
         destination path on THOR
    :param overwrite: bool, optional
        Should files with the same name be overriden, default is no

    :return: None

    """
    if isinstance(source_file, pd.DataFrame):
        df = source_file
    elif isinstance(source_file, str):
        df = pd.read_csv(source_file, encoding='latin')
    else:
        raise TypeError

    record_set = ';'.join(
        [" ".join(a, _get_type(df.dtypes[a])) for a in df.dtypes.to_dict()])

    formatted_df = format_df_for_ecl(df)

    chunks = make_chunks(len(formatted_df), csv=False, chunk_size=chunk_size)

    format_rows = lambda d, start, num: ','.join(
        ["{" + ','.join(i) + "}" for i in
         d.astype(str).values[start:start + num].tolist()]
    )

    target_names = []
    for start_row, num_rows in chunks:
        rowed = format_rows(formatted_df, start_row, num_rows)
        target_name_tmp = "TEMPHPYCC::{}from{}to{}".format(
            logical_file, start_row, start_row + num_rows)
        target_names.append(target_name_tmp)

        script_content = ("a := DATASET([{}], {{{}}});\nOUTPUT(a, ,'{}' , "
                          "EXPIRE(1)").format(
            rowed, record_set, target_name_tmp)
        if overwrite:
            script_content += ", OVERWRITE"
        script_content += ");"

        connection.run_ecl_string(script_content, True)

    read_files = ["DATASET('{}', {{{}}}, THOR)".format(
        nam, record_set) for nam in target_names]
    read_files = '+\n'.join(read_files)

    script = "a := {};\nOUTPUT(a, ,'{}' "
    if overwrite:
        script += ", OVERWRITE"
    script += ");"
    script = script.format(read_files, logical_file)

    delete_script = "STD.File.DeleteLogicalFile('{}')"
    delete_files = [delete_script.format(nam) for nam in target_names]
    delete_files = ';'.join(delete_files)
    script += '\n\nIMPORT std;\n' + delete_files + ';'

    connection.run_ecl_string(script, True)
