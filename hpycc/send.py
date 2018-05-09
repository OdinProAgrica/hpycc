"""
The module contains functions to send files to HPCC.
"""
import pandas as pd

from hpycc.delete import delete_logical_file
from hpycc.utils.filechunker import make_chunks

# TODO make sure this shouts at the user if they use bad column names
# TODO logging


def format_df_for_hpcc(df):
    """
    Return a DataFrame in a format accepted by HPCC.

    Parameters
    ----------
    :param df: DataFrame
        DataFrame to be sprayed.

    Returns
    -------
    :return df: DataFrame
        DataFrame with NaNs filled and single quotes escaped.
    """
    for col in df.columns:
        dtype = df.dtypes[col]
        ecl_type = get_type(dtype)
        if ecl_type == "STRING":
            df[col] = df[col].fillna("").str.replace("'", "\\'")
            df[col] = "'" + df[col] + "'"
        else:
            df[col] = df[col].fillna(0)

    return df.reset_index()


def get_type(typ):
    """
    Return the HPCC data type equivalent of a pandas/ numpy dtype.

    Parameters
    ----------
    :param typ: dtype
        Numpy or pandas dtype.

    Returns
    -------
    :return type: string
        ECL data type.
    """
    typ = str(typ)
    if 'float' in typ:
        # return 'DECIMAL32_12'
        pass
    elif 'int' in typ:
        # return 'INTEGER'
        pass
    elif 'bool' in typ:
        # return 'BOOLEAN'
        pass
    else:
        # return 'STRING'
        pass
    #  TODO: do we need to convert dates more cleanly?
    # TODO: at present we just return string as we have an issue with nans in
    # ECL
    return 'STRING'


def format_rows(df, start_row, num_rows):
    return ','.join(
        ["{" + ','.join(i) + "}" for i in
         df.astype(str).values[start_row:start_row + num_rows].tolist()]
    )


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

    record_set = ";".join([" ".join((col, "STRING")) for col, dtype in
                           df.dtypes.to_dict().items()])

    formatted_df = format_df_for_hpcc(df)

    chunks = make_chunks(len(formatted_df), csv=False, chunk_size=chunk_size)

    rows = (format_rows(formatted_df, start_row, num_rows)
            for start_row, num_rows in chunks)
    target_names = ["TEMPHPYCC::{}from{}to{}".format(
            logical_file, start_row, start_row + num_rows)
        for start_row, num_rows in chunks]

    for row, name in zip(rows, target_names):
        # TODO make concurrent
        spray_formatted_data(connection, row, record_set, name, overwrite)

    concatenate_logical_files(connection, target_names, logical_file,
                              record_set, overwrite)

    for tmp in target_names:
        delete_logical_file(connection, tmp)


def spray_formatted_data(connection, data, record_set, logical_file,
                         overwrite):
    script_content = ("a := DATASET([{}], {{{}}});\nOUTPUT(a, ,'{}' , "
                      "EXPIRE(1)").format(
        data, record_set, logical_file)
    if overwrite:
        script_content += ", OVERWRITE"
    script_content += ");"
    connection.run_ecl_string(script_content, True)


def concatenate_logical_files(connection, to_concat, logical_file, record_set,
                              overwrite):
    read_files = ["DATASET('{}', {{{}}}, THOR)".format(
        nam, record_set) for nam in to_concat]
    read_files = '+\n'.join(read_files)

    script = "a := {};\nOUTPUT(a, ,'{}' "
    if overwrite:
        script += ", OVERWRITE"
    script += ");"
    script = script.format(read_files, logical_file)

    connection.run_ecl_string(script, True)
