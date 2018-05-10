"""
This module contains functions to get either the output(s) of an ECL script,
or a logical file.
"""
import concurrent.futures
import logging
import re
from xml.etree import ElementTree

import pandas as pd

from hpycc.utils import parsers, filechunker

# TODO logging
# TODO make private functions
# TODO tests


def get_output(connection, script, syntax_check=True):
    """
    Return the first output of an ECL script as a DataFrame. See
    save_output() for writing straight to file and get_outputs() for
    downloading multiple outputs.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param script: str
         Path of script to execute.
    :param syntax_check: bool, optional
        Should the script be syntax checked before execution. True by
        default.

    Returns
    -------
    :return: result: DataFrame
        The first output of the script.
    """

    result = connection.run_ecl_script(script, syntax_check)
    # TODO just get the first with regex match
    regex = "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>"
    results = re.findall(regex, result.stdout)

    parsed = parse_xml(results[0][1])

    return parsed


def get_outputs(connection, script, syntax_check=True):
    """
    Return all outputs of an ECL script as a dict of
    DataFrames. See get_output() for downloading a single
    output and save_output() for writing straight to file.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param script: str
         Path of script to execute.
    :param syntax_check: bool, optional
        If the script be syntax checked before execution. True by
        default.

    :return: as_dict: dictionary
        Outputs produced by the script in the form {output_name: df}.
    """
    result = connection.run_ecl_script(script, syntax_check)
    regex = "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>"
    results = re.findall(regex, result.stdout)
    as_dict = {name: parse_xml(xml) for name, xml in results}

    return as_dict


def _get_file_structure(connection, logical_file, csv):
    """
    Get the column names and row count of a logical file.

    Parameters
    ----------
    :param connection
        HPCC Connection instance, see also `Connection`.
    :param logical_file: str
         Logical file to be downloaded
     :param csv: bool
         Is the logical file a CSV?

    Returns
    -------
    :return: list
        List of column names.
    :return: int
        Number of rows.
     """
    response = connection.get_logical_file_chunk(logical_file, 0, 2)
    file_size = response['Total']
    results = response['Result']['Row']
    # TODO make 2 sep functions
    if csv:
        column_names = results[0]['line'].split(',')
    else:
        column_names = results[0].keys()

    column_names = [col for col in column_names if col != '__fileposition__']

    return column_names, file_size


def get_logical_file(connection, logical_file, csv=False, max_workers=15,
                     chunk_size=10000, max_attempts=3):
    """
    Return a DataFrame of a logical file. To write to disk
    see save_file(). Note: Ordering of the resulting DataFrame is
    not deterministic and may not be the same as the logical file.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param logical_file: str
        Logical file to be downloaded.
    :param csv: bool, optional
        Is the logical file a CSV? False by default
    :param max_workers: int, optional
        Number of concurrent threads to use when downloading.
        Warning: too many will likely cause either your machine or
        your cluster to crash! 15 by default.
    :param chunk_size: int, optional.
        Size of chunks to use when downloading file. 10000 by
        default.
    :param max_attempts: int, optional
        Max number of attempts to download a chunk. 3 by default.

    Returns
    -------
    :return: df
        DataFrame of the logical file.
    """
    column_names, file_size = _get_file_structure(
        logical_file, connection, csv)

    chunks = filechunker.make_chunks(file_size, csv, chunk_size)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as \
            executor:
        futures = [
            executor.submit(connection.get_logical_file_chunk, logical_file,
                            start_row, n_rows, max_attempts)
            for start_row, n_rows in chunks
        ]

        finished, _ = concurrent.futures.wait(futures)

    df = pd.concat([parse_json_output(f.result()["Result"]["Row"],
                                      column_names, csv)
                    for f in finished], ignore_index=True)

    return df


def parse_json_output(results, column_names, csv):
    """
    Return a DataFrame from a HPCC workunit JSON.

    Parameters
    ----------
    :param results: dict, json
        JSON to be parsed.
    :param column_names: list
        Column names to parse.
    :param csv: bool
        Is this a csv file?

    :return: df: .DataFrame
        JSON converted to DataFrame.
    """
    if not csv:
        df = pd.DataFrame(results)
    else:
        lines = [",".split(r["line"]) for r in results]
        df = pd.DataFrame(map(list, zip(*lines)), columns=column_names)

    df = make_col_numeric(df)
    df = make_col_bool(df)

    return df


def parse_xml(xml):
    """
    Return a DataFrame from a nested XML.

    :param xml: str
        xml to be parsed.

    :return pd.DataFrame
        Parsed xml.
    """
    vls = []
    lvls = []

    for line in re.findall("<Row>(?P<content>.+?)</Row>", xml):
        with_start = '<Row>' + line + '</Row>'
        newvls = []
        etree = ElementTree.fromstring(with_start)
        for child in etree:
            if child.tag not in lvls:
                lvls.append(child.tag)
            newvls.append(child.text)
        vls.append(newvls)

    df = pd.DataFrame(vls, columns=lvls)

    df = make_col_numeric(df)
    df = make_col_bool(df)

    return df


def make_col_numeric(df):
    """
    Convert string numeric columns to numerics.

    Parameters
    ----------
    :param df: pd.DataFrame
        DataFrame to run conversion on.

    Returns
    -------
    :return: DataFrame
        Data frame with all string numeric columns converted to
        numeric.
    """

    logger = logging.getLogger('make_col_numeric')
    logger.debug('Converting numeric cols')

    for col in df.columns:
        try:
            nums = pd.to_numeric(df[col])
            df[col] = nums
            logger.debug('%s converted to numeric' % col)
        except ValueError:
            logger.debug('%s cannot be converted to numeric' % col)
            continue

    return df


def make_col_bool(df):
    """
    Convert string boolean columns to booleans.

    Parameters
    ----------
    :param df: pd.DataFrame
        DataFrame to run conversion on.

    Returns
    -------
    :return: DataFrame
        Data frame with all string boolean columns converted to
        boolean.
    """
    for col in df.columns:
        # TODO deal with nans?
        if set(df[col].str.lower().unique).issubset({"true", "false"}):
            df.loc[df[col].notnull(), col] = df[col].str.lower() == "true"

    return df