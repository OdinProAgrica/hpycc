"""
This module contains functions to get either the output(s) of an ECL script,
or a logical file.
"""
import concurrent.futures
import re

import pandas as pd

from hpycc.utils import filechunker

# TODO old_tests
# TODO numpy docstrings
from hpycc.utils.parsers import parse_xml, parse_json_output


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

    regex = "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>"
    match = re.search(regex, result.stdout)
    match_content = match.group()

    parsed = parse_xml(match_content)

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


def _get_columns_of_logical_file(connection, logical_file, csv):
    """
    Return the column names of a logical file.

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
    :return: column_names: list
        List of column names in logical file.
    """
    response = connection.get_logical_file_chunk(logical_file, 0, 2)
    results = response['Result']['Row']

    if csv:
        column_names = results[0]['line'].split(',')
    else:
        column_names = results[0].keys()

    column_names = [col for col in column_names if col != '__fileposition__']

    return column_names


def _get_logical_file_row_count(connection, logical_file):
    """
    Return the number of rows in a logical file.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param logical_file: str
         Logical file to be examined.

    Returns
    -------
    :return: file_size, int
        Number of rows in logical file.
    """
    response = connection.get_logical_file_chunk(logical_file, 0, 2)
    file_size = response['Total']

    return file_size


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
    column_names = _get_columns_of_logical_file(connection, logical_file, csv)
    file_size = _get_logical_file_row_count(connection, logical_file)

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
