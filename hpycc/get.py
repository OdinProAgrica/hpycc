"""
Functions to get data out of a HPCC instance.

This module contains functions to get either the output(s) of an
ECL script, or the contents of a logical file. The first input to all
functions is an instance of `Connection`.

Functions
---------
- `get_output` -- Return the first output of an ECL script.
- `get_outputs` -- Return all outputs of an ECL script.
- `get_logical_file` -- Return the contents of a logical file.

"""
__all__ = ["get_output", "get_outputs", "get_logical_file"]

from concurrent.futures import ThreadPoolExecutor, wait
import re
import warnings

import pandas as pd

from hpycc.utils.filechunker import make_chunks

from hpycc.utils.parsers import parse_xml, parse_json_output


def get_output(connection, script, syntax_check=True):
    """
    Return the first output of an ECL script as a pandas.DataFrame.

    Note that whilst attempts are made to preserve the datatypes of
    the result, anything with an ambiguous type will revert to a
    string. If the output of the ECL string is an empty dataset
    (or if the script does not output anything), an empty
    pandas.DataFrame is returned.

    Parameters
    ----------
    connection: hpycc.Connection
        HPCC Connection instance, see also `Connection`.
    script: str
         Path of script to execute.
    syntax_check: bool, optional
        Should the script be syntax checked before execution? True by
        default.

    Returns
    -------
    parsed: pandas.DataFrame
        pandas.DataFrame of the first output of `script`.

    Raises
    ------
    subprocess.CalledProcessError:
        If script fails syntax check.

    See Also
    --------
    get_outputs
    save_output
    Connection.syntax_check

    Examples
    --------
    >>> import hpycc
    >>> conn = hpycc.Connection("user")
    >>> with open("example.ecl", "r+") as file:
    ...     file.write("OUTPUT(2);")
    >>> hpycc.get_output(conn, "example.ecl")
        Result_1
    0          2

    >>> import hpycc
    >>> conn = hpycc.Connection("user")
    >>> with open("example.ecl", "r+") as file:
    >>>     file.write("OUTPUT(2);OUTPUT(3);")
    >>> hpycc.get_output(conn, "example.ecl")
        Result_1
    0          2

    >>> import hpycc
    >>> conn = hpycc.Connection("user")
    >>> with open("example.ecl", "r+") as file:
    ...     file.write(
    ...     "a:= DATASET([{'1', 'a'}],"
    ...     "{STRING col1; STRING col2});",
    ...     "OUTPUT(a);")
    >>> hpycc.get_output(conn, "example.ecl")
       col1 col2
    0     1    a

    >>> import hpycc
    >>> conn = hpycc.Connection("user")
    >>> with open("example.ecl", "r+") as file:
    ...     file.write(
    ...     "a:= DATASET([{'a', 'a'}],"
    ...     "{STRING col1;});",
    ...     "OUTPUT(a(col1 != a));")
    >>> hpycc.get_output(conn, "example.ecl")
    Empty DataFrame
    Columns: []
    Index: []

    """

    result = connection.run_ecl_script(script, syntax_check)
    result = result.stdout.replace("\r\n", "")

    regex = "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>"
    match = re.search(regex, result)
    try:
        match_content = match.group()
    except AttributeError:
        warnings.warn("The output does not appear to contain a dataset. "
                      "Returning an empty DataFrame.")
        return pd.DataFrame()
    else:
        parsed = parse_xml(match_content)
        return parsed


def get_outputs(connection, script, syntax_check=True):
    """
    Return all outputs of an ECL script.

    Note that whilst attempts are made to preserve the datatypes of
    the result, anything with an ambiguous type will revert to a
    string.

    Parameters
    ----------
    connection: hpycc.Connection
        HPCC Connection instance, see also `Connection`.
    script: str
         Path of script to execute.
    syntax_check: bool, optional
        Should the script be syntax checked before execution? True by
        default.

    Returns
    -------
    as_dict: dict of pandas.DataFrames
        Outputs of `script` in the form
        {output_name: pandas.DataFrame}

    Raises
    ------
    subprocess.CalledProcessError:
        If script fails syntax check.

    See Also
    --------
    get_output
    save_outputs
    Connection.syntax_check

    Examples
    --------
    >>> import hpycc
    >>> conn = hpycc.Connection("user")
    >>> with open("example.ecl", "r+") as file:
    ...     file.write("OUTPUT(2);")
    >>> hpycc.get_outputs(conn, "example.ecl")
    {Result_1:
        Result_1
    0          2
    }

    >>> import hpycc
    >>> conn = hpycc.Connection("user")
    >>> with open("example.ecl", "r+") as file:
    ...     file.write(
    ...     "a:= DATASET([{'1', 'a'}],"
    ...     "{STRING col1; STRING col2});",
    ...     "OUTPUT(a);")
    >>> hpycc.get_outputs(conn, "example.ecl")
    {Result_1:
       col1 col2
    0     1    a
    }

    >>> import hpycc
    >>> conn = hpycc.Connection("user")
    >>> with open("example.ecl", "r+") as file:
    ...     file.write(
    ...     "a:= DATASET([{'1', 'a'}],"
    ...     "{STRING col1; STRING col2});",
    ...     "OUTPUT(a);"
    ...     "OUTPUT(a);")
    >>> hpycc.get_outputs(conn, "example.ecl")
    {Result_1:
       col1 col2
    0     1    a,
    Result_2:
       col1 col2
    0     1    a
    }

    >>> import hpycc
    >>> conn = hpycc.Connection("user")
    >>> with open("example.ecl", "r+") as file:
    ...     file.write(
    ...     "a:= DATASET([{'1', 'a'}],"
    ...     "{STRING col1; STRING col2});",
    ...     "OUTPUT(a);"
    ...     "OUTPUT(a, NAMED('ds_2'));")
    >>> hpycc.get_outputs(conn, "example.ecl")
    {Result_1:
       col1 col2
    0     1    a,
    ds_2:
       col1 col2
    0     1    a
    }

    """
    result = connection.run_ecl_script(script, syntax_check)
    regex = "<Dataset name='(?P<name>.+?)'>(?P<content>.*?)</Dataset>"

    result = result.stdout.replace("\r\n", "")
    results = re.findall(regex, result)
    if any([i[1] == "" for i in results]):
        warnings.warn(
            "One or more of the outputs do not appear to contain a dataset. "
            "They have been replaced with an empty DataFrame")
    as_dict = {name.replace(" ", "_"): parse_xml(xml) for name, xml in results}

    return as_dict


def _get_columns_of_logical_file(connection, logical_file, csv, max_attempts,
                                 max_sleep):
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
    response = connection.get_logical_file_chunk(
        logical_file, 0, 2, max_attempts, max_sleep)
    results = response

    if csv:
        column_names = results[0]['line'].split(',')
    else:
        column_names = results[0].keys()

    column_names = [col for col in column_names if col != '__fileposition__']

    return column_names


def _get_logical_file_row_count(connection, logical_file, max_attempts,
                                max_sleep):
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
    url = ("http://{}:{}/WsWorkunits/WUResult.json?LogicalName={}"
           "&Cluster=thor&Start={}&Count={}").format(
        connection.server, connection.port, logical_file, 0, 2)

    r = connection.run_url_request(url, max_attempts, max_sleep)
    rj = r.json()
    try:
        file_size = rj["WUResultResponse"]['Total']
    except KeyError:
        raise KeyError("json: {}".format(rj))

    return file_size


def get_logical_file(connection, logical_file, csv=False, max_workers=15,
                     chunk_size=10000, max_attempts=3, max_sleep=10):
    """
    Return a logical file as a pandas.DataFrame.

    Note: Ordering of the resulting DataFrame is
    not deterministic and may not be the same as the logical file.
    Whilst attempts are made to preserve the datatypes of
    the result, anything with an ambiguous type will revert to a
    string.

    Parameters
    ----------
    connection: hpycc.Connection
        HPCC Connection instance, see also `Connection`.
    logical_file: str
        Name of logical file to be downloaded.
    csv: bool, optional
        Is the logical file a CSV? False by default.
    max_workers: int, optional
        Number of concurrent threads to use when downloading file.
        Warning: too many may cause either your machine or
        your cluster to crash! 15 by default.
    chunk_size: int, optional.
        Size of chunks to use when downloading file. 10000 by
        default.
    max_attempts: int, optional
        Maximum number of times a chunk should attempt to be
        downloaded in the case of an exception being raised.
        3 by default.
    max_sleep: int, optional
            Maximum time, in seconds, to sleep between attempts.
            The true sleep time is a random int between 0 and
            `max_sleep`.

    Returns
    -------
    df: pandas.DataFrame
        Logical file as a pandas.DataFrame.

    See Also
    --------
    save_logical_file

    Examples
    --------
    >>> import hpycc
    >>> import pandas
    >>> conn = hpycc.Connection("user")
    >>> df = pandas.DataFrame({"col1": [1, 2, 3]})
    >>> df.to_csv("example.csv", index=False)
    >>> hpycc.spray_file(conn, "example.csv", "example")
    >>> hpycc.get_logical_file(conn, "thor::example", False)
        col1
    0      1
    1      2
    2      3

    """
    column_names = _get_columns_of_logical_file(
        connection, logical_file, csv, max_attempts, max_sleep)
    file_size = _get_logical_file_row_count(
        connection, logical_file, max_attempts, max_sleep)

    chunks = make_chunks(file_size, csv, chunk_size)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(connection.get_logical_file_chunk, logical_file,
                            start_row, n_rows, max_attempts, max_sleep)
            for start_row, n_rows in chunks
        ]

        finished, _ = wait(futures)

    df = pd.concat([parse_json_output(f.result(),
                                      column_names, csv)
                    for f in finished], ignore_index=True)

    return df
