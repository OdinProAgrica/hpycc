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
- `get_thor_file` -- Return the contents of a thor file.

"""
__all__ = ["get_output", "get_outputs", "get_thor_file", "get_logical_file"]

from concurrent.futures import ThreadPoolExecutor, wait
import re
import warnings

import pandas as pd

from hpycc.utils import filechunker
from hpycc.utils.parsers import parse_xml, parse_schema_from_xml, Schema


def get_output(connection, script, syntax_check=True, delete_workunit=True,
               stored=None):
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
    delete_workunit: bool, optional
        Delete workunit once completed. True by default.
    stored : dict or None, optional
        Key value pairs to replace stored variables within the
        script. Values should be str, int or bool. None by default.


    Returns
    -------
    pandas.DataFrame of the first output of `script`.

    Raises
    ------
    SyntaxError:
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

    result = connection.run_ecl_script(script, syntax_check, delete_workunit,
                                       stored)

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


def get_outputs(connection, script, syntax_check=True, delete_workunit=True,
                stored=None):
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
    delete_workunit: bool,
       Delete the workunit once completed. True by default.
    stored : dict or None, optional
        Key value pairs to replace stored variables within the
        script. Values should be str, int or bool. None by default.

    Returns
    -------
    as_dict: dict of pandas.DataFrames
        Outputs of `script` in the form
        {output_name: pandas.DataFrame}

    Raises
    ------
    SyntaxError:
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
    result = connection.run_ecl_script(script, syntax_check, delete_workunit,
                                       stored)
    regex = "<Dataset name='(?P<name>.+?)'>(?P<content>.*?)</Dataset>"

    result = result.stdout.replace("\r\n", "")
    results = re.findall(regex, result)
    if any([i[1] == "" for i in results]):
        warnings.warn(
            "One or more of the outputs do not appear to contain a dataset. "
            "They have been replaced with an empty DataFrame")
    as_dict = {name.replace(" ", "_"): parse_xml(xml) for name, xml in results}

    return as_dict


def get_logical_file(*args, **kwargs):
    """
    .. deprecated:: 0.1.3
        `get_logical_file` has been deprecated. Use `get_thor_file`.

    """
    _ = kwargs
    _ = args
    raise ImportError("This function has been deprecated, use get_thor_file "
                      "instead.")


def get_thor_file(connection, thor_file, max_workers=15, chunk_size=10000,
                  max_attempts=3, max_sleep=10, dtype=None):
    """
    Return a thor file as a pandas.DataFrame.

    Note: Ordering of the resulting DataFrame is
    not deterministic and may not be the same as on the HPCC cluster.

    Parameters
    ----------
    connection: hpycc.Connection
        HPCC Connection instance, see also `Connection`.
    thor_file: str
        Name of thor file to be downloaded.
    max_workers: int, optional
        Number of concurrent threads to use when downloading file.
        Warning: too many may cause either your machine or
        your cluster to crash! 15 by default.
    chunk_size: int, optional
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
    dtype: type name or dict of col -> type, optional
        Data type for data or columns. E.g. {‘a’: np.float64, ‘b’:
        np.int32}. If converters are specified, they will be applied
        INSTEAD of dtype conversion. If None, or columns are missing
        from the provided dict, they will be converted to one of
        bool, str or int based on the HPCC datatype. None by default.

    Returns
    -------
    df: pandas.DataFrame
        Thor file as a pandas.DataFrame.

    See Also
    --------
    save_thor_file

    Examples
    --------
    >>> import hpycc
    >>> import pandas
    >>> conn = hpycc.Connection("user")
    >>> df = pandas.DataFrame({"col1": [1, 2, 3]})
    >>> df.to_csv("example.csv", index=False)
    >>> hpycc.spray_file(conn,"example.csv","example")
    >>> hpycc.get_logical_file(conn, "example")
        col1
    0     1
    1     2
    2     3

    >>> import hpycc
    >>> import pandas
    >>> conn = hpycc.Connection("user")
    >>> df = pandas.DataFrame({"col1": [1, 2, 3]})
    >>> df.to_csv("example.csv", index=False)
    >>> hpycc.spray_file(conn,"example.csv","example")
    >>> hpycc.get_logical_file(conn, "example", dtype=str)
        col1
    0     '1'
    1     '2'
    2     '3'

    """
    url = ("http://{}:{}/WsWorkunits/WUResult.json?LogicalName={}"
           "&Cluster=thor&Start={}&Count={}").format(
        connection.server, connection.port, thor_file, 0, 1)
    r = connection.run_url_request(url, max_attempts, max_sleep)
    rj = r.json()
    try:
        wuresultresponse = rj["WUResultResponse"]
    except KeyError:
        raise KeyError("json: {}".format(rj))
    schema_str = wuresultresponse["Result"]["XmlSchema"]["xml"]

    # get the schema as named tuples of (name, is_set, type)
    schema = parse_schema_from_xml(schema_str)

    num_rows = wuresultresponse["Total"]

    # if there are no rows to go and get, we should return an empty dataframe
    if not num_rows:
        cols = [c.name for c in schema]
        return pd.DataFrame(columns=cols)

    chunks = filechunker.make_chunks(num_rows, chunk_size)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(connection.get_logical_file_chunk, thor_file,
                            start_row, n_rows, max_attempts, max_sleep)
            for start_row, n_rows in chunks
        ]

        finished, _ = wait(futures)

    results = [i.result() for i in finished]
    flat_list = (item for sublist in results for item in sublist)

    df = pd.DataFrame(flat_list)

    # dtype is a dict
    # we replace all those in the dict, then those not specified with schema
    if not isinstance(dtype, dict) and dtype:
            return df.astype(dtype)

    schema_dict = {i.name: i for i in schema}

    if dtype:
        i = {col: Schema(col, False, dtype[col]) for col in dtype}
        schema_dict.update(i)

    for col in schema_dict:
        c = schema_dict[col]
        if c.is_set:
            df[c.name] = df[c.name].map(
                lambda x: [c.type(i) for i in x["Item"]])
        else:
            df[c.name] = df[c.name].astype(c.type)
    return df
