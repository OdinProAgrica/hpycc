import os

import hpycc.get
import hpycc.utils.parsers
from hpycc import get_output, get


def save_output(connection, script, path_or_buf=None, syntax_check=True,
                delete_workunit=True, stored=None, **kwargs):
    """
    Save the first output of an ECL script as a csv. See
    save_outputs() for saving multiple outputs to file and
    get_output() for returning as a DataFrame.

    Parameters
    ----------
    connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    script: str
        Path of script to execute.
    path_or_buf : string or file handle, default None
        File path or object, if None is provided the result is returned as
        a string.
    syntax_check: bool, optional
        Should script be syntax checked before execution. True by
        default.
    delete_workunit: bool, optional
        Delete workunit once completed. True by default.
    stored : dict or None, optional
        Key value pairs to replace stored variables within the
        script. Values should be str, int or bool. None by default.
    kwargs
        Additional parameters to be provided to
        pandas.DataFrame.to_csv().

    Returns
    -------
    None or str
        if path_or_buf is not None, else a string representation of
        the output csv.
    """
    result = get_output(connection=connection, script=script,
                        syntax_check=syntax_check,
                        delete_workunit=delete_workunit,
                        stored=stored)
    return result.to_csv(path_or_buf=path_or_buf, **kwargs)


def save_outputs(connection, script, directory=".", filenames=None,
                 prefix=None, syntax_check=True, delete_workunit=True,
                 stored=None, **kwargs):
    """
    Save all outputs of an ECL script as csvs. See get_outputs()
    for returning DataFrames and save_output() for writing a single
    output to file.

    Parameters
    ----------
    connection : hpycc.Connection
        HPCC Connection instance, see also `Connection`.
    script : str
         Path of script to execute.
    directory : str, optional
        Directory to save output files in. "." by default.
    filenames : list or None, optional
        File names to save results as. If None, files will
        be named as their output name assigned by the ECL script.
        An IndexError will be raised if this is a different length
        to the number of outputs. None by default.
    prefix : str or None, optional
        Prefix to prepend to all file names. None by default.
    syntax_check : bool, optional
        Should the script be syntax checked before execution. True by
        default.
    delete_workunit : bool, optional
        Delete workunit once completed. True by default.
    stored : dict or None, optional
        Key value pairs to replace stored variables within the
        script. Values should be str, int or bool. None by default.
    kwargs
        Additional parameters to be provided to
        pandas.DataFrame.to_csv().


    Returns
    -------
    None

    Raises
    ------
    IndexError
        If `filenames` is of different length to the number of
        outputs.
    """
    results = hpycc.get_outputs(
        connection, script, syntax_check, delete_workunit, stored)

    parsed_filenames = ["{}.csv".format(res) for res in results]

    # if filenames was none, use parsed filenames
    # if filenames is given and it is the wrong length - raise
    # if filenames is given adn right length - use it

    if not filenames:
        chosen_filenames = parsed_filenames
    elif len(filenames) == len(parsed_filenames):
        chosen_filenames = filenames
    else:
        msg = (
            "{0} filenames were specified. Only {1} outputs were returned. "
            "Filenames should either be of length {1} or None").format(
            len(filenames), len(parsed_filenames)
        )
        raise IndexError(msg)

    if prefix:
        chosen_filenames = [prefix + name for name in chosen_filenames]

    for name, result in zip(chosen_filenames, results.items()):
        path = os.path.join(directory, name)
        result[1].to_csv(path, **kwargs)


def save_thor_file(connection, thor_file, path_or_buf,
                   max_workers=15, chunk_size=10000, max_attempts=3,
                   max_sleep=10, dtype=None, **kwargs):
    """
    Save a logical file to disk, see get_file() for returning a
    DataFrame.

    Parameters
    ----------
    connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    thor_file: str
        Logical file to be downloaded
    path_or_buf : string or file handle, default None
        File path or object, if None is provided the result is returned as
        a string.
    max_workers: int, optional
        Number of concurrent threads to use when downloading.
        Warning: too many will likely cause either your machine or
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
    dtype: type name or dict of col -> type, optional
        Data type for data or columns. E.g. {‘a’: np.float64, ‘b’:
        np.int32}. If converters are specified, they will be applied
        INSTEAD of dtype conversion. If None, or columns are missing
        from the provided dict, they will be converted to one of
        bool, str or int based on the HPCC datatype. None by default.
    kwargs
        Additional parameters to be provided to
        pandas.DataFrame.to_csv().

    Returns
    -------
    None or str
        if path_or_buf is not None, else a string
        representation of the output csv.

    """

    file = get.get_thor_file(connection, thor_file, max_workers,
                             chunk_size, max_attempts, max_sleep,
                             dtype)

    return file.to_csv(path_or_buf, **kwargs)
