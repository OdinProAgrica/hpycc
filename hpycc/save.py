import itertools
import re
import os

import hpycc.get
import hpycc.utils.parsers
from hpycc import get_output, get_logical_file


def save_output(connection, script, path_or_buf=None, syntax_check=True,
                **kwargs):
    """
    Save the first output of an ECL script as a csv. See
    save_outputs() for saving multiple outputs to file and
    get_output() for returning as a DataFrame.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param script: str
        Path of script to execute.
    :param path_or_buf: str, optional.
        File path or object, if None is provided the result is
        returned as a string. None by default.
    :param syntax_check: bool, optional
        Should script be syntax checked before execution. True by
        default.
    :param kwargs
        Additional parameters to be provided to
        pandas.DataFrame.to_csv().

    Returns
    -------
    :return: None if path_or_buf is not None, else a string
        representation of the output csv.
    """
    result = get_output(connection=connection, script=script,
                        syntax_check=syntax_check)
    return result.to_csv(path_or_buf=path_or_buf, **kwargs)


def save_outputs(connection, script, directory=".", filenames=None,
                 prefix=None, syntax_check=True, **kwargs):
    """
    Save all outputs of an ECL script as csvs. See get_outputs()
    for returning DataFrames and save_output() for writing a single
    output to file.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param script: str
         Path of script to execute.
    :param directory: str, optional
        Directory to save output files in. "." by default.
    :param filenames: list, optional
        File names to save results as. If not specified, files will
        be named as their output name assigned by the ECL script.
        None by default.
    :param prefix: str, optional
        Prefix to prepend to all file names. None by default.
    :param syntax_check: bool, optional
        Should the script be syntax checked before execution. True by
        default.
    :param kwargs:
        Additional parameters to be provided to
        pandas.DataFrame.to_csv().


    Returns
    -------
    :return: None
    """
    result = connection.run_ecl_script(script, syntax_check)
    regex = "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>"
    results = re.findall(regex, result.stdout)
    parsed_data_frames = [(name, hpycc.utils.parsers.parse_xml(xml))
                          for name, xml in results]

    parsed_filenames = ["{}.csv".format(res[0]) for res in parsed_data_frames]
    chosen_filenames = [fn if fn else pfn for fn, pfn in
                        itertools.zip_longest(filenames, parsed_filenames)]
    if prefix:
        chosen_filenames = [prefix + name for name in chosen_filenames]

    if filenames is not None and len(filenames) > len(parsed_data_frames):
        add_names = filenames[len(parsed_data_frames):]
        UserWarning("Too many file names specified, ignoring {}".format(
            add_names))
    elif filenames is not None and len(filenames) < len(parsed_data_frames):
        add_names = parsed_filenames[len(filenames):]
        UserWarning("Too few file names specified. Additional files will be "
                    "named {}".format(add_names))

    for name, result in zip(chosen_filenames, parsed_data_frames):
        path = os.path.join(directory, name)
        result[1].to_csv(path, **kwargs)


def save_logical_file(connection, logical_file, path_or_buf, csv=False,
                      max_workers=15, chunk_size=10000, max_attempts=3,
                      **kwargs):
    """
    Save a logical file to disk, see get_file() for returning a
    DataFrame.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param logical_file: str
        Logical file to be downloaded
    :param path_or_buf: str, optional.
        File path or object, if None is provided the result is
        returned as a string. None by default.
    :param csv: bool, optional
        Is the logical file a CSV? False by default.
    :param max_workers: int, optional
        Number of concurrent threads to use when downloading.
        Warning: too many will likely cause either your machine or
        your cluster to crash! 15 by default.
     :param chunk_size: int, optional.
        Size of chunks to use when downloading file. 10000 by
        default.
    :param max_attempts: int, optional
        Max number of attempts to download a chunk. 3 by default.
    :param kwargs
        Additional parameters to be provided to
        pandas.DataFrame.to_csv().

    Returns
    -------
    :return: None if path_or_buf is not None, else a string
        representation of the output csv.
    """

    file = get_logical_file(connection, logical_file, csv, max_workers,
                            chunk_size, max_attempts)

    return file.to_csv(path_or_buf, **kwargs)
