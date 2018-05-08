import re
import os

from hpycc import get_output, get_file
from hpycc.utils import parsers


def save_output(script, connection, path, compression=None, refresh=False,
                silent=False, debg=False, log_to_file=False):
    """
    Save the first output of an ECL script as a csv. See save_outputs() for downloading multiple files
    and get_output() for returning a pandas df.

    :param path: str
        Path of target destination.
    :param script: str
         Path of script to execute.
    :param compression: str, optional
        Compression format to give to pandas. None by default.
    :param refresh: bool, optional
        Should the file be overriden if it's already there? False by default which will cause a FileExistsException
        (unless silent=True, then you'll get a one line acknowledgement)
    :param silent: bool, optional
        Should all feedback except warnings and errors be suppressed. False by default
    :param debg: bool, optional
        Should debug info be logged. False by default
    :param log_to_file: bool, optional
        Should log info be dumped to a file. False by default
    :param logpath: str, optional
        If logging to file, what is the filename? hpycc.log by default.

    :return: None
    """

    # boot_logger(silent, debg, log_to_file, logpath) # No logger as get_output boots logger
    if os.path.isfile(path) and not refresh and not silent:
        raise FileExistsError('File already exists, set refresh=True to override or silent=True to suppress this error')
    elif os.path.isfile(path) and not refresh:
        return None

    result = get_output(script, connection, debg=debg,
                        log_to_file=log_to_file,  silent=silent)
    result.to_csv(path_or_buf=path, compression=compression, index=False)
    return None


def save_outputs(
        script, connection, directory=".", compression=None, filenames=None,
        prefix="", do_syntaxcheck=True, silent=False, debg=False,
        log_to_file=False):

    """
    Save all outputs of an ECL script as csvs using their output
    name. The file names can be changed using the filenames and
    prefix parameters. See get_outputs() for returning pandas
    dataframes and save_output() for single outputs.

    :param script: str
         Path of script to execute.
        using other arguments.
    :param directory: str, optional
        Directory to save output files in. "." by default.
    :param compression: str, optional
        Compression format to give to pandas. None by default.
    :param filenames: list, optional
        File names to save results as. If filenames is shorter than
        number of outputs, only those with a filename will be saved.
        If not specified, all files will be named their output name
        assigned by the ECL script.
    :param prefix: str, optional
        Prefix to prepend to all file names. "" by default.
    :param do_syntaxcheck: bool, optional
        Should a syntax check be completed on the script before running. True by default
    :param silent: bool, optional
        Should all feedback except warnings and errors be suppressed. False by default
    :param debg: bool, optional
        Should debug info be logged. False by default
    :param log_to_file: bool, optional
        Should log info be dumped to a file. False by default
    :param logpath: str, optional
        If logging to file, what is the filename? hpycc.log by default.

    :return: None
    """
    result = connection.run_ecl_script(script, do_syntaxcheck)
    regex = "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>"
    results = re.findall(regex, result.stdout)
    parsed_data_frames = [(name, parsers.parse_xml(xml)) for name, xml in
                          results]

    if filenames:
        if len(filenames) != len(parsed_data_frames):
            zipped = list(zip(parsed_data_frames, filenames))
    else:
        zipped = [(p, "{}.csv".format(p[0])) for p in parsed_data_frames]

    for result in zipped:
        file_name = "{}{}".format(prefix, result[1])
        path = os.path.join(directory, file_name)
        result[0][1].to_csv(path, compression=compression, index=False)

    return None


def save_file(logical_file, path, connection,
              csv_file=False, compression=None,
              refresh=False, silent=False, debg=False,
              log_to_file=False,
              download_threads=15):
    """
    Save an HPCC file to disk, see get_file_internal for returning a dataframe.
    Getting logical files has an advantage over scripts as they can
    be chunked and multi-threaded.

    :param logical_file: str
        Logical file to be downloaded
    :param path: str
        Path of target destination.
    :param csv_file: bool, optional
        Is the logical file a CSV? False by default
    :param compression: str, optional
        Handed to pandas' compression command when writing output file. Note that the path variable is respected.
    :param refresh: bool, optional
        Should the file be overridden if it's already there? False by default which will cause a FileExistsException
        (unless silent=True, then you'll get a one line acknowledgement)
    :param silent: bool, optional
        Should all feedback except warnings and errors be suppressed. False by default
    :param debg: bool, optional
        Should debug info be logged. False by default
    :param log_to_file: bool, optional
        Should log info be dumped to a file. False by default
    :param logpath: str, optional
        If logging to file, what is the filename? hpycc.log by default.
    :param download_threads: int, optional
        Number of concurrent download threads for the file. Warning: too many will likely
        cause either your script or you cluster to crash! 15 by default

    :return: None
    """

    if os.path.isfile(path) and not refresh and not silent:
        raise FileExistsError('File already exists, set refresh=True to override or silent=True to suppress this error')
    elif os.path.isfile(path) and not refresh:
        # print('File exists, nothing to do so exiting.')
        return None

    df = get_file(logical_file, connection, csv_file, debg=debg,
                  log_to_file=log_to_file, max_workers=download_threads,
                  silent=silent)

    df.to_csv(path, index=False, encoding='utf-8', compression=compression)

    return None