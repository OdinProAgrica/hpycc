"""
This module contains the callable functions in hpycc. Basically you may get data via a script with one
result (..._output), a script with multiple results (..._outputs) or a logical file (..._file).
Data may be saved or simply returned as a pandas dataframe. The module also contains a function to
run a script with no import. The main  use case I had in mind was for running a script, saving a
logical file and then accessing with get_file(). This takes advantage of multi-threading, something
which script outputs cannot do.
"""

import logging
import os
from hpycc.filerunning import getfiles
from hpycc.scriptrunning import getscripts
from hpycc.scriptrunning import runscript
from hpycc.utils.logfunctions import boot_logger

LOG_PATH = 'hpycc.log'


def _make_hpcc_server(server, port, repo, username, password, legacy):
    """
    Creates a dict storing HPCC server parameters. Saves verbosity in function calls

    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by default
    :param legacy: bool, optional
        Should ECL commands be sent with the -legacy flag, False by default

    :return: dict
        Basically all the above in a single dict.
    """

    return {
        'server': server,
        'port': port,
        'repo': repo,
        'username': username,
        'password': password,
        'legacy': legacy
    }


def get_output(script, server, port="8010", repo=None,
               username="hpycc_get_output", password='" "',
               legacy=False, do_syntaxcheck=True,
               silent=False, debg=False, log_to_file=False,
               logpath=LOG_PATH):

    """
    Return the first output of an ECL script as a DataFrame. See save_output() for writing
    straight to file and get_outputs() for downloading scripts with multiple outputs.

    :param script: str
         Path of script to execute.
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by default
    :param legacy: bool, optional
        Should ECL commands be sent with the -legacy flag, False by default
    :param do_syntaxcheck: bool, optional
        Should a syntaxcheck be completed on the script before running. True by default
    :param silent: bool, optional
        Should all feedback except warnings and errors be suppressed. False by default
    :param debg: bool, optional
        Should debug info be logged. False by default
    :param log_to_file: bool, optional
        Should log info be dumped to a file. False by default
    :param logpath: str, optional
        If logging to file, what is the filename? hpycc.log by default.

    :return: result: DataFrame
        The first output produced by the script.
    """

    boot_logger(silent, debg, log_to_file, logpath)
    logger = logging.getLogger('get_output')
    logger.debug('Starting get_script_internal')

    hpcc_server = _make_hpcc_server(server, port, repo, username, password, legacy)

    parsed_data_frames = getscripts.get_script_internal(script, hpcc_server, do_syntaxcheck)

    logger.debug('Extracting outputs')
    try:
        first_parsed_result = parsed_data_frames[0][1]
    except IndexError:
        logger.error('Unable to parse response, printing first 500 characters: %s' % parsed_data_frames[:500])
        raise

    return first_parsed_result


def get_outputs(script, server, port="8010", repo=None,
                username="hpycc_get_output", password='" "',
                legacy=False, do_syntaxcheck=True,
                silent=False, debg=False, log_to_file=False,
                logpath=LOG_PATH):
    """
    Return all outputs of an ECL script as a dict of DataFrames. See get_output for single files and
    save_outputs() for writing multiple files to disk.

    :param script: str
         Path of script to execute.
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by default.
    :param legacy: bool, optional
        Should ECL commands be sent with the -legacy flag, False by default
    :param do_syntaxcheck: bool, optional
        Should a syntaxcheck be completed on the script before running. True by default
    :param silent: bool, optional
        Should all feedback except warnings and errors be suppressed. False by default
    :param debg: bool, optional
        Should debug info be logged. False by default
    :param log_to_file: bool, optional
        Should log info be dumped to a file. False by default
    :param logpath: str, optional
        If logging to file, what is the filename? hpycc.log by default.

    :return: as_dict: dictionary
        Outputs produced by the script in the form {output_name: df}.
    """

    boot_logger(silent, debg, log_to_file, logpath)
    logger = logging.getLogger('get_outputs')
    logger.debug('Starting get_script_internal')

    hpcc_server = _make_hpcc_server(server, port, repo, username, password, legacy)
    parsed_data_frames = getscripts.get_script_internal(script, hpcc_server, do_syntaxcheck)

    logger.debug('Converting response to Dict')
    as_dict = dict(parsed_data_frames)

    return as_dict


def get_file(logical_file, server, port='8010',
             username="hpycc_get_output", password='" "',
             csv_file=False, silent=False, debg=False,
             log_to_file=False, logpath=LOG_PATH,
             download_threads=15):

    """
    Download an HPCC logical file and return a pandas dataframe. To save to csv
    without a return use save_file(). This process has an advantage over scripts as it can be
    chunked and threaded.

    :param logical_file: str
        Logical file to be downloaded
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by default
    :param csv_file: bool, optional
        Is the logical file a CSV? False by default
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

    :return: pd.DataFrame
        a DF of the given file
    """

    boot_logger(silent, debg, log_to_file, logpath)
    logger = logging.getLogger('get_file_internal')
    logger.debug('Starting get_file_internal')

    hpcc_server = _make_hpcc_server(server, port, None, username, password, None)
    try:
        df = getfiles.get_file_internal(logical_file, hpcc_server, csv_file, download_threads)
    except KeyError:
        logger.error('Key error, have you specified a CSV or THOR file correctly?')
        raise

    return df


def save_output(script, server, path, port="8010", repo=None,
                username="hpycc_get_output", password='" "',
                compression=None, legacy=False,
                refresh=False, silent=False, debg=False,
                log_to_file=False, logpath=LOG_PATH):
    """
    Save the first output of an ECL script as a csv. See save_outputs() for downloading multiple files
    and get_output() for returning a pandas df.

    :param path: str
        Path of target destination.
    :param script: str
         Path of script to execute.
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by default.
    :param compression: str, optional
        Compression format to give to pandas. None by default.
    :param legacy: bool, optional
        Should ECL commands be sent with the -legacy flag, False by default
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

    result = get_output(script, server, port, repo, username, password, legacy=legacy, debg=debg,
                        log_to_file=log_to_file, logpath=logpath, silent=silent)
    result.to_csv(path_or_buf=path, compression=compression, index=False)
    return None


def save_outputs(
        script, server, directory=".", port="8010", repo=None,
        username="hpycc_get_output", password='" "',
        compression=None, filenames=None, prefix="", legacy=False,
        do_syntaxcheck=True, silent=False, debg=False,
        log_to_file=False, logpath=LOG_PATH):

    """
    Save all outputs of an ECL script as csvs using their output
    name. The file names can be changed using the filenames and
    prefix parameters. See get_outputs() for returning pandas
    dataframes and save_output() for single outputs.

    :param script: str
         Path of script to execute.
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param directory: str, optional
        Directory to save output files in. "." by default.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by default.
    :param compression: str, optional
        Compression format to give to pandas. None by default.
    :param filenames: list, optional
        File names to save results as. If filenames is shorter than
        number of outputs, only those with a filename will be saved.
        If not specified, all files will be named their output name
        assigned by the ECL script.
    :param prefix: str, optional
        Prefix to prepend to all file names. "" by default.
    :param legacy: bool, optional
        Should ECL commands be sent with the -legacy flag, False by default
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

    boot_logger(silent, debg, log_to_file, logpath)
    logger = logging.getLogger('get_outputs')
    logger.debug('Starting get_script_internal')

    hpcc_server = _make_hpcc_server(server, port, None, username, password, None)
    parsed_data_frames = getscripts.get_script_internal(script, hpcc_server, do_syntaxcheck)

    if filenames:
        if len(filenames) != len(parsed_data_frames):
            logger.warning("The number of filenames specified is different to " +
                           "the number of outputs in your script. Adding names to compensate.")
        zipped = list(zip(parsed_data_frames, filenames))
    else:
        zipped = [(p, "{}.csv".format(p[0])) for p in parsed_data_frames]

    for result in zipped:
        file_name = "{}{}".format(prefix, result[1])
        path = os.path.join(directory, file_name)
        result[0][1].to_csv(path, compression=compression, index=False)

    return None


def save_file(logical_file, path, server, port='8010',
              username="hpycc_get_output", password='" "',
              csv_file=False, compression=None,
              refresh=False, silent=False, debg=False,
              log_to_file=False, logpath=LOG_PATH,
              download_threads=15):
    """
    Save an HPCC file to disk, see get_file_internal for returning a dataframe.
    Getting logical files has an advantage over scripts as they can
    be chunked and multi-threaded.

    :param logical_file: str
        Logical file to be downloaded
    :param path: str
        Path of target destination.
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by default
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

    df = get_file(logical_file, server, port, username, password, csv_file, debg=debg,
                  log_to_file=log_to_file, logpath=logpath, download_threads=download_threads,
                  silent=silent)

    df.to_csv(path, index=False, encoding='utf-8', compression=compression)

    return None


def run_script(script, server, port="8010", repo=None,
               username="hpycc_get_output", password='" "',
               legacy=False, do_syntaxcheck=True,
               silent=False, debg=False, log_to_file=False, logpath=LOG_PATH):
    """
    Run an ECL script but do not download, save or process the response.


    :param script: str
         Path of script to execute.
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by default
    :param legacy: bool, optional
        Should ECL commands be sent with the -legacy flag, False by default
    :param do_syntaxcheck: bool, optional
        Should a syntaxcheck be completed on the script before running. True by default
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

    boot_logger(silent, debg, log_to_file, logpath)
    logger = logging.getLogger('run_script_internal')
    logger.debug('Starting run_script_internal')

    hpcc_server = _make_hpcc_server(server, port, None, username, password, None)
    runscript.run_script_internal(script, hpcc_server, do_syntaxcheck)

    return None
