"""
The module contains functions to send files to HPCC.
"""

import logging
from hpycc.get import LOG_PATH
from hpycc.filerunning.sendfiles import send_file_internal
from hpycc.utils.logfunctions import boot_logger
from hpycc.utils.HPCCconnector import HPCCconnector


def send_file(source_file, logical_file, server, port="8010", repo=None,
               username="hpycc_get_output", password='" "',
               legacy=False, overwrite=False, delete=True,
               silent=False, debg=False, log_to_file=False, logpath=LOG_PATH):
    """
    Send a file to HPCC.

    :param source_file: str, pd.DataFrame,
         Either a string path to a csv for upload or a pandas dataframe. Using the
         latter allows you to control how the df is read in (as we haven't implemented
         kwargs yet).
    :param logical_file: str
         destination path on THOR
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
    :param overwrite: bool, optional
        Should files with the same name be overriden, default is no
    :param delete: bool, optional
        Once complete should all temporary scripts and THOR files be deleted?
        defaulut is yes.
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
    logger = logging.getLogger('run_script')
    logger.debug('Starting run_script')

    hpcc_server = HPCCconnector(server, port, repo, username, password, legacy)
    send_file_internal(source_file, logical_file, overwrite, delete, hpcc_server)

    return None
