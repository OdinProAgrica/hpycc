"""
The module contains functions to run a script with data not returned. The main use case I had in mind
was for running a script, saving a logical file and then accessing with get_file(). This methond would
take advantage of multi-threading, something which script outputs cannot do. This also contains a script
for deleting logical files.
"""

import logging
import os
from hpycc.utils.logfunctions import boot_logger


def run_script(script, connection, do_syntaxcheck=True,
               silent=False, debg=False, log_to_file=False):
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

    boot_logger(silent, debg, log_to_file, )
    logger = logging.getLogger('run_script')
    logger.debug('Starting run_script')

    connection.run_ecl_script(script, do_syntaxcheck)

    return None


def delete_logical_file(logical_file, connection, do_syntaxcheck=True,
                        silent=False, debg=False, log_to_file=False):
    """
    Delete a logical file.


    :param logical_file: str
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

    boot_logger(silent, debg, log_to_file)
    logger = logging.getLogger('delete_logical_file file')
    logger.debug('Starting delete_logical_file file')

    script = "IMPORT std; STD.File.DeleteLogicalFile('%s');" % logical_file

    script_loc = 'tempScript.ecl'
    with open(script_loc, 'w') as f:
        f.writelines(script)

    connection.run_ecl_script(script_loc, do_syntaxcheck)
    os.remove(script_loc)

    return None
