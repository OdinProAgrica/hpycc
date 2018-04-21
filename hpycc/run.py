import logging
import hpycc.scriptrunning.runscript
from hpycc.utils.logfunctions import boot_logger

def run_script(script, server, port="8010", repo=None,
               username="hpycc_get_output", password='" "',
               legacy=False, do_syntaxcheck=True,
               silent=False, debg=False, log_to_file=False, logpath=LOG_PATH):
    """
    Return the first output of an ECL script as a DataFrame.

    Parameters
    ----------
    :param script: str
         Path of script to execute.
    :param server: str
        Ip address and port number of HPCC in the form
        XX.XX.XX.XX.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by
    default.
    :param silent: bool
        If False, the program will print out its progress. True by
        default.

    Returns
    -------
    :return: result: DataFrame
        The first output produced by the script.
    """

    boot_logger(silent, debg, log_to_file, logpath)
    logger = logging.getLogger('run_script')
    logger.debug('Starting run_script')

    run_script(
        script, server, port, repo,
        username, password, silent,
        legacy, do_syntaxcheck)

    return None
