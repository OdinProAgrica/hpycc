"""
Internal call for running an ECL script. Hands off to a call to the ecl.exe
via datarequests module in utils. Does retrieve 10 rows of data but this is
dropped.
"""
import re
import logging
import hpycc.utils.datarequests
import hpycc.utils.parsers
from hpycc.utils import syntaxcheck


def run_script_internal(script, server, port, repo, username, password, legacy, do_syntaxcheck):
    """
    Runs an ECL script, waits for completion and then returns None. No
    data is downloaded or returned.

    :param script: str
        Path of script to execute.
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param port: str, optional
        Port number ECL Watch is running on.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit.
    :param password: str, optional
        Password to execute the ECL workunit.
    :param legacy: bool
        Should ECL scripts be run with -legacy flag?
    :param do_syntaxcheck: bool
        Should the script be syntax checked before running?

    :return: None
        No return as just runs a script
    """
    logger = logging.getLogger('runscripts.run_script_internal')
    logger.info('Running %s from %s:XXXXXXX@%s : %s using repo %s. Legacy is %s and syntaxcheck is %s'
                % (script, username, server, port, repo, legacy, do_syntaxcheck))

    if do_syntaxcheck:
        syntaxcheck.syntax_check(script, repo, legacy)

    repo_flag = " -I {}".format(repo) if repo else ""
    legacy_flag = '-legacy ' if legacy else ''

    command = ("ecl run --limit=10 --server {} --port {} --username {} --password {} {}"
               "thor {} {}").format(server, port, username, password, legacy_flag, script, repo_flag)

    logger.info('Running ECL script')
    hpycc.utils.datarequests.run_command(command)

    logger.debug('Script completed, check run_command log for any issues')

    return None
