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


def run_script_internal(script, hpcc_server, do_syntaxcheck):
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
    logger.debug('Connection Parameters %s:' % hpcc_server)

    if do_syntaxcheck:
        syntaxcheck.syntax_check(script, hpcc_server)

    legacy = hpcc_server['legacy']
    repo = hpcc_server['repo']

    if do_syntaxcheck:
        syntaxcheck.syntax_check(script, hpcc_server)

    repo_flag = " -I {}".format(repo) if repo else ""
    legacy_flag = '-legacy ' if legacy else ''

    command = ("ecl run --server {} --port {} --username {} --password {} {}"
               "thor {} {}").format(hpcc_server['server'], hpcc_server['port'],
                                    hpcc_server['username'], hpcc_server['password'],
                                    legacy_flag, script, repo_flag)

    logger.info('Script running')
    hpycc.utils.datarequests.run_command(command)

    logger.info('Script completed')
    logger.debug('Check run_command log for any issues')
    return None
