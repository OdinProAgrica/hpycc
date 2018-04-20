import hpycc.getscripts.scriptinterface
from hpycc.getscripts import getscripts
import os
from warnings import warn
import os
from warnings import warn

from hpycc.getscripts import getscripts


def syntax_check(script, repo=None, silent=False):
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
        Password to execute the ECL workunit. " " by default.
    :param silent: bool
        If False, the program will print out its progress. True by
        default.

    Returns
    -------
    :return: parsed: list of tuples
        List of processed tuples in the form
        [(output_name, output_xml)].
    """

    repo_flag = "-I {}".format(repo) if repo else ""
        
    if not os.path.isfile(script):
        raise FileNotFoundError('Script %s not found' % script)

    command = "eclcc -syntax -legacy {} {}".format(repo_flag, script)
    
    result = hpycc.getscripts.scriptinterface.run_command(command, silent=True, return_error=True)
    err = result['stderr']

    if err and ': error' in err.lower():
        raise EnvironmentError('Script %s does not compile! Errors: \n %s' % (script, err))
    elif err and ': warning' in err.lower() and not silent:
        warn('Script %s raises the following warnings: \n %s' % (script, err))
    elif err and ': warning' not in err.lower():
        raise EnvironmentError('Script %s contains unhandled feedback: \n %s' % (script, err))
    elif not silent:
        print("Script %s passes syntax check" % script)
