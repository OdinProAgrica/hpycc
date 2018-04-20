import re
import hpycc.getscripts.scriptinterface as interface
from hpycc.utils import syntaxcheck

POOL_SIZE = 15
GET_FILE_URL = """WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""
USELESS_COLS = ['updateddatetime', '__fileposition__', 'createddatetime']


def get_script(script, server, port, repo, username, password, silent, legacy, do_syntaxcheck):
    """
    Return the xml portion of the response from HPCC. Can then be parsed by other functions in this class

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

    if do_syntaxcheck:
        syntaxcheck.syntax_check(script, repo, silent, legacy)

    repo_flag = " -I {}".format(repo) if repo else ""
    legacy_flag = '-legacy ' if legacy else ''

    command = ("ecl run --server {} --port {} --username {} --password {} {}"
               "thor {} {}").format(server, port, username, password, legacy_flag, script, repo_flag)

    if not silent:
        print("running ECL script")
        print("command: {}".format(command))

    result = interface.run_command(command, silent)
    result = result['stdout']

    if not silent:
        print("Parsing response")

    results = re.findall("<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>", result)
    results = [(name, interface.parse_xml(xml)) for name, xml in results]

    return results
