import re

from hpycc.getscripts import syntaxcheck
import hpycc.getscripts.scriptinterface as interface

POOL_SIZE = 15
GET_FILE_URL = """WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""
USELESS_COLS = ['updateddatetime', '__fileposition__', 'createddatetime']


def get_script(script, server, port="8010", repo=None,
               username="hpycc_get_output", password='" "',
               silent=False):
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

    syntaxcheck.syntax_check(script, repo=repo)

    repo_flag = " -I {}".format(repo) if repo else ""

    command = ("ecl run --server {} --port {} --username {} --password {} -legacy "
               "thor {} {}").format(server, port, username, password, script, repo_flag)

    if not silent:
        print("running ECL script")
        print("command: {}".format(command))

    result = interface.run_command(command, silent)
    result = result['stdout']

    if not silent:
        print("Parsing response")

    results = re.findall("<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>", result)
    results = [(name, interface.parse_XML(xml)) for name, xml in results]

    return results

