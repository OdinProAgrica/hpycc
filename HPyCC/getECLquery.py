import subprocess
import xml.etree.ElementTree as ET
import pandas as pd
import re
from hpycc import syntaxCheck


POOL_SIZE = 15
GET_FILE_URL = """WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""
USELESS_COLS = ['updateddatetime', '__fileposition__', 'createddatetime']


def run_command(cmd, silent=False, return_error=False):
    """
    Return stdout and optionally print stderr from shell command.
    
    Parameters
    ----------
    cmd: str
        Command to run.
    silent: bool, optional
        If False, the program will print out the stderr. True by
        default.
    Returns
    -------
    stdout: str
        Stdout of the command.
    """
    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        stdin=subprocess.PIPE, shell=True)
                                    
    stderr = result.stderr.decode('utf-8')
    stdout = result.stdout.decode("utf-8")

    windows_path_error = ("'ecl' is not recognized as an internal or external "
                          "command,\r\noperable program or batch file.\r\n")

    if stderr in [windows_path_error]:
        error_string = "{} Have you added client tools to your path?".format(
            stderr)
        raise OSError(error_string)

    elif stderr and not silent:
        errors = stderr.split('\r\r\n')
        print('The following errors were generated:'.format(cmd))
        print("\n".join(errors))

    if return_error:
        return stdout, stderr
    else:
        return stdout


def parse_XML(xml, silent=False):
    """
    Return a DataFrame from a nested XML.

    Parameters
    ----------
    xml: str
        xml to be parsed.
    silent: bool
        If False, the program will print out its progress. True by
        default.

    Returns
    -------
    df: DataFrame
        Parsed xml.
    """

    if not silent:
        print("Parsing Results from XML")
    vls = []
    
    for line in re.findall("<Row>(?P<content>.+?)</Row>", xml):
        with_start = '<Row>' + line + '</Row>'
    
        lvls = []
        newvls = []
        etree = ET.fromstring(with_start)
        for child in etree:
            if child.tag not in lvls:
                lvls.append(child.tag)
            newvls.append(child.text)
        vls.append(newvls)
    
    df = pd.DataFrame(vls, columns=lvls)
    for col in df.columns:
        try:
            nums = pd.to_numeric(df[col])
            df[col] = nums
        except ValueError:
            pass

    return df


def get_parsed_outputs(script, server, port="8010", repo=None,
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

    syntaxCheck.syntax_check(script, repo=repo)

    if repo:
        repo_flag = " -I {}".format(repo)
    else:
        repo_flag = ""

    command = ("ecl run --server {} --port {} --username {} --password {} -legacy "
               "thor {} {}").format(server, port, username, password, script,
                                    repo_flag)

    if not silent:
        print("running ECL script")
        print("command: {}".format(command))

    result = run_command(command, silent)
    stripped = result.strip()
    trimmed = stripped.replace("\r\n", "")

    if not silent:
        print("Parsing response")

    parsed = re.findall(
        "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>", trimmed)

    return parsed

