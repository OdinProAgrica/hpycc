import re
import subprocess
from xml.etree import ElementTree as ET
import pandas as pd


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

    stdout = stdout.strip()
    stdout = stdout.replace("\r\n", "")

    return {'stdout': stdout, 'stderr': stderr}


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

    # TODO: If you hand it an xml with two datasets in it it will concatenate them!!!! Not what you want!
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
