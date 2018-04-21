import re
import subprocess
from xml.etree import ElementTree as ET
import pandas as pd
import logging


def run_command(cmd, silent=False):
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
    result: dict
        dict of stdout and stderr
    """
    logger = logging.getLogger('run_command')
    logger.info('Executing syntax check command: %s' % cmd)

    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        stdin=subprocess.PIPE, shell=True)

    stderr = result.stderr.decode('utf-8')
    stdout = result.stdout.decode("utf-8")
    logger.debug('stderr: %s' % stderr)
    logger.debug('stdout: %s' % stdout)

    windows_path_error = ("'ecl' is not recognized as an internal or external "
                          "command,\r\noperable program or batch file.\r\n")

    if stderr in [windows_path_error]:
        error_string = "{} Have you added client tools to your path?".format(
            stderr)
        raise OSError(error_string)

    elif stderr:
        errors = stderr.split('\r\r\n')
        logger.warning('The following errors were generated:'.format(cmd))
        logger.warning("\n".join(errors))

    stdout = stdout.strip()
    stdout = stdout.replace("\r\n", "")
    out_dict = {'stdout': stdout, 'stderr': stderr}
    logger.debug('Returning: %s' % out_dict)

    return out_dict


def parse_xml(xml, silent=False):
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
    logger = logging.getLogger('parse_xml')
    logger.info("Parsing Results from XML")

    if "Dataset name='Result 2'" in xml:
        raise ValueError('XML contains multiple datasets. These would be concatenated so aborting.')

    vls = []
    lvls = []

    for line in re.findall("<Row>(?P<content>.+?)</Row>", xml):
        logger.debug('Parsing line: \n%s' % line)
        with_start = '<Row>' + line + '</Row>'
        newvls = []
        etree = ET.fromstring(with_start)
        for child in etree:
            if child.tag not in lvls:
                logger.debug('New column found: %s' % child.tag)
                lvls.append(child.tag)
            newvls.append(child.text)
        vls.append(newvls)

    df = pd.DataFrame(vls, columns=lvls)

    logger.debug('Converting numeric cols')
    for col in df.columns:
        try:
            nums = pd.to_numeric(df[col])
            df[col] = nums
            logger.debug('%s converted to numeric' % col)
        except ValueError:
            logger.debug('%s cannot be converted to numeric' % col)
            pass

    logger.debug('Returning: %s' % df)
    return df
