import json
import logging
import re
import subprocess
from time import sleep
import requests

GET_FILE_URL = """/WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""


def make_url_request(server, port, username, password, logical_file, current_row, chunk, silent):

    logger = logging.getLogger('make_url_request')
    logger.info('Getting Chunk %s from %s' % (chunk, server))

    server = re.sub(r'(?i)http://', '', server)
    request = 'http://' + server + ':' + port + GET_FILE_URL % (logical_file, current_row, chunk)
    response = _run_url_request(request, username, password, silent)

    try:
        response = response['WUResultResponse']
    except (KeyError, UnboundLocalError):
        print('Unable to parse request response:\n%s' % response)
        raise

    return response


def _run_url_request(url_request, username, password, silent):
    """

    :param url_request:
    :return:
    """

    logger = logging.getLogger('_run_url_request')
    logger.info('Running url_request: %s' % url_request)

    attempts = 0
    max_attempts = 3
    password = '' if password == '" "' else password

    while attempts < max_attempts:
        try:
            response = requests.get(url_request, auth=(username, password))
            response = response.text
            logger.debug('Response received (not JSON converted yet): %s' % response)
            out_json = json.loads(response)
            logger.debug('Converted to JSON successfully')
            return out_json

        except Exception as e:
            attempts += 1
            logger.warning('Error encountered in URL url_request: %s\n\n retry %s of %s' % (e, attempts, max_attempts))
            sleep(5)

    raise OSError('Unable to get response from HPCC for url_request: %s' % url_request)


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