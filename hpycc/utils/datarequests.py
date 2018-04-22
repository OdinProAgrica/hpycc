import json
import logging
import re
import subprocess
from time import sleep
import requests

GET_FILE_URL = """/WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""


def make_url_request(server, port, username, password, logical_file, current_row, chunk):
    """
    Construct a url request for data and parse the response. Request is actually
    run by _run_url_request()

    :param logical_file: str
        Logical file to be downloaded
    :param server: str
        Ip address of HPCC in the form XX.XX.XX.XX.
    :param port: str
        Port number ECL Watch is running on.
    :param username: str
        Username to execute the ECL workunit.
    :param password: str
        Password to execute the ECL workunit.
    :param current_row: int
        Starting row for chunk
    :param chunk: int
        Size of chunk

    :return: dict
        workunit result as result['WUResultResponse']
    """
    logger = logging.getLogger('make_url_request')
    logger.info('Getting Chunk %s from %s' % (chunk, server))

    server = re.sub(r'(?i)http://', '', server)
    request = 'http://' + server + ':' + port + GET_FILE_URL % (logical_file, current_row, chunk)
    response = _run_url_request(request, username, password)

    try:
        response = response['WUResultResponse']
    except (KeyError, UnboundLocalError):
        print('Unable to parse request response:\n%s' % response)
        raise

    return response


def _run_url_request(url_request, username, password):
    """
    Run a url request for data.

    :param url_request: str
        url for requesting data
    :param username: str
        Username to execute the ECL workunit.
    :param password: str
        Password to execute the ECL workunit.
    :return: dict
        Response, read into dict using json.loads
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


def run_command(cmd):
    """
    Return stdout and stderr from shell command.

    :param cmd: str
        Command to run.

    :return: dict
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
