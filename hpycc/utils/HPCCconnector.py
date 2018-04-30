import json
import re
import subprocess
from time import sleep
import logging
import requests

GET_FILE_URL = """/WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""


class HPCCconnector(object):
    '''
    Class saves the connection details for an HPCC instance and then fires off a test request to ensure it works.
    Also contains functions to fire off requests via url or command line.
    '''

    def __init__(self, server, port, repo, username, password, legacy):
        self.server = server
        self.port = port
        self.repo = repo
        self.username = username
        self.password = password
        self.legacy = legacy
        self.legacy_flag = '-legacy ' if legacy else ''
        self.repo_flag = " " if repo is None else "-I {}".format(repo)

        self.test_connection()

    def get_string(self):
        a_dict = {
                'server': self.server,
                'port': self.port,
                'repo': self.repo,
                'username': self.username,
                'password': self.password,
                'legacy': self.legacy,
                'legacy_flag': self.legacy_flag,
                'repo_flag': self.repo_flag
                }

        return str(a_dict)

    def test_connection(self):
        # TODO: better test - at present if thor is down it can just hang.
        logger = logging.getLogger('HPCC connection test')
        try:
            _ = requests.get('http://%s:%s' % (self.server, self.port)).text
        except ConnectionError:
            logger.error('Unable to connect to HPCC instance: %s' % self.get_string())
            raise

    def make_url_request(self, logical_file, current_row, chunk):
        logger = logging.getLogger('make_url_request')
        logger.debug('Getting Chunk %s from %s' % (chunk, self.server))

        self.server = re.sub(r'(?i)http://', '', self.server)
        request = 'http://' + self.server + ':' + self.port + GET_FILE_URL % (logical_file, current_row, chunk)
        response = self.run_url_request(request)

        try:
            response = response['WUResultResponse']
        except (KeyError, UnboundLocalError):
            print('Unable to parse request response:\n%s' % response)
            raise

        return response

    def run_url_request(self, url_request):
        """
        Run a url request for data.

        :param url_request: str
            url for requesting data
        :return: dict
            Response, read into dict using json.loads
        """

        logger = logging.getLogger('_run_url_request')
        logger.debug('Running url_request: %s' % url_request)

        attempts = 0
        max_attempts = 3
        url_password = '' if self.password == '" "' else self.password

        while attempts < max_attempts:
            try:
                response = requests.get(url_request, auth=(self.username, url_password))
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

    def run_command(self, script, typ):
        """
        Return stdout and stderr from shell command.

        :param script: str
            script to run.
        :param typ: str
            Which ECL executionable to call? Currently supports ECLCC.exe and ECL.exe

        :return: dict
            dict of stdout and stderr
        """
        logger = logging.getLogger('run_command')
        logger.debug('Executing script: %s' % script)

        if typ.lower() == 'eclcc':
            cmd = "eclcc -syntax {}{} {}".format(self.legacy_flag, self.repo_flag, script)

        elif typ.lower() == 'ecl':
            cmd = ("ecl run --server {} --port {} --username {} --password {} {}"
                   "thor {} {}").format(self.server, self.port,
                                        self.username, self.password,
                                        self.legacy_flag, script,
                                        self.repo_flag)
        else:
            raise TypeError('Not a valid ECL exceptionable')

        logger.debug('Running ECL command: %s' % cmd)
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
            error_string = "{} Have you added client tools to your path?".format(stderr)
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


