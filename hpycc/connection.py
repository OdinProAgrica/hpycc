"""
Object for connecting to a HPCC instance.

This module provides a `Connection` class to connect to a HPCC
instance. This connection is used as the first input to the majority
of public functions in the `hpycc` package.

Classes
-------
- `Connection` -- HPCC connection class.

"""
__all__ = ["Connection"]

import collections
import os
import random
import requests
from requests.exceptions import HTTPError, RetryError
import subprocess
from tempfile import TemporaryDirectory
from time import sleep
from warnings import warn

from hpycc.utils.parsers import parse_wuid_from_failed_response, \
    parse_wuid_from_xml
from hpycc import delete


def check_ecl_cmd(cmd='ecl'):
    try:
        subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            stdin=subprocess.PIPE, shell=False)
    except FileNotFoundError:
        warn("ecl is not on the system path! You may continue but functionality will be limited"
             "to get and save_thor_file and deleting workunits. All other tasks will likely fail")

class Connection:
    def __init__(self, username, server="localhost", port=8010, repo=None,
                 password="password", legacy=False, test_conn=True):
        """
        Connection to a HPCC instance.

        Takes the connection details of a HPCC instance and allows
        ECL scripts to be executed using pythonic commands. Note
        that this requires both ecl.exe and eclcc.exe to be in the
        path.

        Parameters
        ----------
        username : str
            The username to provide to HPCC.
        server : str, optional
            The ip address of the HPCC instance in the form
            `XX.XX.XX.XX`. Note that neither `http://` nor the port
            number should be included. 'localhost' by default.
        port : int, optional
            The port number ECL Watch is running on on the HPCC
            instance. 8010 by default.
        repo : str, list, None, optional
            A path to a location or locations to search for ecl
            imports. A single path can be a str, multiple paths
            should be in an iterable. None by default.
        password : str
            Password to provide to HPCC alongside the username.
            "password" by default. Note: this cannot be blank.
        legacy : bool, optional
            Should legacy flag should be enabled when executing ECL
            commands. False by default.
        test_conn : bool, optional
            Test connection to the server on initialisation.
            True by default.

        Attributes
        ----------
        username: str
            The username to provide to HPCC.
        server: str
            The ip address of the HPCC instance in the form
            'XX.XX.XX.XX' or 'localhost'. Note that neither
            `http://` nor the port number is included.
        port: int
            The port number ECL Watch is running on on the HPCC
            instance.
        repo: str, list-like or None, optional
            A path to a location or locations to search for ecl
            imports. A single path can be a str, multiple paths
            should be in an iterable. None by default.
        password: str
            Password to provide to HPCC alongside the username.
        legacy: bool
            If the legacy flag is enabled when executing ECL
            commands.

        """
        if not isinstance(username, str) or not username:
            raise AttributeError("username must be a string, not {}".format(
                username))
        if not isinstance(password, str) or not password.strip():
            raise AttributeError("password must be a string, not {}".format(
                password))

        self.server = server
        self.username = username
        self.port = port
        self.repo = repo
        self.password = password
        self.legacy = legacy

        if test_conn:
            self.test_connection()

        check_ecl_cmd()

    def test_connection(self):
        """
        Assert that the `Connection` can connect to the HPCC
        instance.

        This method attempts to connect to ECL Watch using its
        `server` and `port` attributes. The credentials provided are
        its `username` and `password`.

        Returns
        -------
        True

        Raises
        ------
        Exception:
            If the connection fails, the relevant exception is
            raised.
        """
        r = requests.get("http://{}:{}".format(self.server, self.port),
                         auth=(self.username, self.password),
                         timeout=5)
        r.raise_for_status()

        return True

    @staticmethod
    def _run_command(cmd):
        result = subprocess.run(cmd, stderr=subprocess.PIPE,
                                stdout=subprocess.PIPE)

        stderr = result.stderr.decode('utf-8')

        if result.returncode:
            raise subprocess.SubprocessError(stderr)

        stdout = result.stdout.decode("utf-8")

        Result = collections.namedtuple("Result", ["stdout", "stderr"])
        result_tuple = Result(stdout, stderr)
        return result_tuple

    def check_syntax(self, script):
        """
        Run an ECL syntax check on an ECL script.

        Uses eclcc to run a syntax check on `script`. If the syntax
        check fails, ie. an error is present, a SyntaxError
        will be raised.
        Note that this requires that `eclcc.exe` is on the path.
        Attributes `legacy` and `repo` are also used.

        Parameters
        ----------
        script: str
            path to ECL script.

        Returns
        -------
        None

        Raises
        ------
        SyntaxError
            If the script fails the syntax check.

        """
        b = ["eclcc", "-syntax"]
        b += self._legacy_arg
        b += self._repo_arg
        b += [script]
        try:
            self._run_command(b)
        except subprocess.SubprocessError as e:
            raise SyntaxError(e)

    @property
    def _repo_arg(self):
        r = [self.repo] if isinstance(self.repo, str) else self.repo
        try:
            return ["-I={}".format(repo) for repo in r]
        except TypeError:
            return []

    @property
    def _legacy_arg(self):
        if self.legacy:
            return ["-legacy"]
        else:
            return []

    def run_ecl_script(self, script, syntax_check, delete_workunit,
                       stored):
        """
        Run an ECL script and return the stdout and stderr.

        Run the ECL script `script` on the HPCC instance at
        `server`:`port`, using the credentials `username` and
        `password`. If `syntax_check`, run a syntax
        check before execution. Attributes `legacy` and `repo` are
        also used.

        Parameters
        ----------
        script: str
            path to ECL script.
        syntax_check: bool
            If a syntax check should be ran before the script is
            executed.
        delete_workunit: bool
            Delete workunit once completed.
        stored : dict or None
            Key value pairs to replace stored variables within the
            script. Values should be str, int or bool.

        Returns
        -------
        result: namedtuple
            NamedTuple in the form (stdout, stderr).

        Raises
        ------
        subprocess.CalledProcessError:
            If script fails syntax check.

        See Also
        --------
        syntax_check
        run_ecl_string

        """

        base_cmd = ['ecl', 'run', '-v', '--server={}'.format(self.server),
                    '--port={}'.format(self.port),
                    '--username={}'.format(self.username),
                    '--password={}'.format(self.password)]

        base_cmd += self._legacy_arg

        base_cmd += ['thor', script]
        base_cmd += self._repo_arg

        stored = stored or {}
        for key, value in stored.items():
            string = '-X{}={}'.format(key, value)
            base_cmd.append(string)

        if syntax_check:
            self.check_syntax(script)

        try:
            result = self._run_command(base_cmd)
        except subprocess.SubprocessError as e:
            if delete_workunit:
                wuid = parse_wuid_from_failed_response(e.args[0].decode())
                delete.delete_workunit(self, wuid)
            raise e
        else:
            if delete_workunit:
                wuid = parse_wuid_from_xml(result.stdout)
                delete.delete_workunit(self, wuid)
            return result

    def run_url_request(self, url, max_attempts, max_sleep):
        """
        Return the contents of a url.

        Use attributes `username` and `password` to return the
        contents of `url`. Parameter `max_attempts` can be used to
        retry if an exception is raised. Each attempt is delayed by
        up to `max_sleep` seconds, so a large number of retries may
        be slow.

        Parameters
        ----------
        url: str
            URL to query.
        max_attempts: int
            Maximum number of times url should be queried in the
            case of an exception being raised.
        max_sleep: int
            Maximum time, in seconds, to sleep between attempts.
            The true sleep time is a random int between 0 and
            `max_sleep`.

        Returns
        -------
        r: requests.models.Response
            Response object from `url`

        Raises
        ------
        requests.exceptions.RetryError:
            If max_attempts is exceeded.

        """
        attempts = 0
        while attempts < max_attempts:
            try:
                r = requests.get(url, auth=(self.username, self.password))
                r.raise_for_status()
                return r
            except (HTTPError, ValueError) as e:
                attempts += 1
                time_to_sleep = random.randint(0, max_sleep)
                sleep(time_to_sleep)
                if attempts == max_attempts:
                    raise RetryError(e)

    def get_logical_file_chunk(self, logical_file, start_row, n_rows,
                               max_attempts, max_sleep):
        """
        Return a chunk of a logical file from a HPCC instance.

        Using the HPCC instance at `server`:`port` and the
        credentials `username` and `password`, return a chunk of
        `logical_file` which starts at row `start_row` and is
        `n_rows` long.

        Parameters
        ----------
        logical_file: str
            Name of logical file.
        start_row: int
            First row to return where 0 is the first row of the
            dataset.
        n_rows: int
            Number of rows to return.
        max_attempts: int
            Maximum number of times url should be queried in the
            case of an exception being raised.
        max_sleep: int
            Maximum time, in seconds, to sleep between attempts.
            The true sleep time is a random int between 0 and
            `max_sleep`.

        Returns
        -------
        result_response: list of dicts
            Rows of logical file as list of dicts. In the form
            [{"col1": 1, "col2": 2}, {"col1": 1, "col2": 2}, ...].

        """
        url = ("http://{}:{}/WsWorkunits/WUResult.json?LogicalName={}"
               "&Cluster=thor&Start={}&Count={}").format(
            self.server, self.port, logical_file, start_row, n_rows)
        r = self.run_url_request(url, max_attempts, max_sleep)
        rj = r.json()
        try:
            result_response = rj["WUResultResponse"]["Result"]["Row"]
        except KeyError:
            raise KeyError("json is : {}".format(rj))
        return result_response

    def run_ecl_string(self, string, syntax_check, delete_workunit, stored):
        """
        Run an ECL string and return the stdout and stderr.

        Run the ECL string `string` on the HPCC instance at
        `server`:`port`, using the credentials `username` and
        `password`. If `syntax_check`, run a syntax
        check before execution. Attributes `legacy` and `repo` are
        also used.

        Parameters
        ----------
        string: str
            ECL script as a string.
        syntax_check: bool
            If a syntax check should be ran before the script is
            executed.
        delete_workunit: bool
            Delete workunit once completed.
        stored : dict or None
            Key value pairs to replace stored variables within the
            script. Values should be str, int or bool.

        Returns
        -------
        result: namedtuple
            NamedTuple in the form (stdout, stderr).

        Raises
        ------
        SyntaxError:
            If script fails syntax check.

        See Also
        --------
        syntax_check
        run_ecl_script

        """
        with TemporaryDirectory() as d:
            p = os.path.join(d, "ecl_string.ecl")
            with open(p, "w+") as file:
                file.write(string)

            r = self.run_ecl_script(p, syntax_check, delete_workunit, stored)
        return r
