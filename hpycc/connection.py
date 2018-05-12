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

from collections import namedtuple
import random
import requests
import subprocess
from tempfile import NamedTemporaryFile
from time import sleep


# TODO docstrings
# TODO tests
# TODO numpy docstrings


class Connection:
    def __init__(self, username, server="localhost", port=8010, repo=None,
                 password=None, legacy=False):
        """
        Connection to a HPCC instance.

        Takes the connection details of a HPCC instance and allows
        ECL scripts to be executed using pythonic commands. Note
        that this requires both ecl.exe and eclcc.exe to be in the
        path.

        Parameters
        ----------
        username: str
            The username to provide to HPCC.
        server: str, optional
            The ip address of the HPCC instance in the form
            `XX.XX.XX.XX`. Note that neither `http://` nor the port
            number should be included. 'localhost' by default.
        port: int, optional
            The port number ECL Watch is running on on the HPCC
            instance. 8010 by default.
        repo: str, optional
            A path to locations to search for ecl imports. None by
            default.
        password: str
            Password to provide to HPCC alongside the username.
            None by default.
        legacy: bool, optional
            If the legacy flag should be enabled when executing ECL
            commands. False by default.

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
        repo: str or None
            A path to locations to search for ecl imports. May be
            `None`.
        password: str or None
            Password to provide to HPCC alongside the username. May
            be `None`.
        legacy: bool
            If the legacy flag is enabled when executing ECL
            commands.

        """
        # TODO make take multiple repos
        self.server = server
        self.username = username
        self.port = port
        self.repo = repo
        self.password = password
        self.legacy = legacy

        self.test_connection()

    def test_connection(self):
        """
        Assert that the `Connection` can connect to the HPCC
        instance.

        This method attempts to connect to ECL Watch using its
        `server` and `port` attributes. The credentials provided are
        its `username` and `password`.

        Returns
        -------
        None

        Raises
        ------
        Exception:
            If the connection fails, the relevant exception is
            raised.
        """
        with requests.get("http://{}:{}".format(self.server, self.port),
                          auth=(self.username, self.password),
                          timeout=30) as r:
                    r.raise_for_status()

    @staticmethod
    def _run_command(cmd):
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            stdin=subprocess.PIPE, shell=True)

        stderr = result.stderr.decode('utf-8')
        stdout = result.stdout.decode("utf-8")

        if result.returncode:
            raise OSError(stderr)

        Result = namedtuple("Result", ["stdout", "stderr"])
        result_tuple = Result(stdout, stderr)
        return result_tuple

    def check_syntax(self, script):
        """
        Run an ECL syntax check on an ECL script.

        Uses eclcc to run a syntax check on `script`. If the syntax
        check fails, an OSError will be raised. Note that this
        requires that `eclcc.exe` is on the path. Attributes
        `legacy` and `repo` are also used.

        Parameters
        ----------
        script: str
            path to ECL script.

        Returns
        -------
        None

        Raises
        ------
        OSError:
            If the script fails the syntax check.

        """
        base_cmd = "eclcc -syntax "
        if self.legacy:
            base_cmd += "-legacy "
        if self.repo:
            base_cmd += "-I {} ".format(self.repo)
        base_cmd += script

        self._run_command(base_cmd)

    def run_ecl_script(self, script, syntax_check):
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

        Returns
        -------
        result: tuple
            NamedTuple in the form (stdout, stderr).

        Raises
        ------
        OSError:
            If script fails syntax check.

        See Also
        --------
        syntax_check
        run_ecl_string

        """
        base_cmd = ("ecl run --server {} --port {} --username {} "
                    "--password ").format(
            self.server, self.port, self.username)
        if self.password:
            base_cmd += self.password + " "
        else:
            base_cmd += "' ' "
        if self.legacy:
            base_cmd += "-legacy "
        base_cmd += "thor {}".format(script)
        if self.repo:
            base_cmd += "-I {}".format(self.repo)

        if syntax_check:
            self.check_syntax(script)

        result = self._run_command(script)
        return result

    def run_url_request(self, url, max_attempts=3):
        """
        Return the contents of a url.

        Use attributes `username` and `password` to return the
        contents of `url`. Parameter `max_attempts` can be used to
        retry if an exception is raised. Each attempt is delayed by
        up to 10s, so a large number of retries may be slow.

        Parameters
        ----------
        url: str
            URL to query.
        max_attempts: int, optional
            Maximum number of times url should be queried in the
            case of an exception being raised. 3 by default.

        Returns
        -------
        r: requests.models.Response
            Response object from `url`

        """
        attempts = 0
        while attempts < max_attempts:
            try:
                # TODO should this only return if 200?
                r = requests.get(url, auth=(self.username, self.password))
                return r
            except Exception:
                attempts += 1
                time_to_sleep = random.randint(0, 10)
                sleep(time_to_sleep)

    def get_logical_file_chunk(self, logical_file, start_row, n_rows,
                               max_attempts):
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
            First row to return.
        n_rows: int
            Number of rows to return.
        max_attempts: int
            Maximum number of times url should be queried in the
            case of an exception being raised.

        Returns
        -------
        result_response: dict
            Rows of logical file in dict form.

        """
        url = ("http://{}:{}/WsWorkunits/WUResult.json?LogicalName={}"
               "&Cluster=thor&Start={}&Count={}").format(
            self.server, self.port, logical_file, start_row, n_rows)
        r = self.run_url_request(url, max_attempts)
        rj = r.json()
        result_response = rj["WUResultResponse"]
        return result_response

    def run_ecl_string(self, string, syntax_check):
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

        Returns
        -------
        result: tuple
            NamedTuple in the form (stdout, stderr).

        Raises
        OSError:
            If script fails syntax check.

        See Also
        --------
        syntax_check
        run_ecl_script

        See Also
        --------
        """
        with NamedTemporaryFile() as tmp:
            byte_string = string.encode()
            tmp.write(byte_string)
            r = self.run_ecl_script(tmp.name, syntax_check)
        return r
