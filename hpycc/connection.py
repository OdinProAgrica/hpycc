from collections import namedtuple
import random
import requests
import subprocess
from tempfile import NamedTemporaryFile
from time import sleep


# TODO logging
# TODO docstrings
# TODO tests


class Connection:
    def __init__(self, server, username, port=8010, repo=None, password=None,
                 legacy=False):
        # TODO make take multiple repos
        self.server = server
        self.username = username
        self.port = port
        self.repo = repo
        self.password = password
        self.legacy = legacy

        self.test_connection()

    def test_connection(self):
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
        base_cmd = "eclcc -syntax "
        if self.legacy:
            base_cmd += "-legacy "
        if self.repo:
            base_cmd += "-I {} ".format(self.repo)
        base_cmd += script

        self._run_command(base_cmd)

    def run_ecl_script(self, script, syntax_check):
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
        attempts = 0
        while attempts < max_attempts:
            try:
                r = requests.get(url, auth=(self.username, self.password))
                return r
            except Exception:
                attempts += 1
                time_to_sleep = random.randint(0, 10)
                sleep(time_to_sleep)

    def get_logical_file_chunk(self, logical_file, start_row, n_rows,
                               max_attempts):
        url = ("http://{}:{}/WsWorkunits/WUResult.json?LogicalName={}"
               "&Cluster=thor&Start={}&Count={}").format(
            self.server, self.port, logical_file, start_row, n_rows)
        r = self.run_url_request(url, max_attempts)
        rj = r.json()
        return rj["WUResultResponse"]

    def run_ecl_string(self, string, syntax_check):
        with NamedTemporaryFile() as tmp:
            byte_string = string.encode()
            tmp.write(byte_string)
            r = self.run_ecl_script(tmp.name, syntax_check)
        return r
