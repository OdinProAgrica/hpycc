from collections import namedtuple
import requests
import subprocess


# TODO logging
# TODO docstrings
# TODO make front end take connection

def _run_command(cmd):
    # TODO logging
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

    def check_syntax(self, script):
        base_cmd = "eclcc -syntax "
        if self.legacy:
            base_cmd += "-legacy "
        if self.repo:
            base_cmd += "-I {} ".format(self.repo)
        base_cmd += script

        _run_command(base_cmd)

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

        result = _run_command(script)
        return result
