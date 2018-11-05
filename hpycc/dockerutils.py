"""
Functions to create and control HPCC docker images. Requires Docker to
be installed and running!!!!!!!
"""

# TODO this file should be moved
import os
import tarfile
import time
from io import BytesIO
import pathlib
import docker
from docker.errors import NotFound
import pkg_resources
from time import sleep

# always use slash
# TODO not sure what this does. not sure the setupfiles should live where do too.
ENV_FILEPATH = pkg_resources.resource_filename(__name__, 'dockersetupfiles/environment.xml')
PASS_FILEPATH = pkg_resources.resource_filename(__name__, 'dockersetupfiles/.htpasswd')
PASS_PLAINTEXT_FILEPATH = pkg_resources.resource_filename(__name__, 'dockersetupfiles/plaintext_passwords.txt')
KILL_EXCEPTIONS = (ValueError, FileNotFoundError)


# TODO this starts any container
# TODO there should also be a pull function
def start_hpcc_container(image_to_pull='hpccsystems/platform-ce', containter_name="hpycc_tests"):
    """
    Bootup an HPCC container using docker. Will attempt to kill any other containers
    with the same name. Requires Docker to be installed and running!!!!!!!


    Parameters
    ----------
    image_to_pull: str
        Optional, the name of the docker image to pull. Defaults to hpccsystems/platform-ce
    containter_name: str
        Optional, what to call your new container? Defaults to hpycc_tests.


    Returns
    -------
    A detached docker container.

    """
    client = docker.from_env()
    try:
        client.containers.get(containter_name).stop()
    except NotFound:
        pass
    try:
        client.api.remove_container(containter_name)
    except NotFound:
        pass
    try:
        b = client.containers.run(image_to_pull, "/bin/bash",
                                  detach=True, auto_remove=False, tty=True,
                                  ports={"8010/tcp": 8010, "8015/tcp": 8015},
                                  name=containter_name)
        return b
    except KILL_EXCEPTIONS as e:
        client.containers.get(containter_name).stop()
        client.api.remove_container(containter_name)
        raise e


def password_hpcc(container, env_file=ENV_FILEPATH, pass_file=PASS_FILEPATH ):
    """
    Add password and environment setup files to HPCC. Uses inbuilt by default.

    Parameters
    ----------
    container:
        Docker container to add password to
    env_file:
        Location of the environments file for setup
    pass_file:
        Location of the passwords file to add

    Returns
    -------
    None

    """
    # TODO nope. not having this
    if pathlib.Path.cwd().name == "tests":
        p = pathlib.Path() / 'test_helpers'
    else:
        p = pathlib.Path() / '..' / 'test_helpers'
    try:
        put_file_into_container(container, p / env_file,
                                "etc/HPCCSystems")
        put_file_into_container(container, p / pass_file,
                                "etc/HPCCSystems")
    except KILL_EXCEPTIONS as e:
        container.stop()
        client = docker.from_env()
        client.api.remove_container("hpycc_tests")
        raise e


def start_hpcc(container):
    """
    Start HPCC server on the container (not on by default. Strangely.).

    Parameters
    ----------
    container:
        Docker container to add password to

    Returns
    -------
    None
    """
    try:
        container.exec_run("etc/init.d/hpcc-init start")
    except KILL_EXCEPTIONS as e:
        container.stop()
        raise e
    sleep(20)  # Give it some thinking time.


def create_tar(file):
    """
    Tar a file, used to prep stuff for insertion into an HPCC container.

    Parameters
    ----------
    file:
        File to be tar-ed

    Returns
    -------
    tarstream of the given file
    """
    with open(file, "r") as f:
        file_data = f.read().encode("utf-8")

    pw_tarstream = BytesIO()
    pw_tar = tarfile.TarFile(fileobj=pw_tarstream, mode="w")
    tarinfo = tarfile.TarInfo(name=os.path.basename(file))
    tarinfo.size = len(file_data)
    tarinfo.mtime = time.time()

    pw_tar.addfile(tarinfo, BytesIO(file_data))
    pw_tar.close()
    pw_tarstream.seek(0)

    return pw_tarstream


def put_file_into_container(container, file, destination):
    """
    Add a file to your HPCC docker instance.

    Parameters
    ----------
    container:
        Docker container to add file to
    file:
        File to add
    destination:
        Where to add it

    Returns
    -------
    Bool
        True if completed successfully

    """
    pw_tarstream = create_tar(file)
    try:
        container.put_archive(data=pw_tarstream, path=destination)
    except KILL_EXCEPTIONS as e:
        container.stop()
        raise e

    return True


def stop_hpcc_container(container_name="hpycc_tests"):
    """
    Kill an HPCC docker container.

    Parameters
    ----------
    container_name: str
        Optional, name of container to kill. Defaults to hpycc_tests

    Returns
    -------
    None
    """

    client = docker.from_env()
    client.containers.get(container_name).stop()
    client.api.remove_container(container_name)


def get_default_userpass():
    # TODO cna we just make a password from a dict?
    """
    Return the default user:password that is created by `password_hpcc`.

    Returns
    -------
    str
        Username:password
    """
    p = PASS_PLAINTEXT_FILEPATH
    with open(p, 'r') as f:
        passwrd = f.readlines()

    return passwrd