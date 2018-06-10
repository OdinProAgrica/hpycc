import os
import tarfile
import time
from io import BytesIO

import docker
from docker.errors import NotFound

KILL_EXCEPTIONS = (ValueError, FileNotFoundError)


def start_hpcc_container():
    client = docker.from_env()
    try:
        client.containers.get("hpycc_tests").stop()
    except NotFound:
        pass
    try:
        client.api.remove_container("hpycc_tests")
    except NotFound:
        pass
    try:
        b = client.containers.run("hpccsystems/platform-ce", "/bin/bash",
                                  detach=True, auto_remove=False, tty=True,
                                  ports={"8010/tcp": 8010, "8015/tcp": 8015},
                                  name="hpycc_tests")
        return b
    except KILL_EXCEPTIONS as e:
        client.containers.get("hpycc_tests").stop()
        client.api.remove_container("hpycc_tests")
        raise e


def password_hpcc(container):
    try:
        put_file_into_container(container, "test_helpers//environment.xml",
                                "etc/HPCCSystems")
        put_file_into_container(container, "test_helpers//.htpasswd",
                                "etc/HPCCSystems")
    except KILL_EXCEPTIONS as e:
        container.stop()
        client = docker.from_env()
        client.api.remove_container("hpycc_tests")
        raise e


def start_hpcc(container):
    try:
        container.exec_run("etc/init.d/hpcc-init start")
    except KILL_EXCEPTIONS as e:
        container.stop()
        raise e


def create_tar(file):
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
    pw_tarstream = create_tar(file)
    try:
        container.put_archive(data=pw_tarstream, path=destination)
    except KILL_EXCEPTIONS as e:
        container.stop()
        raise e

    return True


def stop_hpcc_container():
    client = docker.from_env()
    client.containers.get("hpycc_tests").stop()
    client.api.remove_container("hpycc_tests")
