import tarfile
import time
from io import BytesIO

import docker


def start_hpcc_container():
    client = docker.from_env()
    b = client.containers.run("hpccsystems/platform-ce", "/bin/bash",
                              detach=True, auto_remove=False, tty=True,
                              ports={"8010/tcp": 8010, "8015/tcp": 8015})
    return b

def password_hpcc(container):
    put_file_into_container(container, "environment.xml", "/etc/HPCCSystems")
    put_file_into_container(container, ".htpasswd", "/etc/HPCCSystems")


def start_hpcc(container):
    container.exec_run("etc/init.d/hpcc-init start")


def create_tar(file):
    with open(file, "r") as f:
        file_data = f.read().encode("utf-8")

    pw_tarstream = BytesIO()
    pw_tar = tarfile.TarFile(fileobj=pw_tarstream, mode="w")
    tarinfo = tarfile.TarInfo(name=file)
    tarinfo.size = len(file_data)
    tarinfo.mtime = time.time()

    pw_tar.addfile(tarinfo, BytesIO(file_data))
    pw_tar.close()
    pw_tarstream.seek(0)

    return pw_tarstream


def put_file_into_container(container, file, destination):
    pw_tarstream = create_tar(file)
    container.put_archive(data=pw_tarstream, path=destination)

    return True
