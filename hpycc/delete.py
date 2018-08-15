# noinspection PyShadowingNames
def delete_logical_file(connection, logical_file, delete_workunit=True):
    """
    Delete a logical file.

    Parameters
    ----------
    connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    logical_file: str
        Logical file to be downloaded.
    delete_workunit: bool, optional
        Delete workunit once completed. True by default.

    Returns
    -------
    None
    """
    script = "IMPORT std; STD.File.DeleteLogicalFile('{}');".format(
        logical_file)

    connection.run_ecl_string(script, True, delete_workunit=delete_workunit,
                              stored={})


def delete_workunit(connection, wuid, max_attempts=3, max_sleep=5):
    """
    Delete a workunit

    Parameters
    ----------
    connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    wuid: string
        Workunit ID
    max_attempts: int, optional
        Maximum number of times url should be queried in the
        case of an exception being raised. 3 by default.
    max_sleep: int, optional
        Maximum time, in seconds, to sleep between attempts.
        The true sleep time is a random int between 0 and
        `max_sleep`. 5 by default.

    Returns
    -------
    True:
        If the workunit is deleted successfully.

    Raises
    ------
    ValueError:
        If the workunit could not be deleted.

    """

    url = (
        "http://{}:{}/WsWorkunits/WUDelete.json?Wuids={}&"
        "BlockTillFinishTimer=True").format(
        connection.server, connection.port, wuid)

    r = connection.run_url_request(url, max_attempts, max_sleep)
    rj = r.json()
    if rj == {"WUDeleteResponse": {}}:
        return True
    else:
        raise ValueError(rj)
