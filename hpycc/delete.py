def delete_logical_file(connection, logical_file, deleteworkunit=True):
    """
    Delete a logical file.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param logical_file: str
        Logical file to be downloaded.
    :param deleteworkunit: bool
        Whether the workunit created should be deleted. Set to True.

    Returns
    -------
    :return: None
    """
    script = "IMPORT std; STD.File.DeleteLogicalFile('{}');".format(
        logical_file)

    connection.run_ecl_string(script, True, deleteworkunit=deleteworkunit)


def delete_workunit(connection, wuid, max_attempts=1, max_sleep=1):
    """
    Delete a workunit

    Parameters
    ----------
    connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    wuid: string
        The Workunit ID for the
    max_attempts: int
        Maximum number of times url should be queried in the
        case of an exception being raised.
    max_sleep: int
        Maximum time, in seconds, to sleep between attempts.
        The true sleep time is a random int between 0 and
        `max_sleep`.

    Returns
    -------
    :return: result_response - whether the workunit Delete worked or not
    """

    url = (
        "http://{}:{}/WsWorkunits/WUDelete.json?Wuids={}&"
        "BlockTillFinishTimer=True").format(
        connection.server, connection.port, wuid)

    r = connection.run_url_request(url, max_attempts, max_sleep)
    rj = r.json()
    try:
        result_response_2 = rj

    except KeyError:
        raise KeyError('json is : {}'.format(rj))

    return rj