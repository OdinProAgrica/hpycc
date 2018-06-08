def delete_logical_file(connection, logical_file):
    """
    Delete a logical file.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param logical_file: str
        Logical file to be downloaded.

    Returns
    -------
    :return: None
    """
    script = "IMPORT std; STD.File.DeleteLogicalFile('{}');".format(
        logical_file)

    connection.run_ecl_string(script, True)

