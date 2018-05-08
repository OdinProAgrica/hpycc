import os


# TODO logging


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

    script_loc = 'tempScript.ecl'
    with open(script_loc, 'w') as f:
        f.writelines(script)

    try:
        connection.run_ecl_script(script_loc, True)
    except Exception as e:
        raise e
    finally:
        os.remove(script_loc)
