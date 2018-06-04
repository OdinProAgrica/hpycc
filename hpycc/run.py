"""
The module contains functions to run a script with no data returned.
This can be used to run a script, saving a logical file. This can
then be accessing with get_file(). This method takes advantage of
multi-threading, something which script outputs cannot do.
"""

# TODO old_tests
# TODO numpy docstrings


def run_script(connection, script, syntax_check=True):
    """
    Run an ECL script but do not return the response.

    Parameters
    ----------
    :param connection: `Connection`
        HPCC Connection instance, see also `Connection`.
    :param script: str
         Path of script to execute.
    :param syntax_check: bool, optional
        If the script be syntax checked before execution. True by
        default.

    Returns
    -------
    :return: None
    """

    connection.run_ecl_script(script, syntax_check)
