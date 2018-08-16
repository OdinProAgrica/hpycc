"""
Function to run an ECL script

This module provides a function, `run_script`, to run an ECL script
using an existing `Connection`. This can be used to run a script,
saving a logical file which can then be accessing
with `get_thor_file()`. This approach allows for multi-threading,
something which functions in `get_output`, `get_outputs`,
`save_output` and `save_outputs` cannot do.

Functions
---------
- `run_script` -- Run an ECL script.

"""
__all__ = ["run_script"]


def run_script(connection, script, syntax_check=True, delete_workunit=True,
               stored=None):
    """
    Run an ECL script.

    This function runs an ECL script using a `Connection` object. It
    does not return the result.

    Parameters
    ----------
    connection: hpycc.Connection
        HPCC Connection instance, see also `Connection`.
    script: str
         Path of script to execute.
    syntax_check: bool, optional
        Should the script be syntax checked before execution? True by
        default.
    delete_workunit: bool, optional
        Delete workunit once completed. True by default.
    stored : dict or None, optional
        Key value pairs to replace stored variables within the
        script. Values should be str, int or bool. None by default.

    Returns
    -------
    True

    Raises
    ------
    SyntaxError:
        If script fails syntax check.

    """
    connection.run_ecl_script(script, syntax_check, delete_workunit, stored)
    return True
