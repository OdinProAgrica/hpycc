"""
Function to run an ECL script

This module provides a function, `run_script`, to run an ECL script
using an existing `Connection`. This can be used to run a script,
saving a logical file which can then be accessing
with get_file(). This method takes advantage using `hpycc.get`. This
approach allows for multi-threading, something which script outputs
cannot do.

Functions
---------
- `run_script` -- Run an ECL script.

"""
__all__ = ["run_script"]


def run_script(connection, script, syntax_check=True):
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

    Returns
    -------
    None

    Raises
    ------
    subprocess.CalledProcessError:
        If script fails syntax check.

    See Also
    --------
    Connection.run_ecl_string

    """
    connection.run_ecl_script(script, syntax_check)
