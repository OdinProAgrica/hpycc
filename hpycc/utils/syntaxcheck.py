"""
Module contains functions to check ECL scripts prior to runtime, therefore
saving resources on the main server.
"""
import os
import logging


def syntax_check(script, hpcc_connection):
    """
    Use ECLCC to run a syntax check on a script.

    :param script: str
        Path of script to execute.
    :param hpcc_connection: HPCCconnector,
        Connection details for an HPCC instance.

    :return: parsed: list of tuples
        List of processed tuples in the form
        [(output_name, output_xml)].
    """
    logger = logging.getLogger('syntaxcheck')
    logger.debug('Checking script %s against %s' % (script, hpcc_connection.get_string()))

    if not os.path.isfile(script):
        raise FileNotFoundError('Script %s not found' % script)

    result = hpcc_connection.run_command(script, 'eclcc')
    err = result['stderr']

    if err and ': error' in err.lower():
        raise EnvironmentError('Script %s does not compile! Errors: \n %s' % (script, err))
    elif err and ': warning' in err.lower():
        logger.warning('Script %s raises the following warnings: \n %s' % (script, err))
    elif err and ': warning' not in err.lower():
        raise EnvironmentError('Script %s contains unhandled feedback: \n %s' % (script, err))
    else:
        logger.debug("Script %s passes syntax check" % script)
