"""
Internal call for obtaining the results of an ECL script. Hands off to a call to the ecl.exe
via datarequests module in utils. Results are parsed to dataframes using the
relevent parser from utils, in this case XML.
"""
import re
import logging
import hpycc.utils.HPCCconnector
import hpycc.utils.parsers
import hpycc.utils.syntaxcheck as syntaxcheck

GET_FILE_URL = """WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""


def get_script_internal(script, hpcc_connection, do_syntaxcheck):
    """
    Return the xml portion of the response from HPCC. Can then be parsed by other functions in this class

    :param script: str
        Path of script to execute.
    :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
    :param port: str
        Port number ECL Watch is running on.
    :param repo: str
        Path to the root of local ECL repository if applicable.
    :param username: str
        Username to execute the ECL workunit.
    :param password: str
        Password to execute the ECL workunit.
    :param legacy: bool
        Should ECL scripts be run with -legacy flag?
    :param do_syntaxcheck: bool
        Should the script be syntax checked before running?

    :return: parsed: list of tuples
        List of processed tuples in the form [(output_name, output_xml)].
    """

    logger = logging.getLogger('getscripts.get_script_internal')
    logger.debug('Connection Parameters: %s' % hpcc_connection)

    if do_syntaxcheck:
        syntaxcheck.syntax_check(script, hpcc_connection)

    logger.info('Script Running')
    result = hpcc_connection.run_command(script, 'ecl')
    result = result['stdout']

    logger.info("Script complete, Parsing response")
    results = re.findall("<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>", result)
    results = [(name, hpycc.utils.parsers.parse_xml(xml)) for name, xml in results]

    logger.debug('Returning: %s' % results)
    logger.info('Done')
    return results
