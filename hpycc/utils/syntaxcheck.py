import os
import logging
import hpycc.utils.datarequests


def syntax_check(script, repo, silent, legacy):
    """
    Return the first output of an ECL script as a DataFrame.

    Parameters
    ----------s
    :param script: str
        Path of script to execute.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param silent: bool
        If False, the program will print out its progress. True by
        default.

    Returns
    -------
    :return: parsed: list of tuples
        List of processed tuples in the form
        [(output_name, output_xml)].
    """

    logger = logging.getLogger('syntaxcheck')
    logger.info('Checking %s using repo %s and legacy %s' % (script, repo, legacy))

    if not os.path.isfile(script):
        raise FileNotFoundError('Script %s not found' % script)

    repo_flag = " " if repo is None else "-I {}".format(repo)
    legacy_flag = '-legacy ' if legacy else ''

    command = "eclcc -syntax {}{} {}".format(legacy_flag, repo_flag, script)

    result = hpycc.utils.datarequests.run_command(command, silent=True)
    err = result['stderr']

    if err and ': error' in err.lower():
        raise EnvironmentError('Script %s does not compile! Errors: \n %s' % (script, err))
    elif err and ': warning' in err.lower() and not silent:
        logger.warning('Script %s raises the following warnings: \n %s' % (script, err))
    elif err and ': warning' not in err.lower():
        raise EnvironmentError('Script %s contains unhandled feedback: \n %s' % (script, err))
    elif not silent:
        logger.info("Script %s passes syntax check" % script)
