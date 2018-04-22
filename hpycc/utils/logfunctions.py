"""
module contains the function to boot the logger, debug mode and
logging to file are all handled by arguments passed from the
front-end functions in get.
"""
import logging
import sys

logger = logging.getLogger()


def boot_logger(silent, debg, log_to_file, logpath):
    """
    Iinitate logging for the package at the required verbosity.

    :param silent: bool
        Sets log level to Warnings, errors and critical only
    :param debg: bool
        Sets log level to debug, overrides silent
    :param log_to_file: bool
        Sets logging to also dump to file
    :param logpath:
        Path for if logging to file

    :return: None
        loggers are global
    """
    global logger

    if debg:
        logger.setLevel(logging.DEBUG)
    elif silent:
        logger.setLevel(logging.WARN)
    else:
        logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if log_to_file:
        fh = logging.FileHandler(logpath)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return None
