import logging
import sys
logger = logging.getLogger()


def boot_logger(silent, debg, log_to_file, logpath):
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