import json
import requests
from time import sleep
import re
import logging

GET_FILE_URL = """/WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""

def make_url_request(server, port, username, password, logical_file, current_row, chunk, silent):

    logger = logging.getLogger('make_url_request')
    logger.info('Getting Chunk %s from %s' % (chunk, server))

    server = re.sub(r'(?i)http://', '', server)
    request = 'http://' + server + ':' + port + GET_FILE_URL % (logical_file, current_row, chunk)
    response = _run_url_request(request, username, password, silent)

    try:
        response = response['WUResultResponse']
    except (KeyError, UnboundLocalError):
        print('Unable to parse request response:\n%s' % response)
        raise

    return response


def _run_url_request(url_request, username, password, silent):
    """

    :param url_request:
    :return:
    """

    logger = logging.getLogger('_run_url_request')
    logger.info('Running url_request: %s' % url_request)

    attempts = 0
    max_attempts = 3
    password = '' if password == '" "' else password

    while attempts < max_attempts:
        try:
            response = requests.get(url_request, auth=(username, password))
            response = response.text
            logger.debug('Response received (not JSON converted yet): %s' % response)
            out_json = json.loads(response)
            logger.debug('Converted to JSON successfully')
            return out_json

        except Exception as e:
            attempts += 1
            logger.warning('Error encountered in URL url_request: %s\n\n retry %s of %s' % (e, attempts, max_attempts))
            sleep(5)

    raise OSError('Unable to get response from HPCC for url_request: %s' % url_request)


def parse_json_output(results, column_names, csv_file, silent):
    """

    :param results:
    :param column_names:
    :param csv_file:
    :return:
    """

    logger = logging.getLogger('parse_json_output')
    logger.info('Parsing JSON response, converting to dict')
    logger.debug('See _run_url_request log for JSON. Column_names: %s, csv_file: %s' % (column_names, csv_file))

    out_info = {col: [] for col in column_names}

    for i, result in enumerate(results):
        logger.debug('Parsing result %s' % i)
        if csv_file:
            res = result['line']
            if res is None:
                logger.warning('Line', str(i), 'is blank! Row: %s' % result)
                continue
            res = res.split(',')
            for j, col in enumerate(column_names):
                out_info[col].append(res[j])
        else:
            for col in column_names:
                out_info[col].append(result[col])

    sample_data = {col: val[0:5] for (col, val) in out_info.items()}
    logger.debug('Returning (5 row sample): %s' % sample_data)

    return out_info
