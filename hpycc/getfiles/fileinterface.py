import json
import urllib.request
from time import sleep
from urllib.error import HTTPError
import re

GET_FILE_URL = """/WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""


def make_url_request(server, port, username, password, logical_file, current_row, chunk, silent):

    server = re.sub(r'(?i)http://', '', server)

    request = 'http://' + server + ':' + port + GET_FILE_URL % (logical_file, current_row, chunk)
    response = _run_url_request(request)

    try:
        response = response['WUResultResponse']
    except (KeyError, UnboundLocalError):
        print('Unable to parse request response:\n%s' % response)
        raise

    return response


def _run_url_request(url_request, silent):
    """

    :param url_request:
    :return:
    """
    # print(url_request)
    attempts = 0
    max_attempts = 3

    while attempts < max_attempts:
        try:
            # print(url_request)
            response = urllib.request.urlopen(url_request)
            return json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            attempts += 1
            print('Error encountered in URL url_request: %s\n\n retry %s of %s' % (e, attempts, max_attempts))
            sleep(5)

    raise OSError('Unable to get response from HPCC for url_request: %s' % url_request)


def parse_json_output(results, column_names, csv_file, silent):
    """

    :param results:
    :param column_names:
    :param csv_file:
    :return:
    """
    out_info = {col: [] for col in column_names}

    for i, result in enumerate(results):
        if csv_file:
            res = result['line']
            if res is None:
                print('Line', str(i), 'is blank')
                print(result)
                continue
            res = res.split(',')
            for j, col in enumerate(column_names):
                out_info[col].append(res[j])
        else:
            for col in column_names:
                out_info[col].append(result[col])

    return out_info
