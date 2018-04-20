import json
import urllib.request
from time import sleep
from urllib.error import HTTPError

GET_FILE_URL = """/WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""


def make_url_request(hpcc_addr, port, file_name, current_row, chunk):

    request = hpcc_addr + ':' + port + GET_FILE_URL % (file_name, current_row, chunk)

    response = _run_url_request(request)

    try:
        response = response['WUResultResponse']
    except (KeyError, UnboundLocalError):
        print('Unable to parse request response:\n%s' % response)
        raise

    return response


def _run_url_request(request):
    """

    :param request:
    :return:
    """
    #print(request)
    attempts = 0
    max_attempts = 3

    while attempts < max_attempts:
        try:
            # print(request)
            response = urllib.request.urlopen(request)
            return json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            attempts += 1
            print('Error encountered in URL request: %s\n\n retry %s of %s' % (e, attempts, max_attempts))
            sleep(5)

    raise HTTPError('Unable to get response from HPCC for request: %s' % request)


def parse_json_output(results, column_names, csv_file):
    """

    :param results:
    :param column_names:
    :param out_info:
    :param csv_file:
    :return:
    """
    out_info = {col: [] for col in column_names}

    for result in results:
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
