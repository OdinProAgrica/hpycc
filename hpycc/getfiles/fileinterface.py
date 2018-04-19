import json
import sys
import traceback
import urllib.request
from time import sleep
from urllib.error import HTTPError

from hpycc.getfiles.getfiles import GET_FILE_URL


def make_url_request(hpcc_addr, fileName, last, split):

    request = hpcc_addr + GET_FILE_URL % (fileName, last, split)
    response = _run_url_request(request)
    try:
        response = response['WUResultResponse']
    except KeyError:
        print('Unable to parse response to url request:\n%s' % response)
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
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            attempts += 1
            print('Error encountered in URL request: %s\n\n retry %s of %s' % (e, attempts, max_attempts))
            sleep(5)

    raise HTTPError('Unable to get response from HPCC for request: %s' % request)


def parse_json_output(results, columnNames, CSVlogicalFile):
    """

    :param results:
    :param columnNames:
    :param out_info:
    :param CSVlogicalFile:
    :return:
    """
    out_info = {col: [] for col in columnNames}

    for i, resIN in enumerate(results):
        if CSVlogicalFile:
            res = resIN['line']
            if res is None:
                print('Line', str(i), 'is blank')
                print(resIN)
                continue
            res = res.split(',')
            for j, col in enumerate(columnNames):
                out_info[col].append(res[j])
        else:
            cols = resIN.keys()
            for col in cols:
                out_info[col].append(resIN[col])

    return out_info
