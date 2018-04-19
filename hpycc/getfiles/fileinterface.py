import json
import sys
import traceback
import urllib.request
from time import sleep
from urllib.error import HTTPError


def url_request(request):
    """make a request for an HPCC logical file.

    Parameters
    ----------
    request: str
        The request string

    Returns
    -------
    outJSON: str
        JSON of the response
    """

    #print(request)
    attempts = 0
    while attempts < 3:
        try:
            with urllib.request.urlopen(request) as response:
                resp = response.read().decode('utf-8')
            outJSON = json.loads(resp)
            #print(str(outJSON))
            return outJSON
        except HTTPError as e:
            attempts += 1
            print(e)
            sleep(5)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            print(str(traceback.print_exc(file=sys.stdout)))
            attempts += 1
        print('THE FOLLOWING CHUNK FAILED: ', request)
        raise


def _parse_json_output(results, columnNames, outInfo, CSVlogicalFile):
    """

    :param results:
    :param columnNames:
    :param outInfo:
    :param CSVlogicalFile:
    :return:
    """
    for i, resIN in enumerate(results):
        if CSVlogicalFile:
            res = resIN['line']
            if res is None:
                print('Line', str(i), 'is blank')
                print(resIN)
                continue
            res = res.split(',')
            for j, col in enumerate(columnNames):
                outInfo[col].append(res[j])
        else:
            cols = resIN.keys()
            for col in cols:
                outInfo[col].append(resIN[col])

    return outInfo