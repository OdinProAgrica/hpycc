from urllib.error import HTTPError
import urllib.request
import json
import sys
from time import sleep
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
import traceback

POOL_SIZE = 15
HPCC_REPO = '..\\..\\..\\HPCC'
GET_FILE_URL = """/WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""
USELESS_COLS = ['updateddatetime', '__fileposition__', 'createddatetime']

# File acquisition scripts
def makeFileRequest(request):
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


def getFile(fileName, hpcc_addr, CSVlogicalFile):
    """Call to retrieve a file.

    Parameters
    ----------
    fileName:
        logical file to be downloaded
    hpcc_addr: str
        address of the HPCC cluster
    CSVlogicalFile: bool
        IS the logical file a CSV?

    Returns
    -------
    result: pd.DataFrame
        a DF of the given file
    """

    print('Adjusting name to HTML')
    fileName = re.sub('[~]', '', fileName)
    fileName = re.sub(r'[:]', '%3A', fileName)
    outInfo = {}

    print('Determining size and column names')
    response = makeFileRequest(hpcc_addr + GET_FILE_URL % (fileName, 0, 2))
    nRecs = response['WUResultResponse']['Total']
    results = response['WUResultResponse']['Result']['Row']

    if CSVlogicalFile:
        columnNames = results[0]['line']
        columnNames = columnNames.split(',')
        last = 1  # start row, miss first line
    else:
        columnNames = results[0].keys()
        last = 0  # start row, use first line

    print('Columns found: ' + str(columnNames))
    print('Row count: ' + str(nRecs))

    for col in columnNames:
        if col in USELESS_COLS:
            continue
        outInfo[col] = []

    if nRecs > 10000:
        splits = list(range(10000, nRecs - 1, 10000))
        if splits[-1] is not (nRecs - 1): splits.append(nRecs)
        print('Large table, downloading in ' + str(len(splits)) + ' chunks ')
    else:
        print('Small table, running all at once')
        splits = [nRecs]

    pool = ThreadPoolExecutor(POOL_SIZE)
    futures = []
    doneTest = [False]

    for split in splits:
        futures.append(pool.submit(GetFileChunk, fileName, CSVlogicalFile, hpcc_addr, last, split, columnNames, outInfo))
        last = split + 1

    while not all(doneTest):
        print('Waiting for chunks to complete')
        sleep(5)
        doneTest = [future.done() for future in futures]
        print("Unfinished threads: " + str(len(doneTest) - sum(doneTest)))


    print("Locating any excepted threads")
    for future in futures:
        if future.exception() is not None:
            print(" THREAD FAILED FOR " + str(future.exception()))
            raise future.exception()

    print('Concat outputs')
    
    return pd.concat([future.result() for futyure in futures])


def GetFileChunk(fileName, CSVlogicalFile, HPCCaddress, last, split, columnNames, outInfo):
    """

    Process to retrieve large files in chunks. Called by the threadpool made in get_file

    Parameters
    ----------
    fileName:
        logical file to be downloaded
    CSVlogicalFile: bool
        IS the logical file a CSV?    
    hpcc_addr: str
        address of the HPCC cluster
    last: int
        the last row that was read by a previous chunk
    split: 
        the row to split on
    columnNames:
        the column names to parse
    outInfo:
        the column names to return
        

    Returns
    -------
    result: pd.DataFrame
        a DF of the given file chunk
    """
    
    print('Getting rows ' + str(last) + ' to ' + str(split))
    response = makeFileRequest(
        HPCCaddress + GET_FILE_URL % (fileName, last, split))
    results = response['WUResultResponse']['Result']['Row']

    try:
        for i, resIN in enumerate(results):
            if CSVlogicalFile:
                res = resIN['line']
                if res is None:
                    print('Line', str(i), 'is blank')
                    print(resIN)
                    continue
                res = res.split(',')
                for j, col in enumerate(columnNames):
                    if col in USELESS_COLS: continue
                    outInfo[col].append(res[j])
            else:
                cols = resIN.keys()
                for col in cols:
                    if col in USELESS_COLS:
                        continue
                    outInfo[col].append(resIN[col])
    except Exception as E:
        print(resIN)
        print(E)
        raise

    return pd.DataFrame(outInfo)

