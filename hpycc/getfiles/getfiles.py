import re
from concurrent.futures import ThreadPoolExecutor
from time import sleep
import pandas as pd
import hpycc.getfiles.fileinterface as interface

POOL_SIZE = 15
GET_FILE_URL = """/WsWorkunits/WUResult.json?LogicalName=%s&Cluster=thor&Start=%s&Count=%s"""

def get_file(fileName, hpcc_addr, CSVlogicalFile):
    """

    :param fileName:
    :param hpcc_addr:
    :param CSVlogicalFile:
    :return:
    """

    print('Adjusting name to HTML')
    fileName = re.sub('[~]', '', fileName)
    fileName = re.sub(r'[:]', '%3A', fileName)

    columnNames, splits = _get_file_structure(fileName, hpcc_addr, CSVlogicalFile)

    outInfo = {col:[] for col in columnNames}

    pool = ThreadPoolExecutor(POOL_SIZE)
    futures = []
    doneTest = [False]

    for split in splits:
        futures.append(pool.submit(get_file_chunk, fileName, CSVlogicalFile, hpcc_addr, last, split, columnNames, outInfo))
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


def _get_file_structure(fileName, hpcc_addr, CSVlogicalFile):
    print('Determining size and column names')
    response = interface.url_request(hpcc_addr + GET_FILE_URL % (fileName, 0, 2))
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

    if nRecs > 10000:
        splits = list(range(10000, nRecs - 1, 10000))
        if splits[-1] is not (nRecs - 1): splits.append(nRecs)
        print('Large table, downloading in ' + str(len(splits)) + ' chunks ')
    else:
        print('Small table, running all at once')
        splits = [nRecs]

    return columnNames, splits


def get_file_chunk(fileName, CSVlogicalFile, HPCCaddress, last, split, columnNames, outInfo):
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
    request = HPCCaddress + GET_FILE_URL % (fileName, last, split)
    response = interface.url_request(request)
    results = response['WUResultResponse']['Result']['Row']

    try:
        outInfo = interface._parse_json_output(results, columnNames, outInfo, CSVlogicalFile)
    except Exception as E:
        print('Failed to Parse WU response, response written to FailedResponse.txt')
        with open('FailedResponse.txt', 'w') as f: f.writelines(str(response))
        raise

    return pd.DataFrame(outInfo)


