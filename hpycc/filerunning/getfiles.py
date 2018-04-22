"""
Internal calls for obtaining logical files. Handles obtaining file structure and orchestrates
downloading each chunk of the file. Larger files are multi-threaded. Actual downloads are
handled by the datarequests module in utils. Results are parsed to dataframes using the
relevent parser from utils, in this case JSON.
"""

import re
import logging
import concurrent.futures
import pandas as pd
import hpycc.utils.datarequests
import hpycc.utils.parsers


def get_file_internal(logical_file, server, port, username, password, csv_file, download_threads):
    """
     Download an HPCC logical file and return a pandas dataframe. To save to csv
     without a return use save_file(). This process has an advantage over scripts as it can be
     chunked and threaded.

     :param logical_file: str
         Logical file to be downloaded
     :param server: str
        IP address or url of the HPCC server, supply usernames, passwords and ports
        using other arguments.
     :param port: str
         Port number ECL Watch is running on.
     :param username: str
         Username to execute the ECL workunit.
     :param password: str
         Password to execute the ECL workunit.
     :param csv_file: bool
         Is the logical file a CSV?
     :param download_threads: int
         Number of concurrent download threads for the file. Warning: too many will likely
         cause either your script or you cluster to crash!

     :return: pd.DataFrame
         a DF of the given file
     """

    logger = logging.getLogger('getfiles.get_file_internal')
    logger.info('Getting file %s from %s:XXXXXXX@%s : %s. csv_file is %s'
                % (logical_file, username, server, port, csv_file))

    logger.debug('Adjusting name to HTML. Before: %s' % logical_file)
    logical_file = re.sub('[~]', '', logical_file)
    logical_file = re.sub(r'[:]', '%3A', logical_file)
    logger.debug('Adjusted name to HTML. After: %s' % logical_file)

    column_names, chunks, current_row = _get_file_structure(logical_file, server, port,
                                                            username, password,
                                                            csv_file)

    logger.info('Dumping download tasks to thread pools. See _get_file_structure log for file structure')
    logger.debug('No. chunks: %s, start row (0 for Thor, 1 for csv): %s' % (len(chunks), current_row))

    pool = concurrent.futures.ThreadPoolExecutor(download_threads)
    futures = []
    for chunk in chunks:
        logger.debug('Booting chunk: %s' % chunk)
        futures.append(pool.submit(_get_file_chunk,
                                   logical_file, csv_file, server, port,
                                   username, password, current_row, chunk,
                                   column_names))
        current_row = chunk + 1

    logger.info('Waiting for %s threads to complete' % len(futures))
    concurrent.futures.wait(futures)

    logger.info("Downloads Complete. Locating any excepted threads")
    for future in futures:
        if future.exception() is not None:
            logger.error("Chunk failed! Do not have full file: %s" % future.exception())
            raise future.exception()

    logger.debug('Concatanating outputs')
    results = pd.concat([future.result() for future in futures])

    logger.debug('Returning: %s' % results)
    return results


def _get_file_structure(logical_file, server, port, username, password, csv_file):
    """
     Downloads a single row from the given logical file and uses it to get column names and
     row count.

     :param logical_file: str
         Logical file to be downloaded
     :param server: str
         Ip address of HPCC in the form XX.XX.XX.XX.
     :param port: str
         Port number ECL Watch is running on.
     :param username: str
         Username to execute the ECL workunit.
     :param password: str
         Password to execute the ECL workunit.
     :param csv_file: bool
         Is the logical file a CSV?

     :return: list
        list of column names
     :return: list
        list of chunk positions
     :return: int
        starting row for download
     """
    logger = logging.getLogger('_get_file_structure')
    logger.info('Getting file structure for %s' % logical_file)

    logger.info('Getting 1 row to determine structure')
    response = hpycc.utils.datarequests.make_url_request(server, port, username, password, logical_file, 0, 2)
    file_size = response['Total']
    results = response['Result']['Row']

    logger.debug('file_size: %s, first row: %s' % (file_size, results))

    if csv_file:
        logger.debug('csv fiele so parsing out columns from row 0')
        column_names = results[0]['line'].split(',')
        current_row = 1  # start row, miss first line (header)
    else:
        logger.debug('logical file so parsing out columns from result keys')
        column_names = results[0].keys()
        current_row = 0  # start row, use first line

    logger.debug('Returned column names: %s' % column_names)
    column_names = [col for col in column_names if col != '__fileposition__']
    logger.debug('Dropping _file_position_column: %s' % column_names)

    logger.debug('Row count is %s, determining number of chunks....' % file_size)
    if file_size > 10000:
        # TODO: this code can be made much more succinct
        chunks = list(range(10000, file_size - 1, 10000))
        if chunks[-1] is not (file_size - 1):
            chunks.append(file_size)
        logger.info('Large table, downloading in %s chunks' % len(chunks))
    else:
        logger.info('Small table, running all at once')
        chunks = [file_size]

    logger.debug('Chunk List: %s' % chunks)
    return column_names, chunks, current_row


def _get_file_chunk(logical_file, csv_file, server, port,
                    username, password, current_row,
                    chunk, column_names):
    """
    Downloads a part of a logical file.

    :param logical_file: str
        Logical file to be downloaded
    :param csv_file: bool
        Is the logical file a CSV?
    :param server: str
        Ip address of HPCC in the form XX.XX.XX.XX.
    :param port: str
        Port number ECL Watch is running on.
    :param username: str
        Username to execute the ECL workunit.
    :param password: str
       Password to execute the ECL workunit.
    :param current_row: int
        Starting row for chunk
    :param chunk: int
        Size of chunk
    :param column_names: list
        names of columns to download

    :return: pd.DataFrame
        df of requested chunk
    """

    logger = logging.getLogger('_get_file_chunk')
    logger.info('Aquiring file chunk. Row: %s, chunk size: %s' % (current_row, chunk))

    response = hpycc.utils.datarequests.make_url_request(server, port, username, password, logical_file, current_row, chunk)
    logger.debug('Extracting results from response')
    results = response['Result']['Row']

    try:
        logger.debug('Handing to paser to extract data from JSON')
        out_info = hpycc.utils.parsers.parse_json_output(results, column_names, csv_file)
    except Exception:
        logger.error('Failed to Parse WU response, response writing to FailedResponse.txt')
        with open('FailedResponse.txt', 'w') as f:
            f.writelines(str(results))
        raise

    logger.debug('Returning. See Parse_json_output log for contents')
    return out_info
