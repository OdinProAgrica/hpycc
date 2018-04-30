"""
Internal calls for obtaining logical files. Handles obtaining file structure and orchestrates
downloading each chunk of the file. Larger files are multi-threaded. Actual downloads are
handled by the data requests module in utils. Results are parsed to dataframes using the
relevent parser from utils, in this case JSON.
"""

import concurrent.futures
import logging
import re
import pandas as pd
import hpycc.utils.HPCCconnector
import hpycc.utils.parsers
from hpycc.utils.filechunker import make_chunks


def get_file_internal(logical_file, hpcc_connection, csv_file, download_threads):
    """
     Download an HPCC logical file and return a pandas dataframe. To save to csv
     without a return use save_file(). This process has an advantage over scripts as it can be
     chunked and threaded.

     :param logical_file: str
         Logical file to be downloaded
    :param hpcc_connection: HPCCconnector,
        Connection details for an HPCC instance.
     :param csv_file: bool
         Is the logical file a CSV?
     :param download_threads: int
         Number of concurrent download threads for the file. Warning: too many will likely
         cause either your script or you cluster to crash!

     :return: pd.DataFrame
         a DF of the given file
     """

    logger = logging.getLogger('getfiles.get_file_internal')
    logger.debug('Getting file %s from %s. csv_file is %s'
                % (logical_file, hpcc_connection.get_string(), csv_file))

    logger.debug('Adjusting name to HTML. Before: %s' % logical_file)
    logical_file = re.sub('[~]', '', logical_file)
    logical_file = re.sub(r'[:]', '%3A', logical_file)
    logger.debug('Adjusted name to HTML. After: %s' % logical_file)

    column_names, file_size = _get_file_structure(logical_file, hpcc_connection, csv_file)
    start_rows, chunks = make_chunks(file_size, csv_file)

    logger.debug('Dumping download tasks to thread pools.')
    logger.debug('See _get_file_structure log for file structure. Chunks: %s, start rows: %s' % (chunks, start_rows))

    pool = concurrent.futures.ThreadPoolExecutor(download_threads)
    futures = []
    for start, chunk in zip(start_rows, chunks):
        logger.debug('Booting chunk starting at: %s and size: %s' % (start, chunk))
        futures.append(pool.submit(_get_file_chunk, logical_file, csv_file,
                                   hpcc_connection, start, chunk, column_names))

    logger.info('Requests sent. Waiting for downloads to complete')
    concurrent.futures.wait(futures)

    logger.debug("Downloads Complete. Locating any excepted threads")
    for future in futures:
        if future.exception() is not None:
            logger.error("Chunk failed! Do not have full file: %s" % future.exception())
            raise future.exception()

    logger.info('File downloaded, tidying results')
    results = pd.concat([future.result() for future in futures])
    results.reset_index(inplace=True, drop=True)

    logger.debug('Returning: %s' % results)
    logger.info('Done')
    return results


def _get_file_structure(logical_file, hpcc_connection, csv_file):
    """
     Downloads a single row from the given logical file and uses it to get column names and
     row count.

     :param logical_file: str
         Logical file to be downloaded
    :param hpcc_connection: HPCCconnector,
        Connection details for an HPCC instance.
     :param csv_file: bool
         Is the logical file a CSV?

     :return: list
        List of column names
     :return: int
        File size
     """
    logger = logging.getLogger('_get_file_structure')
    logger.debug('Getting file structure for %s' % logical_file)

    logger.debug('Getting 1 row to determine structure')
    response = hpcc_connection.make_url_request(logical_file, 0, 2)
    file_size = response['Total']
    results = response['Result']['Row']

    logger.debug('file_size: %s, first row: %s' % (file_size, results))

    if csv_file:
        logger.debug('csv file so parsing out columns from row 0')
        column_names = results[0]['line'].split(',')
    else:
        logger.debug('logical file so parsing out columns from result keys')
        column_names = results[0].keys()

    logger.debug('Returned column names: %s' % column_names)
    column_names = [col for col in column_names if col != '__fileposition__']
    logger.debug('Dropping _file_position_column: %s' % column_names)

    return column_names, file_size


def _get_file_chunk(logical_file, csv_file, hpcc_connection, current_row, chunk, column_names):
    """
    Downloads a part of a logical file.

    :param logical_file: str
        Logical file to be downloaded
    :param csv_file: bool
        Is the logical file a CSV?
    :param hpcc_connection: HPCCconnector,
        Connection details for an HPCC instance.
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
    logger.debug('Acquiring file chunk. Row: %s, to: %s' % (current_row, chunk))

    response = hpcc_connection.make_url_request(logical_file, current_row, chunk)
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

    # out_info.to_csv(str(current_row) + 'test.csv')
    return out_info


