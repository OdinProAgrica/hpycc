"""
Module contains functions that are able to chunk a file request based on size.
"""

import logging


def make_chunks(file_size, csv_file, chunk_size=10000):
    """
    Makes start row and chunk size lists for threading logical file
    downloads.

    :param file_size: int
        Total size of file for chunking
    :param csv_file: bool
        Is it a CSV file? Alters starting row to avoid headers
    :param chunk_size: int, optional
        Max chunk size, defaults to 10,000

    :return: list
       List of starting rows
    :return: int
       List of chunk sizes

    """
    logger = logging.getLogger('make_chunks')
    logger.debug('Row count is %s, determining number of chunks....' % file_size)

    start_rows = [1] if csv_file else [0]
    if file_size > chunk_size:
        logger.debug('Large table, downloading in chunks')
        chunks = [chunk_size]

        while start_rows[-1] + chunks[-1] < file_size:
            next_chunk_start = start_rows[-1] + chunk_size
            start_rows.append(next_chunk_start)

            ending_row = next_chunk_start + chunk_size
            next_chunk_length = chunk_size if ending_row < file_size else file_size - start_rows[-1]
            chunks.append(next_chunk_length)

    else:
        logger.debug('Small table, running all at once')
        chunks = [file_size]

    return start_rows, chunks