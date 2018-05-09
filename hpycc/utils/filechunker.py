"""
Module contains functions that are able to chunk a file request based on size.
"""


def make_chunks(file_size, csv, chunk_size=10000):
    """
    Makes start row and chunk size lists for threading logical file
    downloads.

    :param file_size: int
        Total size of file for chunking
    :param csv: bool
        Is it a CSV file? Alters starting row to avoid headers
    :param chunk_size: int, optional
        Max chunk size, defaults to 10,000

    :return: list
       List of starting rows
    :return: int
       List of chunk sizes

    """
    start = 1 if csv else 0
    chs = [(ch * chunk_size + start, chunk_size) for ch in
           range(file_size // chunk_size)]
    if (file_size + start) % chunk_size:
        chs.append((file_size - (file_size % chunk_size) + start,
                    file_size % chunk_size + start))

    return chs
