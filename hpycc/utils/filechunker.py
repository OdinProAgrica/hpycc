"""
Module contains functions that are able to chunk a file request based on size.
"""


def make_chunks(file_size, logical_csv, chunk_size=10000):
    """
    Return tuples of start row and chunk size for threading logical
    file actions.

    Parameters
    ----------
    :param file_size: int
        Total number of rows in file.
    :param logical_csv: bool
        If the file is a logical file, is it a csv?
    :param chunk_size: int, optional
        Max chunk size, defaults to 10000.

    Returns
    -------
    :return chs: list of tuples
        List of chunks in the form [(start_row, num_rows)]
    """
    start = 1 if logical_csv else 0
    chs = [(ch * chunk_size + start, chunk_size) for ch in
           range(file_size // chunk_size)]
    if (file_size + start) % chunk_size:
        chs.append((file_size - (file_size % chunk_size) + start,
                    file_size % chunk_size + start))

    return chs
