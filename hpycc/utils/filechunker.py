"""
functions that chunk an iterable.

Functions
---------
- `make_chunks` -- Return tuples of start index and chunk size.

"""
__all__ = ["make_chunks"]


def make_chunks(num, chunk_size=10000):
    """
    Return tuples of start index and chunk size.

    Parameters
    ----------
    num: int
        Total number of items.
    chunk_size: int, optional
        Max chunk size, 10,000 by default.

    Returns
    -------
    chs: list of tuples
        List of chunks in the form [(start_index, num_items)]

    """
    chs = [(ch * chunk_size, chunk_size) for ch in
           range(num // chunk_size)]
    left_over = num % chunk_size
    if left_over:
        chs.append((num - left_over, left_over))

    return chs
