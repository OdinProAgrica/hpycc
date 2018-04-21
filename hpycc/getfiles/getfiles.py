import re
import concurrent.futures
import pandas as pd
import hpycc.getfiles.fileinterface as interface

POOL_SIZE = 15
POOL = concurrent.futures.ThreadPoolExecutor(POOL_SIZE)


def get_file(logical_file, server, port, username, password, csv_file, silent):
    """

    :param logical_file:
    :param server:
    :param csv_file:
    :return:
    """

    print('Adjusting name to HTML')
    logical_file = re.sub('[~]', '', logical_file)
    logical_file = re.sub(r'[:]', '%3A', logical_file)

    column_names, chunks, current_row = _get_file_structure(logical_file, server, port, username, password, csv_file, silent)

    print('Running downloads')
    futures = []
    for split in chunks:
        futures.append(POOL.submit(_get_file_chunk,
                                   logical_file, csv_file, server, port, username, password, current_row, split, column_names, silent))
        current_row = split + 1

    concurrent.futures.wait(futures)
    print("Downloads Complete. Locating any excepted threads")
    for future in futures:
        if future.exception() is not None:
            print("CHUNK FAILED FOR " + str(future.exception()))
            raise future.exception()

    print('Concat outputs')
    return pd.concat([future.result() for future in futures])


def _get_file_structure(logical_file, server, port, username, password, csv_file, silent):
    """

    :param logical_file:
    :param server:
    :param csv_file:
    :return:
    """
    print('Determining size and column names')

    response = interface.make_url_request(server, port, username, password, logical_file, 0, 2, silent)
    file_size = response['Total']
    results = response['Result']['Row']

    if csv_file:
        column_names = results[0]['line'].split(',')
        current_row = 1  # start row, miss first line (header)
    else:
        column_names = results[0].keys()
        current_row = 0  # start row, use first line

    column_names = [col for col in column_names if col != '__fileposition__']

    print('Columns found: ' + str(column_names))
    print('Row count: ' + str(file_size))

    if file_size > 10000:
        # TODO: this code can be made much more succinct
        chunks = list(range(10000, file_size - 1, 10000))
        if chunks[-1] is not (file_size - 1):
            chunks.append(file_size)
        print('Large table, downloading in ' + str(len(chunks)) + ' chunks ')
    else:
        print('Small table, running all at once')
        chunks = [file_size]

    return column_names, chunks, current_row


def _get_file_chunk(file_name, csv_file, server, port,
                    username, password, current_row,
                    chunk, column_names, silent):

    print('Getting rows ' + str(current_row) + ' to ' + str(chunk))
    response = interface.make_url_request(server, port, username, password, file_name, current_row, chunk, silent)
    results = response['Result']['Row']

    try:
        out_info = interface.parse_json_output(results, column_names, csv_file, silent)
    except Exception:
        print('Failed to Parse WU response, response written to FailedResponse.txt')
        with open('FailedResponse.txt', 'w') as f:
            f.writelines(str(results))
        raise

    return pd.DataFrame(out_info)
