import os
from hpycc.getfiles import getfiles
from hpycc.getscripts import getscripts


def get_output(script, server, port="8010", repo=None,
               username="hpycc_get_output", password='" "', silent=False,
               legacy=False, do_syntaxcheck=True):
    """
    Return the first output of an ECL script as a DataFrame.

    Parameters
    ----------
    :param script: str
         Path of script to execute.
    :param server: str
        Ip address and port number of HPCC in the form
        XX.XX.XX.XX.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by
    default.
    :param silent: bool
        If False, the program will print out its progress. True by
        default.

    Returns
    -------
    :return: result: DataFrame
        The first output produced by the script.
    """

    parsed_data_frames = getscripts.get_script(
        script, server, port, repo, username, password, silent, legacy, do_syntaxcheck)

    try:
        first_parsed_result = parsed_data_frames[0][1]
    except IndexError:
        UserWarning('Unable to parse response, printing first 500 characters: %s' % parsed_data_frames[:500])
        raise

    return first_parsed_result


def get_outputs(script, server, port="8010", repo=None,
                username="hpycc_get_output", password='" "', silent=False,
                legacy=False, do_syntaxcheck=True):
    """
    Return all outputs of an ECL script as a dict of DataFrames.

    Parameters
    ----------
    :param script: str
         Path of script to execute.
    :param server: str
        Ip address and port number of HPCC in the form
        XX.XX.XX.XX.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by
    default.
    :param silent: bool
        If False, the program will print out its progress. True by
        default.

    Returns
    -------
    :return: as_dict: dictionary
        Outputs produced by the script in the form {output_name, df}.
    """

    parsed_data_frames = getscripts.get_script(
        script, server, port, repo, username, password, silent, legacy, do_syntaxcheck)

    as_dict = dict(parsed_data_frames)

    return as_dict


def get_file(logical_file, hpcc_addr, port='8010', csv_file=False, output_path=''):
    """
    Main call to process an HPCC file. Advantage over scripts as it can be chunked and threaded.

    Parameters
    ----------
    logical_file: str
        logical file to be downloaded
    csv_file: bool
        IS the logical file a CSV?
    hpcc_addr: str
        address of the HPCC cluster
    output_path: str, optional
        Path to save to. If blank will return a dataframe. Blank by default.
    Returns
    -------
    result: pd.DataFrame
        a DF of the given file
    """

    print('Getting file')
    try:
        df = getfiles.get_file(logical_file, hpcc_addr, port, csv_file)
    except KeyError:
        print('Key error, have you specified a CSV or THOR file correctly?')
        raise

    if output_path:
        save_file(df, output_path)
    return df


def save_output(script, server, path, port="8010", repo=None,
                username="hpycc_get_output", password='" "', silent=False,
                compression=None, legacy=False):
    """
    Save the first output of an ECL script as a csv.

    Parameters
    ----------
    :param path: str
        Path of target destination.
    :param script: str
         Path of script to execute.
    :param server: str
        Ip address and port number of HPCC in the form
        XX.XX.XX.XX.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by
    default.
    :param silent: bool
        If False, the program will print out its progress. True by
        default.
    :param compression: str, optional
        Compression format to give to pandas. None by default.

    Returns
    -------
    :return: None
    """
    result = get_output(script, server, port, repo, username, password, silent, legacy)
    result.to_csv(path_or_buf=path, compression=compression)
    return None


def save_outputs(
        script, server, directory=".", port="8010", repo=None,
        username="hpycc_get_output", password='" "', silent=False,
        compression=None, filenames=None, prefix="", legacy=False,
        do_syntaxcheck=True):

    """
    Save all outputs of an ECL script as csvs using their output
    name. The file names can be changed using the filenames and
    prefix parameters.

    Parameters
    ----------
    :param script: str
         Path of script to execute.
    :param server: str
        Ip address and port number of HPCC in the form
        XX.XX.XX.XX.
    :param directory: str, optional
        Directory to save output files in. "." by default.
    :param port: str, optional
        Port number ECL Watch is running on. "8010" by default.
    :param repo: str, optional
        Path to the root of local ECL repository if applicable.
    :param username: str, optional
        Username to execute the ECL workunit. "hpycc_get_output" by
        default.
    :param password: str, optional
        Password to execute the ECL workunit. " " by
    default.
    :param silent: bool
        If False, the program will print out its progress. True by
        default.
    :param compression: str, optional
        Compression format to give to pandas. None by default.
    :param filenames: list, optional
        File names to save results as. If filenames is shorter than
        number of outputs, only those with a filename will be saved.
        If not specified, all files will be named their output name
        assigned by the ECL script.
    :param prefix: str, optional
        Prefix to prepend to all file names. "" by default.

    Returns
    -------
    :return: None
    """

    parsed_data_frames = getscripts.get_script(
        script, server, port, repo, username, password, silent, legacy, do_syntaxcheck)

    if filenames:
        if len(filenames) != len(parsed_data_frames):
            UserWarning("The number of filenames specified is different to "
                        "the number of outputs in your script. Adding names to compensate.")
        zipped = list(zip(parsed_data_frames, filenames))
    else:
        zipped = [(p, "{}.csv".format(p[0])) for p in parsed_data_frames]

    for result in zipped:
        file_name = "{}{}".format(prefix, result[1])
        path = os.path.join(directory, file_name)
        result[0][1].to_csv(path, compression=compression)

    return None


def save_file(df, output_path, do_compression=False):

    """

    :param df:
    :param output_path:
    :param do_compression:
    :return:
    """

    compress_extension = ''
    if do_compression:
        if output_path[-3:] != '.gz':
            output_path += '.gz'
        compress_extension = 'gzip'

    df.to_csv(output_path, index=False, encoding='utf-8',
              compression=compress_extension)  # , compression='gzip'



# TODO: Run function that runs a script and saves the output. Probably another class.
