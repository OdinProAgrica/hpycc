import os
import re

from hpycc import getECLquery
from hpycc import syntaxCheck



def get_parsed_outputs(script, server, port="8010", repo=None,
                       username="hpycc_get_output", password='" "',
                       silent=True):
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
        Password to execute the ECL workunit. " " by default.
    :param silent: bool
        If False, the program will print out its progress. True by
        default.

    Returns
    -------
    :return: parsed: list of tuples
        List of processed tuples in the form
        [(output_name, output_xml)].
    """
    
    syntaxCheck.syntax_check(script, repo=repo)
    
    if repo:
        repo_flag = " -I {}".format(repo)
    else:
        repo_flag = ""

    command = ("ecl run --server {} --port {} --username {} --password {} -legacy "
               "thor {} {}").format(server, port, username, password, script,
                                    repo_flag)

    if not silent:
        print("running ECL script")
        print("command: {}".format(command))

    result = getECLquery.run_command(command, silent)
    stripped = result.strip()
    trimmed = stripped.replace("\r\n", "")

    if not silent:
        print("Parsing response")

    parsed = re.findall(
        "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>", trimmed)

    return parsed


def get_output(script, server, port="8010", repo=None,
               username="hpycc_get_output", password='" "', silent=True):
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
    outputs = get_parsed_outputs(
        script, server, port, repo, username, password, silent)
    parsed_data_frames = [
        (name, getECLquery.parse_XML(xml)) for name, xml in outputs]

    try:
        first_parsed_result = parsed_data_frames[0][1]
    except IndexError:
        print('Unable to parse response, printing first 500 characters: %s' (str(first_parsed_result)[:500])) 

    return first_parsed_result


def get_outputs(script, server, port="8010", repo=None,
                username="hpycc_get_output", password='" "', silent=True):
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
    :return: as_dict: dictionary
        Outputs produced by the script in the form {output_name, df}.
    """
    outputs = get_parsed_outputs(
        script, server, port, repo, username, password, silent)
    parsed_data_frames = [
        (name, getECLquery.parse_XML(xml)) for name, xml in outputs]

    as_dict = dict(parsed_data_frames)

    return as_dict


def save_output(script, server, path, port="8010", repo=None,
                username="hpycc_get_output", password='" "', silent=True,
                compression=None):
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
    result = get_output(script, server, port, repo, username, password, silent)
    result.to_csv(path_or_buf=path, compression=compression)
    return None


def save_outputs(
        script, server, directory=".", port="8010", repo=None,
        username="hpycc_get_output", password='" "', silent=True,
        compression=None, filenames=None, prefix=""):
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
    outputs = get_parsed_outputs(
        script, server, port, repo, username, password, silent)
    parsed_data_frames = [
        (name, getECLquery.parse_XML(xml)) for name, xml in outputs]

    if filenames:
        if len(filenames) != len(parsed_data_frames):
            UserWarning("The number of filenames specified is different to "
                        "the number of outputs in your script.")
        zipped = list(zip(parsed_data_frames, filenames))
    else:
        zipped = [(p, "{}.csv".format(p[0])) for p in parsed_data_frames]

    for result in zipped:
        file_name = "{}{}".format(prefix, result[1])
        path = os.path.join(directory, file_name)
        result[0][1].to_csv(path, compression=compression)

    return None
