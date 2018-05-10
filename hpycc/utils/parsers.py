"""
Contains functions to parse responses from HPCC. Currently it can convert
both ecl.exe produced XML and WUresult JSON produced via a url request.
"""
import logging
import re
from xml.etree import ElementTree
import pandas as pd


def make_col_numeric(df):
    """
    Convert string numeric columns to numerics.

    Parameters
    ----------
    :param df: pd.DataFrame
        DataFrame to run conversion on.

    Returns
    -------
    :return: DataFrame
        Data frame with all string numeric columns converted to
        numeric.
    """

    logger = logging.getLogger('make_col_numeric')
    logger.debug('Converting numeric cols')

    for col in df.columns:
        try:
            nums = pd.to_numeric(df[col])
            df[col] = nums
            logger.debug('%s converted to numeric' % col)
        except ValueError:
            logger.debug('%s cannot be converted to numeric' % col)
            continue

    return df


def make_col_bool(df):
    """
    Convert string boolean columns to booleans.

    Parameters
    ----------
    :param df: pd.DataFrame
        DataFrame to run conversion on.

    Returns
    -------
    :return: DataFrame
        Data frame with all string boolean columns converted to
        boolean.
    """
    for col in df.columns:
        # TODO deal with nans?
        if set(df[col].str.lower().unique).issubset({"true", "false"}):
            df.loc[df[col].notnull(), col] = df[col].str.lower() == "true"

    return df


def parse_json_output(results, column_names, csv_file):
    """
    Return a dict that can be converted to a pandas df from a JSON.

    :param results: dict
        json to be parsed.
    :param column_names: list
        column names to parse
    :param csv_file: bool
        Is this a csv file? Requires different parsing

    :return: outInfo: pd.DataFrame
        Parsed json converted to DataFrame.
    """

    logger = logging.getLogger('parse_json_output')
    logger.debug('Parsing JSON response, converting to dict')
    logger.debug(
        'See _run_url_request log for JSON. Column_names: %s, csv_file: %s' %
        (column_names, csv_file))

    out_info = {col: [] for col in column_names}

    for i, result in enumerate(results):
        # logger.debug('Parsing result %s' % i)
        if csv_file:
            res = result['line']
            if res is None:
                logger.warning('Line', str(i), 'is blank! Row: %s' % result)
                continue
            res = res.split(',')
            for j, col in enumerate(column_names):
                out_info[col].append(res[j])
        else:
            for col in column_names:
                out_info[col].append(result[col])

    df = pd.DataFrame(out_info)
    df = make_col_numeric(df)
    df = make_col_bool(df)

    logger.debug('Returning: %s' % df)

    return df


def parse_xml(xml):
    """
    Return a DataFrame from a nested XML.

    :param xml: str
        xml to be parsed.

    :return pd.DataFrame
        Parsed xml.
    """
    logger = logging.getLogger('parse_xml')
    logger.debug("Parsing Results from XML")

    vls = []
    lvls = []

    for line in re.findall("<Row>(?P<content>.+?)</Row>", xml):
        logger.debug('Parsing line: \n%s' % line)
        with_start = '<Row>' + line + '</Row>'
        newvls = []
        etree = ElementTree.fromstring(with_start)
        for child in etree:
            if child.tag not in lvls:
                logger.debug('New column found: %s' % child.tag)
                lvls.append(child.tag)
            newvls.append(child.text)
        vls.append(newvls)

    df = pd.DataFrame(vls, columns=lvls)

    df = make_col_numeric(df)
    df = make_col_bool(df)

    logger.debug('Returning: %s' % df)
    return df
