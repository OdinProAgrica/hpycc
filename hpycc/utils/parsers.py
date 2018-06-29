import re
from xml.etree import ElementTree

import pandas as pd
import numpy as np


def parse_xml(xml):
    """
    Return a DataFrame from a nested XML.

    :param xml: str
        xml to be parsed.

    :return pd.DataFrame
        Parsed xml.
    """
    vls = []
    lvls = []

    for line in re.findall("<Row>(?P<content>.+?)</Row>", xml):
        with_start = '<Row>' + line + '</Row>'
        newvls = []
        etree = ElementTree.fromstring(with_start)
        for child in etree:
            if child.tag not in lvls:
                lvls.append(child.tag)
            newvls.append(child.text)
        vls.append(newvls)

    df = pd.DataFrame(vls, columns=lvls)
    df.replace("", np.nan, inplace=True)
    df.fillna(np.nan, inplace=True)

    df = _make_col_numeric(df)
    df = _make_col_bool(df)

    return df


def parse_json_output(results, column_names, csv):
    """
    Return a DataFrame from a HPCC workunit JSON.

    Parameters
    ----------
    :param results: dict, json
        JSON to be parsed.
    :param column_names: list
        Column names to parse.
    :param csv: bool
        Is this a csv file?

    :return: df: .DataFrame
        JSON converted to DataFrame.
    """
    if not csv:
        df = pd.DataFrame(results)
    else:
        lines = [",".split(r["line"]) for r in results]
        df = pd.DataFrame(map(list, zip(*lines)), columns=column_names)

    df = _make_col_numeric(df)
    df = _make_col_bool(df)

    return df


def _make_col_numeric(df):
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
    for col in df.columns:
        try:
            nums = pd.to_numeric(df[col])
            df[col] = nums
        except ValueError:
            continue

    return df


def _make_col_bool(df):
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
        unique_vals = df[col].unique()
        if set(unique_vals).issubset(["true", "false"]):
            df[col] = (df[col] == "true").astype('bool')
        elif set(unique_vals).issubset(["true", "false", np.nan]):
            df[col] = df[col].map({
                "true": True,
                "false": False,
                "": np.nan,
                None: np.nan
            })

    return df


def parse_wuid_from_failed_response(result):
    wuid_regex = result.split("\r\n")[-1]
    regex = "W[0-9]{8}(\S*)"
    wuid = re.search(regex, wuid_regex)
    return wuid


def parse_wuid_from_xml(result):
    """
    Function retrieves a WUID for a script that has run. This retrieves it
    only in the cases where the request response was in XML format.
    Parameters
    ----------
    result: 'XML'
        The XML response for the script that has run.

    Returns
    -------
    :return: wuid - string
        The Workunit ID from the XML.
    """
    regex = "wuid: (.+?)   state:"
    result = result.replace("\r\n", "")

    search = re.search(regex, result).group(0)
    wuid3 = search.replace('wuid: ', '')
    wuid2 = wuid3.replace('   state:', '')
    wuid1 = wuid2.replace(' ', '')
    regex2 = "W[0-9]{8}(\S*)"
    wuid = re.search(regex2, wuid1).group(0)
    return wuid
