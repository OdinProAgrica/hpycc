from collections import namedtuple
import re
from xml.etree import ElementTree

import pandas as pd
import numpy as np


def parse_xml(xml):
    """
    Return a DataFrame from a nested XML.

    Parameters
    ----------
    xml : str
        xml to be parsed.

    Returns
    -------
    df : pd.DataFrame
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


def _make_col_numeric(df):
    """
    Convert string numeric columns to numerics.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to run conversion on.

    Returns
    -------
    df : pd.DataFrame
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
    df : pd.DataFrame
        DataFrame to run conversion on.

    Returns
    -------
    df : pd.DataFrame
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
    regex = "W[0-9]{8}(\S*)"
    matches = re.search(regex, result)
    if matches:
        return matches.group(0)
    else:
        return None


def parse_wuid_from_xml(result):
    """
    Function retrieves a WUID for a script that has run. This retrieves it
    only in the cases where the request response was in XML format.

    Parameters
    ----------
    result : 'XML'
        The XML response for the script that has run.

    Returns
    -------
    wuid : str
        The Workunit ID from the XML.

    """
    regex = "wuid: (.+?)   state:"
    result = result.replace("\r\n", "")

    search = re.search(regex, result).group(0)
    if not search:
        return None
    wuid3 = search.replace('wuid: ', '')
    wuid2 = wuid3.replace('   state:', '')
    wuid1 = wuid2.replace(' ', '')
    regex2 = "W[0-9]{8}(\S*)"
    wuid = re.search(regex2, wuid1)
    if not wuid:
        return None

    return wuid.group(0)


Schema = namedtuple("Schema", ["name", "is_set", "type"])


def parse_schema_from_xml(xml):
    """
    Parse an ECL schema into python types.

    Parameters
    ----------
    xml : str
        xml string returned by ecl run. This is located in the json
        as ["WUResultResponse]["Result"]["XmlSchema"]["xml"].

    Returns
    -------
    list
        List of namedtuples in form [(name, is_set, type)].

    """
    x = xml.replace("\n", "")
    xml = ElementTree.fromstring(x)
    schema = xml[0][0][0][0][0][0]

    return (Schema(
        child.attrib["name"],
        "type" not in child.keys(),
        get_python_type_from_ecl_type(child)
    ) for child in schema)


def get_python_type_from_ecl_type(child):
    """
    Get the python type from an hpcc schema node

    Parameters
    ----------
    child : XML node
       Node of schema xml. See `parse_schema_from_xml`

    Returns
    -------
    type : type
        Pythonic type. If the HPCC type cannot be mapped, is str.

    """
    translated_type = {
        "boolean": bool,
        "decimal": float,
        "double": float,
        "integer": int,
        "udecimal": float,
        "nonnegativeinteger": int
    }

    c = max([z.attrib.get("type", "") for z in child.iter()]).lower()
    typed = re.sub("[0-9_]|(xs:)", "", c)
    return translated_type.get(typed, str)
