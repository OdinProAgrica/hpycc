import logging
import re
from xml.etree import ElementTree as ET
import pandas as pd


def parse_json_output(results, column_names, csv_file):
    """
    Return a dict that can be converted to a pandas df from a JSON.

    Parameters
    ----------
    results: dict
        json to be parsed.
    column_names: list
        column names to parse
    csv_file: bool
        Is this a csv file? Requires different parsing

    Returns
    -------
    outInfo: dict
        Parsed json.
    """

    logger = logging.getLogger('parse_json_output')
    logger.info('Parsing JSON response, converting to dict')
    logger.debug('See _run_url_request log for JSON. Column_names: %s, csv_file: %s' % (column_names, csv_file))

    out_info = {col: [] for col in column_names}

    for i, result in enumerate(results):
        logger.debug('Parsing result %s' % i)
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

    sample_data = {col: val[0:5] for (col, val) in out_info.items()}
    logger.debug('Returning (5 row sample): %s' % sample_data)

    return out_info


def parse_xml(xml):
    """
    Return a DataFrame from a nested XML.

    Parameters
    ----------
    :param xml: str
        xml to be parsed.

    Returns
    -------
    :return pd.DataFrame
        Parsed xml.
    """
    logger = logging.getLogger('parse_xml')
    logger.info("Parsing Results from XML")

    vls = []
    lvls = []

    for line in re.findall("<Row>(?P<content>.+?)</Row>", xml):
        logger.debug('Parsing line: \n%s' % line)
        with_start = '<Row>' + line + '</Row>'
        newvls = []
        etree = ET.fromstring(with_start)
        for child in etree:
            if child.tag not in lvls:
                logger.debug('New column found: %s' % child.tag)
                lvls.append(child.tag)
            newvls.append(child.text)
        vls.append(newvls)

    df = pd.DataFrame(vls, columns=lvls)

    logger.debug('Converting numeric cols')
    for col in df.columns:
        try:
            nums = pd.to_numeric(df[col])
            df[col] = nums
            logger.debug('%s converted to numeric' % col)
        except ValueError:
            logger.debug('%s cannot be converted to numeric' % col)
            pass

    logger.debug('Returning: %s' % df)
    return df