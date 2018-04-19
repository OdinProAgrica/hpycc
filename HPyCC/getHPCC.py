from hpycc import getHPCCfile
from hpycc import getECLquery
from hpycc import getHPCC
import os
import re
import xml.etree.ElementTree as ET
import pandas as pd


def saveFile(df, output_path, zip=False):
    compress = ''
    if zip:
        if output_path[-3:] != '.gz':
            output_path += '.gz'
        compress = 'gzip'
          
    df.to_csv(output_path, index=False, encoding = 'utf-8', 
                  compression=compress)#, compression='gzip'   

def get_script_result(scriptLoc, hpcc_addr, hpcc_repo='', 
                      output_path='', 
                      username='DS_extractinator', password='" "'):
    """
    main function to run an ECL script. Returns a df or a 
    dict of dfs. If saving to file and multiple dfs are 
    returned, each will be named.
    
    Parameters
    ----------
    scriptLoc: str
        Location of the script to run
    hpcc_addr:
        address of the HPCC cluster
    hpcc_repo:
        location of any needed ECL repository
    output_path: str, optional
        Path to save to. If blank will return a dataframe. Blank by default.
    username: str, optional
        Username to give to HPCC. Default is DS_extractinator.
    password: str, optional 
        Password to give to HPCC. Default is " ".
    Returns
    -------
    result: pd.DataFrame
        dataframe of the given query response
    """
    
    if hpcc_repo :
        hpcc_repo = ' -I ' + hpcc_repo  
    
    print('running ECL script')
    command = 'ecl run --server {} --port 8010 --username {} --password {} thor {} {}'.format(hpcc_addr, username, password, scriptLoc, hpcc_repo)
    res = getECLquery.run_command(command)
    decoded = res.decode('utf-8').strip()
    decoded = decoded.replace('\r\n', '')
    decoded = decoded.replace('\r\n', '')
    
    print('Parsing Response')
    results = {i[0]: getECLquery.parse_XML(i[1]) for i in re.findall(
        "<Dataset name='(?P<name>.+?)'>(?P<content>.+?)</Dataset>", decoded)
               }
        
    if len(results) == 1:
        val = list(results.values())[0]
        
        if output_path:
            saveFile(val, output_path)
        else:
            return val
    
    elif output_path:
        for key in results:
                formatted_key = key.lower().replace(' ', '_')
                split = output_path.split('.')
                new_path = split[0] + formatted_key + '.' + split[1]
                getHPCC.saveFile(results[key], new_path)
    
    else:
        return results
    
def get_file(inFileName, hpcc_addr,
             CSVlogicalFile = False, output_path = ''):
    """Main call to process an HPCC file.

    Parameters
    ----------
    inFileName: str
        logical file to be downloaded
    CSVlogicalFile: bool
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
        df = getHPCCfile.getFile(inFileName, hpcc_addr, CSVlogicalFile)
    except KeyError:
        print('Key error, have you specified a CSV or THOR file correctly?')
        raise    
        
    if output_path:
        saveFile(df, output_path)    
    return df