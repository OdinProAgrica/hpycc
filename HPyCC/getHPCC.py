from hpycc import getHPCCfile
import pandas as pd


def saveFile(df, output_path, zip=False):
    compress = ''
    if zip:
        if output_path[-3:] != '.gz':
            output_path += '.gz'
        compress = 'gzip'

    df.to_csv(output_path, index=False, encoding = 'utf-8',
                  compression=compress)#, compression='gzip'


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