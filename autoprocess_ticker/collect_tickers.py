

import subprocess, os, logging
from typing import Dict, List, Text

# Personal modules.
from config.config import (
    ETF_SECTOR_DIR, ETF_EQUITY_DIR, LOG_PROCESSING_FILEPATH, 
    LOG_PIPELINE_SECTOR_DIR, LOG_PIPELINE_EQUITY_DIR, 
    YAHOO_VERSION, LOCAL_SCHEDULER, WORKERS
)
from config.config_logger import setup_logger


# --------------------------------------------------------------
# Logger setup. 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_PROCESSING_FILEPATH) 


# --------------------------------------------------------------
# Collect Multiple Tickers Data.
# --------------------------------------------------------------

def collect_sectors(dict_data:Dict[Text, List]):
    '''
    Purpose : 
        Start collecting the ticker data for each ETF sector by 
        running the Luigi pipeline via commandline. 

    Input   :
        dict_data: Like the following example where the dict key is the (starting year). 
            dict = {
                1999: ['SPY', 'DIA', 'XLB', 'XLE', 'XLF', 'XLI', 'XLP'],
                2000: ['QQQ'],
            } 
    '''

    # (start_yr) and (ticker) will be iterated. 
    for start_yr, ticker_list in dict_data.items(): 
        for ticker in ticker_list:
            try: os.makedirs(LOG_PIPELINE_SECTOR_DIR)
            except: logger.debug(f'----- The ({LOG_PIPELINE_SECTOR_DIR}) directory has already been created.')

            with open(f'{LOG_PIPELINE_SECTOR_DIR}/{ticker}.log', 'w') as log_file:
                logger.debug(f'----- Luigi CLI params -- Scheduler: ({LOCAL_SCHEDULER}) -- Ticker: ({ticker}) -- Start year: ({start_yr}) -- ETF dir: ({ETF_SECTOR_DIR}) -- Download version: ({YAHOO_VERSION})')
                subprocess.run([
                    'python', 'luigi_pipeline.py', LOCAL_SCHEDULER, 'CompileToExcel', 
                    '--ticker', ticker, '--start-yr', str(start_yr), '--etf-dir', ETF_SECTOR_DIR, 
                    '--yahoo-version', YAHOO_VERSION, '--workers', WORKERS
                ], stderr=log_file) 


def collect_equities(dict_data:Dict[Text, Dict[Text, List]]):
    '''
    Purpose : 
        Start collecting the ticker data for each ETF equity by 
        running the Luigi pipeline via commandline. 

    Input   :
        dict_data: Like the following example where the internal dict key is the (starting year). 
            dict = {
                'PPA': {
                    1999: ['BA', 'BLL', 'COL', 'GD', 'HON', 'LMT', 'NOC'],
                }
            }
    '''

    # (etf_dif), (start_yr), and (ticker) will be iterated. 
    for etf, dict_obj in dict_data.items():
        etf_dir = f'{ETF_EQUITY_DIR}/{etf}' 

        for start_yr, ticker_list in dict_obj.items(): 
            for ticker in ticker_list:
                try: os.makedirs(LOG_PIPELINE_EQUITY_DIR) 
                except: logger.debug(f'----- The ({LOG_PIPELINE_EQUITY_DIR}) directory has already been created.')

                with open(f'{LOG_PIPELINE_EQUITY_DIR}/{ticker}.log', 'w') as log_file: 
                    logger.debug(f'----- Luigi CLI params -- Scheduler: ({LOCAL_SCHEDULER}) -- Ticker: ({ticker}) -- Start year: ({start_yr}) -- ETF dir: ({etf_dir}) -- Download version: ({YAHOO_VERSION})')
                    subprocess.run([
                        'python', 'luigi_pipeline.py', LOCAL_SCHEDULER, 'CompileToExcel', 
                        '--ticker', ticker, '--start-yr', str(start_yr), '--etf-dir', etf_dir, 
                        '--yahoo-version', YAHOO_VERSION, '--workers', WORKERS
                    ], stderr=log_file) 
