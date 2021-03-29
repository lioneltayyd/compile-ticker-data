

import subprocess, logging

from autoprocess_ticker import collect_tickers
from config.config_logger import setup_logger
from config.config import DICT_SECTORS, DICT_EQUITIES, DICT_COMMODITIES, LOG_PROCESSING_FILEPATH


# --------------------------------------------------------------
# Logger setup. 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_PROCESSING_FILEPATH) 


# --------------------------------------------------------------
# Run.
# --------------------------------------------------------------

if __name__ == "__main__":
    commline_input = input(
        '''
        What ticker category do you wish to collect? 
        S: ETF Sectors.
        E: ETF Equities. 
        Q: Quit.

        Please select: 
        '''
    )
    logger.info(f'CLI Input: {commline_input}') 

    if commline_input == 'S':
        logger.info('Collecting ETF sector data from Yahoo...') 
        collect_tickers.collect_sectors(DICT_SECTORS)
    elif commline_input == 'E': 
        logger.info('Collecting ETF equity data from Yahoo...') 
        collect_tickers.collect_equities(DICT_EQUITIES)
    elif commline_input == 'Q': 
        logger.info('You have quitted the process.') 
    else:
        logger.error('----- Fail to run the pipeline. Please provide the correct input.') 
