

import logging
import regex as re
# Personal modules. 
from config.config_logger import setup_logger
from config.config import LOG_PROCESSING_FILEPATH


# --------------------------------------------------------------
# Logger setup. 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_PROCESSING_FILEPATH) 

# ----------------------------------------------------------------------
# Write into Excel file.  
# ----------------------------------------------------------------------

def single_sheet_multi_write(excel_writer, pivot_dict, pivot_stats, sheet_name, keys, 
                             startcol, startrow, distance):
    
    startrow1st = startrow
    
    for i, key in enumerate(keys):
        logger.info(f'Saving {key} ticker data...')

        pivot_dict[key].to_excel(excel_writer, index=False, sheet_name=sheet_name, startcol=startcol, startrow=startrow1st)
        startcol2nd = startcol + len(pivot_dict[key].columns) + distance
        
        re_compile = re.compile(f'(?:{key})(?:_R.*Yr)?(?!_)')
        stats_keys = list(filter(re_compile.match, pivot_stats.keys()))
        
        for stats_key in stats_keys:
            pivot_stats[stats_key].to_excel(excel_writer, index=False, sheet_name=sheet_name, 
                                            startcol=startcol2nd, startrow=startrow1st)
            startcol2nd = startcol2nd + len(pivot_stats[stats_key].columns) + distance
        
        startrow1st = startrow1st + len(pivot_dict[key].index) + distance 

        
def multi_sheet_write(excel_writer, pivot_dict, pivot_stats, sheet_name, key, 
                      startcol, startrow, distance):  

    logger.info(f'Saving {key} ticker data...') 

    pivot_dict[key].to_excel(excel_writer, index=False, sheet_name=sheet_name, startcol=startcol, startrow=startrow)
    startcol2nd = startcol + len(pivot_dict[key].columns) + distance

    re_compile = re.compile(f'(?:{key})(?:_R.*Yr)?(?!_)')
    stats_keys = list(filter(re_compile.match, pivot_stats.keys()))
    
    for stats_key in stats_keys:
        pivot_stats[stats_key].to_excel(excel_writer, index=False, sheet_name=sheet_name, 
                                        startcol=startcol2nd, startrow=startrow) 
        startcol2nd = startcol2nd + len(pivot_stats[stats_key].columns) + distance
