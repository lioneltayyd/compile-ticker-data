

from datetime import datetime
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, USLaborDay, \
    USColumbusDay, USThanksgivingDay
from typing import Text, List
import logging
import pandas as pd 

# Personal modules. 
from config.config_logger import setup_logger
from config.config import LOG_PROCESSING_FILEPATH


# --------------------------------------------------------------
# Logger setup. 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_PROCESSING_FILEPATH) 


# ----------------------------------------------------------------------
# Holidays.
# ----------------------------------------------------------------------

class trdrHolidays(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearDay', month=1, day=1, observance=None),
        USMartinLutherKingJr,
        Holiday('Valentine', month=2, day=14, observance=None),
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=None),
        USLaborDay,
        Holiday('USEvent911', month=9, day=11, observance=None),
        USColumbusDay,
        Holiday('USVeterans', month=11, day=11, observance=None),
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=None)
    ]


def get_trdr_holiday_date(year:int):
    '''
    Purpose: 
        Identify the date of the holidays or special days. 
    
    Input  :
        year: Int. The specific year for identifying the 
              holiday and special day date. 
        
    Return :
        List of holiday and special day dates for specific year.
    '''
    
    inst = trdrHolidays()
    return inst.holidays(datetime(year, 1, 1), datetime(year, 12, 31)) 


def compile_trdr_holiday_dates(start_yr:Text, end_yr:Text, holidays_keys:List[Text]):
    '''
    Purpose: 
        Compile all the holiday and special day dates into a dataframe 
        for further analysis later. 
    
    Input  :
        start_yr: Int. Starting year.
        end_yr  : Int. Ending year. 
        
    Return :
        Dateframe containing all the holiday and special day dates.
    '''

    logger.info('Start running (compile_trdrHoliday_dates) function.')
    
    # Compile the dates into a list of list.
    generated_holidays = [get_trdr_holiday_date(year) for year in range(start_yr, end_yr + 1, 1)] 
    logger.debug('----- Generated the holidays dates')
    return pd.DataFrame(generated_holidays, columns=holidays_keys) 


# ----------------------------------------------------------------------
# Triple Witching Week (TWW).
# ----------------------------------------------------------------------

def get_tww_dates(start_yr:Text, end_yr:Text):
    '''
    Purpose: 
        Compile all the Triple Witching Week (TWW) dates into a 
        dataframe for further analysis later. 
    
    Input  :
        start_yr: Int. Starting year.
        end_yr  : Int. Ending year. 
        
    Return :
        Dateframe containing all the Triple Witching Week (TWW) dates.
        
    Note   :
        TWW only appears on Friday of the third week of each quarter. 
    '''

    logger.info('Start running (get_tww_dates) function.')

    tww_dict = {'tww_q1': [], 'tww_q2': [], 'tww_q3': [], 'tww_q4': []}

    for year in range(start_yr, end_yr + 1, 1):
        for i, month in enumerate([3,6,9,12]):
            for day in range(15,22,1):
                date = datetime(year,month,day)
                # In python weekday = 4 means Friday. 
                if date.weekday() == 4: 
                    tww_dict[f'tww_q{i + 1}'].append(date)
                    break

    logger.debug('----- Generated the TWW dates')
    return pd.DataFrame(tww_dict)


# ----------------------------------------------------------------------
# Santa Rally period.
# ----------------------------------------------------------------------

def get_santa_rally_period(df_ticker_data:pd.DataFrame, start_yr:Text, end_yr:Text):
    '''
    Purpose: 
        Compile all the Santa Rally dates into a 
        list for further analysis later. 
    
    Input  :
        df_ticker_data: Dataframe. Must be 'daily_by_trdr_day'. 
        start_yr      : Int. Starting year.
        end_yr        : Int. Ending year. 
        
    Return :
        Tuple containing all the Santa Rally dates, day counts, and specific year 
        of each period. 
        
    Note   :
        Santa Rally occurs at the last five trading days of the current year
        and the first two trading days of the next year. 
    '''

    logger.info('Start running (get_santa_rally_period) function.')
    
    ls_santa_rally = []
    ls_santa_rally_period = []
    ls_santa_rally_day_counts = []
    ls_santa_rally_spec_year= []
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist()
    
    # Find the last trading date of each year. 
    # Ignore weekends. 0 == Monday, 6 == Sunday. 
    for year in range(start_yr - 1, end_yr + 1,1):
        for day in range(31,24,-1):
            date = datetime(year,12,day)
            if date.weekday() != 5 and date.weekday() != 6:
                ls_santa_rally.append(date)
                break
                
    # Find rest of the trading days starting from the last trading day of each year. 
    for date in ls_santa_rally:
        year = date.date().year
        idx = ls_daily_dates.index(date.date()) - 4
        
        for i in range(0,7,1):
            idx_date = idx + i
            ls_santa_rally_day_counts.append(i)
            ls_santa_rally_spec_year.append(year) 
            ls_santa_rally_period.append(datetime.combine(ls_daily_dates[idx_date], datetime.min.time()))
    
    logger.debug('----- Generated the Santa Rally dates')
    return ls_santa_rally_period, ls_santa_rally_day_counts, ls_santa_rally_spec_year


# ----------------------------------------------------------------------
# SuperDay period.
# ----------------------------------------------------------------------

def get_super_day_period(df_ticker_data:pd.DataFrame, date_range:int=5):
    '''
    Purpose: 
        Compile all the Super Day dates into a 
        list for further analysis later. 
    
    Input  :
        df_ticker_data: Dataframe. Must be 'daily_by_trdr_day'. 
        date_range    : Int. Total number of dates to indicate. 
                        If 6, then 3 trading days before the Super Day plus 
                        2 trading days after the Super Day. 
        
    Return :
        Tuple containing all the Super Day dates, day counts, and specific year 
        of each period. 
        
    Note   :
        Super Day period may change depending on the market. 
    '''

    logger.info('Start running (get_super_day_period) function.')
    
    ls_super_day_period = []
    ls_super_day_day_counts = []
    ls_super_day_spec_month = []
    ls_super_day_spec_year= []
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist()
    
    # Find the first trading date of each month. 
    # Remove the first (start_yr - 1) and last date (end_yr + 1). 
    ls_daily_first_trdr_date = df_ticker_data.loc[df_ticker_data['trdr_day'] == 0, 'date'].dt.date.tolist()[1:-1]
                
    # Find rest of the trading days starting from the last trading day of each year. 
    for date in ls_daily_first_trdr_date:
        month = date.month
        year = date.year
        idx = ls_daily_dates.index(date) - 3
        
        for i in range(0,date_range,1):
            idx_date = idx + i
            ls_super_day_day_counts.append(i) 
            ls_super_day_spec_month.append(month) 
            ls_super_day_spec_year.append(year) 
            ls_super_day_period.append(datetime.combine(ls_daily_dates[idx_date], datetime.min.time())) 
        
    logger.debug('----- Generated the Super Day dates')
    return ls_super_day_period, ls_super_day_day_counts, ls_super_day_spec_month, ls_super_day_spec_year
