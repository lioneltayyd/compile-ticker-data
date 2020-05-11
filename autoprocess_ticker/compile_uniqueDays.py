

# Python modules.
from datetime import datetime
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, USLaborDay, \
    USColumbusDay, USThanksgivingDay

import pandas as pd


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


def get_trdrHoliday_date(year):
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


def compile_trdrHoliday_dates(start_yr, end_yr, holidays_keys):
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
    
    generated_holidays = []
    
    # Compile the dates into a list of list.
    for year in range(start_yr, end_yr + 1, 1):
        generated_holidays.append(get_trdrHoliday_date(year))
        
    return pd.DataFrame(generated_holidays, columns=holidays_keys) 


# ----------------------------------------------------------------------
# Triple Witching Week (TWW).
# ----------------------------------------------------------------------

def get_tww_dates(start_yr, end_yr):
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

    tww_dict = {'twwQ1': [], 'twwQ2': [], 'twwQ3': [], 'twwQ4': []}

    for year in range(start_yr, end_yr + 1,1):
        for i, month in enumerate([3,6,9,12]):
            for day in range(15,22,1):
                date = datetime(year,month,day)
                # In python weekday = 4 means Friday. 
                if date.weekday() == 4: 
                    tww_dict[f'twwQ{i + 1}'].append(date)
                    break
                    
    return pd.DataFrame(tww_dict)


# ----------------------------------------------------------------------
# Santa Rally period.
# ----------------------------------------------------------------------

def get_santaRally_period(df_ticker_data, start_yr, end_yr):
    '''
    Purpose: 
        Compile all the Santa Rally dates into a 
        list for further analysis later. 
    
    Input  :
        df_ticker_data: Dataframe. Must be 'daily_byTrdrDay'. 
        start_yr      : Int. Starting year.
        end_yr        : Int. Ending year. 
        
    Return :
        Tuple containing all the Santa Rally dates, day counts, and specific year 
        of each period. 
        
    Note   :
        Santa Rally occurs at the last five trading days of the current year
        and the first two trading days of the next year. 
    '''
    
    ls_santaRally = []
    ls_santaRally_period = []
    ls_santaRally_dayCounts = []
    ls_santaRally_specYear = []
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist()
    
    # Find the last trading date of each year. 
    for year in range(start_yr - 1, end_yr + 1,1):
        for day in range(31,24,-1):
            date = datetime(year,12,day)
            if date.weekday() != 5 and date.weekday() != 6:
                ls_santaRally.append(date)
                break
                
    # Find rest of the trading days starting from the last trading day of each year. 
    for date in ls_santaRally:
        year = date.date().year
        idx = ls_daily_dates.index(date.date()) - 4
        
        for i in range(0,7,1):
            idx_date = idx + i
            ls_santaRally_dayCounts.append(i)
            ls_santaRally_specYear.append(year) 
            ls_santaRally_period.append(datetime.combine(ls_daily_dates[idx_date], datetime.min.time()))
            
    return ls_santaRally_period, ls_santaRally_dayCounts, ls_santaRally_specYear


# ----------------------------------------------------------------------
# SuperDay period.
# ----------------------------------------------------------------------

def get_superDay_period(df_ticker_data, date_range=5):
    '''
    Purpose: 
        Compile all the Super Day dates into a 
        list for further analysis later. 
    
    Input  :
        df_ticker_data: Dataframe. Must be 'daily_byTrdrDay'. 
        date_range    : Int. Total number of dates to indicate. 
                        If 6, then 3 trading days before the Super Day plus 
                        2 trading days after the Super Day. 
        
    Return :
        Tuple containing all the Super Day dates, day counts, and specific year 
        of each period. 
        
    Note   :
        Super Day period may change depending on the market. 
    '''
    
    ls_superDay_period = []
    ls_superDay_dayCounts = []
    ls_superDay_specMonth = []
    ls_superDay_specYear = []
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist()
    
    # Find the first trading date of each month. 
    # Remove the first (start_yr - 1) and last date (end_yr + 1). 
    ls_daily_firstTrdrDate = df_ticker_data.loc[df_ticker_data['trdr_day'] == 0, 'date'].dt.date.tolist()[1:-1]
                
    # Find rest of the trading days starting from the last trading day of each year. 
    for date in ls_daily_firstTrdrDate:
        month = date.month
        year = date.year
        idx = ls_daily_dates.index(date) - 3
        
        for i in range(0,date_range,1):
            idx_date = idx + i
            ls_superDay_dayCounts.append(i) 
            ls_superDay_specMonth.append(month) 
            ls_superDay_specYear.append(year) 
            ls_superDay_period.append(datetime.combine(ls_daily_dates[idx_date], datetime.min.time())) 
            
    return ls_superDay_period, ls_superDay_dayCounts, ls_superDay_specMonth, ls_superDay_specYear 