

from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# Personal module. 
from configuration.config import freq_cols, yr_range


# ----------------------------------------------------------------------
# Preprocessing & Data Summarisation On Ticker Data. 
# ---------------------------------------------------------------------- 

def init_preprocess(df_dict, freq_keys):
    '''
    Purpose: 
        1. Cast the column names to lowercase.
        2. Compute the price change.
        3. Extract year from the date column. 
        4. Create additional columns: month, week, trdr_day for 
           monthly, weekly, and daily data.
    
    Input  :
        df_dict      : Dictionary. Should contain ticker dataframe.
        freq_keys    : List of dictionary keys. Must contain monthly/weekly/daily. 
        
    Return :
        None.
    '''
    
    for freq in freq_keys:
        # Cast column names to lowercase. 
        df_dict[freq].columns = list(map(str.lower, df_dict[freq].columns))

        # Compute price difference.
        df_dict[freq]['price_diff'] = df_dict[freq]['adj close'].pct_change(periods=1)

        # Extract year from date. 
        df_dict[freq]['year'] = df_dict[freq]['date'].dt.year

    # Monthly data. 
    df_dict[freq_keys[0]][freq_cols[0]] = df_dict[freq_keys[0]]['date'].dt.month
    
    # Weekly data. 
    df_dict[freq_keys[1]][freq_cols[1]] = df_dict[freq_keys[1]]['date'].dt.week
    
    # Daily data (byMonth).
    df_dict[freq_keys[2]][freq_cols[0]] = df_dict[freq_keys[2]]['date'].dt.month
    df_dict[freq_keys[2]][freq_cols[1]] = df_dict[freq_keys[2]]['date'].dt.week
    df_dict[freq_keys[2]][freq_cols[2]] = df_dict[freq_keys[2]].set_index(keys='date')\
                                          .groupby(by=pd.Grouper(freq='M')).cumcount().values
    
    # Daily data (byWeekday).
    df_dict[freq_keys[3]][freq_cols[1]] = df_dict[freq_keys[3]]['date'].dt.week
    df_dict[freq_keys[3]][freq_cols[3]] = df_dict[freq_keys[3]]['date'].dt.weekday


def create_pivot(df_dict, pivot_dict, freq_keys, freq_cols, value):
    '''
    Purpose: 
        Create pivot table with 'year' as columns and 'month/week/trdr_day' as index.
    
    Input  :
        df_dict        : Dictionary. Should contain ticker dataframe.
        pivot_dict     : Dictionary. To contain the pivot tables.
        freq_keys      : List of dictionary keys. Must contain monthly/weekly/daily. 
        freq_cols      : List. Must contain month / week / trdr_day.
        Value          : String. The column to perform computation on.
        
    Return :
        None.
        
    Note   :
        No aggregation should be performed although 'aggfunc' is assigned 'mean'. 
        The value should be exactly the same as the data from df_ticker. 
    '''
    
    column = 'year'
    
    for col, freq in zip(freq_cols, freq_keys):    
        index = [col]
        if col == 'trdr_day': 
            index = ['month', col]
        elif col == 'weekday': 
            index = ['week', col]
    
        pivot_dict[freq] = df_dict[freq].pivot_table(values=value, index=index, 
                                                     columns=column, aggfunc='mean')

        
def summarise_pivot(pivot_dict, pivot_dict_stats, freq_keys, start_yr_range, end_yr):
    '''
    Purpose: 
        Perform statistical computation for month/week/trdr_day. 
    
    Input  :
        pivot_dict      : Dictionary. Should contain pivot tables. 
        pivot_dict_stats: Dictionary. To contain the summarised data from pivot_dict. 
        freq_keys       : List of dictionary keys. Must contain monthly/weekly/daily. 
        start_yr_range  : List. Range of starting year to summarise the data on. 
        end_yr          : Int. Ending year to summarise the data on. 

    Return :
        None.

    Note   :
        There are cells that contain NaN. A warning will raise if the entire cells 
        for calculating the mean or standard deviation are NaN. So far, the resulted 
        calculation contains no error. The 'count()' function also outputs the correct 
        counts. 
    '''
    
    for freq in freq_keys:
        repeated_start_yr = False
        
        for i, start_yr in enumerate(start_yr_range):
            if start_yr < start_yr_range[0]: 
                continue
            
            stats_key = f'{freq}_{yr_range[i]}'
            if start_yr == start_yr_range[0]:
                stats_key = freq 
                if repeated_start_yr:
                    continue
                repeated_start_yr = True
        
            # Compute the average price change across years. 
            # Store it as a DataFrame object. 
            pivot_dict_stats[stats_key] = pd.DataFrame(pivot_dict[freq].loc[:,start_yr:end_yr].mean(axis=1), 
                                                       columns=['avg_diff'])

            # Compute median price change.
            pivot_dict_stats[stats_key]['med_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].median(axis=1)

            # Compute total price change.
            pivot_dict_stats[stats_key]['tot_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].sum(axis=1)

            # Compute max and min price change.
            pivot_dict_stats[stats_key]['max_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].max(axis=1)
            pivot_dict_stats[stats_key]['min_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].min(axis=1)

            # Compute standard deviation. 
            pivot_dict_stats[stats_key]['std_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].std(axis=1)

            # Indicate whether the average price change is positive or negative. 
            pivot_dict_stats[stats_key]['up_overall'] = 0
            pivot_dict_stats[stats_key].loc[pivot_dict_stats[stats_key]['avg_diff'] > 0,'up_overall'] = 1

            # Compute average positive price change.
            df = pivot_dict[freq][pivot_dict[freq] > 0]
            pivot_dict_stats[stats_key]['pos_avg_diff'] = df.loc[:,start_yr:end_yr].mean(axis=1)

            # Count positive price change. 
            up_counts = df.loc[:,start_yr:end_yr].count(axis=1)
            pivot_dict_stats[stats_key]['up_counts'] = up_counts

            # Compute average negative price change.
            df = pivot_dict[freq][pivot_dict[freq] < 0]
            pivot_dict_stats[stats_key]['neg_avg_diff'] = df.loc[:,start_yr:end_yr].mean(axis=1)

            # Count negative price change. 
            down_counts = df.loc[:,start_yr:end_yr].count(axis=1)
            pivot_dict_stats[stats_key]['down_counts'] = down_counts

            # Compute the probability of up and down. 
            prob = (up_counts / (up_counts + down_counts)).round(4)
            pivot_dict_stats[stats_key]['up_prob'] = prob
            pivot_dict_stats[stats_key]['down_prob'] = 1 - prob


# ----------------------------------------------------------------------
# Preprocessing & Data Summarisation On Volume Data. 
# ---------------------------------------------------------------------- 

def compute_avgVol(pivot_dict, pivot_dict_avg, freq_keys, start_yr_range, end_yr):
    '''
    Purpose: 
        Compute the average volume across columns and rows.
    
    Input  :
        pivot_dict      : Dictionary. To contain the pivot tables for volume.
        pivot_dict_avg  : Dictionary. To contain the average data from pivot_dict. 
        freq_keys       : List of dictionary keys. Must contain monthly/weekly/daily. 
        start_yr_range  : List. Range of starting year to summarise the data on. 
        end_yr          : Int. Ending year to summarise the data on. 
        
    Return :
        None.

    Note   :
        There are cells that contain NaN. A warning will raise if the entire cells 
        for calculating the mean or standard deviation are NaN. So far, the resulted 
        calculation contains no error. 
    '''

    for freq in freq_keys:
        repeated_start_yr = False
        
        for i, start_yr in enumerate(start_yr_range):
            if start_yr < start_yr_range[0]: 
                continue
            
            stats_key = f'{freq}_{yr_range[i]}'
            if start_yr == start_yr_range[0]:
                stats_key = freq 
                if repeated_start_yr:
                    continue
                repeated_start_yr = True
                
            # Filter the columns to specific year range. 
            pivot_dict_copy = pivot_dict[freq].loc[:,start_yr:end_yr].copy()

            # Compute the average volume across columns and rows. 
            pivot_dict_avg[f'{stats_key}_avgVolRow'] = pd.DataFrame(pivot_dict_copy.mean(axis=1), 
                                                                    columns=['avgVolRow'])
            pivot_dict_avg[f'{stats_key}_avgVolCol'] = pd.DataFrame(pivot_dict_copy.mean(axis=0), 
                                                                    columns=['avgVolCol'])


def summarise_pivot_vol(pivot_dict, pivot_dict_stats, freq_keys, start_yr_range, end_yr):
    '''
    Purpose: 
        Count the months that have volume above and below the average volume for each year. 
    
    Input  :
        pivot_dict      : Dictionary. To contain the pivot tables for volume.
        pivot_dict_stats: Dictionary. To contain the summarised data from pivot_dict. 
        freq_keys       : List of dictionary keys. Must contain monthly/weekly/daily. 
        start_yr_range  : List. Range of starting year to summarise the data on. 
        end_yr          : Int. Ending year to summarise the data on. 
        
    Return :
        None.
        
    Note   :
        There are cells that contain NaN. A warning will raise if the entire cells 
        for calculating the mean or standard deviation are NaN. So far, the resulted 
        calculation contains no error. 
    '''
        
    for freq in freq_keys:
        repeated_start_yr = False
        
        for i, start_yr in enumerate(start_yr_range):
            if start_yr < start_yr_range[0]: 
                continue
            
            stats_key = f'{freq}_{yr_range[i]}'
            if start_yr == start_yr_range[0]:
                stats_key = freq 
                if repeated_start_yr:
                    continue
                repeated_start_yr = True
                
            # List containing dataframes for 'pandas concat'. 
            ls_df = []

            for year in range(start_yr, end_yr + 1, 1):
                # Find rows of each year that are above the average volume. 
                df_filteredYear = pivot_dict[freq][[year]].copy()
                avgVol = pivot_dict_stats[f'{stats_key}_avgVolCol'].loc[year,:][0]
                abv_avgVol = df_filteredYear[year] > avgVol
                blw_avgVol = df_filteredYear[year] < avgVol

                # Indicate the rows that are above the average volume. 
                df_filteredYear[f'abv_avgVol_{year}'] = np.nan
                df_filteredYear.loc[abv_avgVol, f'abv_avgVol_{year}'] = 1 
                df_filteredYear.loc[blw_avgVol, f'abv_avgVol_{year}'] = 0 

                ls_df.append(df_filteredYear[[f'abv_avgVol_{year}']]) 

            # Perform pandas concat. 
            pivot_dict_stats[stats_key] = pd.concat(ls_df, axis=1)

            # Count the total monthly volume that are above or below the average volume. 
            totalCounts = pivot_dict_stats[stats_key].count(axis=1) 
            abvCounts = pivot_dict_stats[stats_key].sum(axis=1)
            pivot_dict_stats[stats_key]['abv_avgVolCounts'] = abvCounts
            pivot_dict_stats[stats_key]['blw_avgVolCounts'] = totalCounts - abvCounts
            pivot_dict_stats[stats_key]['abv_avgVolProb'] = abvCounts / totalCounts


# ----------------------------------------------------------------------
# Preprocessing Holidays/Observances/SpecialDay Data. 
# ---------------------------------------------------------------------- 

def trace_specialDays(df_ticker_data, tup_superDay, tup_santaRally):
    '''
    Purpose:
        Add columns to the dataframe with the following: 
            1. Indicate the 'First Trading of the Month'.
            2. Indicate the Super Day period, day counts, and specific year.
            3. Indicate the Santa Rally period, day counts, and specific year.
    
    Input  :
        df_ticker_data  : Dataframe. Must be 'daily_byTrdrDay'. 
        tup_superDay    : Tuple of Super Day dates, day counts, and specific year.
        tup_santaRally  : Tuple of Santa Rally dates, day counts, and specific year.
        
    Return :
        None. 
    '''
    
    df_ticker_data['firstTrdrDoM'] = 0
    # Remove the first (start_yr - 1) and last date (end_yr + 1). 
    ls_daily_firstTrdrDate = df_ticker_data.loc[df_ticker_data['trdr_day'] == 0,'date'].dt.date.tolist()[1:-1]
    df_ticker_data.loc[df_ticker_data['date'].isin(ls_daily_firstTrdrDate), 'firstTrdrDoM'] = 1 
    
    # Assign tuples to 'period' and 'dayCounts' variable. 
    ls_superDay_period, ls_superDay_dayCounts, ls_superDay_specMonth, ls_superDay_specYear = tup_superDay
    ls_santaRally_period, ls_santaRally_dayCounts, ls_santaRally_specYear = tup_santaRally
    
    # Indicate rows that fall within the special day period. 
    df_ticker_data['superDay'] = 0  
    df_ticker_data.loc[df_ticker_data['date'].isin(ls_superDay_period), 'superDay'] = 1

    df_ticker_data['santaRally'] = 0 
    df_ticker_data.loc[df_ticker_data['date'].isin(ls_santaRally_period), 'santaRally'] = 1
    
    # Add the day counts and specific month or year for each period. 
    period_bool = df_ticker_data['superDay'] == 1 
    df_ticker_data.loc[period_bool, 'superDay_dayCounts'] = ls_superDay_dayCounts 
    df_ticker_data.loc[period_bool, 'superDay_specMonth'] = ls_superDay_specMonth
    df_ticker_data.loc[period_bool, 'superDay_specYear'] = ls_superDay_specYear
    
    period_bool = df_ticker_data['santaRally'] == 1 
    df_ticker_data.loc[period_bool, 'santaRally_dayCounts'] = ls_santaRally_dayCounts 
    df_ticker_data.loc[period_bool, 'santaRally_specYear'] = ls_santaRally_specYear
    
    
def trace_tww_trdrDays(df_ticker_data, df_tww):
    '''
    Purpose: 
        Trace all the Triple Witching Week (TWW) trading days. 
    
    Input  :
        df_ticker_data: Dataframe. Must be 'daily_byTrdrDay' or 'weekly'.
        df_tww        : Dateframe containing all the TWW dates. 
        
    Return :
        None. 
        
    Note   :
        There is a Christmas holiday the following week after TWW 
        on quarter 4.  
    '''

    tww_trdrDays_dict = {} 
    quarters = df_tww.columns
    
    for quarter in quarters:
        # Create new columns. 
        df_ticker_data[quarter] = 0
        df_ticker_data[f'{quarter}_weekAft'] = 0
        
        # Compile the years and weeks into an array.
        tww_years = df_tww[quarter].dt.year.values
        tww_weeks = df_tww[quarter].dt.week.values
        
        for year, week in zip(tww_years, tww_weeks):
            # Perform filter on specific year and week. 
            year_bool = df_ticker_data['year'] == year
            week_bool = df_ticker_data['week'] == week
            weekAft_bool = df_ticker_data['week'] == week + 1
            
            # Indicate the tradings days that fall within TWW and the week after. 
            df_ticker_data.loc[year_bool & week_bool, quarter] = 1 
            df_ticker_data.loc[year_bool & weekAft_bool, f'{quarter}_weekAft'] = 1 
        
        # Compute the day counts for each TWW period of each quarter. 
        insert_dayCountsCol(df_ticker_data, quarter)
        insert_dayCountsCol(df_ticker_data, f'{quarter}_weekAft')


def insert_dayCountsCol(df_ticker_data, period_col):
    '''
    Purpose:
        Create a new column for day counts of specific holiday, 
        observance, or special day period. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_byTrdrDay'. 
        period_col    : Str. Specific column of holiday, observance, or special day period. 
        
    Return :
        None. 
    '''
    
    period_bool = df_ticker_data[period_col] == 1
    df_ticker_data.loc[period_bool, f'{period_col}_dayCounts'] = df_ticker_data[period_bool]\
                                                                 .set_index(keys='date')\
                                                                 .groupby(by=pd.Grouper(freq='M'))\
                                                                 .cumcount().values


def insert_holidayCol(df_ticker_data, holidays_dict, holiday_col):
    '''
    Purpose:
        Create a new column from a holiday list. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_byTrdrDay'. 
        holidays_dict : Dictionary. Contains list of dates within for
                        for each holiday. 
        holiday_col   : Str. Name of the holiday to create new column.
        
    Return :
        None. 
    '''
    
    df_ticker_data[holiday_col] = 0 
    df_ticker_data.loc[df_ticker_data['date'].isin(holidays_dict[holiday_col]),holiday_col] = 1
    
    # Add the day counts for each period of each year. 
    period_bool = df_ticker_data[holiday_col] == 1 
    df_ticker_data.loc[period_bool, f'{holiday_col}_dayCounts'] = holidays_dict[f'{holiday_col}_dayCounts']
    
    # Add the specific year for each period. 
    df_ticker_data.loc[period_bool, f'{holiday_col}_specYear'] = holidays_dict[f'{holiday_col}_specYear'] 
    
    
def trace_newYear(df_ticker_data, holidays_dict, date_range=6): 
    '''
    Purpose:
        Trace all the New Year period. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_byTrdrDay'. 
        holidays_dict : Dictionary. Contains list of dates within for
                        for each holiday. 
        date_range    : Int. Total number of dates to indicate. 
                        If 6, then 3 trading days before the holiday plus 
                        3 trading days after the holiday. 
        
    Return :
        None. 
    '''
    
    holiday_col = 'newYear'
    holidays_dict[f'{holiday_col}_dayCounts'] = [] 
    holidays_dict[f'{holiday_col}_specYear'] = [] 
    
    # Remove the first (start_yr - 1) and last date (end_yr + 1). 
    month_bool = df_ticker_data['month'] == 1
    firstTrdrDate_bool = df_ticker_data['trdr_day'] == 0
    
    ls_daily_firstTrdrDate = df_ticker_data.loc[month_bool & firstTrdrDate_bool,'date'].dt.date.tolist()[:-1]
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist()
    
    for date in ls_daily_firstTrdrDate: 
        year = date.year
        idx = ls_daily_dates.index(date) - 3 
        
        for i in range(0,date_range,1): 
            idx_date = idx + i 
            holidays_dict[f'{holiday_col}_dayCounts'].append(i) 
            holidays_dict[f'{holiday_col}_specYear'].append(year)
            holidays_dict[holiday_col].append(datetime.combine(ls_daily_dates[idx_date], 
                                                               datetime.min.time())) 
            
    insert_holidayCol(df_ticker_data, holidays_dict, holiday_col)

            
def trace_specWeekdayHoliday(df_ticker_data, df_holidays, holidays_dict, holiday_col, 
                             day_forward=1, idx_backtrace=3, date_range=6): 
    '''
    Purpose:
        Trace all the holidays period which happens on a specific weekday. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_byTrdrDay'. 
        df_holidays   : Dataframe. Contains all the holiday dates. 
        holidays_dict : Dictionary. Contains list of dates within for
                        for each holiday. 
        holiday_col   : Str. Name of the holiday to create new column.
        day_forward   : Int. Move forward N number of days (not trading days).
        idx_backtrace : Int. Move backward N number of trading days.
        date_range    : Int. Total number of dates to indicate. 
                        If 6, then 3 trading days before the holiday plus 
                        3 trading days after the holiday. 
        
    Return :
        None. 

    Note   :
        This function will not work for holidays that fall on Friday. It only works
        for holidays that fall between Monday to Thursday. 
    ''' 
        
    holidays_dict[f'{holiday_col}_dayCounts'] = []
    holidays_dict[f'{holiday_col}_specYear'] = [] 
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist() 
    
    for date in df_holidays[holiday_col].tolist(): 
        year = date.date().year
        date = date + timedelta(days=day_forward) 
        idx = ls_daily_dates.index(date.date()) - idx_backtrace 
        
        for i in range(0,date_range,1): 
            idx_date = idx + i 
            holidays_dict[f'{holiday_col}_dayCounts'].append(i)
            holidays_dict[f'{holiday_col}_specYear'].append(year)
            holidays_dict[holiday_col].append(datetime.combine(ls_daily_dates[idx_date], 
                                                               datetime.min.time())) 
    
    insert_holidayCol(df_ticker_data, holidays_dict, holiday_col)
    
    
def trace_nonSpecHoliday(df_ticker_data, df_holidays, holidays_dict, holiday_col, date_range=6): 
    '''
    Purpose:
        Trace all the holidays period which happens on a non-specific weekday. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_byTrdrDay'. 
        df_holidays   : Dataframe. Contains all the holiday dates. 
        holidays_dict : Dictionary. Contains list of dates within for
                        for each holiday. 
        holiday_col   : Str. Name of the holiday to create new column.
        date_range    : Int. Total number of dates to indicate. 
                        If 6, then 3 trading days before the holiday plus 
                        3 trading days after the holiday. 
        
    Return :
        None. 
        
    Note   :
        If holiday falls on weekend, one day before or after the holiday 
        will be a holiday, depending on which is nearest. 
    ''' 
    
    holidays_dict[f'{holiday_col}_dayCounts'] = []
    holidays_dict[f'{holiday_col}_specYear'] = [] 
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist() 
    
    for date in df_holidays[holiday_col].tolist(): 
        year = date.date().year
        
        if date.weekday() == 4: 
            date = date - timedelta(days=1) 
            idx = ls_daily_dates.index(date.date()) - 2
        elif date.weekday() == 5:
            date = date - timedelta(days=2)
            idx = ls_daily_dates.index(date.date()) - 2
        elif date.weekday() == 6:
            date = date + timedelta(days=2)
            idx = ls_daily_dates.index(date.date()) - 3
        else:
            date = date + timedelta(days=1)
            idx = ls_daily_dates.index(date.date()) - 3
            
        for i in range(0,date_range,1): 
            idx_date = idx + i 
            holidays_dict[f'{holiday_col}_dayCounts'].append(i)
            holidays_dict[f'{holiday_col}_specYear'].append(year)
            holidays_dict[holiday_col].append(datetime.combine(ls_daily_dates[idx_date], 
                                                               datetime.min.time())) 
        
    insert_holidayCol(df_ticker_data, holidays_dict, holiday_col)
        

def trace_nonSpecObservance(df_ticker_data, df_holidays, holidays_dict, holiday_col, date_range=7):
    '''
    Purpose:
        Trace all the observances period which happens on a non-specific weekday. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_byTrdrDay'. 
        df_holidays   : Dataframe. Contains all the holiday dates. 
        holidays_dict : Dictionary. Contains list of dates within for
                        for each holiday. 
        holiday_col   : Str. Name of the holiday to create new column.
        date_range    : Int. Total number of dates to indicate. 
                        If 7, then 3 trading days before the observance plus
                        the day of the observance itself (if it's not on weekend)
                        plus 3 trading days after the observance. 
        
    Return : 
        None. 
    
    Note   :
        If the observances date falls on weekend, move one/two day forward until 
        it falls on weekday. If the day after is a holiday then move one 
        more day forward. Please be aware that the observance's effect might lost its 
        significance if the day is too far from the observance date. It will also affect 
        the overall statistical computation making its effect less even if there's really 
        a true effect. 
    ''' 
    
    holidays_dict[f'{holiday_col}_dayCounts'] = []
    holidays_dict[f'{holiday_col}_specYear'] = [] 
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist() 
    
    for date in df_holidays[holiday_col].tolist():
        year = date.date().year
        date_range = 7
        
        # Try running this to see if the date falls on weekend, holiday, or trading day. 
        # If error occurs due to weekend or holiday, run the 'except' section. 
        while True:
            try:
                idx = ls_daily_dates.index(date.date()) - 3
                for i in range(0,date_range,1):
                    idx_date = idx + i 
                    if i >= 3 and date_range == 6:
                        i += 1
                    holidays_dict[f'{holiday_col}_dayCounts'].append(i)
                    holidays_dict[f'{holiday_col}_specYear'].append(year)
                    holidays_dict[holiday_col].append(datetime.combine(ls_daily_dates[idx_date], 
                                                                       datetime.min.time())) 
                break
            except:
                date = date + timedelta(days=1)
                date_range = 6
                
    insert_holidayCol(df_ticker_data, holidays_dict, holiday_col)


def create_pivot_uniqueDays(df_ticker_data, pivot_dict, pivot_dict_keys, 
                            start_yr, end_yr):
    '''
    Purpose: 
        Create pivot tables for holidays, observances, and special day.
    
    Input  :
        df_ticker_data  : Dataframe. Must be 'daily_byTrdrDay'.
        pivot_dict      : Dictionary. To contain pivot tables.
        pivot_dict_keys : List of dictionary contains holidays, observances, 
                          or special day keys. 
        start_yr        : Int. Starting year to compile the data on. 
        end_yr          : Int. Ending year to compile the data on. 

    Return :
        None.
        
    Note   :
        1. Any special day like 'firstTrdrDoM' and 'superDay' which occur monthly 
           should include month as part of the groupby. Otherwise it will average 
           across all the different months. However, this depends on the analysis goal. 
        2. 'firstTrdrDoM' does not contain '_dayCounts'. 
        3. 'tww' does not contain '_specYear'.
        4. 'superDay' requires '_specMonth' instead of 'month' if groupby includes month. 
    '''
        
    for periodCol in pivot_dict_keys:       
        specYear = f'{periodCol}_specYear'
        idxCol = f'{periodCol}_dayCounts'
        
        if periodCol == 'firstTrdrDoM': 
            specYear = 'year'
            idxCol = periodCol
        elif 'tww' in periodCol:
            specYear = 'year'
        
        if 'byMonth' in periodCol:
            idxCol = periodCol[:-8]
            
            if idxCol == 'superDay': 
                specYear = f'{idxCol}_specYear' 
                idxCol = [f'{idxCol}_specMonth', f'{idxCol}_dayCounts']
            elif idxCol == 'firstTrdrDoM':
                specYear = 'year'
                idxCol = ['month', idxCol] 
            
        pivot_dict[periodCol] = df_ticker_data.pivot_table(values='price_diff', 
                                                           index=idxCol, columns=specYear, 
                                                           aggfunc='mean').loc[:,start_yr:end_yr]


def concat_pivot_tww(pivot_dict, pivot_dict_keys):
    '''
    Purpose: 
        Concat pivot tables for TWW data.
    
    Input  :
        pivot_dict     : Dictionary. Must be contain TWW dataframes.
        pivot_dict_keys: List. TWW dictionary indexing keys. 

    Return :
        None.
    '''
    
    for key in pivot_dict_keys:
        tww_key = key 
        tww_weekAft_key = f'{key}_weekAft'
        
        # Concat the 'TWW' and 'TWW week after' dataframes. 
        ls_df = [pivot_dict[tww_key], pivot_dict[tww_weekAft_key]]
        df_concat = pd.concat(ls_df, keys=('tww', 'tww_weekAft'), names=('twwPeriod', 'dayCounts'))
        
        # Update the TWW data. 
        pivot_dict[tww_key] = df_concat