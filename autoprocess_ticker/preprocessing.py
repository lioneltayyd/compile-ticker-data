

from datetime import datetime, timedelta
import logging
from typing import List, Tuple, Dict, Optional, Text
import numpy as np
import pandas as pd

# Personal module. 
from config.config import (
    HOLIDAYS_KEYS, LOG_PROCESSING_FILEPATH, YR_RANGE, 
)
from config.config_logger import setup_logger


# --------------------------------------------------------------
# Logger setup. 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_PROCESSING_FILEPATH) 


# ----------------------------------------------------------------------
# Preprocessing & Data Summarisation On Ticker Data. 
# ---------------------------------------------------------------------- 

def init_preprocess(df_dict:Dict[Text, pd.DataFrame], freq_keys:List[Text], freq_cols:List[Text]):
    '''
    Purpose: 
        1. Cast the column names to lowercase.
        2. Compute the price change.
        3. Extract year from the date column. 
        4. Create additional columns: month, week, trdr_day, weekday for 
           monthly, weekly, and daily data.
    
    Input  :
        df_dict      : Dictionary. Should contain ticker dataframe.
        freq_keys    : List. Example: ['monthly', 'weekly', 'daily_by_trdr_day', 'daily_by_weekday'] 
        freq_cols    : List. Example: ['month', 'week', 'trdr_day', 'weekday']
        
    Return :
        None.
    '''
    
    logger.info('Start running (init_preprocess) function.')

    for freq in freq_keys:
        # Cast column names to lowercase. 
        df_dict[freq].columns = list(map(str.lower, df_dict[freq].columns))
        logger.debug(f'----- Casted column name to lowercase -- ({df_dict[freq].columns}).')

        # Compute price difference.
        df_dict[freq]['price_diff'] = df_dict[freq]['adj close'].pct_change(periods=1)
        logger.debug('----- Added (price_diff) column.')

        # Extract year from date. 
        df_dict[freq]['year'] = df_dict[freq]['date'].dt.year
        logger.debug('----- Added (year) column.')

    # Monthly data. 
    df_dict[freq_keys[0]][freq_cols[0]] = df_dict[freq_keys[0]]['date'].dt.month
    logger.debug(f'----- Added ({freq_cols[0]}) column for ({freq_keys[2]}) ticker data.')
    
    # Weekly data. 
    df_dict[freq_keys[1]][freq_cols[1]] = df_dict[freq_keys[1]]['date'].dt.week
    logger.debug(f'----- Added ({freq_cols[1]}) column for ({freq_keys[1]}) ticker data.')
    
    # Daily data (by_month).
    df_dict[freq_keys[2]][freq_cols[0]] = df_dict[freq_keys[2]]['date'].dt.month
    df_dict[freq_keys[2]][freq_cols[1]] = df_dict[freq_keys[2]]['date'].dt.week
    df_dict[freq_keys[2]][freq_cols[2]] = df_dict[freq_keys[2]].set_index(keys='date')\
                                          .groupby(by=pd.Grouper(freq='M')).cumcount().values
    logger.debug(f'----- Added ({freq_cols[2]}) column for ({freq_keys[2]}) ticker data.')
    
    # Daily data (byWeekday).
    df_dict[freq_keys[3]][freq_cols[1]] = df_dict[freq_keys[3]]['date'].dt.week
    df_dict[freq_keys[3]][freq_cols[3]] = df_dict[freq_keys[3]]['date'].dt.weekday
    logger.debug(f'----- Added ({freq_cols[3]}) column for ({freq_keys[3]}) ticker data.')


def custom_set_index(
        pivot_dict:Dict[Text, pd.DataFrame], 
        pivot_dict_stats:Dict[Text, pd.DataFrame], 
        stats_key:Text, 
        freq:Text, 
    ):

    '''
    Purpose: 
        To set custom index for pivot table (dataframe). 
    
    Input  :
        pivot_dict      : Dictionary. Containing the pivot tables.
        pivot_dict_stats: Dictionary. For storing processes pivot table. 
        stats_key       : Str. Specify the dictionary key for storing the specific dataframe. 
        freq            : Str. Must be 'monthly' / 'weekly' / 'daily_by_trdr_day' / 'daily_by_weekday'. 
        
    Return :
        None.
    '''

    # REFINE: Need to find a better way to set the index. 
    # Add new column(s) to indicate the interval. 
    if  freq == 'monthly': 
        pivot_dict_stats[stats_key]['month'] = pivot_dict[freq]['month'] 
        logger.debug(f'----- Added the (month) column for ({stats_key}) ticker data.') 
    elif  freq == 'weekly': 
        pivot_dict_stats[stats_key]['week'] = pivot_dict[freq]['week'] 
        logger.debug(f'----- Added the (month) column for ({stats_key}) ticker data.') 
    elif freq == 'daily_by_trdr_day':
        pivot_dict_stats[stats_key]['month'] = pivot_dict[freq]['month']
        pivot_dict_stats[stats_key]['trdr_day'] = pivot_dict[freq]['trdr_day']
        logger.debug(f'----- Added the (month) and (trdr_day) column for ({stats_key}) ticker data.') 
    elif freq == 'daily_by_weekday':
        pivot_dict_stats[stats_key]['week'] = pivot_dict[freq]['week']
        pivot_dict_stats[stats_key]['weekday'] = pivot_dict[freq]['weekday']
        logger.debug(f'----- Added the (week) and (weekday) column for ({stats_key}) ticker data.') 
    elif freq == 'first_trdr_dom': 
        pivot_dict_stats[stats_key][freq] = pivot_dict[freq][freq]
        logger.debug(f'----- Added the ({freq}) column for ({stats_key}) ticker data.') 
    elif freq == 'super_day' or freq == 'santa_rally': 
        pivot_dict_stats[stats_key][freq] = pivot_dict[freq][f'{freq}_day_counts']
        logger.debug(f'----- Added the ({freq}) column for ({stats_key}) ticker data.') 
    elif 'first_trdr_dom_by_month' == freq: 
        pivot_dict_stats[stats_key]['month'] = pivot_dict[freq]['month']
        pivot_dict_stats[stats_key]['first_trdr_dom'] = pivot_dict[freq]['first_trdr_dom']
        logger.debug(f'----- Added the (month) and (first_trdr_dom) column for ({stats_key}) ticker data.') 
    elif 'super_day_by_month' == freq: 
        pivot_dict_stats[stats_key]['super_day_spec_month'] = pivot_dict[freq]['super_day_spec_month']
        pivot_dict_stats[stats_key]['super_day_day_counts'] = pivot_dict[freq]['super_day_day_counts']
        logger.debug(f'----- Added the (super_day_spec_month) and (super_day_day_counts) column for ({stats_key}) ticker data.') 
    elif 'tww' in freq: 
        pivot_dict_stats[stats_key]['tww_period'] = pivot_dict[freq]['tww_period']
        pivot_dict_stats[stats_key]['day_counts'] = pivot_dict[freq]['day_counts']
        logger.debug(f'----- Added the (tww_period) and (day_counts) column for ({stats_key}) ticker data.') 
    else: 
        pivot_dict_stats[stats_key]['holiday_category'] = pivot_dict[freq]['holiday_category']
        pivot_dict_stats[stats_key]['day_counts'] = pivot_dict[freq]['day_counts']
        logger.debug(f'----- Added the (holiday_category) and (day_counts) column for ({stats_key}) ticker data.') 


def create_pivot(
        df_dict:Dict[Text, pd.DataFrame], 
        pivot_dict:Dict[Text, pd.DataFrame], 
        freq_keys:List[Text], 
        freq_cols:List[Text], 
        pivot_value:Text
    ):

    '''
    Purpose: 
        Create pivot table with 'year' as columns and 'month/week/trdr_day/weekday' 
        as index.
    
    Input  :
        df_dict        : Dictionary. Should contain ticker dataframe.
        pivot_dict     : Dictionary. To contain the pivot tables.
        freq_keys      : List. Example: ['monthly', 'weekly', 'daily_by_trdr_day', 'daily_by_weekday'] 
        freq_cols      : List. Example: ['month', 'week', 'trdr_day', 'weekday']
        pivot_value    : String. The column to perform processing on.
        
    Return :
        None.
        
    Note   :
        No aggregation should be performed although 'aggfunc' is assigned as 'mean'. 
        The value from 'pivot_value' should be exactly the same as the original value. 
    '''

    logger.info('Start running (create_pivot) function.')
    
    column = 'year'
    
    for freq_col, freq in zip(freq_cols, freq_keys):
        # Modified the 'index' parameter for 'pivot_table' depending 
        # on the value from 'freq_cols'. 
        index = [freq_col]
        if freq_col == 'trdr_day': index = ['month', freq_col]
        elif freq_col == 'weekday': index = ['week', freq_col]

        # Create a pivot table to display the 'price_diff' for each month / week / trading days for each year.  
        pivot_dict[freq] = df_dict[freq].pivot_table(values=pivot_value, index=index, 
                                                     columns=column, aggfunc='mean')
        logger.debug(f'----- Created a pivot table for ({freq}) ticker data.')

        # Reset the index. Some visualisation tools like 'Tableau' require 
        # a specific format of data structure to process and visualise the data. 
        pivot_dict[freq].reset_index(inplace=True)
        logger.debug(f'----- Resetted the index for ({freq}) ticker data.') 

        
def summarise_pivot(
        pivot_dict:Dict[Text, pd.DataFrame], 
        pivot_dict_stats:Dict[Text, pd.DataFrame], 
        freq_keys:List[Text], 
        start_yr_range:List[Text], 
        end_yr:Text
    ):

    '''
    Purpose: 
        Perform statistical computation for month/week/trdr_day/weekday.
    
    Input  :
        pivot_dict      : Dictionary. Should contain pivot tables.
        pivot_dict_stats: Dictionary. To contain the summarised data from pivot_dict.
        freq_keys       : List. Example: ['monthly', 'weekly', 'daily_by_trdr_day', 'daily_by_weekday'] 
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

    logger.info('Start running (summarise_pivot) function.')
    
    for freq in freq_keys:
        repeated_start_yr = False
        
        for i, start_yr in enumerate(start_yr_range):
            # Skip this 'start_yr' if the ticker doesn't contain data for that year. 
            if start_yr < start_yr_range[0]: 
                continue
            
            # Out: Examples for 'stats_key' -- 'monthly' / 'monthly_R20Yr' / 'monthly_R15Yr'. 
            stats_key = f'{freq}_{YR_RANGE[i]}'

            # Remove the 'max_yr' (maximum years) from 'stats_key' 
            # if 'start_yr' == the beginning of the 'start_yr_range' to avoid 
            # variable naming error at later stage if the name contains 'max_yr'. 
            if start_yr == start_yr_range[0]:
                stats_key = freq 
                if repeated_start_yr:
                    continue
                repeated_start_yr = True
        
            # Compute the average price change across years. 
            # Store it as a DataFrame object. 
            pivot_dict_stats[stats_key] = pd.DataFrame(pivot_dict[freq].loc[:,start_yr:end_yr].mean(axis=1), 
                                                       columns=['avg_diff'])
            logger.debug(f'----- Created a column (avg_diff) for ({stats_key}) ticker data.')

            # Compute median price change.
            pivot_dict_stats[stats_key]['med_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].median(axis=1)
            logger.debug(f'----- Created a column (med_diff) for ({stats_key}) ticker data.')

            # Compute total price change.
            pivot_dict_stats[stats_key]['tot_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].sum(axis=1)
            logger.debug(f'----- Created a column (tot_diff) for ({stats_key}) ticker data.')

            # Compute max and min price change.
            pivot_dict_stats[stats_key]['max_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].max(axis=1)
            pivot_dict_stats[stats_key]['min_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].min(axis=1)
            logger.debug(f'----- Created columns (max_diff) and (min_diff) for ({stats_key}) ticker data.')

            # Compute standard deviation. 
            pivot_dict_stats[stats_key]['std_diff'] = pivot_dict[freq].loc[:,start_yr:end_yr].std(axis=1)
            logger.debug(f'----- Created a column (std_diff) for ({stats_key}) ticker data.')

            # Indicate whether the average price change is positive or negative. 
            pivot_dict_stats[stats_key]['up_overall'] = 0
            pivot_dict_stats[stats_key].loc[pivot_dict_stats[stats_key]['avg_diff'] > 0,'up_overall'] = 1
            logger.debug(f'----- Created a column (up_overall) for ({stats_key}) ticker data.')

            # Compute average positive price change.
            df = pivot_dict[freq][pivot_dict[freq].loc[:,start_yr:end_yr] > 0]
            pivot_dict_stats[stats_key]['pos_avg_diff'] = df.loc[:,start_yr:end_yr].mean(axis=1)
            logger.debug(f'----- Created a column (pos_avg_diff) for ({stats_key}) ticker data.')

            # Count positive price change. 
            up_counts = df.loc[:,start_yr:end_yr].count(axis=1)
            pivot_dict_stats[stats_key]['up_counts'] = up_counts
            logger.debug(f'----- Created a column (up_counts) for ({stats_key}) ticker data.')

            # Compute average negative price change.
            df = pivot_dict[freq][pivot_dict[freq].loc[:,start_yr:end_yr] < 0]
            pivot_dict_stats[stats_key]['neg_avg_diff'] = df.loc[:,start_yr:end_yr].mean(axis=1)
            logger.debug(f'----- Created a column (neg_avg_diff) for ({stats_key}) ticker data.')

            # Count negative price change. 
            down_counts = df.loc[:,start_yr:end_yr].count(axis=1)
            pivot_dict_stats[stats_key]['down_counts'] = down_counts
            logger.debug(f'----- Created a column (down_counts) for ({stats_key}) ticker data.')

            # Compute the probability of up and down. 
            prob = (up_counts / (up_counts + down_counts)).round(4)
            pivot_dict_stats[stats_key]['up_prob'] = prob
            pivot_dict_stats[stats_key]['down_prob'] = 1 - prob
            logger.debug(f'----- Created columns (up_prob) and (down_prob) for ({stats_key}) ticker data.')
            
            # Add new column(s) to indicate the interval. 
            custom_set_index(pivot_dict, pivot_dict_stats, stats_key, freq)


# ----------------------------------------------------------------------
# Preprocessing & Data Summarisation On Volume Data. 
# ---------------------------------------------------------------------- 

def compute_avg_vol(
        pivot_dict:Dict[Text, pd.DataFrame], 
        pivot_dict_avg:Dict[Text, pd.DataFrame], 
        freq_keys:List[Text], 
        start_yr_range:List[Text], 
        end_yr:Text
    ):

    '''
    Purpose: 
        Compute the average volume across columns and rows.
    
    Input  :
        pivot_dict      : Dictionary. To contain the pivot tables for volume.
        pivot_dict_avg  : Dictionary. To contain the average data from pivot_dict. 
        freq_keys       : List. Example: ['monthly', 'weekly', 'daily_by_trdr_day', 'daily_by_weekday'] 
        start_yr_range  : List. Range of starting year to summarise the data on. 
        end_yr          : Int. Ending year to summarise the data on. 
        
    Return :
        None.

    Note   :
        There are cells that contain NaN. A warning will raise if the entire cells 
        for calculating the mean or standard deviation are NaN. So far, the resulted 
        calculation contains no error. 
    '''

    logger.info('Start running (compute_avg_vol) function.')

    for freq in freq_keys:
        repeated_start_yr = False
        
        for i, start_yr in enumerate(start_yr_range):
            # Skip this 'start_yr' if the ticker doesn't contain data for that year. 
            if start_yr < start_yr_range[0]: 
                continue
            
            # Out: Examples for 'stats_key' -- 'monthly' / 'monthly_R20Yr' / 'monthly_R15Yr'. 
            stats_key = f'{freq}_{YR_RANGE[i]}'

            # Remove the 'max_yr' (maximum years) from 'stats_key' 
            # if 'start_yr' == the beginning of the 'start_yr_range' to avoid 
            # variable naming error at later stage if the name contains 'max_yr'. 
            if start_yr == start_yr_range[0]:
                stats_key = freq 
                if repeated_start_yr:
                    continue
                repeated_start_yr = True
                
            # Filter the columns to specific year range. 
            pivot_dict_copy = pivot_dict[freq].loc[:,start_yr:end_yr].copy()

            # Compute the average volume across columns and rows. 
            pivot_dict_avg[f'{stats_key}_avg_vol_row'] = pd.DataFrame(pivot_dict_copy.mean(axis=1), 
                                                                      columns=['avg_vol_row'])
            pivot_dict_avg[f'{stats_key}_avg_vol_col'] = pd.DataFrame(pivot_dict_copy.mean(axis=0), 
                                                                      columns=['avg_vol_col'])
            logger.debug(f'----- Created a column (avg_vol_row) for ({stats_key}_avg_vol_row) ticker volume data.')
            logger.debug(f'----- Created a column (avg_vol_col) for ({stats_key}_avg_vol_col) ticker volume data.')

            # Add new column(s) to indicate the interval. 
            custom_set_index(pivot_dict, pivot_dict_avg, f'{stats_key}_avg_vol_row', freq)


def summarise_pivot_vol(
        pivot_dict:Dict[Text, pd.DataFrame], 
        pivot_dict_stats:Dict[Text, pd.DataFrame], 
        freq_keys:List[Text], 
        start_yr_range:List[Text], 
        end_yr:Text
    ):

    '''
    Purpose: 
        Count the months that have volume above and below the average volume for each year. 
    
    Input  :
        pivot_dict      : Dictionary. To contain the pivot tables for volume.
        pivot_dict_stats: Dictionary. To contain the summarised data from pivot_dict. 
        freq_keys       : List. Example: ['monthly', 'weekly', 'daily_by_trdr_day', 'daily_by_weekday'] 
        start_yr_range  : List. Range of starting year to summarise the data on. 
        end_yr          : Int. Ending year to summarise the data on. 
        
    Return :
        None.
        
    Note   :
        There are cells that contain NaN. A warning will raise if the entire cells 
        for calculating the mean or standard deviation are NaN. So far, the resulted 
        calculation contains no error. 
    '''

    logger.info('Start running (summarise_pivot_vol) function.')
        
    for freq in freq_keys:
        repeated_start_yr = False
        
        for i, start_yr in enumerate(start_yr_range):
            # Skip this 'start_yr' if the ticker doesn't contain data for that year. 
            if start_yr < start_yr_range[0]: 
                continue
            
            # Out: Examples for 'stats_key' -- 'monthly' / 'monthly_R20Yr' / 'monthly_R15Yr'. 
            stats_key = f'{freq}_{YR_RANGE[i]}'

            # Remove the 'max_yr' (maximum years) from 'stats_key' 
            # if 'start_yr' == the beginning of the 'start_yr_range' to avoid 
            # variable naming error at later stage if the name contains 'max_yr'. 
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
                avg_vol = pivot_dict_stats[f'{stats_key}_avg_vol_col'].loc[year,:][0]
                abv_avg_vol = df_filteredYear[year] > avg_vol
                blw_avgVol = df_filteredYear[year] < avg_vol

                # Indicate the rows that are above the average volume. 
                df_filteredYear[f'abv_avg_vol_{year}'] = np.nan
                df_filteredYear.loc[abv_avg_vol, f'abv_avg_vol_{year}'] = 1 
                df_filteredYear.loc[blw_avgVol, f'abv_avg_vol_{year}'] = 0 

                ls_df.append(df_filteredYear[[f'abv_avg_vol_{year}']]) 
                logger.debug(f'----- Created a column (abv_avg_vol_{year}) for ({stats_key}_avg_vol_col) ticker volume data.')

            # Perform pandas concat. 
            pivot_dict_stats[stats_key] = pd.concat(ls_df, axis=1)

            # Count the total monthly volume that are above or below the average volume. 
            totalCounts = pivot_dict_stats[stats_key].count(axis=1) 
            abvCounts = pivot_dict_stats[stats_key].sum(axis=1)
            pivot_dict_stats[stats_key]['abv_avg_vol_counts'] = abvCounts
            pivot_dict_stats[stats_key]['blw_avg_vol_counts'] = totalCounts - abvCounts
            pivot_dict_stats[stats_key]['abv_avg_vol_prob'] = abvCounts / totalCounts
            logger.debug(f'----- Created columns (abv_avg_vol_counts), (blw_avg_vol_counts), (abv_avg_vol_prob) for ({stats_key}) ticker volume data.')

            # Add new column(s) to indicate the interval. 
            custom_set_index(pivot_dict, pivot_dict_stats, stats_key, freq)


# ----------------------------------------------------------------------
# Preprocessing Holidays/Observances/SpecialDay Data. 
# ---------------------------------------------------------------------- 

def trace_special_days(df_ticker_data:pd.DataFrame, tup_super_day:Tuple[List], tup_santa_rally:Tuple[List]):
    '''
    Purpose:
        Add columns to the dataframe with the following: 
            1. Indicate the 'First Trading of the Month'.
            2. Indicate the Super Day period, day counts, and specific year.
            3. Indicate the Santa Rally period, day counts, and specific year.
    
    Input  :
        df_ticker_data  : Dataframe. Must be 'daily_by_trdr_day'. 
        tup_super_day    : Tuple of Super Day dates, day counts, and specific year.
        tup_santa_rally  : Tuple of Santa Rally dates, day counts, and specific year.
        
    Return :
        None. 
    '''

    logger.info('Start running (trace_special_days) function.')
    
    df_ticker_data['first_trdr_dom'] = 0
    # Remove the first (start_yr - 1) and last date (end_yr + 1). 
    ls_daily_first_trdr_date = df_ticker_data.loc[df_ticker_data['trdr_day'] == 0,'date'].dt.date.tolist()[1:-1]
    df_ticker_data.loc[df_ticker_data['date'].isin(ls_daily_first_trdr_date), 'first_trdr_dom'] = 1 
    logger.debug(f'----- Created a column (first_trdr_dom) for (trdr_day) ticker data.')
    
    # Assign tuples to 'period' and 'day_counts' variable. 
    ls_super_day_period, ls_super_day_day_counts, ls_super_day_spec_month, ls_super_day_spec_year = tup_super_day
    ls_santa_rally_period, ls_santa_rally_day_counts, ls_santa_rally_spec_year = tup_santa_rally
    
    # Indicate rows that fall within the special day period. 
    df_ticker_data['super_day'] = 0  
    df_ticker_data.loc[df_ticker_data['date'].isin(ls_super_day_period), 'super_day'] = 1
    logger.debug(f'----- Created a column (super_day) for (trdr_day) ticker data.')

    df_ticker_data['santa_rally'] = 0 
    df_ticker_data.loc[df_ticker_data['date'].isin(ls_santa_rally_period), 'santa_rally'] = 1
    logger.debug(f'----- Created a column (santa_rally) for (trdr_day) ticker data.')
    
    # Add the day counts, specific month, and specific year for each period. 
    period_bool = df_ticker_data['super_day'] == 1 
    df_ticker_data.loc[period_bool, 'super_day_day_counts'] = ls_super_day_day_counts 
    df_ticker_data.loc[period_bool, 'super_day_spec_month'] = ls_super_day_spec_month
    df_ticker_data.loc[period_bool, 'super_day_spec_year'] = ls_super_day_spec_year
    logger.debug(f'----- Created columns (super_day_day_counts), (super_day_spec_month), (super_day_spec_year) for (trdr_day) ticker data.')
    
    # Add the day counts and specific year or year for each period. 
    period_bool = df_ticker_data['santa_rally'] == 1 
    df_ticker_data.loc[period_bool, 'santa_rally_day_counts'] = ls_santa_rally_day_counts 
    df_ticker_data.loc[period_bool, 'santa_rally_spec_year'] = ls_santa_rally_spec_year
    logger.debug(f'----- Created columns (santa_rally_day_counts), (santa_rally_spec_year) for (trdr_day) ticker data.')
    
    
def trace_tww_trdr_days(df_ticker_data:pd.DataFrame, df_tww:pd.DataFrame):
    '''
    Purpose: 
        Trace all the Triple Witching Week (TWW) trading days. 
    
    Input  :
        df_ticker_data: Dataframe. Must be 'daily_by_trdr_day' or 'weekly'.
        df_tww        : Dateframe containing all the TWW dates. 
        
    Return :
        None. 
        
    Note   :
        There is a Christmas holiday the following week after TWW 
        on quarter 4.  
    '''

    logger.info('Start running (trace_tww_trdr_days) function.')

    quarters = df_tww.columns
    
    for quarter in quarters:
        # Create new columns. 
        df_ticker_data[quarter] = 0
        df_ticker_data[f'{quarter}_week_aft'] = 0
        
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
            df_ticker_data.loc[year_bool & weekAft_bool, f'{quarter}_week_aft'] = 1 
            logger.debug(f'----- Created columns ({quarter}), ({quarter}_week_aft) for year ({year}), week ({week}), weekAft ({week + 1}) of ticker data.')
        
        # Compute the day counts for each TWW period of each quarter. 
        insert_day_counts_col(df_ticker_data, quarter)
        insert_day_counts_col(df_ticker_data, f'{quarter}_week_aft')


def insert_day_counts_col(df_ticker_data:pd.DataFrame, period_col:Text):
    '''
    Purpose:
        Create a new column for day counts of specific holiday, 
        observance, or special day period. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_by_trdr_day'. 
        period_col    : Str. Specific column of holiday, observance, or special day period. 
        
    Return :
        None. 
    '''

    logger.info('Start running (insert_day_counts_col) function.')
    
    # Number the days from the start till the end of the month. 
    period_bool = df_ticker_data[period_col] == 1
    df_ticker_data.loc[period_bool, f'{period_col}_day_counts'] = df_ticker_data[period_bool]\
                                                                  .set_index(keys='date')\
                                                                  .groupby(by=pd.Grouper(freq='M'))\
                                                                  .cumcount().values
    logger.debug(f'----- Created a column ({period_col}_day_counts) for (daily_by_trdr_day) ticker data.') 


def insert_holiday_col(df_ticker_data:pd.DataFrame, holidays_dict:Dict[Text, List], holiday_col:Text):
    '''
    Purpose:
        Create a new column from a holiday list. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_by_trdr_day'. 
        holidays_dict : Dictionary. Contains list of dates within for
                        for each holiday. 
        holiday_col   : Str. Name of the holiday to create new column.
        
    Return :
        None. 
    '''

    logger.info('Start running (insert_holiday_col) function.')
    
    # Indicate the dates which are holidays for trading days. 
    df_ticker_data[holiday_col] = 0 
    df_ticker_data.loc[df_ticker_data['date'].isin(holidays_dict[holiday_col]),holiday_col] = 1
    logger.debug(f'----- Created a column ({holiday_col}) for (daily_by_trdr_day) ticker data.') 
    
    # Add the day counts for each holiday period for each year. 
    period_bool = df_ticker_data[holiday_col] == 1 
    df_ticker_data.loc[period_bool, f'{holiday_col}_day_counts'] = holidays_dict[f'{holiday_col}_day_counts']
    logger.debug(f'----- Created a column ({holiday_col}_day_counts) for (daily_by_trdr_day) ticker data.') 
    
    # Add the specific year for each holiday period. 
    df_ticker_data.loc[period_bool, f'{holiday_col}_spec_year'] = holidays_dict[f'{holiday_col}_spec_year'] 
    logger.debug(f'----- Created a column ({holiday_col}_spec_year) for (daily_by_trdr_day) ticker data.') 
    
    
def trace_new_year(df_ticker_data:pd.DataFrame, holidays_dict:Dict[Text, List], date_range:int=6): 
    '''
    Purpose:
        Trace all the New Year period. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_by_trdr_day'. 
        holidays_dict : Dictionary. Contains list of dates within for
                        for each holiday. 
        date_range    : Int. Total number of dates to indicate. 
                        If 6, then 3 trading days before the holiday plus 
                        3 trading days after the holiday. 
        
    Return :
        None. 
    '''

    logger.info('Start running (trace_new_year) function.')
    
    holiday_col = 'new_year'
    holidays_dict[f'{holiday_col}_day_counts'] = [] 
    holidays_dict[f'{holiday_col}_spec_year'] = [] 
    
    month_bool = df_ticker_data['month'] == 1
    first_trdr_date_bool = df_ticker_data['trdr_day'] == 0

    # Remove the first (start_yr - 1) and last date (end_yr + 1). 
    ls_daily_first_trdr_date = df_ticker_data.loc[month_bool & first_trdr_date_bool,'date'].dt.date.tolist()[:-1]

    # Get all the dates from the ticker data to get the specific dates of the holiday later. 
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist()
    
    for date in ls_daily_first_trdr_date: 
        year = date.year

        # Count 3 days backward from the New Year Day. 
        idx = ls_daily_dates.index(date) - 3 
        
        for i in range(0, date_range, 1): 
            idx_date = idx + i 

            # Number the holiday period from 3 days backward till 3 days forward. 
            holidays_dict[f'{holiday_col}_day_counts'].append(i) 
            holidays_dict[f'{holiday_col}_spec_year'].append(year)

            # Get the specific dates of the holiday and 
            # add time to the date to avoid error due to 'datetime' dtype conflict. 
            holidays_dict[holiday_col].append(datetime.combine(ls_daily_dates[idx_date], 
                                                               datetime.min.time())) 
        
        logger.debug(f'----- Created a column ({holiday_col}_day_counts) for year ({year}) for (daily_by_trdr_day) ticker data.') 
            
    insert_holiday_col(df_ticker_data, holidays_dict, holiday_col)

            
def trace_spec_weekday_holiday(df_ticker_data:pd.DataFrame, df_holidays:pd.DataFrame, holidays_dict:Dict[Text, List], 
                               holiday_col:Text, day_forward:int=1, idx_backtrace:int=3, date_range:int=6): 
    '''
    Purpose:
        Trace all the holidays period which happens on a specific weekday. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_by_trdr_day'. 
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

    logger.info('Start running (trace_spec_weekday_holiday) function.')
        
    holidays_dict[f'{holiday_col}_day_counts'] = []
    holidays_dict[f'{holiday_col}_spec_year'] = [] 
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist() 
    
    for date in df_holidays[holiday_col].tolist(): 
        year = date.date().year

        # Count N day(s) forward from the specific holiday date if the holiday falls on weekend. 
        date = date + timedelta(days=day_forward) 

        # Count N day(s) backward from the specific holiday date + N forwarded day(s). 
        idx = ls_daily_dates.index(date.date()) - idx_backtrace 
        
        for i in range(0, date_range, 1): 
            idx_date = idx + i 
            
            # Number the holiday period from 3 days backward till 3 days forward. 
            holidays_dict[f'{holiday_col}_day_counts'].append(i)
            holidays_dict[f'{holiday_col}_spec_year'].append(year)

            # Get the specific dates of the holiday and 
            # add time to the date to avoid error due to 'datetime' dtype conflict. 
            holidays_dict[holiday_col].append(datetime.combine(ls_daily_dates[idx_date], 
                                                               datetime.min.time())) 

        logger.debug(f'----- Created a column ({holiday_col}_day_counts) on date ({date}) for (daily_by_trdr_day) ticker data.') 
    
    insert_holiday_col(df_ticker_data, holidays_dict, holiday_col)
    
    
def trace_non_spec_holiday(df_ticker_data:pd.DataFrame, df_holidays:pd.DataFrame, holidays_dict:Dict[Text, List], 
                           holiday_col:Text, date_range:int=6): 
    '''
    Purpose:
        Trace all the holidays period which happens on a non-specific weekday. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_by_trdr_day'. 
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

    logger.info('Start running (trace_non_spec_holiday) function.')
    
    holidays_dict[f'{holiday_col}_day_counts'] = []
    holidays_dict[f'{holiday_col}_spec_year'] = [] 
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist() 
    
    for date in df_holidays[holiday_col].tolist(): 
        year = date.date().year
        
        # Count N day(s) backward from the specific holiday date 
        # depending on which weekend the holiday falls on. 
        # 0 == Monday, 6 == Sunday. 
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
            
        for i in range(0, date_range, 1): 
            idx_date = idx + i 

            # Number the holiday period from 3 days backward till 3 days forward. 
            holidays_dict[f'{holiday_col}_day_counts'].append(i)
            holidays_dict[f'{holiday_col}_spec_year'].append(year)

            # Get the specific dates of the holiday and 
            # add time to the date to avoid error due to 'datetime' dtype conflict. 
            holidays_dict[holiday_col].append(datetime.combine(ls_daily_dates[idx_date], 
                                                               datetime.min.time())) 

        logger.debug(f'----- Created a column ({holiday_col}_day_counts) on date ({date}) for (daily_by_trdr_day) ticker data.') 
        
    insert_holiday_col(df_ticker_data, holidays_dict, holiday_col)
        

def trace_non_spec_observance(df_ticker_data:pd.DataFrame, df_holidays:pd.DataFrame, holidays_dict:Dict[Text, List], 
                              holiday_col:Text, date_range:int=7): 
    '''
    Purpose:
        Trace all the observances period which happens on a non-specific weekday. 
    
    Input  :
        df_ticker_data: Dataframe. Contains ticker data. Must be 'daily_by_trdr_day'. 
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

    logger.info('Start running (trace_non_spec_observance) function.')
    
    holidays_dict[f'{holiday_col}_day_counts'] = []
    holidays_dict[f'{holiday_col}_spec_year'] = [] 
    ls_daily_dates = df_ticker_data['date'].dt.date.tolist() 
    
    for date in df_holidays[holiday_col].tolist():
        year = date.date().year
        date_range = 7
        
        # Try running this to see if the date falls on weekend, holiday, or trading day. 
        # If error occurs due to weekend or holiday, run the 'except' section. 
        while True:
            try:
                idx = ls_daily_dates.index(date.date()) - 3
                for i in range(0, date_range, 1):
                    idx_date = idx + i 

                    # Skip index 3 if 'date_range' has length of 6, 
                    # assuming that 3 == observance day on weekend. 
                    # So the iteration for 'i' will be: 0, 1, 2, 4, 5, 6. 
                    i += 1 if i >= 3 and date_range == 6 else i

                    # Number the holiday period from N days backward till N days forward. 
                    holidays_dict[f'{holiday_col}_day_counts'].append(i)
                    holidays_dict[f'{holiday_col}_spec_year'].append(year)

                    # Get the specific dates of the holiday and 
                    # add time to the date to avoid error due to 'datetime' dtype conflict. 
                    holidays_dict[holiday_col].append(datetime.combine(ls_daily_dates[idx_date], 
                                                                       datetime.min.time())) 
                break
            except:
                date = date + timedelta(days=1)
                date_range = 6

        logger.debug(f'----- Created a column ({holiday_col}_day_counts) on date ({date}) for (daily_by_trdr_day) ticker data.') 
                
    insert_holiday_col(df_ticker_data, holidays_dict, holiday_col)


def create_pivot_unique_days(df_ticker_data: pd.DataFrame, pivot_dict: Dict[Text, pd.DataFrame], 
                            pivot_dict_keys:List[Text], start_yr:Text, end_yr:Text, drop_idx:bool=False): 
    '''
    Purpose: 
        Create pivot tables for holidays, observances, and special day.
    
    Input  :
        df_ticker_data  : Dataframe. Must be 'daily_by_trdr_day'.
        pivot_dict      : Dictionary. To contain pivot tables.
        pivot_dict_keys : List containing holidays, observances, 
                          or special day keys. 
        start_yr        : Int. Starting year to compile the data on. 
        end_yr          : Int. Ending year to compile the data on. 
        drop_idx        : Bool. Indicate whether to drop the index or not. 

    Return :
        None.
        
    Note   :
        1. Any special day like 'first_trdr_dom' and 'super_day' which occur monthly 
           should include month as part of the groupby. Otherwise it will average 
           across all the different months. However, this depends on the analysis goal. 
        2. 'first_trdr_dom' does not contain '_day_counts'. 
        3. 'tww' does not contain '_spec_year'.
        4. 'super_day' requires '_spec_month' instead of 'month' if groupby includes month. 
    '''

    logger.info('Start running (create_pivot_unique_days) function.')
    
    # REFINE: Need to refine this function in the future. 
    for key in pivot_dict_keys: 
        spec_year = f'{key}_spec_year'
        idx_col = f'{key}_day_counts'
        
        if key == 'first_trdr_dom': 
            spec_year, idx_col = 'year', key 
        elif 'tww' in key:
            spec_year = 'year'
        
        # The 'by_month' refers to the partial name of the dataframe which 
        # contains the average value of each month for each year. 
        # Without 'by_month' means the value is averaged across all the months for each year. 
        if 'by_month' in key:
            # Remove '_byMonth' from the str name. 
            idx_col = key[:-9]
            
            if idx_col == 'super_day': 
                spec_year, idx_col = f'{idx_col}_spec_year', [f'{idx_col}_spec_month', f'{idx_col}_day_counts']
            elif idx_col == 'first_trdr_dom':
                spec_year, idx_col = 'year', ['month', idx_col] 
        
        # Create a pivot table. 
        pivot_dict[key] = df_ticker_data.pivot_table(values='price_diff', 
                                                     index=idx_col, columns=spec_year, 
                                                     aggfunc='mean').loc[:,start_yr:end_yr]
        logger.debug(f'----- Created a pivot table for ({key}) for ticker data.') 

        # Reset the index. Some visualisation tools like 'Tableau' require 
        # a specific format of data structure to process and visualise the data. 
        # Except for TWW data. We will reset the index at later stage. 
        if 'tww' not in key: 
            pivot_dict[key].reset_index(inplace=True, drop=drop_idx)
            if key in HOLIDAYS_KEYS: 
                pivot_dict[key]['holiday_category'] = key 
            logger.debug(f'----- Resetted the index for ({key}) ticker data.')


def concat_pivot_tww(pivot_dict:Dict[Text, pd.DataFrame], pivot_dict_keys:List[Text]):
    '''
    Purpose: 
        Concat pivot tables for TWW data.
    
    Input  :
        pivot_dict     : Dictionary. Must be contain TWW dataframes.
        pivot_dict_keys: List. TWW dictionary indexing keys. 

    Return :
        None.
    '''

    logger.info('Start running (concat_pivot_tww) function.')
    
    for key in pivot_dict_keys:
        tww_key = key 
        tww_weekAft_key = f'{key}_week_aft'
        
        # Concat the 'TWW' and 'TWW week after' dataframes. 
        ls_df = [pivot_dict[tww_key], pivot_dict[tww_weekAft_key]]
        df_concat = pd.concat(ls_df, keys=(tww_key, tww_weekAft_key), names=('tww_period', 'day_counts')).copy() 
        logger.debug(f'----- Concatenated ({tww_key}) and ({tww_weekAft_key}) for ticker data.') 

        # Delete the other data after concatenating both of the data. 
        _ = pivot_dict.pop(tww_weekAft_key, None) 
        
        # Assign the updated TWW data back to the dict obj. 
        pivot_dict[tww_key] = df_concat

        # Reset the index. Some visualisation tools like 'Tableau' require 
        # a specific format of data structure to process and visualise the data. 
        pivot_dict[tww_key].reset_index(inplace=True) 
        logger.debug(f'----- Resetted the index for ({tww_key}) ticker data.') 


def concat_pivot_unique_days(pivot_dict:Dict[Text, pd.DataFrame], pivot_dict_keys:List[Text], cat_name:Text):
    '''
    Purpose: 
        Concat pivot tables for holidays, observances, and special day.
    
    Input  :
        pivot_dict      : Dictionary. To contain pivot tables.
        pivot_dict_keys : List containing firstDayOf holidays, observances, 
                          or special day keys. 
        cat_name        : Str. To assign name for the final version of the dataframe 
                          after concatenating all the dataframes into 1. 

    Return :
        None.
    '''

    logger.info('Start running (concat_pivot_unique_days) function.')

    df_list = []
    for key in pivot_dict_keys:
        df_copy = pivot_dict[key].copy()
        df_copy['day_counts'] = df_copy.index
        df_list.append(df_copy)

    pivot_dict[f'compiled_{cat_name}'] = pd.concat(df_list).reset_index(drop=True)
    logger.debug(f'----- Concatenated the dataframes for {cat_name}.') 
