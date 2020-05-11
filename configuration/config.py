

# Python modules.
import os, shutil 


# --------------------------------------------------------------
# Luigi Pipeline Configuration.
# --------------------------------------------------------------

local_scheduler = '--local-scheduler' 
workers = '6'


# ----------------------------------------------------------------------
# Date & Year Range.
# ----------------------------------------------------------------------

# Year range. 
# Be careful when assigning the 'end_yr' to 'range()'. You need to add 1 to it. 
start_yr, end_yr = (1999,2019)

ls_yrStr = [str(yr) for yr in range(start_yr, end_yr + 1)]

yr_range = ['MaxYr', 'R20Yr', 'R15Yr', 'R10Yr', 'R5Yr']
start_yr_range = [start_yr, end_yr-19, end_yr-14, end_yr-9, end_yr-4] 


# ----------------------------------------------------------------------
# Ticker Info.
# ----------------------------------------------------------------------

log_folder = 'ETF_sector'
ETF_folder = f'dataset/{log_folder}'
# ETF_folder = 'dataset/ETF_equity/PPA'
ticker = 'SPY'
ticker_freq = ['1mo', '1wk', '1d']
yahoo_version = 'v7'

ETF_equity = [ 
    'XLB', 'XLE', 'XLF', 'XLI', 'XLP', 'XLU', 'XLV', 'XLY', 'IYR', 
    'IYW', 'IYZ', 'PPA', 'XRT', 'XHB',
]
optionsETF = ['/'.join(['dataset/ETF_equity', ETF]) for ETF in ETF_equity]
optionsETF.append('dataset/ETF_sector')


# --------------------------------------------------------------
# Tickers Dictionary. 
# --------------------------------------------------------------

dict_commodities = {2005: ['GLD']}

dict_sectors = {
    1999: [
        'SPY', 'DIA', 'XLB', 'XLE', 'XLF', 'XLI', 'XLP', 
        'XLU', 'XLV', 'XLY'
    ],
    2000: ['QQQ'],
    2001: ['IYZ'],
    2004: ['IYR', 'IYW'],
    2006: ['PPA'],
    2007: ['XRT', 'XHB'],
} 

dict_equities = {
    'PPA': {
        1999: [
            'BA', 'BLL', 'COL', 'GD', 'HON', 'LMT', 'NOC', 
            'RTN', 'TDG', 'UTX'
        ],
    }
}


# ----------------------------------------------------------------------
# Path Directory.
# ---------------------------------------------------------------------- 

project_path = os.getcwd()
driver_path = os.path.join(project_path, 'system/chromedriver')


# ----------------------------------------------------------------------
# For Ticker Data Preprocessing.
# ---------------------------------------------------------------------- 

# Store keys for dictionary indexing. 
freq_keys = ['monthly', 'weekly', 'daily_byTrdrDay', 'daily_byWeekday']

# Column names for creating pivot tables. 
freq_cols = ['month', 'week', 'trdr_day', 'weekday']

# To store ticker data.
df_ticker = {
    freq_keys[0]: None,
    freq_keys[1]: None,
    freq_keys[2]: None,
    freq_keys[3]: None
}

# To store pivot tables.
pivot_ticker = {
    freq_keys[0]: None, 
    freq_keys[1]: None, 
    freq_keys[2]: None, 
    freq_keys[3]: None
}

# To store statistical summary of pivot tables. 
pivot_stats = {
    freq_keys[0]: None, 
    freq_keys[1]: None, 
    freq_keys[2]: None, 
    freq_keys[3]: None
}


# ----------------------------------------------------------------------
# For Ticker Volume Preprocessing.
# ---------------------------------------------------------------------- 

# To store pivot tables.
pivot_volume = {
    freq_keys[0]: None, 
    freq_keys[1]: None, 
    freq_keys[2]: None, 
    freq_keys[3]: None
}

# To store statistical summary of pivot tables. 
pivot_volume_stats = {
    freq_keys[0]: None, 
    freq_keys[1]: None, 
    freq_keys[2]: None, 
    freq_keys[3]: None
}


# ----------------------------------------------------------------------
# For Holidays/Observances/SpecialDays Data Preprocessing.
# ---------------------------------------------------------------------- 

holidays_dict = {
    'newYear': [],
    'marLutKingJr': [], 
    'valentine': [], 
    'president': [], 
    'goodFriday': [],
    'memorial': [], 
    'independence': [], 
    'labour': [], 
    'event911': [], 
    'columbus': [], 
    'veteran': [], 
    'thanksgiving': [],
    'christmas': []
}

# Store keys for dictionary indexing. 
holidays_keys = [
    'newYear', 'marLutKingJr', 'valentine', 'president', 
    'goodFriday', 'memorial', 'independence', 'labour', 
    'event911', 'columbus', 'veteran', 'thanksgiving','christmas'
]
specialDays_keys = [
    'firstTrdrDoM', 'firstTrdrDoM_byMonth', 'superDay', 'superDay_byMonth',
    'santaRally', 'twwQ1', 'twwQ2', 'twwQ3', 'twwQ4', 
    'twwQ1_weekAft', 'twwQ2_weekAft', 'twwQ3_weekAft', 'twwQ4_weekAft'
]

specWeekdayHolidays = ['marLutKingJr', 'president', 'memorial', 'labour', 'columbus', 'thanksgiving']
specWeekdayHolidays_backward = ['goodFriday']
nonSpecHolidays = ['independence', 'christmas']
nonSpecObservances = ['valentine', 'event911', 'veteran']

# To store pivot tables. 
pivot_holidays = {}
pivot_specialDays = {}
pivot_specialDays_weekly = {}

# To store statistical summary of pivot tables. 
pivot_holidays_stats = {}
pivot_specialDays_stats = {}
pivot_specialDays_weekly_stats ={}


# ----------------------------------------------------------------------
# For Saving Into Excel File. 
# ---------------------------------------------------------------------- 

sheetNames = ['mth', 'wk', 'trdr', 'wkd']

# Excel row and column index. 
startcol = 1
startrow = 2
distance = 2