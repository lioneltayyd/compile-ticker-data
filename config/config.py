

import os, logging 


# --------------------------------------------------------------
# Luigi Pipeline Configuration.
# --------------------------------------------------------------

LOCAL_SCHEDULER = '--local-scheduler' 
WORKERS = '6'


# ----------------------------------------------------------------------
# Date & Year Range.
# ----------------------------------------------------------------------

# Specify the year range to collect the data. 
# Be careful when assigning the 'END_YR' to 'range()', 
# you need to add 1 to capture the last index range. 
START_YR, END_YR = (1999,2020)

YR_RANGE = ['max_yr', 'range_20_yr', 'range_15_yr', 'range_10_yr', 'range_5_yr']
YR_INCREMENT = 1
START_YR_RANGE = [
    START_YR, 
    START_YR + YR_INCREMENT, 
    START_YR + YR_INCREMENT + 5, 
    START_YR + YR_INCREMENT + 10, 
    START_YR + YR_INCREMENT + 15
] 


# ----------------------------------------------------------------------
# Ticker Info.
# ----------------------------------------------------------------------

TICKER = 'SPY'
TICKER_FREQ = ['1mo', '1wk', '1d']
YAHOO_VERSION = 'v7'


# --------------------------------------------------------------
# Tickers Dictionary. 
# --------------------------------------------------------------

DICT_COMMODITIES = {2005: ['GLD']}

DICT_SECTORS = {
    1999: [
        'SPY', 
        'DIA', 'XLB', 'XLE', 'XLF', 'XLI', 'XLP', 
        'XLU', 'XLV', 'XLY', 
    ],
    2000: ['QQQ'],
    2001: ['IYZ'],
    2004: ['IYR', 'IYW'],
    2006: ['PPA'],
    2007: ['XRT', 'XHB'],
} 

DICT_EQUITIES = {
    'PPA': {
        1999: [
            'BA', 'BLL', 'COL', 'GD', 'HON', 'LMT', 'NOC', 
            'RTN', 'TDG', 'UTX'
        ],
    }
}


# ----------------------------------------------------------------------
# Directory / File Path.
# ---------------------------------------------------------------------- 

# Logging file path. 
LOG_DIR = 'ETF_sector'
LOG_PROCESSING_FILEPATH = 'logs/runtime/log_processing.log'
LOG_PIPELINE_SECTOR_DIR = f'logs/pipeline/ETF_sector'
LOG_PIPELINE_EQUITY_DIR = f'logs/pipeline/ETF_equity'

# Directory for storing the ETF data. 
ETF_SECTOR_DIR = f'docs/dataset/ETF_sector'
ETF_EQUITY_DIR = f'docs/dataset/ETF_equity' 

# Others.  
PROJECT_PATH = os.getcwd()
DRIVER_PATH = os.path.join(PROJECT_PATH, 'system/chromedriver')


# ----------------------------------------------------------------------
# For Selenium Scraper.
# ----------------------------------------------------------------------

SLEEP = 5
WEBPAGE_LOADING_TIMEOUT = 10 + SLEEP


# ----------------------------------------------------------------------
# For Ticker Data Preprocessing.
# ---------------------------------------------------------------------- 

# Store keys for dictionary indexing. 
FREQ_KEYS = ['monthly', 'weekly', 'daily_by_trdr_day', 'daily_by_weekday']

# Column names for creating pivot tables. 
FREQ_COLS = ['month', 'week', 'trdr_day', 'weekday']

# To store ticker data.
DF_TICKER = {
    FREQ_KEYS[0]: None,
    FREQ_KEYS[1]: None,
    FREQ_KEYS[2]: None,
    FREQ_KEYS[3]: None
}

# To store pivot tables.
PIVOT_TICKER = {
    FREQ_KEYS[0]: None, 
    FREQ_KEYS[1]: None, 
    FREQ_KEYS[2]: None, 
    FREQ_KEYS[3]: None
}

# To store statistical summary of pivot tables. 
PIVOT_STATS = {
    FREQ_KEYS[0]: None, 
    FREQ_KEYS[1]: None, 
    FREQ_KEYS[2]: None, 
    FREQ_KEYS[3]: None
}


# ----------------------------------------------------------------------
# For Ticker Volume Preprocessing.
# ---------------------------------------------------------------------- 

# To store pivot tables.
PIVOT_VOLUME = {
    FREQ_KEYS[0]: None, 
    FREQ_KEYS[1]: None, 
    FREQ_KEYS[2]: None, 
    FREQ_KEYS[3]: None
}

# To store statistical summary of pivot tables. 
PIVOT_VOLUME_STATS = {
    FREQ_KEYS[0]: None, 
    FREQ_KEYS[1]: None, 
    FREQ_KEYS[2]: None, 
    FREQ_KEYS[3]: None
}


# ----------------------------------------------------------------------
# For Holidays / Observances / Special Days Data Preprocessing.
# ---------------------------------------------------------------------- 

HOLIDAYS_DICT = {
    'new_year': [],
    'mar_lut_king_jr': [], 
    'valentine': [], 
    'president': [], 
    'good_friday': [],
    'memorial': [], 
    'independence': [], 
    'labour': [], 
    'event_911': [], 
    'columbus': [], 
    'veteran': [], 
    'thanksgiving': [],
    'christmas': []
}

# Store keys for dictionary indexing. 
HOLIDAYS_KEYS = [
    'new_year', 'mar_lut_king_jr', 'valentine', 'president', 
    'good_friday', 'memorial', 'independence', 'labour', 
    'event_911', 'columbus', 'veteran', 'thanksgiving','christmas'
]
SPECIAL_DAYS_KEYS = [
    'first_trdr_dom', 'first_trdr_dom_by_month', 'super_day', 'super_day_by_month',
    'santa_rally', 'tww_q1', 'tww_q2', 'tww_q3', 'tww_q4', 
    'tww_q1_week_aft', 'tww_q2_week_aft', 'tww_q3_week_aft', 'tww_q4_week_aft'
]

# Holidays that falls on specific weekday. 
SPEC_WEEKDAY_HOLIDAYS = ['mar_lut_king_jr', 'president', 'memorial', 'labour', 'columbus', 'thanksgiving']

# Holidays that falls on Friday. 
SPEC_WEEKDAY_HOLIDAYS_BACKWARD = ['good_friday']

# Holidays that falls on specific date.
NON_SPEC_HOLIDAYS = ['independence', 'christmas']

# Observances that falls on specific date.
NON_SPEC_OBSERVANCES = ['valentine', 'event_911', 'veteran']

# To store pivot tables. 
PIVOT_HOLIDAYS = {}
PIVOT_SPECIAL_DAYS = {}
PIVOT_SPECIAL_DAYS_WEEKLY = {}

# To store statistical summary of pivot tables. 
PIVOT_HOLIDAYS_STATS = {}
PIVOT_SPECIAL_DAYS_STATS = {}
PIVOT_SPECIAL_DAYS_WEEKLY_STATS ={}


# ----------------------------------------------------------------------
# For Saving Into Excel File. 
# ---------------------------------------------------------------------- 

EXCEL_SHEET_NAMES = ['mth', 'wk', 'trdr', 'wkd']

# Excel row and column index. 
EXCEL_START_COL = 1
EXCEL_START_ROW = 2
EXCEL_DISTANCE = 2
