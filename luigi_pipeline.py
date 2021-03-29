

# Python modules. 
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from luigi.parameter import Parameter, IntParameter, DateParameter
from luigi import LocalTarget, Task

import numpy as np
import pandas as pd
import pickle
from datetime import datetime

# Personal modules.
from config.config import *
from autoprocess_ticker import web_download, data_management, preprocessing, compile_unique_days


# --------------------------------------------------------------
# Pipeline.
# --------------------------------------------------------------

class DownloadTickerData(ExternalProgramTask):
    ticker = luigi.Parameter(default=None)
    ticker_freq = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    etf_dir = luigi.Parameter(default=ETF_SECTOR_DIR) 
    yahoo_version = luigi.Parameter(default=YAHOO_VERSION)
    
    def output(self):
        ticker_filename = f'{self.ticker}_{self.ticker_freq}.csv'
        return luigi.LocalTarget(f"{self.etf_dir}/{self.ticker}/{ticker_filename}") 
    
    def program_args(self):
        start_date = {
            '1mo': int(datetime.timestamp(datetime(self.start_yr - 1, 11, 30))),
            '1wk': int(datetime.timestamp(datetime(self.start_yr - 1, 12, 28))),
            '1d': int(datetime.timestamp(datetime(self.start_yr - 1, 12, 23)))
        }
        end_date = {
            '1mo': int(datetime.timestamp(datetime(END_YR + 1, 1, 3))),
            '1wk': int(datetime.timestamp(datetime(END_YR + 1, 1, 6))),
            '1d': int(datetime.timestamp(datetime(END_YR + 1, 1, 6)))
        }

        # Direct URL link for downloading the ticker data. 
        ticker_download = f'https://query1.finance.yahoo.com/{self.yahoo_version}/finance/download/{self.ticker}?'
        ticker_download_param = f'period1={start_date[self.ticker_freq]}&period2={end_date[self.ticker_freq]}&interval={self.ticker_freq}&events=history'
        ticker_download_url = "".join([ticker_download, ticker_download_param])

        # Directly download the ticker data via 'curl'. 
        with self.output().open('w') as out_file:
            return ["curl", "-L", "-o", self.output().path, ticker_download_url]
            

class ProcessTickerData(luigi.Task):
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    etf_dir = luigi.Parameter(default=ETF_SECTOR_DIR) 
    yahoo_version = luigi.Parameter(default=YAHOO_VERSION)

    def requires(self):
        return {
            FREQ_KEYS[0]: DownloadTickerData(self.ticker, TICKER_FREQ[0], self.start_yr, self.etf_dir, self.yahoo_version),
            FREQ_KEYS[1]: DownloadTickerData(self.ticker, TICKER_FREQ[1], self.start_yr, self.etf_dir, self.yahoo_version),
            FREQ_KEYS[2]: DownloadTickerData(self.ticker, TICKER_FREQ[2], self.start_yr, self.etf_dir, self.yahoo_version),
            FREQ_KEYS[3]: DownloadTickerData(self.ticker, TICKER_FREQ[2], self.start_yr, self.etf_dir, self.yahoo_version)
        }
    
    def output(self):
        return luigi.LocalTarget(f"{self.etf_dir}/{self.ticker}/storage/df_ticker.pickle", format=luigi.format.Nop) 

    def run(self):
        # Read file. 
        df_ticker = {freq: pd.read_csv(self.input()[freq].path, parse_dates=['Date']) for freq in FREQ_KEYS} 

        # Initial preprocessing. 
        preprocessing.init_preprocess(df_ticker, FREQ_KEYS, FREQ_COLS)

        with self.output().open('w') as out_file:
            pickle.dump(df_ticker, out_file)


class PivotTickerSummary(luigi.Task):
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    etf_dir = luigi.Parameter(default=ETF_SECTOR_DIR) 
    yahoo_version = luigi.Parameter(default=YAHOO_VERSION)

    def requires(self):
        return ProcessTickerData(self.ticker, self.start_yr, self.etf_dir, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.etf_dir}/{self.ticker}/storage/pivot_stats.pickle", 
                                 format=luigi.format.Nop) 

    def run(self):
        with self.input().open() as in_file:
            df_ticker = pickle.load(in_file)

        # Create pivot tables. 
        preprocessing.create_pivot(df_ticker, PIVOT_TICKER, FREQ_KEYS, FREQ_COLS, pivot_value='price_diff')

        # Create statistical summary from pivot tables. 
        START_YR_RANGE[0] = self.start_yr
        preprocessing.summarise_pivot(PIVOT_TICKER, PIVOT_STATS, FREQ_KEYS, START_YR_RANGE, END_YR)

        with self.output().open('w') as out_file:
            pickle.dump((PIVOT_TICKER, PIVOT_STATS), out_file) 


class PivotVolSummary(luigi.Task):
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    etf_dir = luigi.Parameter(default=ETF_SECTOR_DIR) 
    yahoo_version = luigi.Parameter(default=YAHOO_VERSION)

    def requires(self):
        return ProcessTickerData(self.ticker, self.start_yr, self.etf_dir, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.etf_dir}/{self.ticker}/storage/pivot_vol_stats.pickle", 
                                 format=luigi.format.Nop) 

    def run(self):
        with self.input().open() as in_file:
            df_ticker = pickle.load(in_file)
        
        # Create pivot tables. 
        preprocessing.create_pivot(df_ticker, PIVOT_VOLUME, FREQ_KEYS, FREQ_COLS, pivot_value='volume')

        # Create statistical summary from pivot tables. 
        START_YR_RANGE[0] = self.start_yr
        preprocessing.compute_avg_vol(PIVOT_VOLUME, PIVOT_VOLUME_STATS, FREQ_KEYS, [START_YR_RANGE[0]], END_YR)
        preprocessing.summarise_pivot_vol(PIVOT_VOLUME, PIVOT_VOLUME_STATS, FREQ_KEYS, [START_YR_RANGE[0]], END_YR)

        with self.output().open('w') as out_file:
            pickle.dump((PIVOT_VOLUME, PIVOT_VOLUME_STATS), out_file) 


class TraceUniquePeriod(luigi.Task):
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    etf_dir = luigi.Parameter(default=ETF_SECTOR_DIR) 
    yahoo_version = luigi.Parameter(default=YAHOO_VERSION)

    def requires(self):
        return ProcessTickerData(self.ticker, self.start_yr, self.etf_dir, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.etf_dir}/{self.ticker}/storage/df_ticker_unique_days.pickle", 
                                 format=luigi.format.Nop) 

    def run(self):
        with self.input().open() as in_file:
            df_ticker = pickle.load(in_file)

            df_ticker_weekly = df_ticker['weekly'].copy()
            df_ticker_trdrDay = df_ticker['daily_by_trdr_day'].copy()

            # Get the holidays, observances, and specialDay dates.
            df_holidays = compile_unique_days.compile_trdr_holiday_dates(self.start_yr, END_YR, HOLIDAYS_KEYS)
            df_tww = compile_unique_days.get_tww_dates(self.start_yr, END_YR)
            tup_super_day = compile_unique_days.get_super_day_period(df_ticker_trdrDay) 
            tup_santa_rally = compile_unique_days.get_santa_rally_period(df_ticker_trdrDay, self.start_yr, END_YR)
        
            # Trace the TWW & special days for 'daily_trdrDay' data. 
            preprocessing.trace_special_days(df_ticker_trdrDay, tup_super_day, tup_santa_rally) 
            preprocessing.trace_tww_trdr_days(df_ticker_trdrDay, df_tww) 

            # Trace the TWW period for 'weekly' data. 
            preprocessing.trace_tww_trdr_days(df_ticker_weekly, df_tww)

            # Trace the holidays and observances. 
            preprocessing.trace_new_year(df_ticker_trdrDay, HOLIDAYS_DICT) 

            for holiday in SPEC_WEEKDAY_HOLIDAYS:
                preprocessing.trace_spec_weekday_holiday(df_ticker_trdrDay, df_holidays, HOLIDAYS_DICT, holiday)
            for holiday in SPEC_WEEKDAY_HOLIDAYS_BACKWARD:
                preprocessing.trace_spec_weekday_holiday(df_ticker_trdrDay, df_holidays, HOLIDAYS_DICT, holiday, 
                                                       day_forward=-1, idx_backtrace=2)
            for holiday in NON_SPEC_HOLIDAYS:
                preprocessing.trace_non_spec_holiday(df_ticker_trdrDay, df_holidays, HOLIDAYS_DICT, holiday) 
            for observance in NON_SPEC_OBSERVANCES:
                preprocessing.trace_non_spec_observance(df_ticker_trdrDay, df_holidays, HOLIDAYS_DICT, observance)

            df_ticker['weekly'] = df_ticker_weekly 
            df_ticker['daily_by_trdr_day'] = df_ticker_trdrDay 

        with self.output().open('w') as out_file:
            pickle.dump(df_ticker, out_file) 


class PivotUniqueDaysSummary(luigi.Task):
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    etf_dir = luigi.Parameter(default=ETF_SECTOR_DIR) 
    yahoo_version = luigi.Parameter(default=YAHOO_VERSION)

    def requires(self):
        return TraceUniquePeriod(self.ticker, self.start_yr, self.etf_dir, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.etf_dir}/{self.ticker}/storage/pivot_unique_days.pickle", 
                                 format=luigi.format.Nop) 

    def run(self):
        with self.input().open() as in_file:
            df_ticker = pickle.load(in_file)

            df_ticker_weekly = df_ticker['weekly'].copy()
            df_ticker_trdrDay = df_ticker['daily_by_trdr_day'].copy()

            # Create pivot tables. 
            preprocessing.create_pivot_unique_days(df_ticker_trdrDay, PIVOT_HOLIDAYS, HOLIDAYS_KEYS, self.start_yr, END_YR, drop_idx=True)
            preprocessing.create_pivot_unique_days(df_ticker_trdrDay, PIVOT_SPECIAL_DAYS, SPECIAL_DAYS_KEYS, self.start_yr, END_YR)
            preprocessing.create_pivot_unique_days(df_ticker_weekly, PIVOT_SPECIAL_DAYS_WEEKLY, SPECIAL_DAYS_KEYS[5:], self.start_yr, END_YR)

            # Concat the TWW data and the data of the week after the TWW. 
            preprocessing.concat_pivot_tww(PIVOT_SPECIAL_DAYS, SPECIAL_DAYS_KEYS[5:9])
            preprocessing.concat_pivot_tww(PIVOT_SPECIAL_DAYS_WEEKLY, SPECIAL_DAYS_KEYS[5:9])

            # Concat all the relevant datasets into 1 dataset. 
            preprocessing.concat_pivot_unique_days(PIVOT_HOLIDAYS, HOLIDAYS_KEYS, 'holiday')
            preprocessing.concat_pivot_unique_days(PIVOT_SPECIAL_DAYS, SPECIAL_DAYS_KEYS[5:9], 'tww') 
            preprocessing.concat_pivot_unique_days(PIVOT_SPECIAL_DAYS_WEEKLY, SPECIAL_DAYS_KEYS[5:9], 'tww') 

            # Create statistical summary from pivot tables. 
            START_YR_RANGE[0] = self.start_yr
            preprocessing.summarise_pivot(PIVOT_HOLIDAYS, PIVOT_HOLIDAYS_STATS, ['compiled_holiday'], START_YR_RANGE, END_YR)
            preprocessing.summarise_pivot(PIVOT_SPECIAL_DAYS, PIVOT_SPECIAL_DAYS_STATS, SPECIAL_DAYS_KEYS[:5], START_YR_RANGE, END_YR)
            preprocessing.summarise_pivot(PIVOT_SPECIAL_DAYS, PIVOT_SPECIAL_DAYS_STATS, ['compiled_tww'], START_YR_RANGE, END_YR)
            preprocessing.summarise_pivot(PIVOT_SPECIAL_DAYS_WEEKLY, PIVOT_SPECIAL_DAYS_WEEKLY_STATS, ['compiled_tww'], START_YR_RANGE, END_YR)

            # # Concat all the relevant datasets into 1 dataset. 
            # preprocessing.concat_pivot_unique_days(PIVOT_HOLIDAYS_STATS, HOLIDAYS_KEYS, 'holiday')
            # preprocessing.concat_pivot_unique_days(PIVOT_SPECIAL_DAYS_STATS, SPECIAL_DAYS_KEYS[5:9], 'tww') 
            # preprocessing.concat_pivot_unique_days(PIVOT_SPECIAL_DAYS_WEEKLY_STATS, SPECIAL_DAYS_KEYS[5:9], 'tww') 

            # Group multiple data into tuples. 
            pivot_holidays = (PIVOT_HOLIDAYS, PIVOT_HOLIDAYS_STATS)
            pivot_special_days = (PIVOT_SPECIAL_DAYS, PIVOT_SPECIAL_DAYS_STATS)
            pivot_special_days_weekly = (PIVOT_SPECIAL_DAYS_WEEKLY, PIVOT_SPECIAL_DAYS_WEEKLY_STATS)

        with self.output().open('w') as out_file:
            pickle.dump((pivot_holidays, pivot_special_days, pivot_special_days_weekly), out_file) 


class CompileToExcel(luigi.Task):
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    etf_dir = luigi.Parameter(default=ETF_SECTOR_DIR) 
    yahoo_version = luigi.Parameter(default=YAHOO_VERSION)

    def requires(self):
        yield PivotTickerSummary(self.ticker, self.start_yr, self.etf_dir, self.yahoo_version),
        yield PivotVolSummary(self.ticker, self.start_yr, self.etf_dir, self.yahoo_version), 
        yield PivotUniqueDaysSummary(self.ticker, self.start_yr, self.etf_dir, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.etf_dir}/{self.ticker}/{self.ticker}_seasonal_stats.xlsx",
                                 format=luigi.format.Nop) 

    def run(self):
        in_file_1, in_file_2, in_file_3 = self.input()
        
        with in_file_1[0].open() as in_file:
            pivot_ticker, pivot_stats = pickle.load(in_file)
        with in_file_2[0].open() as in_file:
            pivot_volume, pivot_volume_stats = pickle.load(in_file)
        with in_file_3.open() as in_file:
            pivot_holidays, pivot_special_days, pivot_special_days_weekly = pickle.load(in_file)

        pivot_holidays, pivot_holidays_stats = pivot_holidays
        pivot_special_days, pivot_special_days_stats = pivot_special_days
        pivot_special_days_weekly, pivot_special_days_weekly_stats = pivot_special_days_weekly

        with pd.ExcelWriter(self.output().path) as out_file:
            # Save pivot ticker data and volume. Multiple sheets. 
            for i, freq in enumerate(FREQ_KEYS): 
                data_management.multi_sheet_write(out_file, pivot_ticker, pivot_stats, f'{EXCEL_SHEET_NAMES[i]}_pv', freq, 
                                                  EXCEL_START_COL, EXCEL_START_ROW, EXCEL_DISTANCE)
                data_management.multi_sheet_write(out_file, pivot_volume, pivot_volume_stats, f'{EXCEL_SHEET_NAMES[i]}_vol', 
                                                  freq, EXCEL_START_COL, EXCEL_START_ROW, EXCEL_DISTANCE) 
            
            # Save pivot tables Special Day data. Single sheet. 
            data_management.single_sheet_multi_write(out_file, pivot_special_days, pivot_special_days_stats, 
                                                    'special_days', SPECIAL_DAYS_KEYS[:5], EXCEL_START_COL, EXCEL_START_ROW, EXCEL_DISTANCE)

            # Save pivot tables holidays/observances data. Single sheet.
            data_management.single_sheet_multi_write(out_file, pivot_holidays, pivot_holidays_stats, 'holiday', ['compiled_holiday'], 
                                                     EXCEL_START_COL, EXCEL_START_ROW, EXCEL_DISTANCE)
                
            # Save pivot tables TWW Trdr data. Single sheet.
            data_management.single_sheet_multi_write(out_file, pivot_special_days, pivot_special_days_stats, 
                                                    'tww_trdr', ['compiled_tww'], EXCEL_START_COL, EXCEL_START_ROW, EXCEL_DISTANCE)
                
            # Save pivot tables TWW Weekly data. Single sheet.
            data_management.single_sheet_multi_write(out_file, pivot_special_days_weekly, pivot_special_days_weekly_stats, 
                                                    'tww_wk', ['compiled_tww'], EXCEL_START_COL, EXCEL_START_ROW, EXCEL_DISTANCE)


# --------------------------------------------------------------
# Run.
# --------------------------------------------------------------

# Run Luigi on command window. 
# Example: luigi --module [python-file] [class] --[parameter-name] 1 --workers 5
# Example: luigi --module [python-file] [class] --[parameter-name] 1 \ --[parameter-name]=$(date +"%Y-%m-%d") \ ...
# Example: python [name].py --local-scheduler [class] --[parameter-name] [value]
# To enable Central Scheduler, write: luigid (at 'localhost:8082')
if __name__ == "__main__":
    luigi.run()
