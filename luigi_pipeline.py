

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
from configuration.config import *
from autoprocess_ticker import web_download, data_management, preprocessing, compile_uniqueDays


# --------------------------------------------------------------
# Pipeline.
# --------------------------------------------------------------

class DownloadTickerData(ExternalProgramTask):
    # Ticker info. 
    ticker = luigi.Parameter(default=None)
    ticker_freq = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    ETF_folder = luigi.Parameter(default=ETF_folder) 
    yahoo_version = luigi.Parameter(default=yahoo_version)
    
    # Chrome Driver path. 
    driver_path = luigi.Parameter(default=driver_path)
    
    def output(self):
        ticker_filename = f'{self.ticker}_{self.ticker_freq}.csv'
        return luigi.LocalTarget(f"{self.ETF_folder}/{self.ticker}/{ticker_filename}") 
    
    def program_args(self):
        start_date = {
            '1mo': int(datetime.timestamp(datetime(self.start_yr - 1, 11, 30))),
            '1wk': int(datetime.timestamp(datetime(self.start_yr - 1, 12, 28))),
            '1d': int(datetime.timestamp(datetime(self.start_yr - 1, 12, 23)))
        }
        end_date = {
            '1mo': int(datetime.timestamp(datetime(end_yr + 1, 1, 1))),
            '1wk': int(datetime.timestamp(datetime(end_yr + 1, 1, 4))),
            '1d': int(datetime.timestamp(datetime(end_yr + 1, 1, 4)))
        }

        ticker_download = f'https://query1.finance.yahoo.com/{self.yahoo_version}/finance/download/{self.ticker}?'
        ticker_download_param = f'period1={start_date[self.ticker_freq]}&period2={end_date[self.ticker_freq]}&interval={self.ticker_freq}&events=history'
        ticker_download_url = "".join([ticker_download, ticker_download_param])

        with self.output().open('w') as out_file:
            return ["curl", "-L", "-o", self.output().path, ticker_download_url]
            

class ProcessTickerData(luigi.Task):
    # Ticker info. 
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    ETF_folder = luigi.Parameter(default=ETF_folder) 
    yahoo_version = luigi.Parameter(default=yahoo_version) 

    def requires(self):
        return {
            freq_keys[0]: DownloadTickerData(self.ticker, ticker_freq[0], self.start_yr, self.ETF_folder, self.yahoo_version),
            freq_keys[1]: DownloadTickerData(self.ticker, ticker_freq[1], self.start_yr, self.ETF_folder, self.yahoo_version),
            freq_keys[2]: DownloadTickerData(self.ticker, ticker_freq[2], self.start_yr, self.ETF_folder, self.yahoo_version),
            freq_keys[3]: DownloadTickerData(self.ticker, ticker_freq[2], self.start_yr, self.ETF_folder, self.yahoo_version)
        }
    
    def output(self):
        return luigi.LocalTarget(f"{self.ETF_folder}/{self.ticker}/storage/df_ticker.pickle", format=luigi.format.Nop) 

    def run(self):
        # Read file. 
        df_ticker = {freq: pd.read_csv(self.input()[freq].path, parse_dates=['Date']) for freq in freq_keys} 

        # Initial preprocessing. 
        preprocessing.init_preprocess(df_ticker, freq_keys)

        with self.output().open('w') as out_file:
            pickle.dump(df_ticker, out_file)


class PivotTickerSummary(luigi.Task):
    # Ticker info. 
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    ETF_folder = luigi.Parameter(default=ETF_folder) 
    yahoo_version = luigi.Parameter(default=yahoo_version) 

    def requires(self):
        return ProcessTickerData(self.ticker, self.start_yr, self.ETF_folder, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.ETF_folder}/{self.ticker}/storage/pivot_stats.pickle", 
                                 format=luigi.format.Nop) 

    def run(self):
        with self.input().open() as in_file:
            df_ticker = pickle.load(in_file)

        # Create pivot tables. 
        preprocessing.create_pivot(df_ticker, pivot_ticker, freq_keys, freq_cols, value='price_diff')

        # Create statistical summary from pivot tables. 
        start_yr_range[0] = self.start_yr
        preprocessing.summarise_pivot(pivot_ticker, pivot_stats, freq_keys, start_yr_range, end_yr)

        with self.output().open('w') as out_file:
            pickle.dump((pivot_ticker, pivot_stats), out_file) 


class PivotVolSummary(luigi.Task):
    # Ticker info. 
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    ETF_folder = luigi.Parameter(default=ETF_folder) 
    yahoo_version = luigi.Parameter(default=yahoo_version) 

    def requires(self):
        return ProcessTickerData(self.ticker, self.start_yr, self.ETF_folder, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.ETF_folder}/{self.ticker}/storage/pivot_vol_stats.pickle", 
                                 format=luigi.format.Nop) 

    def run(self):
        with self.input().open() as in_file:
            df_ticker = pickle.load(in_file)
        
        # Create pivot tables. 
        preprocessing.create_pivot(df_ticker, pivot_volume, freq_keys, freq_cols, value='volume')

        # Create statistical summary from pivot tables. 
        start_yr_range[0] = self.start_yr
        preprocessing.compute_avgVol(pivot_volume, pivot_volume_stats, freq_keys, [start_yr_range[0]], end_yr)
        preprocessing.summarise_pivot_vol(pivot_volume, pivot_volume_stats, freq_keys, [start_yr_range[0]], end_yr)

        with self.output().open('w') as out_file:
            pickle.dump((pivot_volume, pivot_volume_stats), out_file) 


class TraceUniquePeriod(luigi.Task):
    # Ticker info. 
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    ETF_folder = luigi.Parameter(default=ETF_folder) 
    yahoo_version = luigi.Parameter(default=yahoo_version) 

    def requires(self):
        return ProcessTickerData(self.ticker, self.start_yr, self.ETF_folder, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.ETF_folder}/{self.ticker}/storage/df_tickerUniqueDays.pickle", 
                                 format=luigi.format.Nop) 

    def run(self):
        with self.input().open() as in_file:
            df_ticker = pickle.load(in_file)

            df_ticker_weekly = df_ticker['weekly'].copy()
            df_ticker_trdrDay = df_ticker['daily_byTrdrDay'].copy()

            # Get the holidays, observances, and specialDay dates.
            df_holidays = compile_uniqueDays.compile_trdrHoliday_dates(self.start_yr, end_yr, holidays_keys)
            df_tww = compile_uniqueDays.get_tww_dates(self.start_yr, end_yr)
            tup_superDay = compile_uniqueDays.get_superDay_period(df_ticker_trdrDay) 
            tup_santaRally = compile_uniqueDays.get_santaRally_period(df_ticker_trdrDay, self.start_yr, end_yr)
        
            # Trace the TWW & special days for 'daily_trdrDay' data. 
            preprocessing.trace_specialDays(df_ticker_trdrDay, tup_superDay, tup_santaRally) 
            preprocessing.trace_tww_trdrDays(df_ticker_trdrDay, df_tww) 

            # Trace the TWW period for 'weekly' data. 
            preprocessing.trace_tww_trdrDays(df_ticker_weekly, df_tww)

            # Trace the holidays and observances. 
            preprocessing.trace_newYear(df_ticker_trdrDay, holidays_dict) 

            for holiday in specWeekdayHolidays:
                preprocessing.trace_specWeekdayHoliday(df_ticker_trdrDay, df_holidays, holidays_dict, holiday)
            for holiday in specWeekdayHolidays_backward:
                preprocessing.trace_specWeekdayHoliday(df_ticker_trdrDay, df_holidays, holidays_dict, holiday, 
                                                       day_forward=-1, idx_backtrace=2)
            for holiday in nonSpecHolidays:
                preprocessing.trace_nonSpecHoliday(df_ticker_trdrDay, df_holidays, holidays_dict, holiday) 
            for observance in nonSpecObservances:
                preprocessing.trace_nonSpecObservance(df_ticker_trdrDay, df_holidays, holidays_dict, observance)

            df_ticker['weekly'] = df_ticker_weekly 
            df_ticker['daily_byTrdrDay'] = df_ticker_trdrDay 

        with self.output().open('w') as out_file:
            pickle.dump(df_ticker, out_file) 


class PivotUniqueDaysSummary(luigi.Task):
    # Ticker info. 
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    ETF_folder = luigi.Parameter(default=ETF_folder) 
    yahoo_version = luigi.Parameter(default=yahoo_version) 

    def requires(self):
        return TraceUniquePeriod(self.ticker, self.start_yr, self.ETF_folder, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.ETF_folder}/{self.ticker}/storage/pivot_uniqueDays.pickle", 
                                 format=luigi.format.Nop) 

    def run(self):
        with self.input().open() as in_file:
            df_ticker = pickle.load(in_file)

            df_ticker_weekly = df_ticker['weekly'].copy()
            df_ticker_trdrDay = df_ticker['daily_byTrdrDay'].copy()

            # Create pivot tables. 
            preprocessing.create_pivot_uniqueDays(df_ticker_trdrDay, pivot_holidays, holidays_keys, self.start_yr, end_yr)
            preprocessing.create_pivot_uniqueDays(df_ticker_trdrDay, pivot_specialDays, specialDays_keys, self.start_yr, end_yr)
            preprocessing.create_pivot_uniqueDays(df_ticker_weekly, pivot_specialDays_weekly, specialDays_keys[5:], self.start_yr, end_yr)

            # Concat the TWW data and the data of the week after the TWW. 
            preprocessing.concat_pivot_tww(pivot_specialDays, specialDays_keys[5:9])
            preprocessing.concat_pivot_tww(pivot_specialDays_weekly, specialDays_keys[5:9])

            # Create statistical summary from pivot tables. 
            start_yr_range[0] = self.start_yr
            preprocessing.summarise_pivot(pivot_holidays, pivot_holidays_stats, holidays_keys, start_yr_range, end_yr)
            preprocessing.summarise_pivot(pivot_specialDays, pivot_specialDays_stats, specialDays_keys, start_yr_range, end_yr)
            preprocessing.summarise_pivot(pivot_specialDays_weekly, pivot_specialDays_weekly_stats, specialDays_keys[5:9], start_yr_range, end_yr)

            # Group multiple data into tuples. 
            pivot_tables = (pivot_holidays, pivot_specialDays, pivot_specialDays_weekly)
            pivot_tables_stats = (pivot_holidays_stats, pivot_specialDays_stats, pivot_specialDays_weekly_stats)

        with self.output().open('w') as out_file:
            pickle.dump((pivot_tables, pivot_tables_stats), out_file) 


class CompileToExcel(luigi.Task):
    # Ticker info. 
    ticker = luigi.Parameter(default=None)
    start_yr = luigi.IntParameter(default=1999) 
    ETF_folder = luigi.Parameter(default=ETF_folder) 
    yahoo_version = luigi.Parameter(default=yahoo_version) 

    def requires(self):
        yield PivotTickerSummary(self.ticker, self.start_yr, self.ETF_folder, self.yahoo_version),
        yield PivotVolSummary(self.ticker, self.start_yr, self.ETF_folder, self.yahoo_version), 
        yield PivotUniqueDaysSummary(self.ticker, self.start_yr, self.ETF_folder, self.yahoo_version) 

    def output(self):
        return luigi.LocalTarget(f"{self.ETF_folder}/{self.ticker}/{self.ticker}_seasonal_stats.xlsx",
                                 format=luigi.format.Nop) 

    def run(self):
        in_file_1, in_file_2, in_file_3 = self.input()
        
        with in_file_1[0].open() as in_file:
            pivot_ticker, pivot_stats = pickle.load(in_file)
        with in_file_2[0].open() as in_file:
            pivot_volume, pivot_volume_stats = pickle.load(in_file)
        with in_file_3.open() as in_file:
            pivot_tables, pivot_tables_stats = pickle.load(in_file)

        pivot_holidays, pivot_specialDays, pivot_specialDays_weekly = pivot_tables 
        pivot_holidays_stats, pivot_specialDays_stats, pivot_specialDays_weekly_stats = pivot_tables_stats 

        with pd.ExcelWriter(self.output().path) as out_file:
            # Save pivot ticker data and volume. Multiple sheets. 
            for i, freq in enumerate(freq_keys): 
                data_management.multiSheetWrite(out_file, pivot_ticker, pivot_stats, f'{sheetNames[i]}_pv', freq, 
                                                startcol, startrow, distance)
                data_management.multiSheetWrite(out_file, pivot_volume, pivot_volume_stats, f'{sheetNames[i]}_vol', 
                                                freq, startcol, startrow, distance) 
            
            # Save pivot tables holidays/observances data. Single sheet.
            data_management.singleSheetMultiWrite(out_file, pivot_holidays, pivot_holidays_stats, 'holi', holidays_keys, 
                                                  startcol, startrow, distance)
                
            # Save pivot tables TWW Weekly data. Single sheet.
            data_management.singleSheetMultiWrite(out_file, pivot_specialDays_weekly, pivot_specialDays_weekly_stats, 
                                                  'twwWk', specialDays_keys[5:9], startcol, startrow, distance)
                
            # Save pivot tables TWW Trdr data. Single sheet.
            data_management.singleSheetMultiWrite(out_file, pivot_specialDays, pivot_specialDays_stats, 
                                                  'twwTrdr', specialDays_keys[5:9], startcol, startrow, distance)
            
            # Save pivot tables Special Day data. Single sheet. 
            data_management.singleSheetMultiWrite(out_file, pivot_specialDays, pivot_specialDays_stats, 
                                                  'specialD', specialDays_keys[:5], startcol, startrow, distance)


# --------------------------------------------------------------
# Run.
# --------------------------------------------------------------

# Run Luigi on command window. 
# Example: luigi --module [python-file] [class] --[parameter-name] 1 -- workers 5
# Example: luigi --module [python-file] [class] --[parameter-name] 1 \ --[parameter-name]=$(date +"%Y-%m-%d") \ ...
# Example: python [name].py --local-scheduler [class] --[parameter-name] [value]
# To enable Central Scheduler, write: luigid
if __name__ == "__main__":
    luigi.run()