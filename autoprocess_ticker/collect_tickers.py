

import subprocess, os

# Personal modules.
from configuration.config import log_folder, ETF_folder, yahoo_version, local_scheduler, workers


# --------------------------------------------------------------
# Collect Multiple Tickers Data.
# --------------------------------------------------------------

def collect_sectors(dict_data):
    # START_YR and TICKER will be iterated. 
    for start_yr, ticker_list in dict_data.items(): 
        for ticker in ticker_list:
            log_path = f'logging/{log_folder}'
            try: os.makedirs(log_path)
            except: pass

            with open(f'{log_path}/{ticker}_log.txt', 'w') as text_file:
                print(local_scheduler, ticker, start_yr, ETF_folder, yahoo_version)
                subprocess.run([
                    'python', 'luigi_pipeline.py', local_scheduler, 'CompileToExcel', 
                    '--ticker', ticker, '--start-yr', f'{start_yr}', '--ETF-folder', ETF_folder, 
                    '--yahoo-version', yahoo_version, '--workers', workers
                ], stderr=text_file) 


def collect_equities(dict_data):
    log_folder = 'ETF_equity'
    for ETF, dict_obj in dict_data.items():
        ETF_folder = f'dataset/{log_folder}/{ETF}'

        # START_YR and TICKER will be iterated. 
        for start_yr, ticker_list in dict_obj.items(): 
            for ticker in ticker_list:
                log_path = f'logging/{log_folder}'
                try: os.makedirs(log_path)
                except: pass

                with open(f'{log_path}/{ticker}_log.txt', 'w') as text_file:
                    print(local_scheduler, ticker, start_yr, ETF_folder, yahoo_version)
                    subprocess.run([
                        'python', 'luigi_pipeline.py', local_scheduler, 'CompileToExcel', 
                        '--ticker', ticker, '--start-yr', f'{start_yr}', '--ETF-folder', ETF_folder, 
                        '--yahoo-version', yahoo_version, '--workers', workers
                    ], stderr=text_file) 