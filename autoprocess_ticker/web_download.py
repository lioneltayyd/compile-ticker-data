

# Python modules.
from datetime import datetime

# Personal modules.
from configuration.config import ticker, ETF_folder, project_path


def download_ticker_data(driver_path, url):
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import Select
    from time import sleep
    
    # Open Chrome driver to access the webpage. 
    driver = webdriver.Chrome(executable_path=driver_path)
    driver.get(url)
    print('Data downloaded:\n', url, '\n')
    
    # Try the Download button and click it. 
    try: driver.find_element_by_css_selector("svg[data-icon='download']").click()
    except: sleep(5)
        
    driver.close()
    
    
def move_file(ETF_folder, ticker, ticker_filename):
    import os, shutil

    # Create new folder for the specific ticker. 
    try: os.makedirs(f'{ETF_folder}/{ticker}')
    except: pass

    # Go to home directory. 
    os.chdir('/Users/lioneltay')

    # Rename file. 
    os.rename(f'Downloads/{ticker}.csv', f'Downloads/{ticker_filename}')

    # Create directory path. 
    home_path = os.getcwd() 
    trdr_path = os.path.join(home_path, f'Google Drive/TR_research/compile_data/{ETF_folder}/{ticker}') 
    downloads_path = os.path.join(home_path, f'Downloads/{ticker_filename}')

    # Move file from the 'Downloads' directory to the destination. 
    try: shutil.move(downloads_path, trdr_path)
    except: os.replace(downloads_path, os.path.join(trdr_path, ticker_filename))

    # Revert back to project directory. 
    os.chdir(project_path)

    
def multiple_download(driver_path, ticker, ticker_freq, start_date, end_date, yahoo_version):
    # Direct download link if Selenium fails to detect the HTML line. 
    ticker_download = f'https://query1.finance.yahoo.com/{yahoo_version}/finance/download/{ticker}?'
    ticker_download_param = f'period1={start_date}&period2={end_date}&interval={ticker_freq}&events=history'
    ticker_download_url = "".join([ticker_download, ticker_download_param])
    
    # Download data and rename file. 
    download_ticker_data(driver_path, ticker_download_url) 
    ticker_filename = f'{ticker}_{ticker_freq}.csv'
    
    # Move the file to project directory. 
    move_file(ETF_folder, ticker, ticker_filename)
    print('File moved\n')