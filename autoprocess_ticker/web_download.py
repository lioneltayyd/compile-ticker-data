

import os, shutil, logging, time
from typing import Text, Optional

# For webpage interaction. 
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions 
from selenium.common.exceptions import TimeoutException

# Personal modules.
from config.config import (
    DRIVER_PATH, PROJECT_PATH, ETF_SECTOR_DIR, 
    LOG_PROCESSING_FILEPATH,  
    SLEEP, WEBPAGE_LOADING_TIMEOUT
)
from config.config_logger import setup_logger


# --------------------------------------------------------------
# Logger setup. 
# --------------------------------------------------------------

logger = logging.getLogger(__name__)
logger, file_handler, stream_handler = setup_logger(logger, LOG_PROCESSING_FILEPATH) 


# -------------------------------------------------------
# Selenium Loader. 
# -------------------------------------------------------

def launch_browser():
    # Settings for launching Chrome. 
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument(" - incognito")
    options.add_argument("--headless")

    # Set the browser to load the URL. 
    browser = webdriver.Chrome(executable_path=DRIVER_PATH, chrome_options=options)
    return browser


def wait_for_webpage_to_load(locate_element:Text, **kwargs):
    def inner(func):
        def wrapper(*args, **kwargs): 
            # Launch the browser and load the URL. 
            browser = launch_browser()
            browser.get(kwargs['url'])

            # You can't scrape the data until the site completes the load. 
            # So wait for it to load first. 
            try:
                logger.info(f"Waiting for the webpage to load.") 
                time.sleep(SLEEP)
                WebDriverWait(browser, WEBPAGE_LOADING_TIMEOUT).until(
                    expected_conditions.visibility_of_element_located((By.CSS_SELECTOR, locate_element))
                )
            # Raise an error if it takes too long. 
            except TimeoutException:
                logger.exception(f"----- Timed out for ({func}) while waiting for the webpage to load.") 
                browser.quit()
            
            # Return the loaded page. 
            return func(url=kwargs['url'], browser=browser, **kwargs) 
        return wrapper
    return inner


# ----------------------------------------------------------------------
# Download & Move Files. 
# ---------------------------------------------------------------------- 

@wait_for_webpage_to_load(locate_element='''svg[data-icon='download']''')
def download_ticker_data(url:Text, browser:Optional[WebDriver]=None):
    logger.info(f'Downloading the ticker data via ({url})') 
    
    # Try click the 'Download' button. Otherwise, raise exception. 
    try: browser.find_element_by_css_selector("svg[data-icon='download']").click()
    except: logger.exception(f'----- Exception occurs while trying to click the HTML element.')
    browser.quit()
    
    
def move_file(etf_dir:Text, ticker:Text, ticker_filename:Text):
    # Create new folder for the specific ticker. 
    try: os.makedirs(f'{etf_dir}/{ticker}')
    except: logger.debug(f'----- The ({etf_dir}/{ticker}) directory has already been created.')

    # Go to home directory. 
    os.chdir(os.environ['HOME_ABS_DIR'])

    # Rename file. 
    os.rename(f'Downloads/{ticker}.csv', f'Downloads/{ticker_filename}')

    # Create directory path. 
    dataset_abs_dir = os.path.join(os.environ['DATASET_ABS_DIR'], f'{etf_dir}/{ticker}') 
    downloads_abs_filepath = os.path.join(os.environ['HOME_ABS_DIR'], f'Downloads/{ticker_filename}') 

    # Move file from the 'Downloads' directory to the destination if the file doesn't exist. 
    # Otherwise, replace the file if it already exists. 
    try: shutil.move(downloads_abs_filepath, dataset_abs_dir) 
    except: os.replace(downloads_abs_filepath, os.path.join(dataset_abs_dir, ticker_filename))
    logger.debug(f'Moved ({downloads_abs_filepath}) file to ({dataset_abs_dir}) directory') 

    # Revert back to project directory. 
    os.chdir(PROJECT_PATH)

    
def start_downloading(
        etf_dir:Text, 
        ticker:Text, 
        ticker_freq:Text, 
        start_date:Text, 
        end_date:Text, 
        yahoo_version:Text
    ):

    for i, freq in enumerate(start_date.keys()):
        # Direct URL link for downloading the ticker data. 
        ticker_download = f'https://query1.finance.yahoo.com/{yahoo_version}/finance/download/{ticker}?'
        ticker_download_param = f'period1={start_date[freq]}&period2={end_date[freq]}&interval={ticker_freq[i]}&events=history'
        ticker_download_url = "".join([ticker_download, ticker_download_param])
        
        # Download data and rename file. 
        download_ticker_data(url=ticker_download_url) 
        ticker_filename = f'{ticker}_{ticker_freq}.csv'
        
        # Move the file to project directory. 
        move_file(etf_dir, ticker, ticker_filename)
