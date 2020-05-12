

from datetime import datetime
import os, pickle, regex as re
import pandas as pd

import panel as pn
pn.extension()

# Personal modules. 
from configuration.config import project_path, ETF_folder, freq_keys
from configuration.config_dash import * 


# --------------------------------------------------------------
# Styling Dataframe. 
# --------------------------------------------------------------

def formatting_dataframe(df, freq):
    '''
    Purpose: 
        Formatting the dataframe. 

    Input  :
        df  : Dataframe. 
        freq: Str. Must be either monthly / weekly / daily_byTrdrDay / daily_byWeekday. 
        
    Return :
        Formatted dataframe.
    '''
    
    df_copy = df.copy()
    df_copy.reset_index(inplace=True)
    
    # Apply formatting. 
    df_formatted = df_copy.style.format(formatter={
        "avg_diff": "{:.3%}", 
        "med_diff": "{:.3%}", 
        "tot_diff": "{:.3%}",
        "max_diff": "{:.3%}",
        "min_diff": "{:.3%}",
        "std_diff": "{:.3%}",
        "pos_avg_diff": "{:.3%}",
        "neg_avg_diff": "{:.3%}",
        "up_prob": "{:.2%}",
        "down_prob": "{:.2%}",})\
    .hide_index()\
    .highlight_null(null_color='gray')\
    .applymap(lambda x: f'background-color: {"lightblue" if x > 0 else ""}', subset=['up_overall'])\
    .applymap(lambda x: f'background-color: {"lightblue" if x >= .70 else ""}', subset=['up_prob','down_prob'])\
    .bar(align='mid', color=['#FFA07A', 'lightgreen'], 
         subset=["avg_diff",'med_diff','tot_diff','std_diff','pos_avg_diff','neg_avg_diff'])
    
    # Highlight today (light blue). 
    today = datetime.today().month
    freq_col = 'month'
    
    if freq in set(['weekly','daily_byWeekday']):
        today = datetime.today().isocalendar()[1]
        freq_col = 'week'
        
    df_formatted = df_formatted.applymap(
        lambda x: f'background-color: {"lightblue" if x == today else ""}', subset=[freq_col])    
    return df_formatted


def styling_dataframe(pivot_stats, pivot_stats_styled, freq_keys):
    '''
    Purpose: 
        Style the dataframe. 

    Input  :
        pivot_stats_styled: Dict. To store the styled dataframes. 
        freq_keys         : List. Must contain monthly, weekly, daily_byTrdrDay, daily_byWeekday. 
        
    Return :
        None.
    '''
    
    # Extract the relevant keys with regex. 
    for freq in freq_keys:    
        re_compile = re.compile(f'(?:{freq})(?:_R.*Yr)?(?!_)')
        stats_keys = list(filter(re_compile.match, pivot_stats.keys()))
        
        # Perform formatting. 
        for key in stats_keys:
            pivot_stats_styled[key] = formatting_dataframe(pivot_stats[key], freq)


# --------------------------------------------------------------
# Plot. 
# --------------------------------------------------------------

@pn.depends(selectFolderETF.param.value)
def displayTickerData(ETF_folder):
    # Gather a list of ETF ticker symbols. 
    try:
        sectors_ticker_path = os.path.join(project_path, ETF_folder)
        re_compileKey = re.compile(r'(?<!\..*)[A-Z]')
        ls_tickers = list(filter(re_compileKey.match, os.listdir(sectors_ticker_path)))
        default_option = ls_tickers[0]
    except:
        ls_tickers = []
        default_option = ''
    
    # Create widget. 
    selectTicker = pn.widgets.Select(name='Ticker', options=ls_tickers, value=default_option, height=50)
    widgetsTicker = pn.WidgetBox(selectTicker, height=70, width=175, sizing_mode='stretch_width')
    pivot_stats_styled = {selectTicker.value: {}}
    
    # --------------------------------------------------------------
    # Read File.
    # --------------------------------------------------------------
    
    def read_pickle(filename, ticker, tup_idx=False):        
        storage_path = os.path.join(project_path, ETF_folder, ticker, 'storage')    

        with open(f'{storage_path}/{filename}', 'rb') as in_file:
            pivot_stats = pickle.load(in_file)[1]
            if type(tup_idx) is int:
                pivot_stats = pivot_stats[tup_idx]
        return pivot_stats
    
    # --------------------------------------------------------------
    # Display Price Change.
    # --------------------------------------------------------------
    
    @pn.depends(selectTicker.param.value, selectFreq.param.value, selectYrRange.param.value, 
                selectMth.param.value, selectWk.param.value)
    def displayPriceChange(ticker, freq, yrRange, month, week):
        try: list(pivot_stats_styled.keys())[0]
        except: 
            # Read file. 
            pivot_stats = read_pickle('pivot_stats.pickle', ticker)
            pivot_stats_copy = pivot_stats
            
            # Style the dataframe. 
            pivot_stats_styled = {ticker: {}}
            styling_dataframe(pivot_stats, pivot_stats_styled[ticker], freq_keys)
            
        if ticker != list(pivot_stats_styled.keys())[0]: 
            # Read file.
            pivot_stats = read_pickle('pivot_stats.pickle', ticker) 
            pivot_stats_copy = pivot_stats
            
            # Style the dataframe. 
            pivot_stats_styled = {ticker: {}}
            styling_dataframe(pivot_stats_copy, pivot_stats_styled[ticker], freq_keys) 
        
        stats_key = f'{freq}_{yrRange}'
        if yrRange == 'MaxYr':
            stats_key = freq
        return pn.Column(pivot_stats_styled[ticker][stats_key], width=1200, height=500)
    
    # --------------------------------------------------------------
    # Dashboard.
    # --------------------------------------------------------------
    
    dashDf = pn.Row(widgetsTable, displayPriceChange)
    dash_seasonalStatsDf = pn.Column(widgetsFolderETF, widgetsTicker, dashDf)
    return dash_seasonalStatsDf 