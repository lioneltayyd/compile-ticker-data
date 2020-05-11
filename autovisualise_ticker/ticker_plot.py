

# For data preprocessing. 
import pandas as pd

# For file management. 
import os, pickle

# For visualisation. 
import panel as pn
import holoviews as hv
import hvplot.pandas

hv.extension("bokeh", "matplotlib") 
pn.extension()

# Personal modules.
from configuration.config import project_path, ETF_folder
from configuration.config_dash import * 


# --------------------------------------------------------------
# Plot. 
# --------------------------------------------------------------

# @pn.depends(selectFolderETF.param.value)
# def plotTickerData(ETF_folder):
#     # Gather a list of ETF ticker symbols. 
#     try:
#         sectors_ticker_path = os.path.join(project_path, ETF_folder)
#         re_compileKey = re.compile(r'(?<!\..*)[A-Z]')
#         ls_tickers = list(filter(re_compileKey.match, os.listdir(sectors_ticker_path)))
#         default_option = ls_tickers[0]
#     except:
#         ls_tickers = []
#         default_option = ''
    
#     # Create widget. 
#     selectTicker = pn.widgets.Select(name='Ticker', options=ls_tickers, value=default_option, height=50)
#     widgetsTicker = pn.WidgetBox(selectTicker, height=70, width=175, sizing_mode='stretch_width')

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
# Plot Price Change.
# --------------------------------------------------------------

@pn.depends(selectTicker.param.value, selectFreq.param.value, selectYrRange.param.value, 
            selectMth.param.value, selectWk.param.value) 
def plotBarPrice(ticker, freq, YrRange, month, week):
    '''
    Purpose: 
        Visualise price change.

    Input  :
        ticker : Str. Ticker symbol.
        freq   : Str. Must be monthly / weekly / daily_byTrdrDay / daily_byWeekday. 
        YrRange: Str. Year range. 
        month  : Int. Month of the year.
        week   : Int. Week of the year. 

    Return :
        Holoview plot.
    '''

    # Update the dictionary data. 
    pivot_stats = read_pickle('pivot_stats.pickle', ticker=ticker)

    # Look for the dataframe within the dictionary. 
    if YrRange == 'MaxYr':
        pivot_data = pivot_stats[freq]
    else:
        pivot_data = pivot_stats[f'{freq}_{YrRange}']

    # Horizontal lines for probability. 
    upper_probHline = hv.HLine(0.7).opts(line_width=1, color='black')
    lower_probHline = hv.HLine(0.3).opts(line_width=1, color='black')

    if freq == 'daily_byTrdrDay' or freq == 'daily_byWeekday':
        if freq == 'daily_byTrdrDay':
            idxRow = month
        elif freq == 'daily_byWeekday':
            idxRow = week

        # Visualise the price change by month or week. 
        errorbar = pivot_data.loc[idxRow,:].hvplot.errorbars(y='avg_diff', yerr1='std_diff') 
        bar_avgDiff = pivot_data.loc[idxRow,:].hvplot(kind='bar', y='avg_diff', width=width, tools=tools)

        # Visualise the probability counts by month or week. 
        bar_upProb = pivot_data.loc[idxRow,:].hvplot(kind='bar', y='up_prob', width=width, ylim=(0,1), 
                                                        tools=tools)

        # Visualise up and down counts by month or week. 
        bar_counts = pivot_data.loc[idxRow,:].hvplot(kind='bar', y=['up_counts', 'down_counts'], 
                                                        width=width, stacked=True, legend='top', tools=tools)

    else:
        # Visualise the price change.
        errorbar = pivot_data.hvplot.errorbars(y='avg_diff', yerr1='std_diff') 
        bar_avgDiff = pivot_data.hvplot(kind='bar', y='avg_diff', width=width, tools=tools) 

        # Visualise the probability counts. 
        bar_upProb = pivot_data.hvplot(kind='bar', y='up_prob', width=width, ylim=(0,1), tools=tools) 

        # Visualise up and down counts. 
        bar_counts = pivot_data.hvplot(kind='bar', y=['up_counts', 'down_counts'], 
                                        width=width, stacked=True, legend='top', tools=tools)

    return pn.Column(bar_avgDiff * errorbar, bar_upProb * upper_probHline * lower_probHline, bar_counts)

# --------------------------------------------------------------
# Plot Volume.
# --------------------------------------------------------------

@pn.depends(selectTicker.param.value, selectFreq.param.value, selectMth.param.value, 
            selectWk.param.value, toggleOverall.param.value) 
def plotBarVol(ticker, freq, month, week, overallVol):
    '''
    Purpose: 
        Visualise volume. 

    Input  :
        ticker    : Str. Ticker symbol.
        freq      : Str. Must be monthly / weekly / daily_byTrdrDay / daily_byWeekday. 
        month     : Int. Month of the year.
        week      : Int. Week of the year. 
        overallVol: Bool. Plot by the overall average. 

    Return :
        Holoview plot. 
    '''

    # Update the dictionary data. 
    pivot_volume_stats = read_pickle('pivot_vol_stats.pickle', ticker=ticker)

    # Visualise the yearly volume. 
    bar_yearlyVol = pivot_volume_stats[f'{freq}_avgVolCol'].hvplot(kind='bar', y='avgVolCol', 
                                                                    width=width, tools=tools) 

    # Look for the dataframe within the dictionary. 
    pivot_data = pivot_volume_stats[f'{freq}_avgVolRow']

    if (freq == 'daily_byTrdrDay' or freq == 'daily_byWeekday'):
        if freq == 'daily_byTrdrDay':
            idxRow = month
        elif freq == 'daily_byWeekday':
            idxRow = week

        if overallVol:
            # Average volume. 
            avgHline = hv.HLine(pivot_data.mean(level=1, axis=0).mean(axis=0)[0]).opts(
                line_width=1, color='black')

            # Average volume by month / week. 
            bar_freqVol = pivot_data.mean(level=1, axis=0).hvplot(kind='bar', y='avgVolRow', 
                                                                    width=width, tools=tools)

            # Above and below average counts by month / week. 
            bar_counts = pivot_volume_stats[freq].mean(level=1, axis=0).hvplot(
                kind='bar', y=['abv_avgVolCounts','blw_avgVolCounts'], width=width, 
                stacked=True, legend='top', tools=tools)
        else:
            # Average volume. 
            avgHline = hv.HLine(pivot_data.loc[idxRow,:].mean(axis=0)[0]).opts(line_width=1, color='black', 
                                                                                tools=tools)

            # Average volume by month / week. 
            bar_freqVol = pivot_data.loc[idxRow,:].hvplot(kind='bar', y='avgVolRow', 
                                                            width=width, tools=tools)

            # Above and below average counts by month / week. 
            bar_counts = pivot_volume_stats[freq].loc[idxRow,:].hvplot(
                kind='bar', y=['abv_avgVolCounts','blw_avgVolCounts'], width=width, 
                stacked=True, legend='top', tools=tools)

    else:
        # Average volume. 
        avgHline = hv.HLine(pivot_data.mean(axis=0)[0]).opts(line_width=1, color='black', tools=tools)

        # Average volume by month / week. 
        bar_freqVol = pivot_data.hvplot(kind='bar', y='avgVolRow', width=width, tools=tools) 

        # Above and below average counts by month / week. 
        bar_counts = pivot_volume_stats[freq].hvplot(kind='bar', y=['abv_avgVolCounts','blw_avgVolCounts'], 
                                                        width=width, stacked=True, legend='top', tools=tools)

    return pn.Column(bar_yearlyVol, bar_freqVol * avgHline, bar_counts) 

# --------------------------------------------------------------
# Plot Holidays.
# --------------------------------------------------------------

@pn.depends(selectTicker.param.value, selectHolidays.param.value, selectYrRange.param.value) 
def plotBarPriceHolidays(ticker, key, YrRange):
    '''
    Purpose: 
        Visualise price change.

    Input  :
        ticker : Str. Ticker symbol.
        key    : Str. Must be a holiday / observance. 
        YrRange: Str. Year range. 

    Return :
        Holoview plot.
    '''

    # Update the dictionary data. 
    pivot_holidays_stats = read_pickle('pivot_uniqueDays.pickle', ticker=ticker, tup_idx=0)

    # Look for the dataframe within the dictionary. 
    if YrRange == 'MaxYr':
        pivot_data = pivot_holidays_stats[key]
    else:
        pivot_data = pivot_holidays_stats[f'{key}_{YrRange}']

    # Visualise the price change.
    errorbar = pivot_data.hvplot.errorbars(y='avg_diff', yerr1='std_diff') 
    bar_avgDiff = pivot_data.hvplot(kind='bar', y='avg_diff', width=width, tools=tools)

    # Visualise the probability and horizontal lines for probability. 
    upper_probHline = hv.HLine(0.7).opts(line_width=1, color='black')
    lower_probHline = hv.HLine(0.3).opts(line_width=1, color='black')
    bar_upProb = pivot_data.hvplot(kind='bar', y='up_prob', width=width, ylim=(0,1), tools=tools)

    # Visualise up and down counts. 
    bar_counts = pivot_data.hvplot(kind='bar', y=['up_counts', 'down_counts'], 
                                            width=width, stacked=True, legend='top', tools=tools)

    return pn.Column(bar_avgDiff * errorbar, bar_upProb * upper_probHline * lower_probHline, bar_counts)

# --------------------------------------------------------------
# Plot Price Change On Special Days.
# --------------------------------------------------------------

@pn.depends(selectTicker.param.value, selectSpecialDays.param.value, selectYrRange.param.value, 
            selectMth.param.value) 
def plotBarPriceSpecialDays(ticker, key, YrRange, month):
    '''
    Purpose: 
        Visualise price change.

    Input  :
        ticker : Str. Ticker symbol.
        key    : Str. Must be special day. 
        YrRange: Str. Year range. 
        month  : Int. Month of the year.

    Return :
        Holoview plot.
    '''

    # Update the dictionary data. 
    pivot_specialDays_stats = read_pickle('pivot_uniqueDays.pickle', ticker=ticker, tup_idx=1)

    # Look for the dataframe within the dictionary. 
    if YrRange == 'MaxYr':
        pivot_data = pivot_specialDays_stats[key]
    else:
        pivot_data = pivot_specialDays_stats[f'{key}_{YrRange}']

    upper_probHline = hv.HLine(0.7).opts(line_width=1, color='black')
    lower_probHline = hv.HLine(0.3).opts(line_width=1, color='black')

    if key == 'firstTrdrDoM_byMonth' or key == 'superDay_byMonth': 
        idxRow = month 

        # Visualise the price change by month or week. 
        errorbar = pivot_data.loc[idxRow,:].hvplot.errorbars(y='avg_diff', yerr1='std_diff') 
        bar_avgDiff = pivot_data.loc[idxRow,:].hvplot(kind='bar', y='avg_diff', width=width, tools=tools)

        # Visualise the probability counts by month or week. 
        bar_upProb = pivot_data.loc[idxRow,:].hvplot(kind='bar', y='up_prob', width=width, 
                                                        ylim=(0,1), tools=tools)

        # Visualise up and down counts by month or week. 
        bar_counts = pivot_data.loc[idxRow,:].hvplot(kind='bar', y=['up_counts', 'down_counts'], 
                                                        width=width, stacked=True, legend='top', tools=tools)

    else:
        # Visualise the price change.
        errorbar = pivot_data.hvplot.errorbars(y='avg_diff', yerr1='std_diff') 
        bar_avgDiff = pivot_data.hvplot(kind='bar', y='avg_diff', width=width, tools=tools)

        # Visualise the probability counts. 
        bar_upProb = pivot_data.hvplot(kind='bar', y='up_prob', width=width, ylim=(0,1), tools=tools)

        # Visualise up and down counts. 
        bar_counts = pivot_data.hvplot(kind='bar', y=['up_counts', 'down_counts'], 
                                        width=width, stacked=True, legend='top', tools=tools)

    return pn.Column(bar_avgDiff * errorbar, bar_upProb * upper_probHline * lower_probHline, bar_counts)

# --------------------------------------------------------------
# Plot Price Change On TWW.
# --------------------------------------------------------------

@pn.depends(selectTicker.param.value, selectTWW.param.value, selectYrRange.param.value, 
            toggleWk.param.value) 
def plotBarPriceTWW(ticker, key, YrRange, weekly): 
    '''
    Purpose: 
        Visualise price change.

    Input  :
        ticker : Str. Ticker symbol.
        key    : Str. Must be TWW. 
        YrRange: Str. Year range. 
        Weekly : Bool. Plot by weekly data. 

    Return :
        Holoview plot.
    '''

    idx_file = 1
    if weekly:
        idx_file = 2

    # Update the dictionary data. 
    pivot_specialDays_stats = read_pickle('pivot_uniqueDays.pickle', ticker=ticker, tup_idx=idx_file)

    # Look for the dataframe within the dictionary. 
    if YrRange == 'MaxYr':
        pivot_data = pivot_specialDays_stats[key]
    else:
        pivot_data = pivot_specialDays_stats[f'{key}_{YrRange}']

    # Reduce the multi-index to single level. 
    pivot_copy = pivot_data.reset_index(level=0).copy()

    # Visualise the price change.
    bar_avgDiff = pivot_copy.hvplot(kind='bar', y='avg_diff', by='twwPeriod', width=width, 
                                    shared_axes=False, tools=tools)

    # Visualise the probability. 
    bar_upProb = pivot_copy.hvplot(kind='bar', y='up_prob', by='twwPeriod',width=width, 
                                    ylim=(0,1), tools=tools)

    # Visualise up and down counts. 
    bar_counts = pivot_copy.hvplot(kind='bar', y=['up_counts','down_counts'], by='twwPeriod',
                                    width=width, stacked=False, legend='top', tools=tools)

    return pn.Column(bar_avgDiff , bar_upProb, bar_counts)


# --------------------------------------------------------------
# Compile Plots. 
# --------------------------------------------------------------

# # Dashboard.
# dashPrice = pn.Row(widgetsInfo, plotBarPrice)
# dashVol = pn.Row(widgetsVol, plotBarVol)
# dashHolidays = pn.Row(widgetsHolidays, plotBarPriceHolidays)
# dashSpecialDays = pn.Row(widgetsSpecialDays, plotBarPriceSpecialDays)
# dashTWW = pn.Row(widgetsTWW, plotBarPriceTWW)

# dash_seasonalStats = pn.Column(
#     widgetsFolderETF, widgetsTicker, pn.Tabs(
#         ('TickerPrice', pn.Column(dashPrice, margin=[vSpace,0])),
#         ('TickerVol', pn.Column(dashVol, margin=[vSpace,0])),
#         ('TickerHolidays', pn.Column(dashHolidays, margin=[vSpace,0])), 
#         ('TickerSpecialDays', pn.Column(dashSpecialDays, margin=[vSpace,0])), 
#         ('TickerTWW', pn.Column(dashTWW, margin=[vSpace,0])),
#         margin=[vSpace + 5,hSpace], sizing_mode='stretch_width'), 
# margin=[vSpace + 5,hSpace], sizing_mode='stretch_width')

# return dash_seasonalStats 