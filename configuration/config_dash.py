

import os
import regex as re
import panel as pn

# Personal modules.
from configuration.config import ETF_folder, optionsETF, project_path, freq_keys, yr_range, holidays_keys, specialDays_keys


# ----------------------------------------------------------------------
# Styled Dataframes. 
# ---------------------------------------------------------------------- 

# To store styled dataframes. 
pivot_stats_styled = {}


# ----------------------------------------------------------------------
# Style Customisation. 
# ---------------------------------------------------------------------- 

height, width = (700,850)
vSpace, hSpace = (10,10)

# CSS styling. 
style_title = {
    "text-align": "center",
    "border-radius": "0px",
    "padding": "0px",
    "background": "rgb(230,230,230)",
    "color": "rgb(50,50,50)",
    "border": "0px solid Gray",
} 


# ----------------------------------------------------------------------
# Elements. 
# ---------------------------------------------------------------------- 

# Text and pane element. 
title = pn.pane.Markdown('#Seasonal Statisitcal Analysis', sizing_mode='stretch_width', height=65, style=style_title)

# Hover.
tools = ['crosshair']


# ----------------------------------------------------------------------
# Widgets. 
# ---------------------------------------------------------------------- 

sectors_ticker_path = os.path.join(project_path, ETF_folder)
re_compileKey = re.compile(r'(?<!\..*)[A-Z]')
ls_tickers = list(filter(re_compileKey.match, os.listdir(sectors_ticker_path)))
default_option = ls_tickers[0]

# Widgets. 
selectFolderETF = pn.widgets.Select(name='FolderETF', options=optionsETF, value='dataset/ETF_sector', height=50)
selectTicker = pn.widgets.Select(name='Ticker', options=ls_tickers, value=default_option, height=50)
selectYrRange = pn.widgets.Select(name="YrRange", options=yr_range, value="MaxYr", height=50)
selectFreq = pn.widgets.Select(name="Freq", options=freq_keys, value="monthly", height=50)
selectMth = pn.widgets.IntSlider(name="TrdrDayByMonth", start=1, end=12, value=1)
selectWk = pn.widgets.IntSlider(name="TrdrWeekdayByWeek", start=1, end=53, value=1)
selectHolidays = pn.widgets.Select(name="Holidays", options=holidays_keys, value="newYear", height=50)
selectSpecialDays = pn.widgets.Select(name="SpecialDays", options=specialDays_keys[:5], 
                                      value="firstTrdrDoM", height=50)
selectTWW = pn.widgets.Select(name="TWW", options=specialDays_keys[5:9], value="twwQ1", height=50)
toggleWk = pn.widgets.Toggle(name="viewByWeeklyData", button_type='default', value=False)
toggleOverall = pn.widgets.Toggle(name="viewOverallDailyVol", button_type='default', value=False)

# Widget box. 
widgetsFolderETF = pn.WidgetBox(selectFolderETF, height=70, width=175, sizing_mode='stretch_width')
widgetsTicker = pn.WidgetBox(selectTicker, height=70, width=175, sizing_mode='stretch_width')
widgetsInfo = pn.WidgetBox(selectFreq, selectYrRange, selectMth, selectWk, height=250, width=175)
widgetsTable = pn.WidgetBox(selectFreq, selectYrRange, height=250, width=175)
widgetsVol = pn.WidgetBox(selectFreq, toggleOverall, selectMth, selectWk, height=250, width=175)
widgetsHolidays = pn.WidgetBox(selectHolidays, selectYrRange, height=130, width=175)
widgetsSpecialDays = pn.WidgetBox(selectSpecialDays, selectYrRange, selectMth, height=190, width=175)
widgetsTWW = pn.WidgetBox(selectTWW, selectYrRange, toggleWk, height=190, width=175)