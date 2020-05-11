

# For visualisation. 
from bokeh.io import curdoc
import panel as pn
import holoviews as hv
import hvplot.pandas

# Configuration. 
hv.extension("bokeh", "matplotlib") 
pn.extension()

# Personal modules. 
from configuration.config_dash import * 
from autovisualise_ticker import ticker_plot


# --------------------------------------------------------------
# Create Dashboard. 
# --------------------------------------------------------------

renderer = hv.renderer('bokeh').instance(mode='server')

# Dashboard.
dashPrice = pn.Row(widgetsInfo, ticker_plot.plotBarPrice)
dashVol = pn.Row(widgetsVol, ticker_plot.plotBarVol)
dashHolidays = pn.Row(widgetsHolidays, ticker_plot.plotBarPriceHolidays)
dashSpecialDays = pn.Row(widgetsSpecialDays, ticker_plot.plotBarPriceSpecialDays)
dashTWW = pn.Row(widgetsTWW, ticker_plot.plotBarPriceTWW)

dash_seasonalStats = pn.Column(
    widgetsTicker, pn.Tabs(
        ('TickerPrice', pn.Column(dashPrice, margin=[vSpace,0])),
        ('TickerVol', pn.Column(dashVol, margin=[vSpace,0])),
        ('TickerHolidays', pn.Column(dashHolidays, margin=[vSpace,0])), 
        ('TickerSpecialDays', pn.Column(dashSpecialDays, margin=[vSpace,0])), 
        ('TickerTWW', pn.Column(dashTWW, margin=[vSpace,0])),
        margin=[vSpace + 5,hSpace], sizing_mode='stretch_width'), 
margin=[vSpace + 5,hSpace])


# --------------------------------------------------------------
# View Dashboard On Local Server. 
# --------------------------------------------------------------

# !panel serve --show [notebook.ipynb / file.py] --port [5000]
dash_seasonalStats.servable(title='SeasonalStats')
# doc = renderer.server_doc(dash_seasonalStats)
# doc.title = 'SeasonalStats'