

# For visualisation. 
from bokeh.io import curdoc
import panel as pn
import holoviews as hv
import hvplot.pandas

# Configuration. 
hv.extension("bokeh", "matplotlib") 
pn.extension()

# Personal modules. 
from configuration.config_dash import widgetsFolderETF, widgetsTicker 
from autovisualise_ticker import ticker_table


# --------------------------------------------------------------
# Create Dashboard. 
# --------------------------------------------------------------

renderer = hv.renderer('bokeh').instance(mode='server')

# Dashboard. 
dash_seasonalStatsDf = pn.Column(ticker_table.displayTickerData)


# --------------------------------------------------------------
# View Dashboard On Local Server. 
# --------------------------------------------------------------

# !panel serve --show [notebook.ipynb / file.py] --port [5000]
dash_seasonalStatsDf.servable(title='SeasonalStats Tables')
# renderer.app(dash_eco, show=True, websocket_origin='localhost:5000')
# doc = renderer.server_doc(dash_eco)
# doc.title = 'FRED Economic Data'