import panel as pn
import param
import holoviews as hv
import geoviews as gv
import datashader as ds
import datashader.transfer_functions as tf
from holoviews.operation.datashader import datashade, dynspread
from holoviews import opts
from bokeh.tile_providers import get_provider, CARTODBPOSITRON

# Load tile source
tile_provider = gv.tile_sources.OSM()

# Assuming df and custom_fire are defined earlier in your code
# If not, you'll need to add the code to load and prepare your data here

import datashader as ds, datashader.transfer_functions as tf, numpy as np
import dask.dataframe as dd
import pandas as pd

import pyarrow.parquet as pq

print("reading metadata")
metadata = pq.read_metadata('~/Python Projects/Jupyter_Notebooks/data/census2010.parq/_metadata')
print(metadata.schema)

print("reading dataframe")
df = pd.read_parquet('~/Python Projects/Jupyter_Notebooks/data/census2010.parq')

# print(df.info())  # takes a long time for such a big dataframe
print(df.describe())
print(df.dtypes)

class CensusApp(param.Parameterized):
    show_map = param.Boolean(default=False, doc="Toggle to show/hide map overlay")

    @param.depends('show_map')
    def view(self):
        points = gv.Points(df, kdims=['easting', 'northing'])
        
        # Use datashade to aggregate points
        shaded = datashade(points, cmap='fire', aggregator=ds.count())
        
        # Apply dynamic spreading with more aggressive parameters
        plot = dynspread(shaded, max_px=20, threshold=0.9, how='over')
        
        if self.show_map:
            # Add tile layer using Bokeh's tile provider
            tile_provider = get_provider(CARTODBPOSITRON)
            tiles = gv.WMTS(tile_provider)
            plot = tiles * plot
            print("Map overlay added")  # Debugging information
        else:
            print("Map overlay not added")  # Debugging information
        
        plot_opts = dict(
            width=1000, height=500, 
            bgcolor='black',
        )
        
        # Print the range of the data
        x_range = df['easting'].min(), df['easting'].max()
        y_range = df['northing'].min(), df['northing'].max()
        print(f"Data range: x={x_range}, y={y_range}")
        
        return plot.opts(**plot_opts)

app = CensusApp()

layout = pn.Column(
    pn.Param(app.param, widgets={'show_map': pn.widgets.Checkbox(name="Show Map Overlay")}),
    pn.pane.HoloViews(app.view)
)

pn.extension()
pn.serve(layout)

