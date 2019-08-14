
from random import random
import pandas as pd
import numpy as np

from bokeh.io import show

from bokeh.layouts import row, column
from bokeh.models import Button
from bokeh.models import ColumnDataSource
from bokeh.palettes import RdYlBu3
from bokeh.plotting import figure, curdoc
from bokeh.models.widgets import Slider, Select

countries = ["FR", "HU", "IT", "NL", "PL", "PO", "SP"]
country = Select(title="Country", value="FR",
               options=countries)
month = Select(title='Month', value='3', options=["3", "4", "5", "6", "7", "8", "9", "10", "11"])

source = ColumnDataSource(data=dict(x=[], y=[]))


TOOLTIPS=[
    ("Location", "@name"),
    ("Year", "@year"),
    ("Val", "@value")
]

plot = figure(plot_height=400, plot_width=400, title="Precipitations", toolbar_location=None, tooltips=TOOLTIPS, sizing_mode="scale_both")

def select_options():
    country_val = country.value
    month_val = month.value
    path = 'data/rolling_averages_csv/{}.csv/data.csv'.format(country_val)
    data = pd.read_csv(path)
    m = data[data['month'] == int(month_val)]
    x = m['year'].array
    y = m['rolling_average_irrigation_requirement'].array
    return x, y


def update():
    x, y = select_options()
    source = ColumnDataSource(data=dict(x=x, y=y))
    par = np.polyfit(x, y, 1, full=True)
    slope=par[0][0]
    intercept=par[0][1]
    y_predicted = [slope*i + intercept  for i in x]
    plot.circle(x, y)
    plot.line(x,y_predicted,color='red',legend='y='+str(round(slope,2))+'x+'+str(round(intercept,2)))

update()


for widget in [country, month]:
    widget.on_change('value', lambda attr, old, new: update())

inputs = column(country, month)

curdoc().add_root(row(inputs, plot))

