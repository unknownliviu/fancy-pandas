import pandas as pd
import altair as alt
from altair import datum
import pdvega

data = pd.read_csv('data/rolling_averages_csv/FR.csv/data.csv')
data = data[data['month'] == 7]
data = data[data['year'] > 1990]

alt.Chart(data).mark_point().interactive().encode(
    alt.X("year:O"),
    alt.Y("rolling_average_irrigation_requirement"), 
    color='name'
)
