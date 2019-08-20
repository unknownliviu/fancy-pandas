# Precipitation overview

## Wanted to explore a large dataset of precipitation data and extract the irrigation requirements of a walnut orchard, as described in the reference document

Downloaded data from here https://www.ncei.noaa.gov/data/daily-summaries/archive/

Extracted the the relevant data from the CSVs and transformed it into the parquet format in `initial_parser.py`

The data is available here https://drive.google.com/drive/folders/1eMpx5CEcynIi5jHQ3MHr2HoPKHDvgMw0?usp=sharing

Then look to `example.py` where I calculate the irrigation requirements for any data point, then I calculated some rolling averages for each month and exported them to CSV

Finally, run `bokeh serve --show explore.py` to see some visualizations (the can be improved)

Additionally, there's a jupyter notebook visualisation in altair that can be used (code is also in a`ltair.py`)
