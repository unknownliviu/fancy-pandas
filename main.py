from __future__ import (absolute_import, division, print_function)
import os

import matplotlib as mpl
import matplotlib.pyplot as plt

from shapely.geometry import Point
import pandas as pd
import geopandas as gpd
from geopandas import GeoSeries, GeoDataFrame

world = gpd.read_file(os.path.join('countries', "ne_10m_admin_0_countries.shp"))

romania = world[world['NAME'] == 'Romania']

europe = world[world['CONTINENT'] == 'Europe']

#somewhere in bucharest it's 26.05, 44,47
print(romania['geometry'].contains(Point(26.05, 44.47)))

#somwehere in sopron
print(romania['geometry'].contains(Point(16.582035, 47.676720)))

# Boolean telling if point is in any european country
print(True in(europe['geometry'].contains(Point(16.582035, 47.676720)).values))
