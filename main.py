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


#RUN COMMANDS HERE
# pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions
def get_spark_context():
    """Configures SparkSession for all jobs in uld-analytics"""
    spark = (
        SparkSession.builder.appName("uld-analytics")
        .config("spark.default.partitions", 555)
        .config("spark.sql.shuffle.partitions", 555)
        .enableHiveSupport()
        .getOrCreate()
    )



# Folder with the ~114k CSV files 
folder = "/Volumes/Mac/datascience/daily-summaries-latest/"

path = "/Volumes/Mac/datascience/daily-summaries-latest/ROE00100829.csv"

parquet_folder = "/Volumes/Mac/datascience/parquet/"

ssd_parquet = "/Users/bws/work/fecske/data/parquet/"

# df = spark.read.csv(path, header=True)
# df = spark.read.csv(folder, header=True)

# df.show()
# df.printSchema()

# df = df.withColumn('PRECIPITATION', df['PRCP'].cast('double'))
# df = df.withColumn('LATITUDE', df['LATITUDE'].cast('double'))
# df = df.withColumn('LONGITUDE', df['LONGITUDE'].cast('double'))
# df = df.withColumn('ELEVATION', df['ELEVATION'].cast('double'))
# df = df.withColumn('COUNTRY_CODE', pyspark.sql.functions.split(df['NAME'], ', ').getItem(1))
# df = df.withColumn('DATE', df['DATE'].cast('date'))

# df.persist()

# df.repartition(200).write.partitionBy('COUNTRY_CODE').parquet(parquet_folder, mode='append')


# rofiles = ['ROE00100829.csv', 'ROE00100898.csv', 'ROE00100899.csv', 'ROE00100900.csv', 'ROE00100901.csv', 'ROE00100902.csv', 'ROE00100903.csv', 'ROE00100904.csv', 'ROE00100905.csv', 'ROE00108887.csv', 'ROE00108888.csv', 'ROE00108889.csv', 'ROE00108890.csv', 'ROE00108891.csv', 'ROE00108892.csv', 'ROE00108893.csv', 'ROE00108894.csv', 'ROE00108895.csv', 'ROE00108896.csv', 'ROE00108897.csv', 'ROE00108898.csv', 'ROE00108899.csv', 'ROE00108900.csv', 'ROE00108901.csv', 'ROE00108903.csv', 'ROM00015023.csv', 'ROM00015085.csv', 'ROM00015247.csv', 'ROM00015280.csv', 'ROM00015360.csv']

# schema= StructType([
#     StructField("STATION", StringType(), True),
#     StructField("DATE", DateType(), True),
#     StructField("LATITUDE", DoubleType(), True),
#     StructField("LONGITUDE", DoubleType(), True),
#     StructField("ELEVATION", DoubleType(), True),
#     StructField("NAME", StringType(), True)
#     StructField("COUNTRY_CODE", StringType(), False),
#     StructField("PRECIPITATION", IntegerType(), False),
#     ])

# df = spark.read.csv(folder + rofiles[0], header=True)

# df = df.withColumn('PRECIPITATION', df['PRCP'].cast('double'))
# df = df.withColumn('LATITUDE', df['LATITUDE'].cast('double'))
# df = df.withColumn('LONGITUDE', df['LONGITUDE'].cast('double'))
# df = df.withColumn('ELEVATION', df['ELEVATION'].cast('double'))
# df = df.withColumn('COUNTRY_CODE', pyspark.sql.functions.split(df['NAME'], ', ').getItem(1))
# df = df.withColumn('DATE', df['DATE'].cast('date'))

# df=df.rdd.toDF(schema)


# df = df.withColumn('PRECIPITATION', df['PRCP'].cast('double'))
# df = df.withColumn('LATITUDE', df['LATITUDE'].cast('double'))
# df = df.withColumn('LONGITUDE', df['LONGITUDE'].cast('double'))
# df = df.withColumn('ELEVATION', df['ELEVATION'].cast('double'))
# df = df.withColumn('COUNTRY_CODE', pyspark.sql.functions.split(df['NAME'], ', ').getItem(1))
# df = df.withColumn('DATE', df['DATE'].cast('date'))
# df.persist()

# df1 = spark.read.csv(folder + rofiles[1], header=True)
# df1 = df1.withColumn('PRCP', df1['PRCP'].cast('double'))
# df1 = df1.withColumn('COUNTRY_CODE', pyspark.sql.functions.split(df1['NAME'], ', ').getItem(1))
# df1.persist()

# df = df.union(df1)

df = spark.read.csv(folder + 'A*', header=True)

# df = spark.read.csv(folder + 'RO*', header=True)

df=df.select(['STATION', "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "PRCP"])

df = df.withColumn('PRECIPITATION', df['PRCP'].cast('double'))
df = df.withColumn('LATITUDE', df['LATITUDE'].cast('double'))
df = df.withColumn('LONGITUDE', df['LONGITUDE'].cast('double'))
df = df.withColumn('ELEVATION', df['ELEVATION'].cast('double'))
df = df.withColumn('COUNTRY_CODE', pyspark.sql.functions.split(df['NAME'], ', ').getItem(1))
df = df.withColumn('DATE', df['DATE'].cast('date'))

df.repartition(200).write.partitionBy('COUNTRY_CODE').parquet(parquet_folder, mode='append')
letters=["UC*","UG*","UK*","UP*", "UV*", "UY*", "UZ*"]
for l in letters:
    print(l)
    try:
        df = spark.read.csv(folder + l, header=True)
        df = df.select(['STATION', "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "PRCP"])
        df = df.withColumn('PRECIPITATION', df['PRCP'].cast('double'))
        df = df.withColumn('LATITUDE', df['LATITUDE'].cast('double'))
        df = df.withColumn('LONGITUDE', df['LONGITUDE'].cast('double'))
        df = df.withColumn('ELEVATION', df['ELEVATION'].cast('double'))
        df = df.withColumn('COUNTRY_CODE', pyspark.sql.functions.split(df['NAME'], ', ').getItem(1))
        df = df.withColumn('DATE', df['DATE'].cast('date'))
        df.repartition(200).write.partitionBy('COUNTRY_CODE').parquet(parquet_folder, mode='append')
    except:
        e = sys.exc_info()[0]
        print(e)


# NOW WE HAVE PARQUET
df = spark.read.load(parquet_folder)
df.persist()
df.createOrReplaceTempView("precs")


