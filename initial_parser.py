# Purpose of file: demonstrate thought process. 

import pyspark

from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
sc = SparkContext('local[4]')
spark = SparkSession(sc)

# Folder with the ~114k CSV files, ~120 GB
# Source https://www.ncei.noaa.gov/data/daily-summaries/archive/
folder = "/Volumes/Mac/datascience/daily-summaries-latest/"

# Destination folder
parquet_folder = "data/parquet/"

# I'm lazy, I ran this in ruby `('A*'..'Z*').to_a`
letters = ["A*", "B*", "C*", "D*", "E*", "F*", "G*", "H*", "I*", "J*", "K*", "L*", "M*", "N*", "O*", "P*", "Q*", "R*", "S*", "T*", "U*", "V*", "W*", "X*", "Y*", "Z*"]

for l in letters:
    print(l)
    try:
    	# I had to do this trick to avoid out-of-memory errors on local machine
        df = spark.read.csv(folder + l, header=True)
        df = df.select(['STATION', "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", "PRCP"])
        df = df.withColumn('PRECIPITATION', df['PRCP'].cast('double'))
        df = df.withColumn('LATITUDE', df['LATITUDE'].cast('double'))
        df = df.withColumn('LONGITUDE', df['LONGITUDE'].cast('double'))
        df = df.withColumn('ELEVATION', df['ELEVATION'].cast('double'))
        df = df.withColumn('COUNTRY_CODE', F.split(df['NAME'], ', ').getItem(1))
        df = df.withColumn('DATE', df['DATE'].cast('date'))
        df.repartition(200).write.partitionBy('COUNTRY_CODE').parquet(parquet_folder, mode='append')
    except:
        e = sys.exc_info()[0]
        print(e)