import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import udf, broadcast, unix_timestamp, lit
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("Precipitations")
    .config("spark.default.partitions", 555)
    .config("spark.sql.shuffle.partitions", 555)
    .config("spark.driver.memory", "2G")
    .config("spark.executor.cores", 3)
    .config("spark.executor.memory", "8G")
    # .config("spark.eventLog.enabled", True)
    # .config("spark.history.fs.logDirectory", "file:/Users/bws/work/fecske/log/")
    .getOrCreate()
)

# How I processed the reference CSV from the documentation 
def parse_reference():
    reference = spark.read.csv('data/ref_cover_cropped_full_grass.csv', header=True)
    reference = reference.withColumn("water_use_inches", reference['WATER_USE_ET_INCHES'].cast('double'))
    reference = reference.withColumn('date_start', F.split(reference["DATE"], '-').getItem(0))
    reference.repartition(2).write.partitionBy('date_start').parquet('data/reference_parquet', mode='append')


# Extracts the beginning of the date range for any date, to be joined with the reference data
@udf('string')
def to_daterange(date):
    month, day = date.strftime('%b %d').split()
    beginning = 1 if int(day) < 16 else 16
    return "{} {}".format(month, beginning)

some_eu_countries = ["FR", "HU", "IT", "NL", "PL", "PO", "SP"]

def calculate_irrigation_req(countries):
    parquet_folder = "data/parquet/"
    df = spark.read.load(parquet_folder)
    df = df.filter(F.col('COUNTRY_CODE').isin(countries))
    df.persist()
    reference = spark.read.load('data/reference_parquet')
    reference.persist()

    df = df.withColumn('date_range', to_daterange(df.DATE))
    df.persist()
    df = df.join(broadcast(reference), df.date_range == reference.date_start).select(df.DATE, df.NAME, df.PRECIPITATION, df.COUNTRY_CODE, reference.water_use_inches)
    df.createOrReplaceTempView("dfsql")

    # Converting precipitation from mm to inch
    df = spark.sql('select name, COUNTRY_CODE, date, abs(water_use_inches - (PRECIPITATION/25.4)) as irrigation_requirement from dfsql where precipitation is not null')
    df.persist()

    df.repartition(2).write.partitionBy('COUNTRY_CODE').parquet('data/processed', mode='append')

# I've already precalculated this for "FR", "HU", "IT", "NL", "PL", "PO", "SP"

def rolling_averages():
    days = lambda i: i * 86400
    df   = spark.read.load('data/processed')
    df   = df.withColumn('dateGMT', df.date.cast('timestamp'))
    df   = df.withColumn('month', F.month(df.date))
    df   = df.withColumn('year', F.year(df.date))
    df.createOrReplaceTempView("rosql")

    window = (Window.partitionBy(F.col("month"), F.col('year')).orderBy(F.col("dateGMT").cast('long')).rangeBetween(-days(30), 0))
    df     = df.withColumn('rolling_avg', F.avg('irrigation_requirement').over(window))
    df.createOrReplaceTempView("rosql")
    df.persist()
    return spark.sql('select year, month, name, COUNTRY_CODE, avg(rolling_avg) as rolling_average_irrigation_requirement from rosql group by year, month, name, COUNTRY_CODE order by year, month, name')

def to_csv(countries):
    ra = rolling_averages()

    for country in countries:
        temp = ra.filter(F.col('COUNTRY_CODE') == country)
        temp.persist()
        temp.coalesce(1).write.option("header", True).csv('data/rolling_averages_csv/{}.csv'.format(country))

to_csv(some_eu_countries)
