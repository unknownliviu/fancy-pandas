import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import udf
sc = SparkContext('local[4]')
spark = SparkSession(sc)
# spark = (
#     SparkSession.builder.appName("Precipitations")
#     .config("spark.default.partitions", 555)
#     .config("spark.sql.shuffle.partitions", 555)
#     .enableHiveSupport()
#     .getOrCreate()
# )

# parquet_folder = "/Users/bws/work/fecske/data/parquet/"
# ro_folder = parquet_folder + 'COUNTRY_CODE=FR'

# df = spark.read.load(ro_folder)
# df.persist()
# df.createOrReplaceTempView("precs")
# df.count()

# @udf('double')
# # Input/output are both a single double value
# def plus_one(v):
#     try:
#         return v + 1
#     except:
#         return 0
#         

# ROE00108889 is BANEASA, RO

# print(spark.sql("select DATE, PRECIPITATION from precs where station = 'ROE00108889' and PRECIPITATION IS NOT NULL ORDER BY DATE desc").show(20, False))

# def delta(x):
#     if(x['PRECIPITATION'] < 1000):
#         x['DELTA'] = 100 - x['PRECIPITATION']
#     return x

# newdf = df.withColumn('delta', 200 - df.PRECIPITATION)

# pd.date_range(start='16/3/2018', end='31/3/2018')
# pd.date_range(start='1/4/2018', end='15/4/2018')


# import calendar
# date_ranges = []

# From april to oct
# for y in range(2009, 2019):
#     for m in range(4, 10):
#         print (m)
#         start = "{}/{}/{}".format(1, m, y)
#         end = "{}/{}/{}".format(15)
#         date_ranges.append()

# reference = spark.read.csv('data/ref_cover_cropped_full_grass.csv', header=True)
# reference = reference.withColumn("water_use_inches", reference['WATER_USE_ET_INCHES'].cast('double'))
# reference = reference.withColumn('date_start', pyspark.sql.functions.split(reference["DATE"], '-').getItem(0))
# reference.repartition(2).write.partitionBy('date_start').parquet('data/reference_parquet', mode='append')




# df=df.withColumn('tenfold', times_ten(df.PRECIPITATION) )
# df = df.withColumn("tf", times_ten(df.PRECIPITATION))
# print(df.show())

# df=df.withColumn('date_range', to_daterange(df.DATE))
# df.repartition(2).write.partitionBy('date_range').parquet('data/french_parquet', mode='append')
# spark.sql('select rfsql.water_use_inches, dfsql.date, dfsql.name, dfsql.precipitation from dfsql, rfsql where dfsql.date_range = rfsql.date_start  limit 100').show()

# WHAT I"VE DONE
parquet_folder = "/Users/bws/work/fecske/data/parquet/"

# reference = spark.read.csv('data/ref_cover_cropped_full_grass.csv', header=True)
# reference = reference.withColumn("water_use_inches", reference['WATER_USE_ET_INCHES'].cast('double'))
# reference = reference.withColumn('date_start', pyspark.sql.functions.split(reference["DATE"], '-').getItem(0))
# reference.repartition(2).write.partitionBy('date_start').parquet('data/reference_parquet', mode='append')


folder = parquet_folder + 'COUNTRY_CODE=FR'
french_parquet = 'data/french_parquet'

df = spark.read.load(french_parquet)
reference = spark.read.load('data/reference_parquet')
df.persist()
df.createOrReplaceTempView("dfsql")
reference.createOrReplaceTempView("rfsql")

print(df.count())

# something like df.rdd.take(1)[0].DATE.strftime('%b %d') as input
# @udf('string')
# def to_daterange(date):
#     month, day = date.strftime('%b %d').split()
#     beginning = 1 if int(day) < 16 else 16
#     return "{} {}".format(month, beginning)


# df=df.withColumn('date_range', to_daterange(df.DATE))
# df.repartition(2).write.partitionBy('date_range').parquet('data/french_parquet', mode='append')

# print(spark.sql('select (rfsql.water_use_inches - dfsql.precipitation) as irrigation_requirement, rfsql.water_use_inches, dfsql.date, dfsql.name, dfsql.precipitation from dfsql, rfsql where dfsql.date_range = rfsql.date_start  limit 100').show())
print(spark.sql('select abs(avg(rfsql.water_use_inches - (dfsql.precipitation/25.4))) as average_irrigation_requirement, dfsql.name from dfsql, rfsql where dfsql.date_range = rfsql.date_start group by dfsql.name order by average_irrigation_requirement asc ').show(10,False))