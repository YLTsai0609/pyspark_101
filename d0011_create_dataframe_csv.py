'''
https://sparkbyexamples.com/pyspark/different-ways-to-create-dataframe-in-pyspark/

https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=read%20csv#pyspark.sql.DataFrame

'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# without header
df_from_csv_1 = spark.read.csv('data/zipcodes.csv')
df_from_csv_1.printSchema()

# with header
df_from_csv_2 = spark.read.option("header", True) \
    .csv("data/zipcodes.csv")
df_from_csv_2.printSchema()
df_from_csv_2.describe().show()

# with header and dtype
schema = StructType() \
    .add("RecordNumber", IntegerType(), True) \
    .add("Zipcode", IntegerType(), True) \
    .add("ZipCodeType", StringType(), True) \
    .add("City", StringType(), True) \
    .add("State", StringType(), True) \
    .add("LocationType", StringType(), True) \
    .add("Lat", DoubleType(), True) \
    .add("Long", DoubleType(), True) \
    .add("Xaxis", IntegerType(), True) \
    .add("Yaxis", DoubleType(), True) \
    .add("Zaxis", DoubleType(), True) \
    .add("WorldRegion", StringType(), True) \
    .add("Country", StringType(), True) \
    .add("LocationText", StringType(), True) \
    .add("Location", StringType(), True) \
    .add("Decommisioned", BooleanType(), True) \
    .add("TaxReturnsFiled", StringType(), True) \
    .add("EstimatedPopulation", IntegerType(), True) \
    .add("TotalWages", IntegerType(), True) \
    .add("Notes", StringType(), True)


df_from_csv_3 = spark.read.format('csv') \
    .option("header", True) \
    .schema(schema) \
    .load("data/zipcodes.csv")

df_from_csv_3.printSchema()


# write
# df2.write.option("header",True) \
#  .csv("/tmp/spark_output/zipcodes123")
