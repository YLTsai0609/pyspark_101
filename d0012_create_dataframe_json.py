'''
https://sparkbyexamples.com/pyspark/different-ways-to-create-dataframe-in-pyspark/

https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=read%20csv#pyspark.sql.DataFrame

we can read from multiple type even database and hadoop, don't worry about them.

'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# json will aotumatically infer data type
df_from_json = spark.read.json('data/zipcodes.json')
df_from_json.printSchema()
df_from_json.show(n=5, truncate=3)
