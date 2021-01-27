'''

https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-drop-null.py

'''
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

filePath = "data/small_zipcode.csv"
df = spark.read.options(header='true', inferSchema='true') \
          .csv(filePath)

df.printSchema()
df.show(truncate=False)

df.na.drop().show(truncate=False)

df.na.drop(how="any").show(truncate=False)

df.na.drop(subset=["population", "type"]) \
    .show(truncate=False)

df.dropna().show(truncate=False)
