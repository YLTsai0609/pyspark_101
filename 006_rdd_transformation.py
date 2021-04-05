"""
PySpark RDD Operations

https://sparkbyexamples.com/pyspark-rdd

RDD transformations – 
    Transformations are lazy operations,(Until you call action on RDD)
    instead of updating an RDD, these operations return another RDD.
RDD actions – 
    operations that trigger computation and return RDD values.

RDD transformation -
     wide dependency - need connection to exchange data with the other partition
     narrow dependency - can compute in individual partition
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()

rdd = spark.sparkContext.textFile("data/simple_text.txt")

print(rdd, type(rdd))

# transform functions perform a lazy computing
rdd2 = rdd.flatMap(lambda x: x.split(" "))

for element in rdd.collect():
    print(element)

# Flatmap
for element in rdd2.collect():
    print(element)
