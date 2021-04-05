"""
https://sparkbyexamples.com/pyspark/pyspark-accumulator-with-example/
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("accumulator").getOrCreate()

accuSum = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])


def countFun(x):
    global accuSum
    accuSum += x


rdd.foreach(countFun)
print(accuSum.value)
