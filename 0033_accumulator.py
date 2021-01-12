'''
https://sparkbyexamples.com/pyspark/pyspark-accumulator-with-example/
'''

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("accumulator").getOrCreate()
accumCount = spark.sparkContext.accumulator(0)
rdd2 = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd2.foreach(lambda x: accumCount.add(1))
print(accumCount.value)
