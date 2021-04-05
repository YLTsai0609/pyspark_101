"""
PySpark RDD Operations

https://sparkbyexamples.com/pyspark-rdd

RDD transformations – 
    Transformations are lazy operations,(Until you call action on RDD)
    instead of updating an RDD, these operations return another RDD.
RDD actions – 
    operations that trigger computation and return RDD values.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("Project", 1),
    ("Gutenberg’s", 1),
    ("Alice’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1),
]

rdd = spark.sparkContext.parallelize(data)

# reduce by key
# element tuple as a key, value
# perform a groupby count

rdd2 = rdd.reduceByKey(lambda a, b: a + b)
for element in rdd2.collect():
    print(element)
