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
    "Project",
    "Gutenberg’s",
    "Alice’s",
    "Adventures",
    "in",
    "Wonderland",
    "Project",
    "Gutenberg’s",
    "Adventures",
    "in",
    "Wonderland",
    "Project",
    "Gutenberg’s",
]

rdd = spark.sparkContext.parallelize(data)

# map a element x to (x, 1)
# in this case -> add into a tuple
rdd2 = rdd.map(lambda x: (x, 1))
for element in rdd2.collect():
    print(element)
