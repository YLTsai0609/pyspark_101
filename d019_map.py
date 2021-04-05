"""
https://sparkbyexamples.com/pyspark/pyspark-map-transformation/


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

rdd2 = rdd.map(lambda x: (x, 1))
for element in rdd2.collect():
    print(element)
