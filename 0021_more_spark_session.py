'''
https://github.com/YLTsai0609/pyspark-examples/blob/master/pyspark-sparksession.py

author SparkByExamples.com
'''


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

print("First SparkContext:")
print("APP Name :" + spark.sparkContext.appName)
print("Master :" + spark.sparkContext.master)

sparkSession2 = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExample-test") \
    .getOrCreate()

print("Second SparkContext:")
print("APP Name :" + sparkSession2.sparkContext.appName)
print("Master :" + sparkSession2.sparkContext.master)


# This one won't work on spark 3.0
# sparkSession3 = SparkSession.newSession

# print("Second SparkContext:")
# print("APP Name :" + sparkSession3.sparkContext.appName)
# print("Master :" + sparkSession3.sparkContext.master)
