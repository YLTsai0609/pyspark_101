'''
RDD --> DataFrame
txt, csv, json, orv, avro,
parquet, xml, hdfs, s3, dbfs, azure blob file
rdbms, nosql 

Since your dataframe is distributed on cluster

we will get a object, if we wanna manipulate the dataframe,
we use some partition work(multi-processing and networking)
so there are some custom method we need call.

https://sparkbyexamples.com/pyspark/different-ways-to-create-dataframe-in-pyspark/

'''

import pyspark
from pyspark.sql import SparkSession, Row
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# from pyspark.sql.functions import *

columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.printSchema()

dfFromData2 = spark.createDataFrame(data).toDF(*columns)
dfFromData2.printSchema()

rowData = map(lambda x: Row(*x), data)
dfFromData3 = spark.createDataFrame(rowData, columns)
dfFromData3.printSchema()
