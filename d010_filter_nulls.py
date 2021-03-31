"""
https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-filter.py

https://sparkbyexamples.com/pyspark/pyspark-where-filter/

to filter the row you don't want.

use filter() or where( for sql background), they are exactly the same.

filter(condition)

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark: SparkSession = SparkSession.builder.master("local[1]").appName(
    "SparkByExamples.com"
).getOrCreate()

data = [("James", None, "M"), ("Anna", "NY", "F"), ("Julia", None, None)]

columns = ["name", "state", "gender"]
df = spark.createDataFrame(data, columns)

df.printSchema()
df.show()

df.filter("state is NULL").show()
df.filter(df.state.isNull()).show()
df.filter(col("state").isNull()).show()

df.filter("state IS NULL AND gender IS NULL").show()
df.filter(df.state.isNull() & df.gender.isNull()).show()

df.filter("state is not NULL").show()
df.filter("NOT state is NULL").show()
df.filter(df.state.isNotNull()).show()
df.filter(col("state").isNotNull()).show()
df.na.drop(subset=["state"]).show()

df.createOrReplaceTempView("DATA")
spark.sql("SELECT * FROM DATA where STATE IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show()
spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show()
