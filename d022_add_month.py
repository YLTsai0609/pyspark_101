"""
https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-split-function.py
excute sql, is any dataframe-like operation?
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

from pyspark.sql.functions import col, expr

data = [("2019-01-23", 1), ("2019-06-24", 2), ("2019-09-20", 3)]
spark.createDataFrame(data).toDF("date", "increment").select(
    col("date"),
    col("increment"),
    expr("add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int))").alias(
        "inc_date"
    ),
).show()
