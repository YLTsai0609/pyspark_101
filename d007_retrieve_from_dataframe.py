"""
https://sparkbyexamples.com/pyspark/pyspark-collect/

https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-collect.py

The cost fo show function(spark need to collect data from worker node and display to you)
It's might spend some time, and it should be computed in your inference time.

You can use collect when you have a small data(usually after filter, group, count )

collect vs select

collect(action) - list of Raw object

select(transformation) return a spark dataframe

"""


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

dataCollect = deptDF.collect()

print(dataCollect, type(dataCollect), dir(dataCollect))

dataCollect2 = deptDF.select("dept_name").collect()
print(dataCollect2)

for row in dataCollect:
    print(row["dept_name"] + "," + str(row["dept_id"]))
