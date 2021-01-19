'''
PySpark RDD Operations

https://sparkbyexamples.com/pyspark-rdd

RDD transformations – 
    Transformations are lazy operations,(Until you call action on RDD)
    instead of updating an RDD, these operations return another RDD.
RDD actions – 
    operations that trigger computation and return RDD values.
'''

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance", 10),
        ("Marketing", 20),
        ("Sales", 30),
        ("IT", 40)
        ]
rdd = spark.sparkContext.parallelize(dept)

df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

deptColumns = ["dept_name", "dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


from pyspark.sql.types import StructType, StructField, StringType
deptSchema = StructType([
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(data=dept, schema=deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)
