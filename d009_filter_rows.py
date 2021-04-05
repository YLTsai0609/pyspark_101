"""
https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-filter.py

https://sparkbyexamples.com/pyspark/pyspark-where-filter/

to filter the row you don't want.

use filter() or where( for sql background), they are exactly the same.

filter(condition)

Support multiple condition! like

cond = (df.state  == "OH") & (df.gender  == "M") 
df.filter(cond).show(truncate=False)

Support array_contains() like 'Java' in [Java, Scala, C++]
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

arrayStructureData = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M"),
]

arrayStructureSchema = StructType(
    [
        StructField(
            "name",
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("middlename", StringType(), True),
                    StructField("lastname", StringType(), True),
                ]
            ),
        ),
        StructField("languages", ArrayType(StringType()), True),
        StructField("state", StringType(), True),
        StructField("gender", StringType(), True),
    ]
)


df = spark.createDataFrame(data=arrayStructureData, schema=arrayStructureSchema)
df.printSchema()
df.show(truncate=False)

# df.column_name seems return a column object

df.filter(df.state == "OH").show(truncate=False)

df.filter(col("state") == "OH").show(truncate=False)

df.filter("gender  == 'M'").show(truncate=False)

df.filter((df.state == "OH") & (df.gender == "M")).show(truncate=False)

# return a column object, seems like a lazy computing
cond = (df.state == "OH") & (df.gender == "M")
print(cond, type(cond))
df.filter(cond).show(truncate=False)

df.filter(array_contains(df.languages, "Java")).show(truncate=False)

df.filter(df.name.lastname == "Williams").show(truncate=False)
