"""
https://sparkbyexamples.com/pyspark/select-columns-from-pyspark-dataframe/

https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-select-columns.py

"""


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [
    ("James", "Smith", "USA", "CA"),
    ("Michael", "Rose", "USA", "NY"),
    ("Robert", "Williams", "USA", "CA"),
    ("Maria", "Jones", "USA", "FL"),
]

columns = ["firstname", "lastname", "country", "state"]
df = spark.createDataFrame(data=data, schema=columns)

print("Simple example original df")
print("-" * 60)
df.show(truncate=False)

# df.scelect(col_name)

df.select("firstname").show()

df.select("firstname", "lastname").show()

# Using Dataframe object name
df.select(df.firstname, df.lastname).show()

# Using col function
from pyspark.sql.functions import col

df.select(col("firstname"), col("lastname")).show()


data = [
    (("James", None, "Smith"), "OH", "M"),
    (("Anna", "Rose", ""), "NY", "F"),
    (("Julia", "", "Williams"), "OH", "F"),
    (("Maria", "Anne", "Jones"), "NY", "M"),
    (("Jen", "Mary", "Brown"), "NY", "M"),
    (("Mike", "Mary", "Williams"), "OH", "M"),
]

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType(
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
        StructField("state", StringType(), True),
        StructField("gender", StringType(), True),
    ]
)


print("Complex example original df")
print("-" * 60)

df2 = spark.createDataFrame(data=data, schema=schema)
df2.printSchema()
df2.show(truncate=False)  # shows all columns

print("select name")
print("-" * 60)

df2.select("name").show(truncate=False)

print("select name.atrribute")
print("-" * 60)

df2.select("name.firstname", "name.lastname").show(truncate=False)
df2.select("name.*").show(truncate=False)  # shell-like command
