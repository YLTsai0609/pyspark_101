"""

https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/

https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-structtype.py

https://spark.apache.org/docs/latest/sql-ref-datatypes.html

struct helps you create a complex value in dataframe

like array, map

StructType is a collection of StuctField

Once you put a complex column, printSchema() will show you a "struct" in the column data type

StructField : (column name, column type, nullable coumn, metadata)

e.g.  StructField("firstname",StringType(),True)

StructField can contain another StructField so you can play it nestedly, also cover the json data type
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    MapType,
)
from pyspark.sql.functions import col, struct, when

spark = (
    SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
)

data = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1),
]

schema = StructType(
    [
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True),
    ]
)

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

# struct in struct
structureData = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1),
]
structureSchema = StructType(
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
        StructField("id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True),
    ]
)

df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)


updatedDF = df2.withColumn(
    "OtherInfo",
    struct(
        col("id").alias("identifier"),
        col("gender").alias("gender"),
        col("salary").alias("salary"),
        when(col("salary").cast(IntegerType()) < 2000, "Low")
        .when(col("salary").cast(IntegerType()) < 4000, "Medium")
        .otherwise("High")
        .alias("Salary_Grade"),
    ),
).drop("id", "gender", "salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)


""" Array & Map"""


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
        StructField("hobbies", ArrayType(StringType()), True),  # This is an array type!
        StructField(
            "properties", MapType(StringType(), StringType()), True
        ),  # This is a map type
    ]
)
