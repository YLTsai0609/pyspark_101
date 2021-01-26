'''
https://sparkbyexamples.com/pyspark/pyspark-withcolumn/

https://github.com/spark-examples/pyspark-examples/blob/master/pyspark-withcolumn.py

withColumn - transformation - change / update (values or dtype)

helper object lit() - add contant value to a dataframe column

QA :

How to add a series-like object to pyspark dataframe?

'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

# you need col object and cast method(Java is an annoying language)
df2 = df.withColumn("salary", col("salary").cast("Integer"))
df2.printSchema()
df2.show(truncate=False)

# update the valu of an existing column
df3 = df.withColumn("salary", col("salary") * 100)
df3.printSchema()
df3.show(truncate=False)

# create a new column from an existing column
df4 = df.withColumn("CopiedColumn", col("salary") * -1)
df4.printSchema()

# create a new column with contant
df5 = df.withColumn("Country", lit("USA"))
df5.printSchema()

df6 = df.withColumn("Country", lit("USA")) \
    .withColumn("anotherColumn", lit("anotherValue"))
df6.printSchema()

# rename column
df.withColumnRenamed("gender", "sex") \
  .show(truncate=False)

# drop
df4.drop("CopiedColumn") \
    .show(truncate=False)

dataStruct = [(("James", "", "Smith"), "36636", "M", "3000"),
              (("Michael", "Rose", ""), "40288", "M", "4000"),
              (("Robert", "", "Williams"), "42114", "M", "4000"),
              (("Maria", "Anne", "Jones"), "39192", "F", "4000"),
              (("Jen", "Mary", "Brown"), "", "F", "-1")
              ]

schemaStruct = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('dob', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', StringType(), True)
])


df7 = spark.createDataFrame(data=dataStruct, schema=schemaStruct)
df7.printSchema()
df7.show(truncate=False)
