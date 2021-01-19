'''
https://sparkbyexamples.com/pyspark/convert-pyspark-dataframe-to-pandas/

well-done by the api,

collect the data from cluster then put in into single machine memory


####### Types

StructType is a collection or list of StructField objects.

printSchema() method on the DataFrame shows StructType columns as “struct”.
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James", "", "Smith", "36636", "M", 60000),
        ("Michael", "Rose", "", "40288", "M", 70000),
        ("Robert", "", "Williams", "42114", "", 400000),
        ("Maria", "Anne", "Jones", "39192", "F", 500000),
        ("Jen", "Mary", "Brown", "", "F", 0)]

columns = ["first_name", "middle_name", "last_name", "dob", "gender", "salary"]
pyspark_df_1 = spark.createDataFrame(data=data, schema=columns)
pandas_df_1 = pyspark_df_1.toPandas()

# nested example
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

pyspark_df_2 = spark.createDataFrame(data=dataStruct, schema=schemaStruct)
pandas_df_2 = pyspark_df_2.toPandas()


for (spark_df, pandas_df) in [
    (pyspark_df_1, pandas_df_1),
    (pyspark_df_2, pandas_df_2)
]:
    print(type(spark_df), type(pandas_df))
    spark_df.printSchema()
    spark_df.show(truncate=False)
    print(pandas_df)
