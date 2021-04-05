"""
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DataType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

arrayType = ArrayType(IntegerType(), False)
print(arrayType.jsonValue)

print(arrayType.simpleString)

print(arrayType.typeName)
