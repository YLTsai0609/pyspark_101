"""

https://sparkbyexamples.com/pyspark/pyspark-create-an-empty-dataframe/

check all types

https://github.com/apache/spark/blob/master/python/pyspark/sql/types.py

"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

schema = StructType(
    [
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
    ]
)

# empty RDD + schema -> df
df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
df.printSchema()

# empty list -> rdd -> df
df1 = spark.sparkContext.parallelize([]).toDF(schema)
df1.printSchema()

# high level api
df2 = spark.createDataFrame([], schema)
df2.printSchema()

# high level api
# not supported in 3.0.1
# df3 = spark.emptyDataFrame()
# df3.printSchema()
