"""
https://medium.com/swlh/regular-expressions-in-python-and-pyspark-explained-code-included-53cbb22d4117

"""
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *


sc = SparkContext(conf=SparkConf())
spark = SparkSession(sc)

text = [
    "March 8,2019",
    "march 8, 2019",
    "march 8 2019",
    "mar 30 2019",
    "Countermarch 8,2019 feet",
    "marched",
    "Feb 1 2014",
    "mar 22 2021",
]

df = spark.createDataFrame(text, StringType()).toDF("text")

df = df.withColumn(
    "date_from_text",
    F.regexp_extract(df.text, r"(\b(?:[M|m]ar(?:ch)?)\b [0-9]+,?(?: |)\d{4})", 0),
)
df.show()
