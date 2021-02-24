'''

https://stackoverflow.com/questions/49538327/pyspark-string-pattern-from-columns-values-and-regexp-expression

several images in one article

Input : contecnt column with full html
Output : img url list
Optional : show all columns content by df.show()
https://stackoverflow.com/questions/33742895/how-to-show-full-column-content-in-a-spark-dataframe

'''
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import re

C = F.col

sc = SparkContext(conf=SparkConf())
spark = SparkSession(sc)

with open('data/webpage_1.txt', 'r') as f:
    text = [f.read()]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("text", StringType(), True),
])
# data format
# [(r1_col1, r1_col2, ...),
#  (r2_col1, r2_col2, ...),
# ]
df = spark.createDataFrame([(0, text)], schema=schema)
df.show()

# pat = re.compile('<img[^>]+src="([^">]+[jpg|png])"')
# This one didn't work, api support one index of group only
# df = df.withColumn("img_link_list", F.regexp_extract(
#     df.text, r'<img[^>]+src="([^">]+[jpg|png])"', 0))

# df.show(n=50, truncate=False, vertical=True)
# Sol 1 User defined function, due to the linitation of pyspark.dataframe.regexp_extract(),
######## idx is needed ################


# def get_img_url_list(text):
#     img_url_list = re.findall(r'<img[^>]+src="([^">]+[jpg|png])"', text)
#     print(img_url_list)
#     return img_url_list


# get_img_url_list_udf = F.udf(
#     lambda text: get_img_url_list(text),
#     StringType()
# )

# df = df.withColumn('img_url_list', get_img_url_list_udf('text'))
# df.show(n=50, truncate=False, vertical=True)

# SOl 2 先split, 在explore，在regex_extract()，return image level dataframe
# pass
# df = df.withColumn("split_img_tag", F.split(df['text'], '<img.*jpg'))\
#     .drop(df['text'])\
#     # .show(n=20, truncate=False, vertical=True)

# df.select(F.explode(df['split_img_tag']))\
#     .show(n=20, truncate=False, vertical=True)

# Sol 3 work around, 透過string split來切分，explode之後，最後在groupby起來
# 需要一個row number
df = (
    df
    .withColumn("img_url", F.split(C('text'), "src=\""))
    .withColumn("img_url", F.explode(C('img_url')))
    .withColumn("img_url", F.split(C('img_url'), "\" title"))
    .withColumn("img_url", F.explode(C('img_url')))
    .where(C("img_url").rlike(".jpg|.png"))
    .where(C("img_url").rlike("https"))
    .where(~C("img_url").rlike("href"))
    .groupBy("id").agg(F.collect_list(C("img_url")).alias("img_url"))
    .drop(C('text'))

)

df.show(n=20, truncate=False, vertical=True)
