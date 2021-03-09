'''
https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/
https://zhuanlan.zhihu.com/p/61589956
Broadcasting : 

1. read-only shared variables
2. are cached and available on all nodes in a cluster
3. spark using efficient broadcast algorithms to reduce comminucation cost

How it work?

1. break the job into stages that have distributed shuffling and actions are executed with in the stage

2. Later Stages are also broken into tasks

3. Spark broadcast the commin data(reusable) needed by task within each stage

4. The broafcasted data is cache in serialized format and deserialized before executing each task

'''
import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

def state_convert(code):
    return broadcastStates.value[code]

result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)
result.show(truncate=False)

# Broadcast variable on filter

filteDf= df.where((df['state'].isin(broadcastStates.value)))
