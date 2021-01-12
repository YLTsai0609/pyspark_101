'''
https://sparkbyexamples.com/pyspark/pyspark-accumulator-with-example/

author SparkByExamples.com

1. 多個地方一起算的時候，需要有一個global variable，spark幫你處理好lock和溝通問題了
2. spark的accumulator是一個write-only，且只會初始化1次的變數，只有在worker更新資訊時才會寫值進來
3. sparkContext.accumulator()用來建立 accumulator variables
    add()用來更新值
    value用來取得結果
'''

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("accumulator").getOrCreate()

accum = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd.foreach(lambda x: accum.add(x))
print(accum.value)
