'''
https://sparkbyexamples.com/pyspark/pyspark-what-is-sparksession/
1. Spark是一個分散式in-memory的運算框架，所以中間包含網路溝通層，進行一次任務就必須開啟一次Session
2. SparkSesson在spark 2.0之後有，作為entry point
3. SparkSession有多種context，透過SparkSession.builder / SparkSession.newSession，
你爽開幾個就可以開幾個
    1. Spark Context
    2. SQL Context
    3. Streamming Context
    4. Hive Context
4. PySpark提供了一個spark物件，是一個SparkSession class
'''

import pyspark
from pyspark.sql import SparkSession

# master - 你即將管理一個運算叢集，所以你需要一個大腦(master)來管理其他運算節點
# 這裡我們建立一個master，名字是local[1]，表示是一個stand alone的master
# 或是yarn, mesos
# local[x], x通常是你有幾個CPU，spark幫你包好了多進程or多線程的API
# appName，能夠透過一個域名來訪問master
# getOrCreate() - 單例模式
spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()


# 一個session有多種方法可以使用
# createDataFrame
# readStream
# sql
# streams
# table
# udf...
print(spark.version)
print(dir(spark))


# 下面這行則依定會起一個新的Session
# sparkSession3 = SparkSession.newSession
