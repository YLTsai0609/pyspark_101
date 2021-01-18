'''
Repartition(重分區) and Coalesce(合併)

1. Repartition and Coalesce section in rdd https://sparkbyexamples.com/pyspark-rdd/
2. PySpark Repartition() vs Coalesce() https://sparkbyexamples.com/pyspark/pyspark-repartition-vs-coalesce/

有些時候我們希望對cluster上的RDD重新分配計算節點(主機、Thread、Process)
PySpark提供兩個api
    1. repartition() - 把節點重新分配，也稱作full shuffle
    2. coalesce() - 把節點變少，例如從4個partition變成2個

執行程式時會有以下現象 : 
1. 兩個操作都是非常昂貴的操作，因為需要重新安排資料到計算節點上，若有這樣的操作在你的程式中，試著把這些操作降到最少
2. repartition()以及coalesce()同樣會產生新的RDD，因為所有RDD都是Immutable

LOCAL MODE
    builder.master("local[1]") - means by default, you will create N partitions
HDFS cluster mode
    PySpark creates one Partition for rach bock of the file
    Hadoop version 1 block size = 64MB
    Hadoop version 2 block size = 128MB 
    E.g. 640MB on Hadoop version 2
    640 / 128 = 5, you will create 5 partitions
    You can also create 10 partitions
PySpark configuration
    spark.default.parallelism 
        configuration default value set to 
        the number of all cores on all nodes in a cluster,
        on local it is set to number of cores on your system.
    spark.sql.shuffle.partitions 
        configuration default value is set to 200 and be used 
        when you call shuffle operations like reduceByKey()  , groupByKey(), join() and many more.
        This property is available only in DataFrame API but not in RDD.
    可以透過
    spark.conf.set("spark.sql.shuffle.partitions", "500")來設定[?]    
'''
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()
sc = spark.sparkContext

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = sc.parallelize(data, 4)
repar_rdd = rdd.repartition(10)
coal_rdd = repar_rdd.coalesce(2)
print('rdd id', rdd.id, 'number of partitions : ', rdd.getNumPartitions())
print('repartition rdd id', repar_rdd.id,
      'number of partitions : ', repar_rdd.getNumPartitions())
print('coal rdd id', coal_rdd.id,
      'number of partitions : ', coal_rdd.getNumPartitions())

for dataset, save_folder in zip([rdd, repar_rdd, coal_rdd], ['original', 'repartition', 'coalesce']):
    save_path = f'tmp/{save_folder}'
    dataset.saveAsTextFile(save_path)
    print(save_path)
