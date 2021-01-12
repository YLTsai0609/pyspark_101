'''
1. https://sparkbyexamples.com/pyspark-rdd
2. https://kknews.cc/zh-tw/code/j24qnle.html

RDD - Resilient Distributed Dataset 可回覆式(或稱彈性式)分散式資料集

這要說到Hadoop，Hadoop這篇論文是Google實作的一個檔案系統外加Map Reduce

Hadoop解決了分散工作時，某台計算節點換掉時的補救機制，主要思想也很簡單，
把計算過程(或是中間值)存到別台計算節點上，每一台都多花一些檔案空間來做這些事，
並且識別是否有計算節點壞掉，重新讀取值中間計算結果的機制

Spark是In memory的分散式架構，自然也繼承了Hadoop這個設計特色，
並且讓資料在記憶體存不下時，暫存到檔案系統中，
RDD資料集格式已經廣泛被分散式儲存/計算框架所採用

對比來說，RDDs其實就像Python的list，啥都能裝
但RDD可以被分散在不同機器上，不同行程中，並且RDD可以被平行執行

對RDD的操作最常用的就是map() filter() persist()

PySpark RDD的特色

1. In Memory 
    對比Hadoop的Map Reduce(寫到硬碟中)，saprk走記憶體，
    意味著更快的存取，不會有IO bound
2. Immutability
    每一個RDD都是不可變，每經過一次Transform，就會開一個新的RDD，
    並記得上一個RDD和新的RDD的關係
3. Fault Tolerance
    在HDFS, S3上跑任意的RDD壞掉時，會從其他計算節點找回資料，
    並且Pyspark任務失敗時會從新嘗試
4. Lazy Evolution
    RDD transform是先說明運算規則（map），在進行轉換(transform)，
    和舊版tensorflow是同樣的，中間使用DAG來記錄順序關係
5. Partitioning
    當你從資料建立RDD時，預設會把它做partition，
    預設是看你有幾個core就做幾個partition
'''

# 建立RDD

# 1. parallelizing from existing collection
# 2. referencing a dataset in an external sotrage ststem(HDFS, S3, HBase)

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()


data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = spark.sparkContext.parallelize(data)
print('Method of RDD : ', dir(rdd))
print("RDD count :" + str(rdd.count()))
