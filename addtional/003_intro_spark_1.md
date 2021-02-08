# Spark

事實上UC Berkeley AMP lab在開發Spark時，是為了和Hadoop配合，而不是為了取代Hadoop，主要差異就是Spark不用寫回檔案系統

| hadoop| spark|
|-------|------|
|1. 抽象層次低，需要管理相當多細節(坑多)，使得應用這套框架的使用者較難上手，花費較多開發時間 | 設計了一層高階API，RDD(彈性是分布資料集)，講白了就是包成一個好用的class，邀請社群包裝Python, R API |
|2. 若需要比較直觀的操作，還要自己繼承Map, Reduce自己重包class, function | RDD寫了很多好用方法，Sort, Join等 
|3. 現實生活中的任務太過複雜，需要大量的Map function，難以管理|每種操作都return RDD物件，而且存在記憶體裡面，統一物件格式
|4. 中間過程要存回HDFS，中間過程太多時浪費大量時間在IO上|存記憶體，不用IO了|
|5. Reduce task一定要等到map task都完成才能開始|分區相同的轉換可以在一個task以streanmming進行，只有分區不同才需要等其他worker|
|6. 由於中間可能要大量IO，只能用於批次處理|開始支援streamming

# RDD

RDD(Resilent Distributed Datasets) 稱為彈性分散式資料集，是Spark的底層資料結構，Spark API的操作都是基於RDD
當資料表丟失時，可以進行重建，Spark 1.5之後，增加了Spark-DataFrame這個高級API，底層仍然是RDD

RDD裡面並不是真的資料，而是一些meta info，紀錄要怎麼做可以變出想要的資料，(scala是lazy computing)

這些meta info會形成一個有向無環圖(DAG)，spark會先建立一個優化器，重新用他認為運算效率最佳的方式來進行計算，並且cache起來，就算該次運算是失敗的，下一次直接讀cache，修正meta info繼續算即可

### RDD Operations

| Opeations      | function                                                          | difference                                          |
|----------------|-------------------------------------------------------------------|-----------------------------------------------------|
| Transformation | Map, filter, groupBy, join, <br> union, reduce, sort, partitionBy | return RDD，不會馬上提交到Spark cluster進行計算         |
| Action         | count, collect, take, save, show                                  | 返回值不是RDD，會形成DAG，提交到Spark cluster並立刻返回結果 |

透過這樣的思維，可以想像基本上pipeline的寫法就是

    Transform_1 -> Transform_2 -> Transform_3 -> ... -> Action

### shuffle and stage

RDD的Transformation函數中，又分為窄相依(narrow dependency)和寬相依(wide dependency)，區別是否shuffle

narrow dependency : partition的運算不依賴其他partition(map in hadoop)

wide dependency : RDD各個partition會依賴於其他partition(reduce in hadoop)

例如groupby : 

``` Python
val rdd2 =\
     rdd1.groupBy(x => x._1).Map(x => (x._1, x._2.toList.length))
```

為了計算相同key下元素的個數，**需要相同key的元素聚集到同一個partition之下，所以必須讓partition進行溝通，資料在記憶體中重新分配，這是一個shuffle操作，shuffle操作是spark中最慢的操作，盡量避免不必要的shulle**

### Shuffle write / Shuffle fetch

寬相依主要有兩個過程(shuffle write, shuffle fetch)，類似Hadoop的Map, Reduce

shuffle write 將 ShuffleMapTask任務產生的中間結果暫存到記憶體中

Shuffle fetch獲得ShuffleMapTask記憶體中的中間結果並進行ShuffleReduceTask的計算，**這個過程容易OOM**，shuffle過程中記憶體的分配是使用 `ShuffleMemoryManager` 進行管理，會針對每個Task分配記憶體，Task完成後透過 `Executor` 釋放記憶體

最近亦有很多論文是針對shuffle過程進行記憶體優化

# Reference

[Spark 学习: spark 原理简述 550+ 贊同](https://zhuanlan.zhihu.com/p/34436165)
