# Spark(MapReduce 2.0)

| hadoop| spark|
|-------|------|
|1. 抽象層次低，需要管理相當多細節(坑多)，使得應用這套框架的使用者較難上手，花費較多開發時間 | 設計了一層高階API，RDD(彈性是分布資料集)，講白了就是包成一個好用的class，邀請社群包裝Python, R API |
|2. 若需要比較直觀的操作，還要自己繼承Map, Reduce自己重包class, function | RDD寫了很多好用方法，Sort, Join等 
|3. 現實生活中的任務太過複雜，需要大量的Map function，難以管理|每種操作都return RDD物件，而且存在記憶體裡面，統一物件格式
|4. 中間過程要存回HDFS，中間過程太多時浪費大量時間在IO上|存記憶體，不用IO了|
|5. Reduce task一定要等到map task都完成才能開始|分區相同的轉換可以在一個task以streanmming進行，只有分區不同才需要等其他worker|
|6. 由於中間可能要大量IO，只能用於批次處理|開始支援streamming

# Reference

[Spark 学习: spark 原理简述](https://zhuanlan.zhihu.com/p/34436165)
