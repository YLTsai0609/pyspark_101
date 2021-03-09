# Spark 系統架構

## 5個名詞
1. Application : 基於Spark的使用者程式，包含一個Driver Program和Cluster中的多個Executor

2. Driver : 執行Application中的main()，並創建SparkContext

3. Executor : 是為了Application在Worker Node上的一個進程，進程負責執行Task，並將資料存在記憶體中或是硬碟上，每個Application有各自獨立的Executors

4. Cluster Manager : 從叢集獲取資源的外部服務，例如Local, Standalone, Mesos 或是 Yarn

5. Operation : 作用於RDD的各種操作，分為Transformation以及Action

## 2種節點

整個Spark Cluster中，分為Master節點以及Worker節點

Master Node : 節點上常駐Daemon進程以及Driver進程，Master負責將任務變成可平行執行的Tasks，並負責處理出錯時的問題處理，Master Node主要進行Worker Node的負載管理

Worker Node : 節點上常駐Worker Daemon，並執行任務

Spark支援不同的運作模式，包含Local，Standalonem Mesoes, Yarn

不同的運作模式會將Driver 調度到不同的節點上實行，local一班用於本地端測試

每個Worker上存在一個或是多個Executor進程，Executor擁有一個線程池，每個線程負責一個Task的執行，根據Executor上CPU-core的數量，可以並行多個和Core一樣數量的Task

TODO 補圖

# Ref

[Spark 学习: spark 原理简述 550+ 贊同](https://zhuanlan.zhihu.com/p/34436165)