"""
https://sparkbyexamples.com/pyspark-rdd#rdd-types

Shuffling is a mechanism PySpark used to redistribute the data across different executors and even across machines

Such like when you calling
    - groupByKey()
    - reduceByKey()
    - join()

Why PySpark is an expensive operation?
It might involves the following
    - Disk I/O(if you save RDD on disk instead of memory)
    - data serialization and deserializatiom
    - Network I/O  

Case study - Createing an RDD and reduceByKey()
1. creation - noe need to store the data for all keys in partition.
2. reduceByKey()
    2-1 map tasks on all partitions which group all values or a single key
    2-2 the result of the map tasks are kept in memory, if cannot fit in the memory, store the data into a disk.
    2-3 shffles the mapped data across partitions, 
    some times it also stores the shuffled data into a disk for reuse when it needs to recalculate
    2-4 run garbage collection
    2-5 run reduce task on each partition based on the key.

similar progress when run repartition, coalesce, groupByKey, reduceByKey, cogroup, join, countByKey

Shuffle partition size & Performance
Based on your dataset size, a number of cores and memory Pyspark shuffling can benefit or harm your jobs.

1. When you dealing with less amount of data. you should set small cores(partitions), otherwise there will a be long time when 
the partitions exchange the data(Network I/O) 
2. When you have too much of data and having less number of partitions results in fewer longer running tasks and 
some times you may also get out of memory error.

Getting the right size of the shuffle partition is always tricky and takes many runs with different values to achieve the 
optimized number.

This is one of the key properties to look for when you have performance ussues on PySpark jobs.
"""
pass
