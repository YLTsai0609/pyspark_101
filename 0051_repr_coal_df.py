"""
同樣的邏輯，太大的csv檔案可以透過cluster架一個RDD，並包一層DataFrame API
亦能夠透過repartition, coalesce來重新分配計算節點
"""
import pyspark
from pyspark.sql import SparkSession

# default we create 5 partitions
spark = (
    SparkSession.builder.master("local[5]").appName("SparkByExamples.com").getOrCreate()
)

df = spark.range(0, 20)  # spark中的內建dataframe
df2 = df.repartition(6)
df3 = df.coalesce(2)
# spark內建的groupby會repartition到200個partition，算好還給你
# 想要改這個值，可以用spark.sql.shuffle.partitions
df4 = df.groupby("id").count()

for frame, partition_i in zip([df, df2, df3, df4], [5, 6, 2, 200]):
    save_path = f"tmp/partition_df_{partition_i}.csv"
    print(frame.rdd.getNumPartitions())
    frame.write.mode("overwrite").csv(save_path)
