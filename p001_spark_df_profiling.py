"""

dtype schema 

https://data.nasa.gov/Space-Science/Meteorite-Landings/gh4g-9sfh

坑 : 

1. spark不會自動讀取header

spark.read.csv(...) - 沒有header，請把header選項打開

2. spark不會自動判定型別，請手動設定型別

3. 把它存成parquet file

結論 : 若可抽樣到單機，作為資料頗析的第一步，幾乎沒必要使用spark

抽樣100萬筆到單機的pandas profiling，report選minimum即可

pandas profiling 比起 spark profiling 精細又快速好幾個檔次

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructType, DateType
import spark_df_profiling
import time

spark = (
    SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
)

# filed, dtype, nullable
schema = (
    StructType()
    .add("name", StringType(), True)
    .add("id", StringType(), True)
    .add("nametype", StringType(), True)
    .add("recclass", StringType(), True)
    .add("mass (g)", FloatType(), True)
    .add("fall", StringType(), True)
    .add("year", DateType(), True)
    .add("reclat", FloatType(), True)
    .add("reclong", FloatType(), True)
    .add("GeoLocation", StringType(), True)
)
df = (
    spark.read.format("csv")
    .option("header", True)
    .schema(schema)
    .load("data/Meteorite_Landings.csv")
)
df.printSchema()

df.describe().show()

start = time.time()
report = spark_df_profiling.ProfileReport(df)
report.to_file("output/meteorite_landings.html")
print("processing ... report : ", time.time() - start, "s")
