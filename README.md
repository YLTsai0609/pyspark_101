# Pyspark 101

Polish up your data processing skill using pyspark!

# Installation

[check here to install spark 3.0+](https://github.com/YLTsai0609/DataScience_Note/blob/master/spark.md)

# Marathon

**This repo contains 50+ example scripts so far.**

The tutorial is from [spark-examples/pyspark-examples](https://github.com/spark-examples/pyspark-examples)

**[The notebook is a cheatsheet contains 60+ problem and pyspark solutions](notebook/pyspark_challenge.ipynb)**

## Pyspark basic

| Content ID   |Date| Content | Note |
|--------------|----|---------|------|
| 001          |1/11|[hello_world](001_hello_world.py)  | |
| 002          |1/12|[create_spark_session](002_create_spark_session.py)  | |
| 003          |1/12|[accumulator](003_accumulator.py)  ||
| 004          |1/13|[RDD creation](004_rdd_creation.py)  ||
| 005          |1/13|[RDD pararllelization Repartition() vs Coalesce()](005_rdd_repartition_coalesce.py)  ||
| 006          |1/18|[RDD operations - transformations (from 006 - 0064)](006_rdd_transformation.py)  ||
| 007          |2/8|[cluster managers](addtional/004_cluster_manager.md)  ||
| 008          |2/22|[spark UI](addtional/007_spark_ui.py)  ||
| 009          |2/23|[RDD shuffle](009_rdd_shuffle.py)  ||
| 009          |2/23|[RDD persist](010_rdd_persist.py)  ||
| 010          |3/9|[Broadcasting](011_broadcasting.py)  ||

## Pyspark DataFrame

| Content ID |Date| Content | Note |
|------------|----|---------|------|
| d001 |1/18| [create_dataframe (from d001 - d0012)](d001_create_dataframe.py)  |  |
| d0011 |1/18| [create_dataframe_csv](d0011_create_dataframe_csv.py)  |  |
| d0012 |1/18| [create_dataframe_json](d0012_create_dataframe_json.py)  |  |
| d002 |1/18| [create_empty_dataframe](d002_create_empty_dataframe.py)  |  |
| d003 |1/18| [spark_frame_to_pandas_frame](d003_pyspark_dataframe_to_pandas.py)  |  |
| d004 |1/20| [structType/structField from d004 - d0042](d004_structtype.py)  |  |
| d005 |1/20| [Row object d005](d005_row.py)  |  |
| d006 |1/20| [select column from dataframe](d006_select_column.py)  |  |
| d007 |1/26| [retreve_data_from_dataframe](d007_retrieve_from_dataframe.py)  |  |
| d008 |1/26| [add, update, drop column in a dataframe](d008_add_update_column.py)  |  |
| d009 |1/27| [filter rows](d009_filter_rows.py)  |  |
| d010 |1/27| [filter null](d010_filter_nulls.py)  |  |
| d011 |1/27| [drop_na](d011_drop_na.py)  |  |
| d012 |1/27| [drop_duplicated](d012_drop_duplicated.py)  |  |
| d013 |1/27| [sorting](d013_orderby_vs_sort.py)  |  |
| d014 |2/8| [groupby, pivot from d014 to d 0141](d014_groupby.py) |  |
| d015 |2/8| [join](d015_join.py) |  |
| d016 |2/8| [union](d016_union.py) |  |
| d017 |2/9| [udf](d017_udf.py) |  |
| d018 |2/9| [flatmap](d018_flatmap.py) |  |
| d019 |2/9| [map](d019_map.py) |  |
| d020 |2/13| [sampling](d020_sampling.py) |  |
| d021 |2/13| [aggregation](d021_aggregation.py) |  |
| d022 |2/13| [add_month](d022_add_month.py) |  |
| d023 |2/13| [split](d023_split.py) |  |
| d024 |2/23| [regular expression on pyspark dataframe](d024_re_dataframe.py) |  |
| d025 |3/1| [extract img src tag in html by pyspark](d0242_re_extract_img_url_in_html.py) |  |

## PySpark data processing package

| Content ID |Date| Content | Note |
|------------|----|---------|------|
|p001|2/13|[spark-df-profiling](https://github.com/julioasotodv/spark-df-profiling)|setup doc on `pkg/p001` |

## Concept

| Content ID |Date| Content | Note |
|------------|----|---------|------|
| 001 |1/21| MapReduce  |  |
| 002 |1/26| Introduction to Spark(I) - rdd ops, shuffle and stage | revisited 4/13 |
| 003 |2/14| Apache Parquet 2.0 |  |
| 004 |2/16| Introduction to Parquet |  |
| 005 |4/13| Introduction to Spark(II) - Driver, Executor, Application, ... |  |
| 006 |4/27| spark join I  |  |
| 007 |4/27| spark join II  |  |
| 008 || detect data skew in sparkUI  |  |
| 009 | 7/21| Spark OOM||

# Terminology

* [x] rdd
* [x] repartition/coalesce
* [x] map-reduce
* [x] yarn
* [x] mesos
* [x] parquet

# Optimizing Technique

* [x] [2017 - Optimizing Apache Spark SQL Joins: Spark Summit East talk by Vida Ha](https://www.youtube.com/watch?v=fp53QhSfQcI)
* [ ] [2019 - Optimizing Apache Spark SQL at LinkedIn](https://www.youtube.com/watch?v=Rok5wwUx-XI)

# Additional

* [x] - [The good, bad and ugly of spark](additional/good_bad_ugly_spark.md)
* [ ] - [Image Processing on Delta Lake - Databricks](additional/image_processsing_on_spark.md)
* [ ] - [Why Spark OOM](addtional/oom.md)


# Reference

[kenttw/spark_tutorial](https://github.com/kenttw/spark_tutorial)

[spark-examples/pyspark-examples](https://github.com/spark-examples/pyspark-examples)

[spark python api documentation 3.0.1](https://spark.apache.org/docs/latest/api/python/index.html)

[pandas 101 from yulong's note](https://github.com/YLTsai0609/pandas_101)

[Apache Parquet 2.0](https://parquet.apache.org/)

[Learning Apache Spark with Python](https://runawayhorse001.github.io/LearningApacheSpark/pyspark.pdf)

[pyspark cheatsheet](https://github.com/kevinschaich/pyspark-cheatsheet)

[2017 - Optimizing Apache Spark SQL Joins: Spark Summit East talk by Vida Ha](https://www.youtube.com/watch?v=fp53QhSfQcI)

[2019 - Optimizing Apache Spark SQL at LinkedIn](https://www.youtube.com/watch?v=Rok5wwUx-XI)
