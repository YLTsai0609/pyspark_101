# Pyspark 101

Polish up your data processing skill using pyspark!

# Installation(stand alone)

[check here to install spark 3.0+](https://github.com/YLTsai0609/DataScience_Note/blob/master/spark.md)

# Marathon

**This repo contains 19 example scripts so far.**

The tutorial is from [spark-examples/pyspark-examples](https://github.com/spark-examples/pyspark-examples)

## Pyspark basic

| Content ID   |Date| Content | Note |
|--------------|----|---------|------|
| 001          |1/11|[hello_world](001_hello_world.py)  | |
| 002          |1/12|[create_spark_session](002_create_spark_session.py)  | |
| 003          |1/12|[accumulator](003_accumulator.py)  ||
| 004          |1/13|[RDD creation](004_rdd_creation.py)  ||
| 005          |1/13|[RDD pararllelization Repartition() vs Coalesce()](005_rdd_repartition_coalesce.py)  ||
| 006          |1/18|[RDD operations - transformations (from 006 - 0064)](006_rdd_transformation.py)  ||

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

TODO SparkUI

## Addtiional

| Content ID |Date| Content | Note |
|------------|----|---------|------|
| 001 |1/21| MapReduce  |  |
| 002 |1/21| spark_best_practice(continuously updating)  |  |
| 003 |1/26| Introduction to Spark(I) |  |
| 004 || TODO Introduction to Spark(II) stage |  |

# Terminology

* [x] rdd
* [x] repartition/coalesce
* [x] map-reduce
* [ ] yarn
* [ ] mesos

# Reference

[kenttw/spark_tutorial](https://github.com/kenttw/spark_tutorial)

[spark-examples/pyspark-examples](https://github.com/spark-examples/pyspark-examples)
