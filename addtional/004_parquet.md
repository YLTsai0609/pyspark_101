# Apache Parquet 2.0

[official website](https://parquet.apache.org/)

[slide](https://www.slideshare.net/cloudera/hadoop-summit-36479635?ref=https://parquet.apache.org/)


Apache Parquet is a columnar storage format available to any project in thr Hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language.

<img src='./assets/parquet_1.png'></img>

<img src='./assets/parquet_2.png'></img>

# Efficiency

<img src='./assets/parquet_3.png'></img>

<img src='./assets/parquet_4.png'></img>

<img src='./assets/parquet_5.png'></img>

# Parquet

Interoperability : 互通性

<img src='./assets/parquet_6.png'></img>

<img src='./assets/parquet_7.png'></img>

## Interoperability

we want this thing cross language, like json

<img src='./assets/parquet_8.png'></img>

<img src='./assets/parquet_9.png'></img>

## Enabling efficiency

<img src='./assets/parquet_10.png'></img>

row layout and column layout is different. 

<img src='./assets/parquet_11.png'></img>

<img src='./assets/parquet_12.png'></img>

## Properties of efficient algorithms

<img src='./assets/parquet_13.png'></img>

<img src='./assets/parquet_14.png'></img>

## Encodings in Apache Parquet 2.0

<img src='./assets/parquet_15.png'></img>

### Delta encoding

<img src='./assets/parquet_16.png'></img>

in short, create a reference, transform the value by reference, then we can compress the data size!

<img src='./assets/parquet_17.png'></img>

<img src='./assets/parquet_18.png'></img>

<img src='./assets/parquet_19.png'></img>

### Binary packing

<img src='./assets/parquet_20.png'></img>

<img src='./assets/parquet_21.png'></img>

<img src='./assets/parquet_22.png'></img>

by this technique, we can compress data well.

PKey : 20%~40%
FKey : 25%~60%

<img src='./assets/parquet_23.png'></img>

<img src='./assets/parquet_24.png'></img>

<img src='./assets/parquet_25.png'></img>

## Parquet roadmap 2.x

<img src='./assets/parquet_26.png'></img>

<img src='./assets/parquet_27.png'></img>