# Cluster Manager

| type         | meaning                                                                                               | note |
|--------------|-------------------------------------------------------------------------------------------------------|------|
| Standalone   | a simple cluster manager included with Spark that makes it easy to set up a cluster                   |      |
| Apache Mesos | Mesons is a Cluster manager that can also run Hadoop MapReduce and Spark applications                 |      |
| Hadoop YARN  | the resource manager in Hadoop 2. This is mostly used cluster manager                                 |      |
| Kubernetes   | an open-source system for automating deployment, scaling and management of containerized applications |      |

local - which is not really a cluster manager but still I wanted to mention as we use **local** for `master()` in order to run Spark on your laptop/computer.

[Setup your spark on Hadoop Yarn Cluster](https://sparkbyexamples.com/spark/spark-setup-on-hadoop-yarn/)

[Setup your spark on Mesos](https://spark.apache.org/docs/latest/running-on-mesos.html)

[Spark Standalone](https://spark.apache.org/docs/latest/spark-standalone.html)

[Set up spark on kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)

kubernetes scheduler is current experimental - means they might unstable, change a lot. Feb 8, 2021

# Reference

https://sparkbyexamples.com/
