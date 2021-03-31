"""
https://sparkbyexamples.com/spark/spark-web-ui-understanding/

When running the job
Spark Application UI: http://localhost:4040/
Resource Manager: http://localhost:9870
Spark JobTracker: http://localhost:8088/
Node Specific Info: http://localhost:8042/

When you wanna check the histroy

You need to start spark histroy server

https://sparkbyexamples.com/spark/spark-setup-on-hadoop-yarn/#spark-history-server


please run 

$SPARK_HOME/sbin/start-history-server.sh

TODO set up histroy server

############################################################
narrow dependency : no connection to other partition

wide dependency : need connection to other partition

Scheduling Mode
    MODE : syandalone, yarn, mesos, 
    NUMBER OF JOBS : equal to the number of actions, e.g.

    Job 0. read the CSV file.
    Job 1. Inferschema from the file.
    Job 2. Count Check

    NUMBER OF STAGES : 
        Wide Transformation result in a seperate NUmber of Stages
        So job 0 and job 1 have individual single stages, but job3
        have two stages that are because of the partition of data.
    DESCRIPTION
    Job Status, DAG VIsualization, Coplete Stages
"""
pass
