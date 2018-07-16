#!/bin/sh
/home/hadoop/spark-2.1.0/bin/spark-submit --class com.alex.spark.WordCount \
--master yarn-cluster \
--name "wordCount" \
--executor-memory 1g \
--num-executors 1 \
--driver-memory 2g \
--executor-cores 1 \
hdfs://ns:9000/projects/livy/jar/livy-demo.jar
