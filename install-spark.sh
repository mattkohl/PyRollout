#!/usr/bin/env bash

SPARK_VERSION='2.1.2'
HADOOP_VERSION='2.7'

curl http://apache.mirror.gtcomm.net/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz --output /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
cd /tmp && tar -xvzf /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz