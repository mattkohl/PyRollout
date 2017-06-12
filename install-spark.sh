#!/usr/bin/env bash

SPARK_VERSION='2.1.1'
HADOOP_VERSION='2.7'

curl http://apache.mirror.gtcomm.net/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz --output /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
cd /tmp && tar -xvzf /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

export SPARK_HOME=/tmp/spark-2.1.1-bin-hadoop2.7