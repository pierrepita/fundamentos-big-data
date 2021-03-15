#!/bin/bash

source /home/bigdata/.bashrc
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS

spark-submit --master spark://node1:7077 /home/bigdata/repos/fundamentos-big-data/airflow/01-read-and-slice.py
