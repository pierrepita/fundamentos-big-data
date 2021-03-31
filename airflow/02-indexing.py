import pyspark
import pandas as pd
import numpy as np

from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch(http_compress=True)

from datetime import datetime

from es_pandas import es_pandas

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = pyspark.SparkContext(master = "spark://node1:7077", appName='indexing')

spark = SparkSession(sc)


# Lendo parquet
df = spark.read.parquet('hdfs://node1:9000/raw/DNPBA2017-airflow', header=True)

# Transformando em pandas
df_pd = df.toPandas()

# Preparing for indexing step
df_pd.dropna(inplace=True)
df_pd.columns= df_pd.columns.str.lower()

# Information of es cluseter
es_host = 'localhost:9200'
index = 'dnpba_2017_airflow'

# create es_pandas instance
ep = es_pandas(es_host)
es = Elasticsearch()

# init template if you want
doc_type = 'nascidosvisvos-airflow'
ep.init_es_tmpl(df_pd, doc_type)

# set use_index=True if you want to use DataFrame index as records' _id
ep.to_es(df_pd, index, doc_type=doc_type, use_index=True, thread_count=2, chunk_size=10000)

